package datafusion

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"runtime"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/arrio"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// ArrowReader streams Arrow record batches returned by DataFusion.
//
// Callers must close the reader when they are done with it. Close cancels any
// in-flight native execution and releases native Arrow stream resources.
type ArrowReader interface {
	arrio.Reader
	Schema() *arrow.Schema
	Close() error
}

// QueryArrowContext runs query on a DataFusion *sql.Conn and returns Arrow
// record batches without converting them through database/sql values.
func QueryArrowContext(ctx context.Context, sqlConn *sql.Conn, query string, args ...any) (ArrowReader, error) {
	named, err := namedValues(args)
	if err != nil {
		return nil, err
	}

	var reader ArrowReader
	err = sqlConn.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("not a datafusion driver connection: %T", driverConn)
		}

		r, err := conn.QueryArrowContext(ctx, query, named)
		if err != nil {
			return err
		}
		reader = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return reader, nil
}

// RegisterArrowReader registers the remaining batches in reader as a DataFusion
// in-memory table visible to SQL executed on sqlConn.
//
// This safe path serializes the reader to an Arrow IPC stream and lets the
// native side decode that stream into Rust-owned Arrow batches. That copy is
// intentional: the registered table can outlive this call, while ordinary Go
// Arrow arrays may use Go-owned buffers that must not be retained by native code
// after a cgo call returns.
func RegisterArrowReader(ctx context.Context, sqlConn *sql.Conn, tableName string, reader array.RecordReader) error {
	if err := validateArrowRegistration(ctx, sqlConn, tableName, reader); err != nil {
		return err
	}

	var data bytes.Buffer
	writer := ipc.NewWriter(&data, ipc.WithSchema(reader.Schema()))
	for reader.Next() {
		if err := ctx.Err(); err != nil {
			_ = writer.Close()
			return err
		}
		if err := writer.Write(reader.RecordBatch()); err != nil {
			_ = writer.Close()
			return err
		}
	}
	if err := reader.Err(); err != nil {
		_ = writer.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	return withDataFusionConn(sqlConn, func(conn *Conn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return conn.conn.RegisterArrowIPC(tableName, data.Bytes())
	})
}

// RegisterArrowReaderZeroCopy registers the remaining batches in reader as a
// DataFusion in-memory table by exporting reader through the Arrow C Stream
// Interface.
//
// Unlike RegisterArrowReader, this path does not copy buffers into Rust-owned
// memory. Use it only when every exported Arrow buffer is safe for native code
// to retain until the registered table is dropped or the owning DataFusion
// session/connector is closed, for example buffers allocated with Arrow Go's
// mallocator or another C/foreign allocator. Passing ordinary Go-allocated
// Arrow buffers can violate cgo pointer lifetime rules because DataFusion keeps
// table batches after this call returns.
func RegisterArrowReaderZeroCopy(ctx context.Context, sqlConn *sql.Conn, tableName string, reader array.RecordReader) error {
	if err := validateArrowRegistration(ctx, sqlConn, tableName, reader); err != nil {
		return err
	}

	return withDataFusionConn(sqlConn, func(conn *Conn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return conn.conn.RegisterArrowReaderZeroCopy(tableName, reader)
	})
}

func validateArrowRegistration(ctx context.Context, sqlConn *sql.Conn, tableName string, reader array.RecordReader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sqlConn == nil {
		return fmt.Errorf("datafusion sql connection is nil")
	}
	if tableName == "" {
		return fmt.Errorf("datafusion arrow table name is empty")
	}
	if reader == nil {
		return fmt.Errorf("datafusion arrow reader is nil")
	}
	if reader.Schema() == nil {
		return fmt.Errorf("datafusion arrow reader schema is nil")
	}
	return nil
}

func withDataFusionConn(sqlConn *sql.Conn, fn func(*Conn) error) error {
	return sqlConn.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("not a datafusion driver connection: %T", driverConn)
		}
		return fn(conn)
	})
}

func namedValues(args []any) ([]driver.NamedValue, error) {
	named := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		switch arg := arg.(type) {
		case sql.NamedArg:
			value, err := normalizeParameterValue(arg.Value)
			if err != nil {
				return nil, err
			}
			named[i] = driver.NamedValue{Name: arg.Name, Ordinal: i + 1, Value: value}
		case driver.NamedValue:
			value, err := normalizeParameterValue(arg.Value)
			if err != nil {
				return nil, err
			}
			ordinal := arg.Ordinal
			if ordinal == 0 {
				ordinal = i + 1
			}
			named[i] = driver.NamedValue{Name: arg.Name, Ordinal: ordinal, Value: value}
		default:
			value, err := normalizeParameterValue(arg)
			if err != nil {
				return nil, err
			}
			named[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
		}
	}
	return named, nil
}

func closeReader(reader arrio.Reader) {
	switch closer := reader.(type) {
	case interface{ Close() error }:
		_ = closer.Close()
	case interface{ Close() }:
		closer.Close()
	}
}

type serializedArrowReader struct {
	ArrowReader
	unlock func()
	once   sync.Once
	err    error
}

func newSerializedArrowReader(reader ArrowReader, unlock func()) ArrowReader {
	if unlock == nil {
		return reader
	}
	serialized := &serializedArrowReader{ArrowReader: reader, unlock: unlock}
	runtime.SetFinalizer(serialized, (*serializedArrowReader).finalize)
	return serialized
}

func (reader *serializedArrowReader) Close() error {
	runtime.SetFinalizer(reader, nil)
	reader.once.Do(func() {
		reader.err = reader.ArrowReader.Close()
		reader.unlock()
	})
	return reader.err
}

func (reader *serializedArrowReader) finalize() {
	_ = reader.Close()
}

package datafusion

import (
	"context"
	"database/sql/driver"
	"errors"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/datafusion-contrib/datafusion-go/internal/native"
)

// Session is an Arrow-oriented DataFusion session without database/sql pooling.
// Queries use the same execution, cancellation and ownership implementation as
// the SQL driver. A Session is safe for concurrent use. Close releases its own
// references; active readers and returned batches retain their native owners.
type Session struct {
	connector *Connector
	conn      *Conn
}

// NewSession creates a directly usable session. Existing connector options and
// DSN configuration work unchanged. Close the session when it is no longer used.
func NewSession(dsn string, options ...ConnectorOption) (*Session, error) {
	connector, err := NewConnectorWithInitContext(dsn, nil, options...)
	if err != nil {
		return nil, err
	}
	conn, err := connector.Connect(context.Background())
	if err != nil {
		_ = connector.Close()
		return nil, err
	}
	return &Session{connector: connector, conn: conn.(*Conn)}, nil
}

// QueryArrowContext executes SQL and transfers ownership of the batch reader.
func (s *Session) QueryArrowContext(ctx context.Context, query string, args ...any) (ArrowReader, error) {
	named, err := namedValues(args)
	if err != nil {
		return nil, err
	}
	return s.conn.QueryArrowContext(ctx, query, named)
}

// ExecContext executes SQL and consumes the result, including any write work.
func (s *Session) ExecContext(ctx context.Context, query string, args ...any) (driver.Result, error) {
	named, err := namedValues(args)
	if err != nil {
		return nil, err
	}
	return s.conn.ExecContext(ctx, query, named)
}

// Close is idempotent. It releases registrations after their remaining users finish.
func (s *Session) Close() error { return errors.Join(s.conn.Close(), s.connector.Close()) }

// RegisterArrowReader copies the remaining batches into an atomic in-memory
// registration using bounded IPC staging. The caller retains the input reader.
func (s *Session) RegisterArrowReader(ctx context.Context, name string, reader array.RecordReader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reader == nil || reader.Schema() == nil {
		return errors.New("datafusion arrow reader or schema is nil")
	}
	var importer *native.Import
	if err := s.conn.withNative(func(nc *native.Connection) (err error) { importer, err = nc.NewImport(name); return err }); err != nil {
		return err
	}
	defer importer.Close()
	return importArrowReader(ctx, importer, reader, func() error {
		return s.conn.withNative(func(nc *native.Connection) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return importer.Commit(nc)
		})
	})
}

// DeregisterTable removes a catalog entry. Existing readers and batches stay valid.
func (s *Session) DeregisterTable(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.conn.withNative(func(nc *native.Connection) error { return nc.DeregisterTable(name) })
}

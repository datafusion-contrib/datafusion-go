//go:build cgo

package datafusion

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
)

func TestQueryWithNULByteIsRejected(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	_, err = db.QueryContext(context.Background(), "select 1 \x00 and dropped_predicate")
	if err == nil || !strings.Contains(err.Error(), "NUL") {
		t.Fatalf("query with NUL byte: got %v, want NUL rejection", err)
	}
}

type callbackFailureReader struct {
	array.RecordReader
	fault     string
	nextCalls int
}

func (r *callbackFailureReader) Schema() *arrow.Schema {
	// Public validation calls Schema once before entering the native wrapper.
	if r.fault == "Schema" && r.nextCalls == -1 {
		panic("Schema failure")
	}
	if r.fault == "Schema" {
		r.nextCalls = -1
	}
	return r.RecordReader.Schema()
}
func (r *callbackFailureReader) Next() bool {
	r.nextCalls++
	if r.nextCalls == 1 {
		return r.RecordReader.Next()
	}
	if r.fault == "Next" {
		panic("Next failure")
	}
	return r.fault == "RecordBatch" || r.fault == "nil batch"
}
func (r *callbackFailureReader) RecordBatch() arrow.RecordBatch {
	if r.nextCalls > 1 {
		if r.fault == "RecordBatch" {
			panic("RecordBatch failure")
		}
		if r.fault == "nil batch" {
			return nil
		}
	}
	return r.RecordReader.RecordBatch()
}
func (r *callbackFailureReader) Err() error {
	switch r.fault {
	case "Err":
		panic("Err failure")
	case "Error":
		return callbackFailureError{}
	case "ordinary error":
		return errors.New("ordinary error")
	}
	return nil
}

// Registration borrows the reader; neither callback should run from Rust.
func (r *callbackFailureReader) Retain()  { panic("unexpected Retain") }
func (r *callbackFailureReader) Release() { panic("unexpected Release") }

type callbackFailureError struct{}

func (callbackFailureError) Error() string { panic("Error failure") }

func TestZeroCopyCallbackFailuresReleaseBuffers(t *testing.T) {
	for _, fault := range []string{"Schema", "Next", "RecordBatch", "nil batch", "Err", "Error", "ordinary error", "success"} {
		t.Run(fault, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(mallocator.NewMallocator())
			defer alloc.AssertSize(t, 0)
			conn := openTestConn(t)
			inner := nativeTableReader(t, alloc)
			reader := &callbackFailureReader{RecordReader: inner, fault: fault}
			err := RegisterArrowReaderZeroCopy(context.Background(), conn, "callback_table", reader)
			inner.Release()
			if fault == "success" {
				if err != nil {
					t.Fatal(err)
				}
				var total int
				if err := conn.QueryRowContext(context.Background(), "select sum(value) from callback_table").Scan(&total); err != nil {
					t.Fatal(err)
				}
				if total != 6 {
					t.Fatalf("sum %d, want 6", total)
				}
				if _, err := conn.ExecContext(context.Background(), "drop table callback_table"); err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), fault) {
				t.Fatalf("got %v, want %s failure", err, fault)
			}
			alloc.AssertSize(t, 0)
		})
	}
}

func TestNativeStreamSanitizesNULInExecutionError(t *testing.T) {
	conn := openTestConn(t)
	reader, err := QueryArrowContext(context.Background(), conn,
		"select cast(concat(cast(value as varchar), $1) as bigint) from range(3)", "a\x00b")
	if err != nil {
		t.Fatalf("expected an error during Read, got %v", err)
	}
	defer closeNoError(t, reader)
	batch, err := reader.Read()
	if batch != nil {
		batch.Release()
	}
	if err == nil || !strings.Contains(err.Error(), "a\\0b") || strings.ContainsRune(err.Error(), 0) {
		t.Fatalf("expected a sanitized stream error, got %v", err)
	}
}

type gcBlockingReader struct {
	entered chan struct{}
	resume  chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (r *gcBlockingReader) Schema() *arrow.Schema { return arrow.NewSchema(nil, nil) }
func (r *gcBlockingReader) Read() (arrow.RecordBatch, error) {
	close(r.entered)
	<-r.resume
	return nil, io.EOF
}
func (r *gcBlockingReader) Close() error { r.once.Do(func() { close(r.closed) }); return nil }

func TestSerializedReaderStaysAliveDuringRead(t *testing.T) {
	inner := &gcBlockingReader{entered: make(chan struct{}), resume: make(chan struct{}), closed: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		// No retained wrapper reference after this call except its receiver.
		_, err := newSerializedArrowReader(inner, func() {}).Read()
		done <- err
	}()
	<-inner.entered
	for range 8 {
		runtime.GC()
		runtime.Gosched()
	}
	select {
	case <-inner.closed:
		t.Error("finalizer closed a reader during Read")
	default:
	}
	close(inner.resume)
	select {
	case err := <-done:
		if err != io.EOF {
			t.Fatalf("Read got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read did not finish")
	}
}

func TestQueryArrowConcurrentCloseAndReset(t *testing.T) {
	for _, action := range []string{"close", "reset"} {
		t.Run(action, func(t *testing.T) {
			connector, err := NewConnectorWithInitContext("", nil, WithSharedSession(false))
			if err != nil {
				t.Fatal(err)
			}
			defer closeNoError(t, connector)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			driverConn, err := connector.Connect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			conn := driverConn.(*Conn)
			defer closeNoError(t, conn)
			start := make(chan struct{})
			errs := make(chan error, 5)
			var workers sync.WaitGroup
			for range 4 {
				workers.Go(func() {
					<-start
					for range 64 {
						reader, err := conn.QueryArrowContext(ctx, "select 1", []driver.NamedValue{})
						if err != nil {
							var de *Error
							if action == "close" && errors.As(err, &de) && de.Type == ErrorClosed {
								return
							}
							errs <- err
							return
						}
						rec, err := reader.Read()
						if rec != nil {
							rec.Release()
						}
						closeErr := reader.Close()
						if err != nil {
							errs <- err
							return
						}
						if closeErr != nil {
							errs <- closeErr
							return
						}
					}
				})
			}
			workers.Go(func() {
				<-start
				for range 64 {
					var err error
					if action == "close" {
						err = conn.Close()
					} else {
						err = conn.ResetSession(ctx)
					}
					if err != nil {
						errs <- err
						return
					}
					runtime.Gosched()
				}
			})
			close(start)
			workers.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
		})
	}
}

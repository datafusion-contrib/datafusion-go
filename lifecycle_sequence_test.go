//go:build cgo

package datafusion

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
)

// The model tracks only observable catalog and ownership state. It does not
// reproduce the driver's locking or reference-count implementation.
type lifecycleHarness struct {
	t         *testing.T
	connector *Connector
	conn      *Conn
	stmt      *Stmt
	alloc     *memory.CheckedAllocator
	shared    bool
	zeroCopy  bool
	present   bool
	held      arrow.RecordBatch
}

func newLifecycleHarness(t *testing.T, shared, zeroCopy bool) *lifecycleHarness {
	t.Helper()
	h := &lifecycleHarness{t: t, shared: shared, zeroCopy: zeroCopy, alloc: memory.NewCheckedAllocator(mallocator.NewMallocator())}
	connector, err := NewConnectorWithInitContext("", nil, WithSharedSession(shared))
	if err != nil {
		t.Fatal(err)
	}
	h.connector = connector
	conn, err := connector.Connect(context.Background())
	if err != nil {
		closeNoError(t, connector)
		t.Fatal(err)
	}
	h.conn = conn.(*Conn)
	t.Cleanup(h.close)
	return h
}

func (h *lifecycleHarness) close() {
	if h.stmt != nil {
		closeNoError(h.t, h.stmt)
	}
	closeNoError(h.t, h.conn)
	closeNoError(h.t, h.connector)
	// A returned batch remains readable after every session and handle closes.
	h.checkHeld()
	if h.held != nil {
		h.held.Release()
		h.held = nil
	}
	h.alloc.AssertSize(h.t, 0)
}

func (h *lifecycleHarness) checkHeld() {
	h.t.Helper()
	if h.held != nil {
		values := h.held.Column(0).(*array.Int64)
		if values.Len() != 3 || values.Value(0) != 1 || values.Value(2) != 3 {
			h.t.Fatal("retained batch changed during lifecycle operations")
		}
	}
}

func (h *lifecycleHarness) step(op int) {
	h.t.Helper()
	ctx := context.Background()
	switch op {
	case 0: // Register after an initial open, deregistration, or isolated reset.
		if h.present {
			return
		}
		reader := nativeTableReader(h.t, h.alloc)
		defer reader.Release()
		var err error
		if h.zeroCopy {
			err = h.conn.conn.RegisterArrowReaderZeroCopy("sequence_data", reader)
		} else {
			var data bytes.Buffer
			writer := ipc.NewWriter(&data, ipc.WithSchema(reader.Schema()))
			for reader.Next() {
				if err := writer.Write(reader.RecordBatch()); err != nil {
					h.t.Fatal(err)
				}
			}
			if err := writer.Close(); err != nil {
				h.t.Fatal(err)
			}
			err = h.conn.conn.RegisterArrowIPC("sequence_data", data.Bytes())
		}
		if err != nil {
			h.t.Fatal(err)
		}
		h.present = true
	case 1, 2: // Direct and reused prepared SQL must follow the same session.
		var rows driver.Rows
		var err error
		if op == 1 {
			rows, err = h.conn.QueryContext(ctx, "select sum(value) from sequence_data", nil)
		} else {
			if h.stmt == nil {
				stmt, prepareErr := h.conn.Prepare("select sum(value) from sequence_data")
				if prepareErr != nil {
					h.t.Fatal(prepareErr)
				}
				h.stmt = stmt.(*Stmt)
			}
			rows, err = h.stmt.QueryContext(ctx, nil)
		}
		if !h.present {
			if err == nil {
				closeNoError(h.t, rows)
				h.t.Fatal("query found a table absent from the session model")
			}
			return
		}
		if err != nil {
			h.t.Fatal(err)
		}
		defer closeNoError(h.t, rows)
		value := make([]driver.Value, 1)
		if err := rows.Next(value); err != nil || value[0] != int64(6) {
			h.t.Fatalf("aggregate got %v, %v; want 6", value, err)
		}
		if err := rows.Next(value); err != io.EOF {
			h.t.Fatalf("aggregate terminal read: %v", err)
		}
	case 3: // Retain an independently owned batch after closing its reader.
		if !h.present || h.held != nil {
			return
		}
		reader, err := h.conn.QueryArrowContext(ctx, "select value from sequence_data", nil)
		if err != nil {
			h.t.Fatal(err)
		}
		h.held, err = reader.Read()
		closeNoError(h.t, reader)
		closeNoError(h.t, reader)
		if err != nil {
			h.t.Fatal(err)
		}
	case 4:
		if err := h.conn.ResetSession(ctx); err != nil {
			h.t.Fatal(err)
		}
		if !h.shared {
			h.present = false
		}
	case 5:
		if h.present {
			if err := h.conn.conn.DeregisterTable("sequence_data"); err != nil {
				h.t.Fatal(err)
			}
			h.present = false
		}
	case 6:
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if reader, err := h.conn.QueryArrowContext(cancelled, "select 1", nil); !errors.Is(err, context.Canceled) {
			if reader != nil {
				closeNoError(h.t, reader)
			}
			h.t.Fatalf("cancelled query: %v", err)
		}
	case 7:
		if h.held != nil {
			h.held.Release()
			h.held = nil
		}
	case 8:
		if h.stmt != nil {
			closeNoError(h.t, h.stmt)
			closeNoError(h.t, h.stmt)
			h.stmt = nil
		}
	}
	h.checkHeld()
}

func TestLifecycleSequences(t *testing.T) {
	seeds := []uint64{1, 53, 55, 20260909}
	if value := os.Getenv("DFGO_TEST_SEED"); value != "" {
		seed, err := strconv.ParseUint(value, 0, 64)
		if err != nil {
			t.Fatal(err)
		}
		seeds = []uint64{seed}
	}
	steps := 50
	if value := os.Getenv("DFGO_TEST_STEPS"); value != "" {
		var err error
		steps, err = strconv.Atoi(value)
		if err != nil || steps < 1 || steps > 100000 {
			t.Fatal("DFGO_TEST_STEPS must be between 1 and 100000")
		}
	}
	for _, seed := range seeds {
		for _, shared := range []bool{false, true} {
			for _, zeroCopy := range []bool{false, true} {
				t.Run(fmt.Sprintf("seed=%d/shared=%t/zero-copy=%t", seed, shared, zeroCopy), func(t *testing.T) {
					h := newLifecycleHarness(t, shared, zeroCopy)
					random := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
					// Every seed covers retained buffers across reset and removal.
					sequence := []int{0, 2, 3, 4, 1, 2, 5, 0, 2}
					for range steps {
						sequence = append(sequence, random.IntN(9))
					}
					t.Logf("replay: DFGO_TEST_SEED=%d DFGO_TEST_STEPS=%d; operations=%v", seed, steps, sequence)
					for i, op := range sequence {
						t.Logf("step %d: operation %d", i, op)
						h.step(op)
					}
				})
			}
		}
	}
}

type failingBatchReader struct {
	array.RecordReader
	failAt int
	reads  int
	failed bool
}

func (r *failingBatchReader) Next() bool {
	if r.reads == r.failAt {
		r.failed = true
		return false
	}
	r.reads++
	return r.RecordReader.Next()
}

func (r *failingBatchReader) Err() error {
	if r.failed {
		return errors.New("injected batch failure")
	}
	return r.RecordReader.Err()
}

func TestRegistrationFailureAtEveryBatch(t *testing.T) {
	for _, zeroCopy := range []bool{false, true} {
		for failAt := 0; failAt <= 4; failAt++ {
			t.Run(fmt.Sprintf("zero-copy=%t/fail-at=%d", zeroCopy, failAt), func(t *testing.T) {
				alloc := memory.NewCheckedAllocator(mallocator.NewMallocator())
				defer alloc.AssertSize(t, 0)
				conn := openTestConn(t)
				source := nativeTableReader(t, alloc)
				if !source.Next() {
					t.Fatal("fixture is empty")
				}
				record := source.RecordBatch()
				base, err := array.NewRecordReader(source.Schema(), []arrow.RecordBatch{record, record, record, record})
				source.Release()
				if err != nil {
					t.Fatal(err)
				}
				reader := &failingBatchReader{RecordReader: base, failAt: failAt}
				register := RegisterArrowReader
				if zeroCopy {
					register = RegisterArrowReaderZeroCopy
				}
				err = register(context.Background(), conn, "failed_input", reader)
				reader.Release()
				if err == nil || !strings.Contains(err.Error(), "injected batch failure") {
					t.Fatalf("registration at batch %d: %v", failAt, err)
				}
				alloc.AssertSize(t, 0)
				if rows, err := conn.QueryContext(context.Background(), "select * from failed_input"); err == nil {
					closeNoError(t, rows)
					t.Fatal("failed registration published a partial table")
				}
				// The same session must recover with a complete registration.
				good := nativeTableReader(t, alloc)
				err = register(context.Background(), conn, "failed_input", good)
				good.Release()
				if err != nil {
					t.Fatal(err)
				}
				var sum int64
				if err := conn.QueryRowContext(context.Background(), "select sum(value) from failed_input").Scan(&sum); err != nil || sum != 6 {
					t.Fatalf("recovery query: %d, %v", sum, err)
				}
				if _, err := conn.ExecContext(context.Background(), "drop table failed_input"); err != nil {
					t.Fatal(err)
				}
				alloc.AssertSize(t, 0)
			})
		}
	}
}

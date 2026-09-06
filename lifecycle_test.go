//go:build cgo

package datafusion

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
)

func TestParameterizedDDL(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)
	for _, query := range []string{
		"create table bound_table as select cast($1 as bigint) as value",
		"create or replace table bound_table as select cast($1 as bigint) as value",
		"create view bound_view as select cast($1 as bigint) as value",
		"create or replace view bound_view as select cast($1 as bigint) as value",
	} {
		t.Run(query, func(t *testing.T) {
			if _, err := db.Exec(query, int64(42)); err != nil {
				t.Fatal(err)
			}
			table := "bound_table"
			if strings.Contains(query, "view") {
				table = "bound_view"
			}
			var got int64
			if err := db.QueryRow("select value from " + table).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != 42 {
				t.Fatalf("got %d, want 42", got)
			}
		})
	}
	// Correct arity but invalid parameter names must fail before REPLACE mutates
	// the existing table, even when IF NOT EXISTS would skip executing its input.
	_, err = db.Exec("create or replace table bound_table as select cast($1 as bigint) as value", sql.Named("wrong", 99))
	if err == nil {
		t.Fatal("expected invalid binding error")
	}
	var got int64
	if err := db.QueryRow("select value from bound_table").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("failed DDL changed the existing table to %d", got)
	}
}

func TestPreparedStatementFollowsIsolatedSessionReset(t *testing.T) {
	generation := 0
	connector, err := NewConnectorWithInitContext("", func(ctx context.Context, exec driver.ExecerContext) error {
		generation++
		_, err := exec.ExecContext(ctx, fmt.Sprintf("create view generation as select %d as value", generation), nil)
		return err
	}, WithSharedSession(false))
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer closeNoError(t, db)
	db.SetMaxOpenConns(1)
	stmt, err := db.Prepare("select value from generation")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, stmt)
	for range 3 {
		var got int64
		if err := stmt.QueryRow().Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != int64(generation) {
			t.Fatalf("cached statement read session %d, current session is %d", got, generation)
		}
	}
}

func TestFailedSessionResetDiscardsConnection(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprint(persistent), func(t *testing.T) {
			attempt := 0
			initErr := errors.New("session initialization failed")
			connector, err := NewConnectorWithInitContext("", func(ctx context.Context, exec driver.ExecerContext) error {
				attempt++
				if attempt == 2 || (persistent && attempt > 2) {
					return initErr
				}
				_, err := exec.ExecContext(ctx, fmt.Sprintf("create view generation as select %d as value", attempt), nil)
				return err
			}, WithSharedSession(false))
			if err != nil {
				t.Fatal(err)
			}
			db := sql.OpenDB(connector)
			defer closeNoError(t, db)
			db.SetMaxOpenConns(1)
			var got int64
			if err := db.QueryRow("select value from generation").Scan(&got); err != nil {
				t.Fatal(err)
			}
			err = db.QueryRow("select value from generation").Scan(&got)
			if persistent {
				if !errors.Is(err, initErr) {
					t.Fatalf("got %v, want initializer error", err)
				}
			} else if err != nil || got != 3 {
				t.Fatalf("query after failed reset got %d, %v; want new session 3", got, err)
			}
		})
	}
}

func TestDriverOpenOwnsConnector(t *testing.T) {
	for range 10 {
		c, err := (Driver{}).Open("")
		if err != nil {
			t.Fatal(err)
		}
		conn := c.(*Conn)
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.connector.Connect(context.Background()); err == nil {
			t.Fatal("closing a Driver.Open connection left its native database open")
		}
		closeNoError(t, conn)
	}
}

func TestDDLWaitHonorsContext(t *testing.T) {
	connector, err := NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, connector)
	one, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, one)
	two, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, two)
	reader, err := one.(*Conn).QueryArrowContext(context.Background(), "create view first_ddl as select 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, reader)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := two.(*Conn).ExecContext(ctx, "create view second_ddl as select 2", nil); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v, want deadline", err)
		}
	case <-time.After(2 * time.Second):
		closeNoError(t, reader)
		<-done
		t.Fatal("DDL waited for the other reader after cancellation")
	}
}

func TestSharedInitializationWaitHonorsContext(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	connector, err := NewConnectorWithInitContext("", func(context.Context, driver.ExecerContext) error {
		close(entered)
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, connector)
	first := make(chan error, 1)
	go func() {
		conn, err := connector.Connect(context.Background())
		if conn != nil {
			_ = conn.Close()
		}
		first <- err
	}()
	<-entered
	defer func() {
		close(release)
		if err := <-first; err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() {
		conn, err := connector.Connect(ctx)
		if conn != nil {
			_ = conn.Close()
		}
		second <- err
	}()
	select {
	case err := <-second:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v, want deadline", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Connect ignored its deadline while initialization was busy")
	}
}

func nativeTableReader(t *testing.T, alloc memory.Allocator) array.RecordReader {
	t.Helper()
	b := array.NewInt64Builder(alloc)
	b.AppendValues([]int64{1, 2, 3}, nil)
	values := b.NewArray()
	b.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 3)
	values.Release()
	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{record})
	record.Release()
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func TestIsolatedResetReleasesOldStatementTables(t *testing.T) {
	alloc := memory.NewCheckedAllocator(mallocator.NewMallocator())
	defer alloc.AssertSize(t, 0)
	connector, err := NewConnectorWithInitContext("", nil, WithSharedSession(false))
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, connector)
	driverConn, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conn := driverConn.(*Conn)
	defer closeNoError(t, conn)
	reader := nativeTableReader(t, alloc)
	if err := conn.conn.RegisterArrowReaderZeroCopy("retained", reader); err != nil {
		reader.Release()
		t.Fatal(err)
	}
	reader.Release()
	stmt, err := conn.Prepare("select * from retained")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, stmt)
	if alloc.CurrentAlloc() == 0 {
		t.Fatal("registered table did not retain its buffers")
	}
	if err := conn.ResetSession(context.Background()); err != nil {
		t.Fatal(err)
	}
	alloc.AssertSize(t, 0)
	if rows, err := stmt.(*Stmt).QueryContext(context.Background(), nil); err == nil {
		closeNoError(t, rows)
		t.Fatal("statement could still query the old session")
	}
}

func TestReturnedBatchOutlivesTableDeregistration(t *testing.T) {
	alloc := memory.NewCheckedAllocator(mallocator.NewMallocator())
	defer alloc.AssertSize(t, 0)
	conn := openTestConn(t)
	reader := nativeTableReader(t, alloc)
	if err := RegisterArrowReaderZeroCopy(context.Background(), conn, "retained", reader); err != nil {
		reader.Release()
		t.Fatal(err)
	}
	reader.Release()
	result, err := QueryArrowContext(context.Background(), conn, "select * from retained")
	if err != nil {
		t.Fatal(err)
	}
	batch, err := result.Read()
	if err != nil {
		closeNoError(t, result)
		t.Fatal(err)
	}
	defer batch.Release()
	closeNoError(t, result)
	if _, err := conn.ExecContext(context.Background(), "drop table retained"); err != nil {
		t.Fatal(err)
	}
	if alloc.CurrentAlloc() == 0 {
		t.Fatal("deregistering released a live returned batch")
	}
	if got := batch.Column(0).(*array.Int64).Value(2); got != 3 {
		t.Fatalf("retained batch value %d, want 3", got)
	}
}

func TestMemoryBudgetInitialization(t *testing.T) {
	connector, err := NewConnectorWithInitContext("", func(ctx context.Context, exec driver.ExecerContext) error {
		_, err := exec.ExecContext(ctx, "SET datafusion.runtime.memory_limit = '1K'", nil)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer closeNoError(t, db)
	rows, err := db.Query("select value, count(*) from range(100000) group by value")
	if err == nil {
		for rows.Next() {
		}
		err = rows.Err()
		closeNoError(t, rows)
	}
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "memory") {
		t.Fatalf("got %v, want memory budget error", err)
	}
	var one int
	if err := db.QueryRow("select 1").Scan(&one); err != nil {
		t.Fatal(err)
	}
}

func TestSafeRegistrationCopiesSourceBuffers(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	conn := openTestConn(t)
	reader := nativeTableReader(t, alloc)
	if err := RegisterArrowReader(context.Background(), conn, "copied", reader); err != nil {
		reader.Release()
		t.Fatal(err)
	}
	reader.Release()
	alloc.AssertSize(t, 0)
	result, err := QueryArrowContext(context.Background(), conn, "select sum(value) from copied")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, result)
	batch, err := result.Read()
	if err != nil {
		t.Fatal(err)
	}
	if got := batch.Column(0).(*array.Int64).Value(0); got != 6 {
		t.Fatalf("sum %d, want 6", got)
	}
	batch.Release()
	if _, err := result.Read(); err != io.EOF {
		t.Fatalf("got %v, want EOF", err)
	}
}

func TestFailedZeroCopyRegistrationReleasesBuffers(t *testing.T) {
	alloc := memory.NewCheckedAllocator(mallocator.NewMallocator())
	defer alloc.AssertSize(t, 0)
	conn := openTestConn(t)
	for range 16 {
		reader := nativeTableReader(t, alloc)
		// The native side consumes the stream before rejecting a blank name.
		err := RegisterArrowReaderZeroCopy(context.Background(), conn, " ", reader)
		reader.Release()
		if err == nil {
			t.Fatal("expected invalid table name")
		}
		alloc.AssertSize(t, 0)
	}
}

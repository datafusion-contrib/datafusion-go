//go:build cgo

package datafusion

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
)

type schemaOnlyReader struct {
	schema *arrow.Schema
}

func (r schemaOnlyReader) Schema() *arrow.Schema {
	return r.schema
}

func (r schemaOnlyReader) Read() (arrow.RecordBatch, error) {
	return nil, io.EOF
}

type oneRecordReader struct {
	rec       arrow.RecordBatch
	readCount int
	closed    bool
}

func (r *oneRecordReader) Read() (arrow.RecordBatch, error) {
	r.readCount++
	if r.readCount > 1 {
		return nil, io.EOF
	}
	return r.rec, nil
}

func (r *oneRecordReader) Close() error {
	r.closed = true
	return nil
}

type closableSchemaReader struct {
	schema     *arrow.Schema
	closeCount int
}

func (r *closableSchemaReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *closableSchemaReader) Read() (arrow.RecordBatch, error) {
	return nil, io.EOF
}

func (r *closableSchemaReader) Close() error {
	r.closeCount++
	return nil
}

func TestSerializedArrowReaderCloseAndFinalizeReleaseOnce(t *testing.T) {
	base := &closableSchemaReader{schema: arrow.NewSchema(nil, nil)}
	unlocks := 0
	reader := newSerializedArrowReader(base, func() { unlocks++ })

	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if base.closeCount != 1 {
		t.Fatalf("underlying reader closed %d times, want 1", base.closeCount)
	}
	if unlocks != 1 {
		t.Fatalf("unlock called %d times, want 1", unlocks)
	}

	finalizedBase := &closableSchemaReader{schema: arrow.NewSchema(nil, nil)}
	finalizedUnlocks := 0
	finalized := newSerializedArrowReader(finalizedBase, func() { finalizedUnlocks++ }).(*serializedArrowReader)
	finalized.finalize()
	if err := finalized.Close(); err != nil {
		t.Fatal(err)
	}
	if finalizedBase.closeCount != 1 {
		t.Fatalf("finalized underlying reader closed %d times, want 1", finalizedBase.closeCount)
	}
	if finalizedUnlocks != 1 {
		t.Fatalf("finalized unlock called %d times, want 1", finalizedUnlocks)
	}
}

func TestQueryRow(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	var got int64
	if err := db.QueryRowContext(context.Background(), "select 1").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("got %d, want 1", got)
	}
}

func TestScalarScans(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	row := db.QueryRowContext(
		context.Background(),
		"select true as b, cast(42 as bigint) as i, cast(1.5 as double) as f, 'x' as s, cast(null as int) as n",
	)

	var (
		b bool
		i int64
		f float64
		s string
		n sql.NullInt64
	)
	if err := row.Scan(&b, &i, &f, &s, &n); err != nil {
		t.Fatal(err)
	}
	if !b || i != 42 || f != 1.5 || s != "x" || n.Valid {
		t.Fatalf("unexpected values: b=%v i=%d f=%f s=%q n=%+v", b, i, f, s, n)
	}
}

func TestQueryArrowContext(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn)

	reader, err := QueryArrowContext(context.Background(), conn, "select 1 as one")
	if err != nil {
		t.Fatal(err)
	}
	if reader.Schema().Field(0).Name != "one" {
		t.Fatalf("got schema %s, want column one", reader.Schema())
	}

	rec, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()

	if rec.NumRows() != 1 || rec.NumCols() != 1 || rec.Schema().Field(0).Name != "one" {
		t.Fatalf("unexpected record: rows=%d cols=%d schema=%s", rec.NumRows(), rec.NumCols(), rec.Schema())
	}

	_, err = reader.Read()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("got err %v, want io.EOF", err)
	}
}

func TestRegisterArrowReaderNestedData(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn)

	rec := nestedArrowRecord(t, memory.DefaultAllocator)
	defer rec.Release()

	rdr, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()

	if err := RegisterArrowReader(context.Background(), conn, "safe_arrow_table", rdr); err != nil {
		t.Fatal(err)
	}
	assertRegisteredNestedRecord(t, conn, "safe_arrow_table", rec)
}

func TestRegisterArrowReaderZeroCopyNestedData(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn)

	alloc := mallocator.NewMallocator()
	rec := nestedArrowRecord(t, alloc)

	rdr, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatal(err)
	}

	if err := RegisterArrowReaderZeroCopy(context.Background(), conn, "zero_copy_arrow_table", rdr); err != nil {
		t.Fatal(err)
	}

	// The registered table now owns Arrow release callbacks for the exported
	// buffers. Releasing the caller's handles here verifies the table is not
	// borrowing Go RecordBatch objects directly.
	rdr.Release()
	rec.Release()

	expected := nestedArrowRecord(t, memory.DefaultAllocator)
	defer expected.Release()
	assertRegisteredNestedRecord(t, conn, "zero_copy_arrow_table", expected)
}

func nestedArrowRecord(t *testing.T, mem memory.Allocator) arrow.RecordBatch {
	t.Helper()

	idBuilder := array.NewInt64Builder(mem)
	idBuilder.AppendValues([]int64{1, 2}, nil)
	ids := idBuilder.NewArray()
	idBuilder.Release()
	defer ids.Release()

	itemsBuilder := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
	values := itemsBuilder.ValueBuilder().(*array.Int64Builder)
	itemsBuilder.Append(true)
	values.AppendValues([]int64{10, 20}, nil)
	itemsBuilder.Append(true)
	values.AppendValues([]int64{30}, nil)
	items := itemsBuilder.NewArray()
	itemsBuilder.Release()
	defer items.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "items", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
	}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{ids, items}, 2)
}

func assertRegisteredNestedRecord(t *testing.T, conn *sql.Conn, tableName string, want arrow.RecordBatch) {
	t.Helper()

	reader, err := QueryArrowContext(context.Background(), conn, "select id, items from "+tableName)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, reader)

	got, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	if !array.RecordEqual(want, got) {
		t.Fatalf("registered Arrow record mismatch\nwant: %s\ngot:  %s", want, got)
	}

	_, err = reader.Read()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("got err %v, want io.EOF", err)
	}
}

func TestZeroRowQueryColumns(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	rows, err := db.QueryContext(context.Background(), "select cast(1 as bigint) as one, 'x' as two where false")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, rows)

	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cols, []string{"one", "two"}) {
		t.Fatalf("got columns %v, want [one two]", cols)
	}
	if rows.Next() {
		t.Fatal("expected no rows")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestQueryArrowContextParametersAndClose(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn)

	reader, err := QueryArrowContext(context.Background(), conn, "select $1 + 1 as value", int64(41))
	if err != nil {
		t.Fatal(err)
	}

	rec, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	values := rec.Column(0).(*array.Int64)
	if got := values.Value(0); got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
	rec.Release()

	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(); !errors.Is(err, io.EOF) {
		t.Fatalf("got err %v, want io.EOF after close", err)
	}
}

func TestDSNConfigOptions(t *testing.T) {
	for _, dsn := range []string{
		"?datafusion.execution.batch_size=2",
		"datafusion://",
		"datafusion://?datafusion.execution.batch_size=2",
		"?datafusion.go.shared_session=true&datafusion.execution.batch_size=2",
	} {
		t.Run(dsn, func(t *testing.T) {
			db, err := sql.Open("datafusion", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer closeNoError(t, db)

			rows, err := db.QueryContext(context.Background(), "select 1 union all select 2 union all select 3")
			if err != nil {
				t.Fatal(err)
			}
			defer closeNoError(t, rows)

			var count int
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if count != 3 {
				t.Fatalf("got %d rows, want 3", count)
			}
		})
	}
}

func TestInvalidDSNConfigOption(t *testing.T) {
	db, err := sql.Open("datafusion", "?datafusion.nope=1")
	if err != nil {
		return
	}
	defer closeNoError(t, db)

	err = db.PingContext(context.Background())
	if err == nil {
		t.Fatal("expected invalid config option error")
	}
	var driverErr *Error
	if !errors.As(err, &driverErr) {
		t.Fatalf("got %T, want *datafusion.Error", err)
	}
	if driverErr.NativeKind != NativeErrorKindInvalidArgument {
		t.Fatalf("got native kind %q, want %q", driverErr.NativeKind, NativeErrorKindInvalidArgument)
	}
	if !errors.Is(err, ErrNativeInvalidArgument) {
		t.Fatalf("got %v, want ErrNativeInvalidArgument", err)
	}
}

func TestUnsupportedDSN(t *testing.T) {
	if _, err := sql.Open("datafusion", "file.db"); err == nil {
		t.Fatal("expected file path DSN to be rejected")
	}
	if _, err := sql.Open("datafusion", ":memory:"); err == nil {
		t.Fatal("expected SQLite-style memory DSN to be rejected")
	}
	if _, err := sql.Open("datafusion", "file:///tmp/datafusion"); err == nil {
		t.Fatal("expected URL DSN to be rejected")
	}
	if _, err := sql.Open("datafusion", "datafusion://disk/path"); err == nil {
		t.Fatal("expected datafusion URL with host to be rejected")
	}
}

func TestPreparedStatementSerializationMetadata(t *testing.T) {
	connector, err := NewConnector("", nil)
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

	tests := []struct {
		query string
		want  bool
	}{
		{query: "select 1", want: false},
		{query: " with cte as (select 1) select * from cte", want: false},
		{query: "show tables", want: false},
		{query: "explain select 1", want: false},
		{query: "-- comment\nselect 1", want: false},
		{query: "/* comment */ create view v as select 1", want: true},
		{query: "create table t as select 1", want: true},
		{query: "drop view v", want: true},
		{query: "set datafusion.execution.batch_size = 1024", want: true},
		{query: "insert into t values (1)", want: true},
	}

	for _, tt := range tests {
		stmt, err := conn.PrepareContext(context.Background(), tt.query)
		if err != nil {
			t.Fatalf("PrepareContext(%q): %v", tt.query, err)
		}
		got := stmt.(*Stmt).stmt.Serializes()
		if err := stmt.Close(); err != nil {
			t.Fatal(err)
		}
		if got != tt.want {
			t.Fatalf("statement serializes %q = %v, want %v", tt.query, got, tt.want)
		}
	}
}

func TestConnectionSharedSessionDefault(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)
	db.SetMaxOpenConns(2)

	ctx := context.Background()
	conn1, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn1)

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn2)

	if _, err := conn1.ExecContext(ctx, "create view local_view as select 42 as value"); err != nil {
		t.Fatal(err)
	}

	var got int64
	if err := conn1.QueryRowContext(ctx, "select value from local_view").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}

	if err := conn2.QueryRowContext(ctx, "select value from local_view").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

func TestExecStatements(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	ctx := context.Background()
	err = ExecStatements(ctx, db, []string{
		"create view exec_statements_view as select 9 as value",
		"",
		"select value from exec_statements_view",
	})
	if err != nil {
		t.Fatal(err)
	}

	var got int64
	if err := db.QueryRowContext(ctx, "select value from exec_statements_view").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 9 {
		t.Fatalf("got %d, want 9", got)
	}
}

func TestConnectionIsolationOptOut(t *testing.T) {
	db, err := sql.Open("datafusion", "?datafusion.go.shared_session=false")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)
	db.SetMaxOpenConns(2)

	ctx := context.Background()
	conn1, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn1)

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn2)

	if _, err := conn1.ExecContext(ctx, "create view isolated_view as select 42 as value"); err != nil {
		t.Fatal(err)
	}

	var got int64
	if err := conn1.QueryRowContext(ctx, "select value from isolated_view").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}

	if err := conn2.QueryRowContext(ctx, "select value from isolated_view").Scan(&got); err == nil {
		t.Fatal("expected isolated second connection not to see first connection view")
	}
}

func TestConnectionLifecycleHooks(t *testing.T) {
	ctx := context.Background()
	initCount := 0
	connector, err := NewConnectorWithInitContext("", func(_ context.Context, exec driver.ExecerContext) error {
		initCount++
		_, err := exec.ExecContext(ctx, "create view init_view as select 7 as value", nil)
		return err
	}, WithSharedSession(false))
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, connector)

	driverConn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conn := driverConn.(*Conn)
	defer closeNoError(t, conn)

	readValue := func(query string) (int64, error) {
		rows, err := conn.QueryContext(ctx, query, nil)
		if err != nil {
			return 0, err
		}
		defer closeNoError(t, rows)

		values := []driver.Value{nil}
		if err := rows.Next(values); err != nil {
			return 0, err
		}
		value, ok := values[0].(int64)
		if !ok {
			return 0, fmt.Errorf("got %T, want int64", values[0])
		}
		return value, nil
	}

	if !conn.IsValid() {
		t.Fatal("expected open connection to be valid")
	}
	if got, err := readValue("select value from init_view"); err != nil || got != 7 {
		t.Fatalf("init view before reset got value=%d err=%v, want 7,nil", got, err)
	}
	if _, err := conn.ExecContext(ctx, "create view transient_view as select 42 as value", nil); err != nil {
		t.Fatal(err)
	}
	if got, err := readValue("select value from transient_view"); err != nil || got != 42 {
		t.Fatalf("transient view before reset got value=%d err=%v, want 42,nil", got, err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := conn.ResetSession(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("ResetSession canceled got %v, want context.Canceled", err)
	}
	if _, err := conn.BeginTx(canceled, driver.TxOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("BeginTx canceled got %v, want context.Canceled", err)
	}

	if err := conn.ResetSession(ctx); err != nil {
		t.Fatal(err)
	}
	if initCount != 2 {
		t.Fatalf("got init count %d, want 2", initCount)
	}
	if got, err := readValue("select value from init_view"); err != nil || got != 7 {
		t.Fatalf("init view after reset got value=%d err=%v, want 7,nil", got, err)
	}
	if _, err := readValue("select value from transient_view"); err == nil {
		t.Fatal("expected reset session to discard transient view")
	}
	if _, err := conn.BeginTx(ctx, driver.TxOptions{}); err == nil {
		t.Fatal("expected BeginTx to return unsupported error")
	}

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if conn.IsValid() {
		t.Fatal("expected closed connection to be invalid")
	}
	if err := conn.ResetSession(ctx); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("ResetSession closed got %v, want driver.ErrBadConn", err)
	}
}

func TestSharedSessionInitRunsOnce(t *testing.T) {
	ctx := context.Background()
	initCount := 0
	connector, err := NewConnectorWithInitContext("", func(ctx context.Context, exec driver.ExecerContext) error {
		initCount++
		_, err := exec.ExecContext(ctx, "create view shared_init_view as select 11 as value", nil)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, connector)

	driverConn1, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, driverConn1)
	conn1 := driverConn1.(*Conn)

	driverConn2, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, driverConn2)
	conn2 := driverConn2.(*Conn)

	if initCount != 1 {
		t.Fatalf("got init count %d, want 1", initCount)
	}

	var got int64
	rows, err := conn2.QueryContext(ctx, "select value from shared_init_view", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, rows)
	values := []driver.Value{nil}
	if err := rows.Next(values); err != nil {
		t.Fatal(err)
	}
	got, ok := values[0].(int64)
	if !ok {
		t.Fatalf("got %T, want int64", values[0])
	}
	if got != 11 {
		t.Fatalf("got %d, want 11", got)
	}

	if err := conn1.ResetSession(ctx); err != nil {
		t.Fatal(err)
	}
	if initCount != 1 {
		t.Fatalf("got init count after reset %d, want 1", initCount)
	}
}

func TestContextInitFn(t *testing.T) {
	type contextKey struct{}

	ctx := context.WithValue(context.Background(), contextKey{}, "ok")
	connector, err := NewConnectorWithInitContext("", func(ctx context.Context, exec driver.ExecerContext) error {
		if got := ctx.Value(contextKey{}); got != "ok" {
			return fmt.Errorf("init context value = %v, want ok", got)
		}
		_, err := exec.ExecContext(ctx, "create view context_init_view as select 9 as value", nil)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, connector)

	driverConn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conn := driverConn.(*Conn)
	defer closeNoError(t, conn)

	rows, err := conn.QueryContext(ctx, "select value from context_init_view", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, rows)

	values := []driver.Value{nil}
	if err := rows.Next(values); err != nil {
		t.Fatal(err)
	}
	if values[0] != int64(9) {
		t.Fatalf("got %v, want 9", values[0])
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	cancelConnector, err := NewConnectorWithInitContext("", func(ctx context.Context, _ driver.ExecerContext) error {
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, cancelConnector)

	if _, err := cancelConnector.Connect(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Connect canceled got %v, want context.Canceled", err)
	}
}

func TestConcurrentQueries(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)
	db.SetMaxOpenConns(4)

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var got int64
			if err := db.QueryRowContext(context.Background(), "select $1 + 1", int64(i)).Scan(&got); err != nil {
				errs <- err
				return
			}
			if got != int64(i+1) {
				errs <- fmt.Errorf("query %d got %d, want %d", i, got, i+1)
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

func TestInvalidPrepare(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	if _, err := db.PrepareContext(context.Background(), "select from"); err == nil {
		t.Fatal("expected invalid SQL to fail during prepare")
	}
}

func TestPositionalParameters(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	row := db.QueryRowContext(
		context.Background(),
		"select $1 + 1 as i, $2 as s, $3 as b, $4 as by, $5 as n",
		int64(41),
		"x",
		true,
		[]byte{1, 2, 3},
		nil,
	)

	var (
		i  int64
		s  string
		b  bool
		by []byte
		n  sql.NullInt64
	)
	if err := row.Scan(&i, &s, &b, &by, &n); err != nil {
		t.Fatal(err)
	}
	if i != 42 || s != "x" || !b || !slices.Equal(by, []byte{1, 2, 3}) || n.Valid {
		t.Fatalf("unexpected values: i=%d s=%q b=%v by=%v n=%+v", i, s, b, by, n)
	}
}

func TestQuestionMarkParameters(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	row := db.QueryRowContext(
		context.Background(),
		"select ? + ? as i, '?' as literal, ? as s",
		int64(40),
		int64(2),
		"x",
	)

	var (
		i       int64
		literal string
		s       string
	)
	if err := row.Scan(&i, &literal, &s); err != nil {
		t.Fatal(err)
	}
	if i != 42 || literal != "?" || s != "x" {
		t.Fatalf("unexpected values: i=%d literal=%q s=%q", i, literal, s)
	}
}

func TestUnsupportedParameter(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	_, err = db.QueryContext(context.Background(), "select $1", struct{}{})
	if err == nil {
		t.Fatal("expected unsupported parameter error")
	}
}

func TestDuplicateNamedParameters(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	_, err = db.QueryContext(
		context.Background(),
		"select $value",
		sql.Named("value", int64(1)),
		sql.Named("value", int64(2)),
	)
	if err == nil {
		t.Fatal("expected duplicate named parameter error")
	}
}

func TestNamedParameters(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	var got int64
	if err := db.QueryRowContext(context.Background(), "select $value + 1", sql.Named("value", int64(41))).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

func TestMixedNamedAndPositionalParameters(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	_, err = db.QueryContext(
		context.Background(),
		"select $1, $value",
		int64(1),
		sql.Named("value", int64(2)),
	)
	if err == nil {
		t.Fatal("expected mixed named and positional parameters to be rejected")
	}

	if _, err := db.QueryContext(context.Background(), "select ?, $1", int64(1), int64(2)); err == nil {
		t.Fatal("expected mixed question-mark and dollar-numbered parameters to be rejected")
	}
	if _, err := db.QueryContext(context.Background(), "select ?, $value", int64(1), sql.Named("value", int64(2))); err == nil {
		t.Fatal("expected mixed question-mark and named parameters to be rejected")
	}
}

func TestNamedParameterShapeValidation(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	tests := []struct {
		name  string
		query string
		args  []any
	}{
		{
			name:  "positional argument for named statement",
			query: "select $value",
			args:  []any{int64(1)},
		},
		{
			name:  "wrong named argument",
			query: "select $value",
			args:  []any{sql.Named("other", int64(1))},
		},
		{
			name:  "named argument for positional statement",
			query: "select $1",
			args:  []any{sql.Named("value", int64(1))},
		},
		{
			name:  "missing named argument",
			query: "select $left, $right",
			args:  []any{sql.Named("left", int64(1)), sql.Named("other", int64(2))},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.QueryContext(context.Background(), tt.query, tt.args...)
			if err == nil {
				t.Fatal("expected parameter validation error")
			}
			if !errors.Is(err, ErrNativeInvalidArgument) {
				t.Fatalf("got %v, want ErrNativeInvalidArgument", err)
			}
		})
	}
}

func TestNumInput(t *testing.T) {
	ctx := context.Background()
	connector, err := NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, connector)

	driverConn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, driverConn)

	conn := driverConn.(*Conn)
	tests := []struct {
		query string
		want  int
	}{
		{query: "select 1", want: 0},
		{query: "select '$1' as literal", want: 0},
		{query: "select $1, $2, $1", want: 2},
		{query: "select $3", want: 3},
		{query: "select $value, $value", want: 1},
		{query: "select $left, $right", want: 2},
		{query: "select ?, ?, '?' as literal", want: 2},
	}

	for _, tt := range tests {
		stmt, err := conn.PrepareContext(ctx, tt.query)
		if err != nil {
			t.Fatalf("prepare %q: %v", tt.query, err)
		}
		got := stmt.NumInput()
		if err := stmt.Close(); err != nil {
			t.Fatal(err)
		}
		if got != tt.want {
			t.Fatalf("NumInput(%q) = %d, want %d", tt.query, got, tt.want)
		}
	}

	if _, err := conn.PrepareContext(ctx, "select $1, $name"); err == nil {
		t.Fatal("expected mixed named and positional parameters to fail during prepare")
	}
	if _, err := conn.PrepareContext(ctx, "select $0"); err == nil {
		t.Fatal("expected zero positional parameter to fail during prepare")
	}
	if _, err := conn.PrepareContext(ctx, "select 1; select 2"); err == nil {
		t.Fatal("expected multiple SQL statements to fail during prepare")
	}
	if _, err := conn.PrepareContext(ctx, "select ?1"); err == nil {
		t.Fatal("expected indexed question-mark parameter syntax to fail during prepare")
	}
}

func TestDirectQueryParameterArity(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	if _, err := db.QueryContext(context.Background(), "select $1, $2", int64(1)); err == nil {
		t.Fatal("expected too few arguments to fail before native execution")
	}
	if _, err := db.QueryContext(context.Background(), "select ?, ?", int64(1)); err == nil {
		t.Fatal("expected too few question-mark arguments to fail before native execution")
	}
	if _, err := db.QueryContext(context.Background(), "select 1", int64(1)); err == nil {
		t.Fatal("expected extra arguments to fail before native execution")
	}
}

func TestConcurrentPreparedStatements(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)
	db.SetMaxOpenConns(4)

	ctx := context.Background()
	add, err := db.PrepareContext(ctx, "select $1 + 1")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, add)

	mul, err := db.PrepareContext(ctx, "select $1 * 2")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, mul)

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := range 16 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			stmt := add
			want := int64(i + 1)
			if i%2 == 1 {
				stmt = mul
				want = int64(i * 2)
			}

			var got int64
			if err := stmt.QueryRowContext(ctx, int64(i)).Scan(&got); err != nil {
				errs <- err
				return
			}
			if got != want {
				errs <- fmt.Errorf("query %d got %d, want %d", i, got, want)
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

func TestConcurrentArrowReaders(t *testing.T) {
	db, err := sql.Open("datafusion", "?datafusion.execution.batch_size=1")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	ctx := context.Background()
	conn1, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn1)
	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn2)

	reader1, err := QueryArrowContext(ctx, conn1, "select * from range(3)")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, reader1)
	reader2, err := QueryArrowContext(ctx, conn2, "select * from range(3)")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, reader2)

	for i, reader := range []ArrowReader{reader1, reader2, reader1, reader2} {
		rec, err := reader.Read()
		if err != nil {
			t.Fatalf("reader step %d: %v", i, err)
		}
		if rec.NumRows() != 1 {
			t.Fatalf("reader step %d got %d rows, want 1", i, rec.NumRows())
		}
		rec.Release()
	}
}

func TestArrowReaderOutlivesSQLConnHandle(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := QueryArrowContext(ctx, conn, "select 1 as value")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, reader)

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	rec, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	if rec.NumRows() != 1 {
		t.Fatalf("got %d rows, want 1", rec.NumRows())
	}
}

func TestDateTimeAndDecimalScans(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	row := db.QueryRowContext(
		context.Background(),
		"select cast('2026-01-02' as date) as d, cast('12:34:56' as time) as t, cast(123.45 as decimal(10, 2)) as dec",
	)

	var (
		d   time.Time
		tm  time.Time
		dec string
	)
	if err := row.Scan(&d, &tm, &dec); err != nil {
		t.Fatal(err)
	}
	if got := d.Format("2006-01-02"); got != "2026-01-02" {
		t.Fatalf("got date %s, want 2026-01-02", got)
	}
	if got := tm.Format("15:04:05"); got != "12:34:56" {
		t.Fatalf("got time %s, want 12:34:56", got)
	}
	if dec != "123.45" {
		t.Fatalf("got decimal %q, want 123.45", dec)
	}
}

func TestColumnTypeMetadata(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	rows, err := db.QueryContext(
		context.Background(),
		"select cast('abc' as varchar) as s, cast(null as decimal(10, 2)) as dec, cast(null as time) as tm, cast(null as bigint) as n",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, rows)

	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if len(types) != 4 {
		t.Fatalf("got %d column types, want 4", len(types))
	}

	if got := types[1].DatabaseTypeName(); got != "DECIMAL(10,2)" {
		t.Fatalf("got decimal database type %q, want DECIMAL(10,2)", got)
	}
	precision, scale, ok := types[1].DecimalSize()
	if !ok || precision != 10 || scale != 2 {
		t.Fatalf("got decimal precision=%d scale=%d ok=%v, want 10,2,true", precision, scale, ok)
	}
	if got := types[1].ScanType(); got != reflect.TypeOf(sql.NullString{}) {
		t.Fatalf("got decimal scan type %v, want sql.NullString", got)
	}

	if got := types[2].DatabaseTypeName(); got != "TIME64[ns]" && got != "TIME64[us]" {
		t.Fatalf("got time database type %q, want exact TIME64 unit", got)
	}
	if got := types[2].ScanType(); got != reflect.TypeOf(sql.NullTime{}) {
		t.Fatalf("got time scan type %v, want sql.NullTime", got)
	}

	nullable, ok := types[3].Nullable()
	if !ok || !nullable {
		t.Fatalf("got nullable=%v ok=%v, want true,true", nullable, ok)
	}
	if got := types[3].ScanType(); got != reflect.TypeOf(sql.NullInt64{}) {
		t.Fatalf("got nullable integer scan type %v, want sql.NullInt64", got)
	}
}

func TestArrowSchemaColumnTypeMetadata(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "fixed", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Nullable: true},
		{Name: "var_string", Type: arrow.BinaryTypes.String},
		{Name: "dec", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
		{Name: "iv", Type: &arrow.MonthDayNanoIntervalType{}},
	}, nil)
	rows, err := newRows(schemaOnlyReader{schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, rows)

	length, ok := rows.ColumnTypeLength(0)
	if !ok || length != 16 {
		t.Fatalf("got fixed length=%d ok=%v, want 16,true", length, ok)
	}
	if got := rows.ColumnTypeDatabaseTypeName(0); got != "FIXED_SIZE_BINARY(16)" {
		t.Fatalf("got fixed database type %q, want FIXED_SIZE_BINARY(16)", got)
	}
	if length, ok := rows.ColumnTypeLength(1); ok {
		t.Fatalf("got variable string length=%d ok=true, want unavailable length", length)
	}

	precision, scale, ok := rows.ColumnTypePrecisionScale(2)
	if !ok || precision != 10 || scale != 2 {
		t.Fatalf("got decimal precision=%d scale=%d ok=%v, want 10,2,true", precision, scale, ok)
	}
	if got := rows.ColumnTypeScanType(2); got != reflect.TypeOf(sql.NullString{}) {
		t.Fatalf("got nullable decimal scan type %v, want sql.NullString", got)
	}

	if got := rows.ColumnTypeDatabaseTypeName(3); got != "TIMESTAMP[ns, tz=UTC]" {
		t.Fatalf("got timestamp database type %q, want TIMESTAMP[ns, tz=UTC]", got)
	}
	if got := rows.ColumnTypeDatabaseTypeName(4); got != "INTERVAL_MONTH_DAY_NANO" {
		t.Fatalf("got interval database type %q, want INTERVAL_MONTH_DAY_NANO", got)
	}
}

func TestDurationConversion(t *testing.T) {
	durationType := &arrow.DurationType{Unit: arrow.Millisecond}
	builder := array.NewDurationBuilder(memory.DefaultAllocator, durationType)
	builder.Append(arrow.Duration(1500))
	arr := builder.NewDurationArray()
	defer builder.Release()
	defer arr.Release()

	got, err := arrowValue(arr, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != int64(1500*time.Millisecond) {
		t.Fatalf("got %v (%T), want int64 nanoseconds %d", got, got, int64(1500*time.Millisecond))
	}
	if got := scanType(durationType, false); got != reflect.TypeOf(int64(0)) {
		t.Fatalf("got duration scan type %v, want int64", got)
	}
	if got := scanType(durationType, true); got != reflect.TypeOf(sql.NullInt64{}) {
		t.Fatalf("got nullable duration scan type %v, want sql.NullInt64", got)
	}
}

func TestUnsupportedComplexArrowTypeMetadata(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "items", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
	}, nil)

	_, err := newRows(schemaOnlyReader{schema: schema})
	if err == nil {
		t.Fatal("expected unsupported complex Arrow type error")
	}
	for _, want := range []string{`column "items"`, "Arrow list type", "QueryArrowContext"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("got error %q, want substring %q", err, want)
		}
	}
}

func TestRowsNextResultSet(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rows, err := newRows(schemaOnlyReader{schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, rows)

	if rows.HasNextResultSet() {
		t.Fatal("expected no additional result sets")
	}
	if err := rows.NextResultSet(); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

func TestLastInsertIDZero(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	result, err := db.ExecContext(context.Background(), "select 1")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := result.LastInsertId(); err != nil || got != 0 {
		t.Fatalf("LastInsertId got %d, %v; want 0,nil", got, err)
	}
}

func TestRowsAffected(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	result, err := db.ExecContext(context.Background(), "select 1 union all select 2")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := result.RowsAffected(); err != nil || got != 0 {
		t.Fatalf("RowsAffected for non-count output got %d, %v; want 0,nil", got, err)
	}

	result, err = db.ExecContext(context.Background(), "select cast(2 as bigint) as count")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := result.RowsAffected(); err != nil || got != 2 {
		t.Fatalf("RowsAffected for count output got %d, %v; want 2,nil", got, err)
	}

	result, err = db.ExecContext(context.Background(), "create view rows_affected_view as select 1 as value")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := result.RowsAffected(); err != nil || got < 0 {
		t.Fatalf("RowsAffected for DDL got %d, %v; want non-negative,nil", got, err)
	}
}

func TestExecResultShortCircuitsNonCountResult(t *testing.T) {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.AppendValues([]int64{1, 2}, nil)
	values := builder.NewArray()
	builder.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rec := array.NewRecordBatch(schema, []arrow.Array{values}, int64(values.Len()))
	values.Release()

	reader := &oneRecordReader{rec: rec}
	result, err := execResult(reader)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := result.RowsAffected(); err != nil || got != 0 {
		t.Fatalf("RowsAffected got %d, %v; want 0,nil", got, err)
	}
	if !reader.closed {
		t.Fatal("expected reader to be closed")
	}
	if reader.readCount != 1 {
		t.Fatalf("got %d reads, want 1", reader.readCount)
	}
}

func TestCloseIdempotent(t *testing.T) {
	ctx := context.Background()
	connector, err := NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := connector.Close(); err != nil {
		t.Fatal(err)
	}
	if err := connector.Close(); err != nil {
		t.Fatal(err)
	}

	connector, err = NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, connector)

	driverConn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conn := driverConn.(*Conn)

	stmt, err := conn.PrepareContext(ctx, "select 1")
	if err != nil {
		t.Fatal(err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}

	rows, err := conn.QueryContext(ctx, "select 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := conn.QueryArrowContext(ctx, "select 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTimeParameter(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	want := time.Date(2026, time.January, 2, 3, 4, 5, 123456789, time.FixedZone("offset", -5*60*60))

	var got time.Time
	if err := db.QueryRowContext(context.Background(), "select $1", want).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !got.Equal(want) {
		t.Fatalf("got %s, want %s", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

func TestBareTimeParameterPreservesIANAZone(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn)

	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	value := time.Date(2026, time.January, 2, 3, 4, 5, 123456789, location)

	reader, err := QueryArrowContext(ctx, conn, "select $1 as ts", value)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, reader)

	if got := reader.Schema().Field(0).Type.String(); got != "timestamp[ns, tz=America/New_York]" {
		t.Fatalf("got timestamp type %q, want timestamp[ns, tz=America/New_York]", got)
	}
}

func TestParameterWrapperAccessors(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, time.January, 2, 23, 0, 0, 0, location)
	clock := time.Date(2000, time.January, 1, 12, 34, 56, 789, location)
	timestamp := time.Date(2026, time.January, 2, 3, 4, 5, 123, location)

	date := DateFromTime(day)
	if date.Days() == 0 || !date.Time().Equal(time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected date accessors: days=%d time=%s", date.Days(), date.Time())
	}

	tm, err := NewTimeNanos(int64(12 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if tm.Nanoseconds() != int64(12*time.Hour) || !tm.Time().Equal(time.Unix(0, int64(12*time.Hour)).UTC()) {
		t.Fatalf("unexpected time accessors: nanos=%d time=%s", tm.Nanoseconds(), tm.Time())
	}
	if _, err := NewTimeNanos(int64(24 * time.Hour)); err == nil {
		t.Fatal("expected invalid NewTimeNanos error")
	}
	if TimeFromTime(clock).Nanoseconds() == 0 {
		t.Fatal("expected TimeFromTime to capture clock nanoseconds")
	}

	ts := TimestampWithTimeZone(timestamp, "America/New_York")
	if !ts.Time().Equal(timestamp) || ts.TimeZone() != "America/New_York" {
		t.Fatalf("unexpected timestamp accessors: time=%s zone=%q", ts.Time(), ts.TimeZone())
	}

	dur := DurationFromTime(2 * time.Second)
	if dur.Nanoseconds() != int64(2*time.Second) || dur.Duration() != 2*time.Second {
		t.Fatalf("unexpected duration accessors: nanos=%d duration=%s", dur.Nanoseconds(), dur.Duration())
	}

	dec, err := NewDecimalString("123.45", 10, 2)
	if err != nil {
		t.Fatal(err)
	}
	if dec.String() != "123.45" || dec.Precision() != 10 || dec.Scale() != 2 {
		t.Fatalf("unexpected decimal accessors: value=%q precision=%d scale=%d", dec.String(), dec.Precision(), dec.Scale())
	}

	null := NullTimestamp("America/New_York")
	if null.Type() != ParameterTimestamp || null.TimeZone() != "America/New_York" {
		t.Fatalf("unexpected null accessors: type=%d zone=%q", null.Type(), null.TimeZone())
	}
	if UInt64(42).Uint64() != 42 {
		t.Fatal("unexpected UInt64 accessor")
	}
}

func TestTypedParameterWrappers(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn)

	date := time.Date(2026, time.January, 2, 23, 0, 0, 0, time.FixedZone("offset", -5*60*60))
	clock := time.Date(2000, time.January, 1, 12, 34, 56, 789, time.FixedZone("offset", -5*60*60))
	timestamp := time.Date(2026, time.January, 2, 3, 4, 5, 123456789, time.UTC)

	reader, err := QueryArrowContext(
		ctx,
		conn,
		"select $1 as u, $2 as d, $3 as tm, $4 as ts, $5 as dur, $6 as dec",
		UInt64(math.MaxUint64),
		DateFromTime(date),
		TimeFromTime(clock),
		TimestampWithTimeZone(timestamp, "UTC"),
		DurationFromTime(1500*time.Millisecond),
		DecimalString("123.45", 10, 2),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, reader)

	schema := reader.Schema()
	if got := schema.Field(0).Type.ID(); got != arrow.UINT64 {
		t.Fatalf("got uint parameter type %v, want UINT64", got)
	}
	if got := schema.Field(1).Type.ID(); got != arrow.DATE32 {
		t.Fatalf("got date parameter type %v, want DATE32", got)
	}
	if got := schema.Field(2).Type.String(); got != "time64[ns]" {
		t.Fatalf("got time parameter type %q, want time64[ns]", got)
	}
	if got := schema.Field(3).Type.String(); got != "timestamp[ns, tz=UTC]" {
		t.Fatalf("got timestamp parameter type %q, want timestamp[ns, tz=UTC]", got)
	}
	if got := schema.Field(4).Type.String(); got != "duration[ns]" {
		t.Fatalf("got duration parameter type %q, want duration[ns]", got)
	}
	if got := schema.Field(5).Type.String(); got != "decimal(10, 2)" {
		t.Fatalf("got decimal parameter type %q, want decimal(10, 2)", got)
	}

	rec, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()

	if got := rec.Column(0).(*array.Uint64).Value(0); got != math.MaxUint64 {
		t.Fatalf("got uint64 %d, want %d", got, uint64(math.MaxUint64))
	}
	if got := rec.Column(1).(*array.Date32).Value(0); got != arrow.Date32(DateFromTime(date).days) {
		t.Fatalf("got date32 %d, want %d", got, DateFromTime(date).days)
	}
	wantTime := int64(12*time.Hour + 34*time.Minute + 56*time.Second + 789)
	if got := rec.Column(2).(*array.Time64).Value(0); got != arrow.Time64(wantTime) {
		t.Fatalf("got time64 %d, want %d", got, wantTime)
	}
	if got := rec.Column(3).(*array.Timestamp).Value(0); got != arrow.Timestamp(timestamp.UnixNano()) {
		t.Fatalf("got timestamp %d, want %d", got, timestamp.UnixNano())
	}
	if got := rec.Column(4).(*array.Duration).Value(0); got != arrow.Duration(1500*time.Millisecond) {
		t.Fatalf("got duration %d, want %d", got, int64(1500*time.Millisecond))
	}
	if got := rec.Column(5).(*array.Decimal128).ValueStr(0); got != "123.45" {
		t.Fatalf("got decimal %q, want 123.45", got)
	}
}

func TestTypedNullParameters(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	rows, err := db.QueryContext(
		context.Background(),
		"select $1 as i, $2 as dec, $3 as dur",
		NullOf(ParameterInt64),
		NullDecimal(10, 2),
		NullOf(ParameterDuration),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, rows)

	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if got := types[0].DatabaseTypeName(); got != "INT64" {
		t.Fatalf("got int null type %q, want INT64", got)
	}
	if got := types[1].DatabaseTypeName(); got != "DECIMAL(10,2)" {
		t.Fatalf("got decimal null type %q, want DECIMAL(10,2)", got)
	}
	if got := types[2].DatabaseTypeName(); got != "DURATION[ns]" {
		t.Fatalf("got duration null type %q, want DURATION[ns]", got)
	}

	if !rows.Next() {
		t.Fatal("expected typed null row")
	}
	var (
		i   sql.NullInt64
		dec sql.NullString
		dur sql.NullInt64
	)
	if err := rows.Scan(&i, &dec, &dur); err != nil {
		t.Fatal(err)
	}
	if i.Valid || dec.Valid || dur.Valid {
		t.Fatalf("got valid typed nulls: i=%+v dec=%+v dur=%+v", i, dec, dur)
	}
}

func TestAcceptedUnsignedAndDurationParameters(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	var (
		u   int64
		dur int64
	)
	if err := db.QueryRowContext(context.Background(), "select $1, $2", uint64(42), 2*time.Second).Scan(&u, &dur); err != nil {
		t.Fatal(err)
	}
	if u != 42 || dur != int64(2*time.Second) {
		t.Fatalf("got u=%d dur=%d, want 42,%d", u, dur, int64(2*time.Second))
	}
}

func TestInvalidTypedParameters(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	if _, err := db.QueryContext(context.Background(), "select $1", TimeNanos(int64(24*time.Hour))); err == nil {
		t.Fatal("expected invalid time parameter error")
	}
	if _, err := db.QueryContext(context.Background(), "select $1", DecimalString("123.456", 10, 2)); err == nil {
		t.Fatal("expected invalid decimal parameter error")
	}
	if _, err := db.QueryContext(context.Background(), "select $1", DecimalString("", 10, 2)); err == nil || !strings.Contains(err.Error(), "decimal value is empty") {
		t.Fatalf("got empty decimal error %v, want decimal value is empty", err)
	}
	if _, err := db.QueryContext(context.Background(), "select $1", NullDecimal(0, 0)); err == nil {
		t.Fatal("expected invalid decimal null parameter error")
	}
	if _, err := db.QueryContext(context.Background(), "select $1", NullOf(ParameterDecimal)); err == nil || !strings.Contains(err.Error(), "use NullDecimal") {
		t.Fatalf("got NullOf(ParameterDecimal) error %v, want NullDecimal guidance", err)
	}
}

func TestQueryCancellation(t *testing.T) {
	db, err := sql.Open("datafusion", "?datafusion.execution.batch_size=1")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	ctx, cancel := context.WithCancel(context.Background())
	rows, err := db.QueryContext(ctx, "select * from range(1000000)")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, rows)

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		t.Fatal("expected first row before cancellation")
	}

	cancel()
	if rows.Next() {
		t.Fatal("expected cancellation to stop rows")
	}
	if err := rows.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("got rows error %v, want context.Canceled", err)
	}
}

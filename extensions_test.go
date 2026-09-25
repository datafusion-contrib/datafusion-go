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
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type extensionProvider struct {
	scans   atomic.Int32
	options chan ScanOptions
}

func (*extensionProvider) Schema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
}
func (p *extensionProvider) Scan(context.Context) (array.RecordReader, error) {
	p.scans.Add(1)
	return providerRecords(p.Schema(), nil)
}
func providerRecords(schema *arrow.Schema, projection []int) (array.RecordReader, error) {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues([]int64{1, 2, 3}, nil)
	col := b.NewArray()
	defer col.Release()
	cols := []arrow.Array{col}
	fields := schema.Fields()
	if projection != nil {
		cols = make([]arrow.Array, len(projection))
		fields = make([]arrow.Field, len(projection))
		for i, n := range projection {
			cols[i] = col
			fields[i] = schema.Field(n)
		}
	}
	schema = arrow.NewSchema(fields, nil)
	rec := array.NewRecordBatch(schema, cols, 3)
	defer rec.Release()
	return array.NewRecordReader(schema, []arrow.RecordBatch{rec})
}

type extensionPushdown struct{ extensionProvider }

func (p *extensionPushdown) ScanWithOptions(_ context.Context, opts ScanOptions) (array.RecordReader, error) {
	p.scans.Add(1)
	p.options <- opts
	return providerRecords(p.Schema(), opts.Projection)
}
func sessionForTest(t *testing.T) *Session {
	t.Helper()
	s, err := NewSession("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func readIDs(t *testing.T, r ArrowReader) []int64 {
	t.Helper()
	defer closeNoError(t, r)
	var values []int64
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		a := rec.Column(0).(*array.Int64)
		values = append(values, a.Int64Values()...)
		rec.Release()
	}
	return values
}

func TestGoProviderStreamingOwnershipAndPushdown(t *testing.T) {
	ctx := context.Background()
	s := sessionForTest(t)
	p := &extensionPushdown{extensionProvider{options: make(chan ScanOptions, 8)}}
	if err := s.RegisterTableProvider(ctx, "events", p); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterTableProvider(ctx, "events", p); err == nil {
		t.Fatal("duplicate accepted")
	}
	r, err := s.QueryArrowContext(ctx, "select id from events where id > 1 order by id")
	if err != nil {
		t.Fatal(err)
	}
	values := readIDs(t, r)
	if len(values) != 2 || values[0] != 2 || values[1] != 3 {
		t.Fatalf("advisory filtering: %v", values)
	}
	opts := <-p.options
	if len(opts.Filters) == 0 || opts.Limit != -1 {
		t.Fatalf("pushdown: %+v", opts)
	}
	r, err = s.QueryArrowContext(ctx, "select id from events")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.DeregisterTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	values = readIDs(t, r)
	if len(values) != 3 {
		t.Fatalf("reader outlived session: %v", values)
	}
}

type blockingProvider struct {
	extensionProvider
	started chan struct{}
}

func (p *blockingProvider) Scan(ctx context.Context) (array.RecordReader, error) {
	close(p.started)
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestGoProviderCloseCancelsBlockedScan(t *testing.T) {
	s := sessionForTest(t)
	p := &blockingProvider{started: make(chan struct{})}
	if err := s.RegisterTableProvider(context.Background(), "blocked", p); err != nil {
		t.Fatal(err)
	}
	r, err := s.QueryArrowContext(context.Background(), "select * from blocked")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		rec, err := r.Read()
		if rec != nil {
			rec.Release()
		}
		done <- err
	}()
	select {
	case <-p.started:
	case <-time.After(5 * time.Second):
		t.Fatal("scan never started")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("missing cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read did not cancel")
	}
}
func TestGoScalarFunctionAndZeroArguments(t *testing.T) {
	ctx := context.Background()
	s := sessionForTest(t)
	f := ScalarFunction{Name: "go_double", Arguments: []arrow.DataType{arrow.PrimitiveTypes.Int64}, ReturnType: arrow.PrimitiveTypes.Int64, Volatility: Immutable,
		Evaluate: func(_ context.Context, args []arrow.Array, rows int) (arrow.Array, error) {
			b := array.NewInt64Builder(memory.DefaultAllocator)
			defer b.Release()
			a := args[0].(*array.Int64)
			for i := 0; i < rows; i++ {
				if a.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(a.Value(i) * 2)
				}
			}
			return b.NewArray(), nil
		}}
	if err := s.RegisterScalarFunction(ctx, f); err != nil {
		t.Fatal(err)
	}
	r, err := s.QueryArrowContext(ctx, "select go_double(cast(value as bigint)) from range(3)")
	if err != nil {
		t.Fatal(err)
	}
	values := readIDs(t, r)
	if len(values) != 3 || values[2] != 4 {
		t.Fatalf("UDF output %v", values)
	}
	f.Name = "go_constant"
	f.Arguments = nil
	f.Volatility = Volatile
	f.Evaluate = func(_ context.Context, _ []arrow.Array, rows int) (arrow.Array, error) {
		b := array.NewInt64Builder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < rows; i++ {
			b.Append(42)
		}
		return b.NewArray(), nil
	}
	if err := s.RegisterScalarFunction(ctx, f); err != nil {
		t.Fatal(err)
	}
	r, err = s.QueryArrowContext(ctx, "select go_constant() from range(3)")
	if err != nil {
		t.Fatal(err)
	}
	values = readIDs(t, r)
	if len(values) != 3 || values[0] != 42 {
		t.Fatalf("zero argument UDF output %v", values)
	}
}

type reviewCatalog struct{ calls atomic.Int32 }

func (c *reviewCatalog) ResolveTable(ctx context.Context, schema, table string) (TableProvider, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.calls.Add(1)
	if schema != "public" || table != "events" {
		return nil, errors.New("lookup rejected")
	}
	return &extensionProvider{}, nil
}
func TestGoCatalogQuerySnapshots(t *testing.T) {
	ctx := context.Background()
	s := sessionForTest(t)
	catalog := &reviewCatalog{}
	if err := s.RegisterCatalog(ctx, "remote", catalog); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		r, err := s.QueryArrowContext(ctx, "select a.id from remote.public.events a join remote.public.events b on a.id=b.id")
		if err != nil {
			t.Fatal(err)
		}
		if got := readIDs(t, r); len(got) != 3 {
			t.Fatal(got)
		}
	}
	if catalog.calls.Load() != 2 {
		t.Fatalf("want one lookup per query, got %d", catalog.calls.Load())
	}
	if _, err := s.QueryArrowContext(ctx, "select * from remote.public.missing"); err == nil {
		t.Fatal("catalog error lost")
	}
}

func TestGoExtensionCTASAndImmutableLiteral(t *testing.T) {
	ctx := context.Background()
	s := sessionForTest(t)
	if err := s.RegisterTableProvider(ctx, "source", &extensionProvider{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExecContext(ctx, "create table copied as select * from source"); err != nil {
		t.Fatal(err)
	}
	r, err := s.QueryArrowContext(ctx, "select id from copied order by id")
	if err != nil {
		t.Fatal(err)
	}
	if got := readIDs(t, r); len(got) != 3 {
		t.Fatal(got)
	}
	f := ScalarFunction{Name: "constant_identity", Arguments: []arrow.DataType{arrow.PrimitiveTypes.Int64}, ReturnType: arrow.PrimitiveTypes.Int64, Volatility: Immutable, Evaluate: func(_ context.Context, args []arrow.Array, _ int) (arrow.Array, error) {
		args[0].Retain()
		return args[0], nil
	}}
	if err := s.RegisterScalarFunction(ctx, f); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExecContext(ctx, "create table udf_copy as select constant_identity(cast(21 as bigint)) as id"); err != nil {
		t.Fatal(err)
	}
	r, err = s.QueryArrowContext(ctx, "select id from udf_copy")
	if err != nil {
		t.Fatal(err)
	}
	if got := readIDs(t, r); len(got) != 1 || got[0] != 21 {
		t.Fatal(got)
	}
	// SET must still mutate the real session after Go extensions are installed.
	if _, err := s.ExecContext(ctx, "set datafusion.execution.batch_size = 7"); err != nil {
		t.Fatal(err)
	}
	r, err = s.QueryArrowContext(ctx, "show datafusion.execution.batch_size")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, r)
	rec, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	if got := rec.Column(1).(*array.String).Value(0); got != "7" {
		t.Fatalf("SET lost: %q", got)
	}
}

type panicSchemaReader struct {
	array.RecordReader
	released *atomic.Int32
}

func (*panicSchemaReader) Schema() *arrow.Schema { panic("schema failure") }
func (r *panicSchemaReader) Release()            { r.released.Add(1); r.RecordReader.Release() }

type panicSchemaProvider struct {
	extensionProvider
	released atomic.Int32
}

func (p *panicSchemaProvider) Scan(ctx context.Context) (array.RecordReader, error) {
	r, err := p.extensionProvider.Scan(ctx)
	return &panicSchemaReader{r, &p.released}, err
}
func TestGoProviderSchemaPanicReleasesReader(t *testing.T) {
	s := sessionForTest(t)
	p := &panicSchemaProvider{}
	if err := s.RegisterTableProvider(context.Background(), "panics", p); err != nil {
		t.Fatal(err)
	}
	r, err := s.QueryArrowContext(context.Background(), "select * from panics")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, r)
	if rec, err := r.Read(); err == nil {
		if rec != nil {
			rec.Release()
		}
		t.Fatal("panic lost")
	}
	if p.released.Load() != 1 {
		t.Fatalf("reader release count %d", p.released.Load())
	}
}

func TestPreparedSyntaxCacheUsesCurrentSession(t *testing.T) {
	connector, err := NewConnectorWithInitContext("", nil, WithPreparedStatementCache(true))
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, connector)
	conn, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn)
	c := conn.(*Conn)
	ctx := context.Background()
	if _, err := c.ExecContext(ctx, "create table current_value as select 1 as id", nil); err != nil {
		t.Fatal(err)
	}
	stmt, err := c.PrepareContext(ctx, "select id from current_value where id >= ?")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, stmt)
	for _, want := range []int64{1, 2} {
		if want == 2 {
			if _, err := c.ExecContext(ctx, "drop table current_value", nil); err != nil {
				t.Fatal(err)
			}
			if _, err := c.ExecContext(ctx, "create table current_value as select 2 as id", nil); err != nil {
				t.Fatal(err)
			}
		}
		rows, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, []driver.NamedValue{{Ordinal: 1, Value: int64(0)}})
		if err != nil {
			t.Fatal(err)
		}
		values := make([]driver.Value, 1)
		if err := rows.Next(values); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		if values[0] != want {
			t.Fatalf("stale plan: %v", values)
		}
	}
}

type writeProvider struct {
	extensionProvider
	written atomic.Int64
}

func (p *writeProvider) InsertInto(ctx context.Context, op InsertOp, reader array.RecordReader) (uint64, error) {
	if op != InsertAppend {
		return 0, errors.New("only append supported")
	}
	var count uint64
	for reader.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		count += uint64(reader.RecordBatch().NumRows())
	}
	if err := reader.Err(); err != nil {
		return 0, err
	}
	p.written.Add(int64(count))
	return count, nil
}

type writeCatalog struct{ provider *writeProvider }

func (c writeCatalog) ResolveTable(context.Context, string, string) (TableProvider, error) {
	return c.provider, nil
}
func TestGoProviderInsertAndCatalogCapabilities(t *testing.T) {
	ctx := context.Background()
	s := sessionForTest(t)
	provider := &writeProvider{}
	if err := s.RegisterTableProvider(ctx, "sink", provider); err != nil {
		t.Fatal(err)
	}
	result, err := s.ExecContext(ctx, "insert into sink select cast(value as bigint) from range(10000)")
	if err != nil {
		t.Fatal(err)
	}
	n, err := result.RowsAffected()
	if err != nil || n != 10000 {
		t.Fatalf("count %d: %v", n, err)
	}
	if provider.written.Load() != 10000 {
		t.Fatal("input missing")
	}
	if err := s.RegisterCatalog(ctx, "writable", writeCatalog{provider}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExecContext(ctx, "insert into writable.public.sink values (1),(2)"); err != nil {
		t.Fatal(err)
	}
	if provider.written.Load() != 10002 {
		t.Fatal("catalog lost write capability")
	}
}

type lateReader struct {
	array.RecordReader
	released chan struct{}
}

func (r *lateReader) Release() { r.RecordReader.Release(); close(r.released) }

type lateProvider struct {
	extensionProvider
	started, released chan struct{}
}

func (p *lateProvider) Scan(ctx context.Context) (array.RecordReader, error) {
	close(p.started)
	<-ctx.Done()
	r, err := p.extensionProvider.Scan(context.Background())
	return &lateReader{r, p.released}, err
}
func TestGoProviderLateCallbackReleasesResult(t *testing.T) {
	s := sessionForTest(t)
	p := &lateProvider{started: make(chan struct{}), released: make(chan struct{})}
	if err := s.RegisterTableProvider(context.Background(), "late", p); err != nil {
		t.Fatal(err)
	}
	r, err := s.QueryArrowContext(context.Background(), "select * from late")
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan struct{})
	go func() {
		rec, _ := r.Read()
		if rec != nil {
			rec.Release()
		}
		close(readDone)
	}()
	select {
	case <-p.started:
	case <-time.After(5 * time.Second):
		t.Fatal("scan did not start")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-readDone:
	case <-time.After(5 * time.Second):
		t.Fatal("read did not end")
	}
	select {
	case <-p.released:
	case <-time.After(5 * time.Second):
		t.Fatal("late callback result leaked")
	}
}

func TestIsolatedMutationLocksAreIndependent(t *testing.T) {
	ctx := context.Background()
	connector, err := NewConnectorWithInitContext("", nil, WithSharedSession(false))
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, connector)
	first, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, first)
	second, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, second)
	held, err := first.(*Conn).QueryArrowContext(ctx, "create table first_table as select 1 as id", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, held)
	timeout, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := second.(*Conn).ExecContext(timeout, "create table second_table as select 2 as id", nil); err != nil {
		t.Fatalf("isolated session blocked: %v", err)
	}
}

func TestConcurrentSharedFunctionRegistrationRejectsDuplicate(t *testing.T) {
	ctx := context.Background()
	connector, err := NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, connector)
	first, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, first)
	second, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, second)
	f := ScalarFunction{Name: "same_name", ReturnType: arrow.PrimitiveTypes.Int64, Evaluate: func(context.Context, []arrow.Array, int) (arrow.Array, error) { return nil, errors.New("unused") }}
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, c := range []*Conn{first.(*Conn), second.(*Conn)} {
		go func(c *Conn) { <-start; results <- registerScalarFunction(ctx, c, f) }(c)
	}
	close(start)
	successes := 0
	for range 2 {
		if <-results == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful registrations %d", successes)
	}
}

func TestExplicitTuningOptionsOverrideDSN(t *testing.T) {
	// Invalid DSN values prove that explicit false/zero really override them.
	s, err := NewSession("?datafusion.go.runtime_workers=invalid&datafusion.go.shared_runtime=invalid&datafusion.go.cache_statements=invalid", WithRuntimeWorkers(0), WithSharedRuntime(false), WithPreparedStatementCache(false))
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, s)
	r, err := s.QueryArrowContext(context.Background(), "select cast(1 as bigint)")
	if err != nil {
		t.Fatal(err)
	}
	if got := readIDs(t, r); len(got) != 1 || got[0] != 1 {
		t.Fatal(got)
	}
}

func TestPreparedSyntaxCacheHonorsParserChanges(t *testing.T) {
	ctx := context.Background()
	connector, err := NewConnectorWithInitContext("?datafusion.sql_parser.dialect=postgresql", nil, WithPreparedStatementCache(true))
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, connector)
	conn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn)
	c := conn.(*Conn)
	stmt, err := c.PrepareContext(ctx, "SELECT 7 # 3")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, stmt)
	rows, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	values := make([]driver.Value, 1)
	if err := rows.Next(values); err != nil {
		t.Fatal(err)
	}
	closeNoError(t, rows)
	if values[0] != int64(4) {
		t.Fatal(values)
	}
	if _, err := c.ExecContext(ctx, "SET datafusion.sql_parser.dialect = 'generic'", nil); err != nil {
		t.Fatal(err)
	}
	rows, err = stmt.(driver.StmtQueryContext).QueryContext(ctx, nil)
	if err == nil {
		closeNoError(t, rows)
		t.Fatal("retained PostgreSQL syntax after dialect changed")
	}
}

func TestGoProviderEmptyProjectionPreservesRows(t *testing.T) {
	s := sessionForTest(t)
	p := &extensionPushdown{extensionProvider{options: make(chan ScanOptions, 1)}}
	ctx := context.Background()
	if err := s.RegisterTableProvider(ctx, "events", p); err != nil {
		t.Fatal(err)
	}
	r, err := s.QueryArrowContext(ctx, "select count(*) from events")
	if err != nil {
		t.Fatal(err)
	}
	if got := readIDs(t, r); len(got) != 1 || got[0] != 3 {
		t.Fatal(got)
	}
	if opts := <-p.options; opts.Projection == nil || len(opts.Projection) != 0 {
		t.Fatalf("projection: %+v", opts)
	}
}

type failingInputReader struct {
	array.RecordReader
	yielded bool
}

func (r *failingInputReader) Next() bool {
	if r.yielded {
		panic("input failed after first batch")
	}
	r.yielded = true
	return r.RecordReader.Next()
}

type failingInputProvider struct{ extensionProvider }

func (p *failingInputProvider) Scan(ctx context.Context) (array.RecordReader, error) {
	r, err := p.extensionProvider.Scan(ctx)
	if err != nil {
		return nil, err
	}
	return &failingInputReader{RecordReader: r}, nil
}
func TestGoProviderInsertRejectsPartialInput(t *testing.T) {
	s := sessionForTest(t)
	ctx := context.Background()
	destination := &writeProvider{}
	if err := s.RegisterTableProvider(ctx, "destination", destination); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterTableProvider(ctx, "failing_input", &failingInputProvider{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExecContext(ctx, "insert into destination select * from failing_input"); err == nil {
		t.Fatal("input panic lost")
	}
	if got := destination.written.Load(); got != 0 {
		t.Fatalf("committed %d rows from incomplete input", got)
	}
}

func TestConcurrentFirstGoProviderRegistration(t *testing.T) {
	ctx := context.Background()
	connector, err := NewConnectorWithInitContext("", nil, WithRuntimeWorkers(2))
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer closeNoError(t, db)
	registration, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, registration)
	start := make(chan struct{})
	done := make(chan error, 4)
	for i := 0; i < 4; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			defer func() { _ = conn.Close() }()
			<-start
			for j := 0; j < 50; j++ {
				var count int64
				err := conn.QueryRowContext(ctx, "select count(*) from arriving_provider").Scan(&count)
				// Queries planned before publication may see a missing table.
				// Any discovered provider must already have callback context.
				if err != nil && !strings.Contains(err.Error(), "not found") {
					done <- err
					return
				}
				if err == nil && count != 3 {
					done <- fmt.Errorf("count = %d", count)
					return
				}
			}
			done <- nil
		}()
	}
	close(start)
	if _, err := RegisterTableProvider(ctx, registration, "arriving_provider", &extensionProvider{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("concurrent registration stalled")
		}
	}
}

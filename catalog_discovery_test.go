//go:build cgo

package datafusion

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
)

type discoveryCatalog struct {
	mu                                    sync.Mutex
	schemas                               []string
	tables                                map[string][]string
	namesErr                              error
	started                               chan struct{}
	block                                 bool
	schemaCalls, tableCalls, resolveCalls atomic.Int32
	provider                              extensionProvider
}

func newDiscoveryCatalog() *discoveryCatalog {
	return &discoveryCatalog{schemas: []string{"public", "empty", "public"}, tables: map[string][]string{"public": {"events", "events"}}}
}
func (c *discoveryCatalog) SchemaNames(ctx context.Context) ([]string, error) {
	c.schemaCalls.Add(1)
	if c.started != nil {
		close(c.started)
	}
	if c.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.schemas, c.namesErr
}
func (c *discoveryCatalog) TableNames(_ context.Context, schema string) ([]string, error) {
	c.tableCalls.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tables[schema], c.namesErr
}
func (c *discoveryCatalog) ResolveTable(_ context.Context, schema, table string) (TableProvider, error) {
	c.resolveCalls.Add(1)
	if schema == "public" && table == "events" {
		return &c.provider, nil
	}
	return nil, nil
}

func queryStrings(t *testing.T, s *Session, query string) []string {
	t.Helper()
	r, err := s.QueryArrowContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, r)
	var result []string
	for {
		batch, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			result = append(result, strings.Clone(batch.Column(0).(*array.String).Value(i)))
		}
		batch.Release()
	}
	return result
}

func TestCatalogDiscoveryNamesAreLazyAndRefreshed(t *testing.T) {
	s := sessionForTest(t)
	c := newDiscoveryCatalog()
	if err := s.RegisterCatalog(context.Background(), "remote", c); err != nil {
		t.Fatal(err)
	}
	if got := queryStrings(t, s, "SELECT schema_name FROM information_schema.schemata WHERE catalog_name='remote' ORDER BY schema_name"); strings.Join(got, ",") != "empty,public" {
		t.Fatal(got)
	}
	if c.tableCalls.Load() != 0 || c.resolveCalls.Load() != 0 {
		t.Fatal("schema listing resolved tables")
	}
	if got := queryStrings(t, s, "SELECT table_name FROM information_schema.tables WHERE table_catalog='remote' AND table_schema='public'"); strings.Join(got, ",") != "events" {
		t.Fatal(got)
	}
	if c.resolveCalls.Load() != 0 || c.provider.scans.Load() != 0 {
		t.Fatal("table listing opened providers")
	}
	// Original shared slices remain unchanged even though results are sorted and deduplicated.
	if strings.Join(c.schemas, ",") != "public,empty,public" {
		t.Fatal("listing modified caller names")
	}
	c.mu.Lock()
	c.tables["public"] = []string{"events", "new.table"}
	c.mu.Unlock()
	if got := queryStrings(t, s, "SELECT table_name FROM remote.information_schema.tables WHERE table_catalog='remote' AND table_schema='public' ORDER BY table_name"); strings.Join(got, ",") != "events,new.table" {
		t.Fatal(got)
	}
	if c.resolveCalls.Load() != 0 {
		t.Fatal("refresh resolved providers")
	}
}

func TestCatalogDiscoveryColumnsAndOrdinaryQueries(t *testing.T) {
	s := sessionForTest(t)
	c := newDiscoveryCatalog()
	if err := s.RegisterCatalog(context.Background(), "remote", c); err != nil {
		t.Fatal(err)
	}
	if got := queryStrings(t, s, "SELECT column_name FROM information_schema.columns WHERE table_catalog='remote'"); strings.Join(got, ",") != "id" {
		t.Fatal(got)
	}
	if c.resolveCalls.Load() != 1 || c.provider.scans.Load() != 0 {
		t.Fatal("column listing scanned or resolved repeatedly")
	}
	before := c.schemaCalls.Load()
	r, err := s.QueryArrowContext(context.Background(), "SELECT a.id FROM remote.public.events a JOIN remote.public.events b ON a.id=b.id ORDER BY a.id")
	if err != nil {
		t.Fatal(err)
	}
	if got := readIDs(t, r); len(got) != 3 {
		t.Fatal(got)
	}
	if c.schemaCalls.Load() != before || c.resolveCalls.Load() != 2 {
		t.Fatal("ordinary query enumerated or resolved repeatedly")
	}
	queryStrings(t, s, "SHOW datafusion.execution.batch_size")
	if c.schemaCalls.Load() != before {
		t.Fatal("SHOW setting enumerated remote catalogs")
	}
	queryStrings(t, s, "SELECT table_name FROM information_schema.tables WHERE table_catalog='remote'")
	if c.resolveCalls.Load() != 2 {
		t.Fatal("listing resolved providers")
	}
	r, err = s.QueryArrowContext(context.Background(), "SHOW TABLES")
	if err != nil {
		t.Fatal(err)
	}
	closeNoError(t, r)
	queryStrings(t, s, "SHOW COLUMNS FROM remote.public.events")
}

func TestCatalogDiscoveryErrorsCancellationAndRecovery(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		s := sessionForTest(t)
		c := newDiscoveryCatalog()
		c.namesErr = errors.New("listing unavailable")
		if err := s.RegisterCatalog(context.Background(), "remote", c); err != nil {
			t.Fatal(err)
		}
		_, err := s.QueryArrowContext(context.Background(), "SHOW TABLES")
		if err == nil || !strings.Contains(err.Error(), "listing unavailable") {
			t.Fatal(err)
		}
		// Ordinary data reads still work while enumeration is unavailable.
		r, err := s.QueryArrowContext(context.Background(), "SELECT id FROM remote.public.events")
		if err != nil {
			t.Fatal(err)
		}
		if len(readIDs(t, r)) != 3 {
			t.Fatal("missing rows")
		}
		c.mu.Lock()
		c.namesErr = nil
		c.mu.Unlock()
		queryStrings(t, s, "SELECT table_name FROM information_schema.tables WHERE table_catalog='remote'")
	})
	t.Run("cancel", func(t *testing.T) {
		s := sessionForTest(t)
		c := newDiscoveryCatalog()
		c.block = true
		c.started = make(chan struct{})
		if err := s.RegisterCatalog(context.Background(), "remote", c); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			r, err := s.QueryArrowContext(ctx, "SHOW TABLES")
			if r != nil {
				_ = r.Close()
			}
			done <- err
		}()
		select {
		case <-c.started:
		case <-time.After(5 * time.Second):
			t.Fatal("discovery did not start")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("discovery did not cancel")
		}
	})
}

func TestCatalogDiscoveryRejectsInvalidNames(t *testing.T) {
	for _, name := range []string{"", "bad\x00name"} {
		t.Run("invalid", func(t *testing.T) {
			s := sessionForTest(t)
			c := newDiscoveryCatalog()
			c.schemas = []string{name}
			if err := s.RegisterCatalog(context.Background(), "remote", c); err != nil {
				t.Fatal(err)
			}
			_, err := s.QueryArrowContext(context.Background(), "SHOW TABLES")
			if err == nil || !strings.Contains(err.Error(), "NUL-containing") {
				t.Fatal(err)
			}
		})
	}
}

type controlledDiscoveryCatalog struct {
	*discoveryCatalog
	tableErr       error
	resolveStarted chan struct{}
	resolveDone    chan struct{}
}

func (c *controlledDiscoveryCatalog) TableNames(ctx context.Context, schema string) ([]string, error) {
	if c.tableErr != nil {
		return nil, c.tableErr
	}
	return c.discoveryCatalog.TableNames(ctx, schema)
}
func (c *controlledDiscoveryCatalog) ResolveTable(ctx context.Context, schema, table string) (TableProvider, error) {
	if c.resolveStarted != nil {
		close(c.resolveStarted)
		<-ctx.Done()
		close(c.resolveDone)
		return nil, ctx.Err()
	}
	return c.discoveryCatalog.ResolveTable(ctx, schema, table)
}

func TestCatalogDiscoveryTableErrorsAndLiteralNames(t *testing.T) {
	s := sessionForTest(t)
	c := &controlledDiscoveryCatalog{discoveryCatalog: newDiscoveryCatalog(), tableErr: errors.New("table listing failed")}
	if err := s.RegisterCatalog(t.Context(), "remote", c); err != nil {
		t.Fatal(err)
	}
	_, err := s.QueryArrowContext(t.Context(), "SHOW TABLES")
	if err == nil || !strings.Contains(err.Error(), "table listing failed") {
		t.Fatal(err)
	}
	c.tableErr = nil
	c.schemas = []string{"public", "space.and\"quote", "雪", "information_schema"}
	c.tables["space.and\"quote"] = []string{"select", "literal\"name"}
	got := queryStrings(t, s, "SELECT table_name FROM information_schema.tables WHERE table_catalog='remote' AND table_schema='space.and\"quote' ORDER BY table_name")
	if strings.Join(got, ",") != "literal\"name,select" {
		t.Fatal(got)
	}
	// Names may disappear before resolution; missing tables do not produce columns.
	c.tables["public"] = []string{"events", "missing"}
	if got := queryStrings(t, s, "SELECT column_name FROM information_schema.columns WHERE table_catalog='remote'"); strings.Join(got, ",") != "id" {
		t.Fatal(got)
	}
	c.tables["public"] = []string{"bad\x00name"}
	_, err = s.QueryArrowContext(t.Context(), "SHOW TABLES")
	if err == nil || !strings.Contains(err.Error(), "NUL-containing") {
		t.Fatal(err)
	}
}

func TestCatalogDiscoveryLazyLookupCancellation(t *testing.T) {
	s := sessionForTest(t)
	c := &controlledDiscoveryCatalog{discoveryCatalog: newDiscoveryCatalog(), resolveStarted: make(chan struct{}), resolveDone: make(chan struct{})}
	if err := s.RegisterCatalog(t.Context(), "remote", c); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		reader, err := s.QueryArrowContext(ctx, "SELECT column_name FROM information_schema.columns WHERE table_catalog='remote'")
		if err == nil {
			defer func() { _ = reader.Close() }()
			for {
				batch, readErr := reader.Read()
				if batch != nil {
					batch.Release()
				}
				if readErr != nil {
					err = readErr
					break
				}
			}
		}
		done <- err
	}()
	select {
	case <-c.resolveStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("lookup did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read did not cancel")
	}
	select {
	case <-c.resolveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("lookup context not canceled")
	}
}

func TestCatalogDiscoveryConcurrentSnapshotsAndDirectReferences(t *testing.T) {
	s := sessionForTest(t)
	c := newDiscoveryCatalog()
	// Direct references remain queryable even when absent from a listing.
	c.tables["public"] = nil
	if err := s.RegisterCatalog(t.Context(), "remote", c); err != nil {
		t.Fatal(err)
	}
	r, err := s.QueryArrowContext(t.Context(), "SELECT e.id FROM remote.public.events e CROSS JOIN (SELECT count(*) FROM information_schema.tables) m ORDER BY e.id")
	if err != nil {
		t.Fatal(err)
	}
	if len(readIDs(t, r)) != 3 {
		t.Fatal("direct table lost")
	}
	c.tables["public"] = []string{"events"}
	before := c.resolveCalls.Load()
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			// Repeated metadata references share one lazy table resolution per query.
			reader, err := s.QueryArrowContext(t.Context(), "SELECT column_name FROM information_schema.columns WHERE table_catalog='remote' UNION ALL SELECT column_name FROM information_schema.columns WHERE table_catalog='remote'")
			if err != nil {
				t.Error(err)
				return
			}
			defer closeNoError(t, reader)
			var rows int64
			for {
				batch, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Error(err)
					return
				}
				rows += batch.NumRows()
				batch.Release()
			}
			if rows != 2 {
				t.Errorf("got %d rows", rows)
			}
		})
	}
	wg.Wait()
	if c.resolveCalls.Load()-before != 4 {
		t.Fatal("metadata snapshot shared across queries or resolved repeatedly")
	}
}

func TestCatalogDiscoverySQLConnectionAndSessionLifetime(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn)
	if err := RegisterCatalog(t.Context(), conn, "remote", newDiscoveryCatalog()); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := conn.QueryRowContext(t.Context(), "SELECT table_name FROM information_schema.tables WHERE table_catalog='remote'").Scan(&name); err != nil || name != "events" {
		t.Fatalf("%q: %v", name, err)
	}
	s := sessionForTest(t)
	if err := s.RegisterCatalog(t.Context(), "remote", newDiscoveryCatalog()); err != nil {
		t.Fatal(err)
	}
	reader, err := s.QueryArrowContext(t.Context(), "SELECT column_name FROM information_schema.columns WHERE table_catalog='remote'")
	if err != nil {
		t.Fatal(err)
	}
	closeNoError(t, s)
	defer closeNoError(t, reader)
	batch, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()
	if batch.Column(0).(*array.String).Value(0) != "id" {
		t.Fatal("metadata stream lost its catalog")
	}
}

type panickingDiscoveryCatalog struct{ *discoveryCatalog }

func (c panickingDiscoveryCatalog) SchemaNames(context.Context) ([]string, error) {
	panic("listing panic")
}
func TestCatalogDiscoveryPanicAndEmptyListings(t *testing.T) {
	s := sessionForTest(t)
	if err := s.RegisterCatalog(t.Context(), "remote", panickingDiscoveryCatalog{newDiscoveryCatalog()}); err != nil {
		t.Fatal(err)
	}
	_, err := s.QueryArrowContext(t.Context(), "SHOW TABLES")
	if err == nil || !strings.Contains(err.Error(), "listing panic") {
		t.Fatal(err)
	}
	reader, err := s.QueryArrowContext(t.Context(), "SELECT id FROM remote.public.events")
	if err != nil {
		t.Fatal(err)
	}
	if len(readIDs(t, reader)) != 3 {
		t.Fatal("session unusable after panic")
	}
	emptySession := sessionForTest(t)
	empty := newDiscoveryCatalog()
	empty.schemas = nil
	if err := emptySession.RegisterCatalog(t.Context(), "remote", empty); err != nil {
		t.Fatal(err)
	}
	if got := queryStrings(t, emptySession, "SELECT table_name FROM information_schema.tables WHERE table_catalog='remote' AND table_schema<>'information_schema'"); len(got) != 0 {
		t.Fatal(got)
	}
}

type customInformationSchemaCatalog struct{ discoveryCatalog }

func (c *customInformationSchemaCatalog) ResolveTable(context.Context, string, string) (TableProvider, error) {
	return &c.provider, nil
}
func TestCatalogDiscoveryDisabledPreservesCustomSchema(t *testing.T) {
	s, err := NewSession("?datafusion.catalog.information_schema=false")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, s)
	c := &customInformationSchemaCatalog{}
	if err := s.RegisterCatalog(t.Context(), "remote", c); err != nil {
		t.Fatal(err)
	}
	reader, err := s.QueryArrowContext(t.Context(), "SELECT id FROM remote.information_schema.events")
	if err != nil {
		t.Fatal(err)
	}
	if len(readIDs(t, reader)) != 3 {
		t.Fatal("custom schema unavailable")
	}
	if c.schemaCalls.Load() != 0 || c.tableCalls.Load() != 0 {
		t.Fatal("disabled information_schema enumerated")
	}
}

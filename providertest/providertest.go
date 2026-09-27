// Package providertest checks the reader and ownership contracts of Go table
// providers. Use it from provider tests with small, finite fixtures whose schema
// and row count stay stable for the duration of each test. It does not require a
// native DataFusion library and does not test storage-specific values or writes.
package providertest

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	datafusion "github.com/datafusion-contrib/datafusion-go"
)

// Factory creates a fixture for one subtest. Register resource cleanup with t.
// Readers returned by the provider are owned and released by this package.
type Factory func(t *testing.T) datafusion.TableProvider

// Run checks repeatable row counts, independently consumable concurrent scans,
// returned schemas, retained batches, and optional projection/limit contracts.
// It does not require a stable row order. A provider may ignore a limit hint;
// it must return at least min(limit, full row count) rows and no extra rows.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	if factory == nil {
		t.Fatal("providertest: nil factory")
	}
	for _, name := range []string{"repeat_scans", "concurrent_scans", "pushdown"} {
		t.Run(name, func(t *testing.T) {
			p := factory(t)
			if p == nil || p.Schema() == nil {
				t.Fatal("providertest: nil provider or schema")
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			schema := p.Schema()
			baseline, err := scan(ctx, p, schema, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "repeat_scans":
				for range 2 {
					rows, err := scan(ctx, p, schema, nil, nil)
					if err != nil {
						t.Fatal(err)
					}
					if rows != baseline {
						t.Fatalf("repeated scan returned %d rows, want %d", rows, baseline)
					}
				}
			case "concurrent_scans":
				ready := make(chan struct{}, 2)
				start := make(chan struct{})
				results := make(chan error, 2)
				for range 2 {
					go func() {
						rows, err := scan(ctx, p, schema, nil, func() { ready <- struct{}{}; <-start })
						if err == nil && rows != baseline {
							err = fmt.Errorf("concurrent scan returned %d rows, want %d", rows, baseline)
						}
						results <- err
					}()
				}
				// Scan always reaches the barrier, including when opening fails.
				<-ready
				<-ready
				close(start)
				for range 2 {
					if err := <-results; err != nil {
						t.Error(err)
					}
				}
			case "pushdown":
				if _, ok := p.(datafusion.PushdownTableProvider); !ok {
					t.Skip("provider does not implement pushdown")
				}
				reverse := make([]int, schema.NumFields())
				for i := range reverse {
					reverse[i] = len(reverse) - 1 - i
				}
				for _, opts := range []datafusion.ScanOptions{
					{Projection: nil, Limit: -1}, {Projection: []int{}, Limit: -1}, {Projection: reverse, Limit: -1}, {Projection: nil, Limit: 2},
				} {
					expected := schema
					if opts.Projection != nil {
						fields := make([]arrow.Field, len(opts.Projection))
						for i, j := range opts.Projection {
							fields[i] = schema.Field(j)
						}
						metadata := schema.Metadata()
						expected = arrow.NewSchemaWithEndian(fields, &metadata, schema.Endianness())
					}
					rows, err := scan(ctx, p, expected, &opts, nil)
					if err != nil {
						t.Fatal(err)
					}
					minimum := baseline
					if opts.Limit >= 0 && minimum > opts.Limit {
						minimum = opts.Limit
					}
					if rows < minimum || rows > baseline {
						t.Fatalf("projection %v, limit %d: got %d rows, want [%d,%d]", opts.Projection, opts.Limit, rows, minimum, baseline)
					}
				}
			}
		})
	}
}

func scan(ctx context.Context, p datafusion.TableProvider, schema *arrow.Schema, opts *datafusion.ScanOptions, ready func()) (int64, error) {
	var reader array.RecordReader
	var err error
	if opts == nil {
		reader, err = p.Scan(ctx)
	} else {
		reader, err = p.(datafusion.PushdownTableProvider).ScanWithOptions(ctx, *opts)
	}
	if ready != nil {
		ready()
	}
	defer func() {
		if reader != nil {
			reader.Release()
		}
	}()
	if err != nil {
		return 0, err
	}
	if reader == nil {
		return 0, fmt.Errorf("provider returned a nil reader")
	}
	if !sameSchema(schema, reader.Schema()) {
		return 0, fmt.Errorf("provider returned an incorrect reader schema")
	}
	var rows int64
	var retained arrow.RecordBatch
	defer func() {
		if retained != nil {
			retained.Release()
		}
	}()
	for reader.Next() {
		record := reader.RecordBatch()
		if record == nil || !sameSchema(schema, record.Schema()) {
			return 0, fmt.Errorf("provider returned a nil batch or incorrect batch schema")
		}
		if retained == nil {
			record.Retain()
			retained = record
		}
		rows += record.NumRows()
	}
	if err := reader.Err(); err != nil {
		return 0, err
	}
	// Keep the first batch past all subsequent Next calls and reader release.
	reader.Release()
	reader = nil
	if retained != nil {
		for _, col := range retained.Columns() {
			if int64(col.Len()) != retained.NumRows() {
				return 0, fmt.Errorf("retained batch column length changed")
			}
			// Exercise retained buffers, including variable-size/nested values.
			if col.Len() > 0 {
				_ = col.ValueStr(col.Len() - 1)
			}
		}
	}
	return rows, nil
}

func sameSchema(a, b *arrow.Schema) bool {
	return a != nil && b != nil && a.Equal(b) && a.Metadata().Equal(b.Metadata())
}

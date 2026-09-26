package providertest

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	datafusion "github.com/datafusion-contrib/datafusion-go"
)

type fixture struct{ mem memory.Allocator }

func (f fixture) Schema() *arrow.Schema {
	metadata := arrow.NewMetadata([]string{"fixture"}, []string{"true"})
	return arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "name", Type: arrow.BinaryTypes.String}}, &metadata)
}
func (f fixture) Scan(ctx context.Context) (array.RecordReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	builder := array.NewRecordBuilder(f.mem, f.Schema())
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"one", "two", "three"}, nil)
	batch := builder.NewRecordBatch()
	defer batch.Release()
	return array.NewRecordReader(f.Schema(), []arrow.RecordBatch{batch, batch})
}
func (f fixture) ScanWithOptions(ctx context.Context, opts datafusion.ScanOptions) (array.RecordReader, error) {
	reader, err := f.Scan(ctx)
	if err != nil {
		return nil, err
	}
	projected, err := datafusion.ProjectReader(reader, opts.Projection)
	if err != nil {
		reader.Release()
		return nil, err
	}
	limited, err := datafusion.LimitReader(projected, opts.Limit)
	if err != nil {
		projected.Release()
		return nil, err
	}
	return limited, nil
}

func TestRun(t *testing.T) {
	Run(t, func(t *testing.T) datafusion.TableProvider {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		t.Cleanup(func() { mem.AssertSize(t, 0) })
		return fixture{mem: mem}
	})
}

type badProvider struct {
	fixture
	reader array.RecordReader
	err    error
}

func (p badProvider) Scan(context.Context) (array.RecordReader, error) { return p.reader, p.err }
func TestScanDetectsContractFailures(t *testing.T) {
	f := fixture{mem: memory.DefaultAllocator}
	sentinel := errors.New("scan failed")
	if _, err := scan(t.Context(), badProvider{fixture: f, err: sentinel}, f.Schema(), nil, nil); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if _, err := scan(t.Context(), badProvider{fixture: f}, f.Schema(), nil, nil); err == nil {
		t.Fatal("accepted nil reader")
	}
	wrongSchema := arrow.NewSchema(nil, nil)
	reader, err := array.NewRecordReader(wrongSchema, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scan(t.Context(), badProvider{fixture: f, reader: reader}, f.Schema(), nil, nil); err == nil {
		t.Fatal("accepted wrong schema")
	}
}

package main

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	datafusion "github.com/datafusion-contrib/datafusion-go"
)

type events struct{}

func (events) Schema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
}
func (e events) Scan(ctx context.Context) (array.RecordReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 3}, nil)
	values := builder.NewArray()
	defer values.Release()
	batch := array.NewRecordBatch(e.Schema(), []arrow.Array{values}, 3)
	defer batch.Release()
	return array.NewRecordReader(e.Schema(), []arrow.RecordBatch{batch})
}

// Filters are advisory and rechecked by DataFusion. This fixture ignores them.
func (e events) ScanWithOptions(ctx context.Context, opts datafusion.ScanOptions) (array.RecordReader, error) {
	reader, err := e.Scan(ctx)
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

type catalog struct{}

func (catalog) SchemaNames(ctx context.Context) ([]string, error) {
	return []string{"public"}, ctx.Err()
}
func (catalog) TableNames(ctx context.Context, schema string) ([]string, error) {
	if schema == "public" {
		return []string{"events"}, ctx.Err()
	}
	return nil, ctx.Err()
}
func (catalog) ResolveTable(ctx context.Context, schema, table string) (datafusion.TableProvider, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if schema == "public" && table == "events" {
		return events{}, nil
	}
	return nil, nil
}

func run() error {
	ctx := context.Background()
	session, err := datafusion.NewSession("")
	if err != nil {
		return err
	}
	defer func() { _ = session.Close() }()
	if err := session.RegisterTableProvider(ctx, "events", events{}); err != nil {
		return err
	}
	if err := session.RegisterCatalog(ctx, "remote", catalog{}); err != nil {
		return err
	}
	// Metadata is available through information_schema.tables/columns and SHOW TABLES.
	if err := session.RegisterScalarFunction(ctx, datafusion.ScalarFunction{
		Name: "double_id", Arguments: []arrow.DataType{arrow.PrimitiveTypes.Int64}, ReturnType: arrow.PrimitiveTypes.Int64, Volatility: datafusion.Immutable,
		Evaluate: func(ctx context.Context, args []arrow.Array, rows int) (arrow.Array, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			input := args[0].(*array.Int64)
			builder := array.NewInt64Builder(memory.DefaultAllocator)
			defer builder.Release()
			for i := 0; i < rows; i++ {
				if input.IsNull(i) {
					builder.AppendNull()
				} else {
					builder.Append(input.Value(i) * 2)
				}
			}
			return builder.NewArray(), nil
		},
	}); err != nil {
		return err
	}
	reader, err := session.QueryArrowContext(ctx, "select double_id(id) from remote.public.events where id > 1 order by id")
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()
	for {
		batch, err := reader.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		fmt.Println(batch.Column(0))
		batch.Release()
	}
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

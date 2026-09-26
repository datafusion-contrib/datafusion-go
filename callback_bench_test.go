//go:build cgo

package datafusion

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type benchmarkProvider struct{ batches []arrow.RecordBatch }

func (p benchmarkProvider) Schema() *arrow.Schema { return p.batches[0].Schema() }
func (p benchmarkProvider) Scan(context.Context) (array.RecordReader, error) {
	return array.NewRecordReader(p.Schema(), p.batches)
}

// BenchmarkCallbackTransfer isolates callback transport from source I/O and
// computation. UDF input comes from a native table registered before timing;
// the identity evaluator retains its input, exercising borrowed-array lifetimes.
// Results are drained completely. B/op measures Go allocations, not native RAM.
func BenchmarkCallbackTransfer(b *testing.B) {
	for _, shape := range []string{"int64", "wide16", "utf8"} {
		for _, rows := range []int{128, 8192} {
			for _, mode := range []string{"provider", "udf"} {
				b.Run(fmt.Sprintf("%s/%s/%d", mode, shape, rows), func(b *testing.B) {
					ctx := context.Background()
					s, err := NewSession("", WithRuntimeWorkers(4))
					if err != nil {
						b.Fatal(err)
					}
					defer closeNoError(b, s)
					width := 1
					if shape == "wide16" {
						width = 16
					}
					fields := make([]arrow.Field, width)
					cols := make([]arrow.Array, width)
					names := make([]string, width)
					types := make([]arrow.DataType, width)
					for col := range cols {
						if shape == "utf8" {
							builder := array.NewStringBuilder(memory.DefaultAllocator)
							for i := 0; i < rows; i++ {
								builder.Append(strings.Repeat("abcdef01", 8))
							}
							cols[col] = builder.NewArray()
							builder.Release()
						} else {
							builder := array.NewInt64Builder(memory.DefaultAllocator)
							for i := 0; i < rows; i++ {
								builder.Append(int64(i + col))
							}
							cols[col] = builder.NewArray()
							builder.Release()
						}
						defer cols[col].Release()
						names[col] = fmt.Sprintf("c%d", col)
						types[col] = cols[col].DataType()
						fields[col] = arrow.Field{Name: names[col], Type: types[col]}
					}
					batch := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, int64(rows))
					defer batch.Release()
					const batchCount = 32
					batches := make([]arrow.RecordBatch, batchCount)
					for i := range batches {
						batches[i] = batch
					}
					provider := benchmarkProvider{batches}
					query := "SELECT * FROM callback_bench"
					if mode == "provider" {
						err = s.RegisterTableProvider(ctx, "callback_bench", provider)
					} else {
						reader, readErr := provider.Scan(ctx)
						if readErr != nil {
							b.Fatal(readErr)
						}
						err = s.RegisterArrowReader(ctx, "callback_bench", reader)
						reader.Release()
						if err == nil {
							err = s.RegisterScalarFunction(ctx, ScalarFunction{
								Name: "callback_identity", Arguments: types, ReturnType: types[0],
								Evaluate: func(_ context.Context, args []arrow.Array, _ int) (arrow.Array, error) {
									args[0].Retain()
									return args[0], nil
								},
							})
						}
						query = "SELECT callback_identity(" + strings.Join(names, ",") + ") FROM callback_bench"
					}
					if err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						reader, err := s.QueryArrowContext(ctx, query)
						if err != nil {
							b.Fatal(err)
						}
						var count int64
						for {
							record, err := reader.Read()
							if err == io.EOF {
								break
							}
							if err != nil {
								_ = reader.Close()
								b.Fatal(err)
							}
							count += record.NumRows()
							record.Release()
						}
						closeNoError(b, reader)
						if count != int64(rows*batchCount) {
							b.Fatalf("got %d rows", count)
						}
					}
				})
			}
		}
	}
}

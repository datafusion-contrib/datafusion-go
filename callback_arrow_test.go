//go:build cgo

package datafusion

import (
	"context"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// These cases exercise buffer layouts which a primitive-only bridge can
// silently corrupt: sliced validity/offset buffers, children and dictionaries.
func TestGoCallbackArrowLayoutsAndLifetimes(t *testing.T) {
	meta := arrow.NewMetadata([]string{"source"}, []string{"callback-test"})
	for _, tc := range []struct {
		name string
		dt   arrow.DataType
		json string
	}{
		{"int64", arrow.PrimitiveTypes.Int64, `[{"value":0},{"value":1},{"value":null},{"value":3}]`},
		{"bool", arrow.FixedWidthTypes.Boolean, `[{"value":false},{"value":true},{"value":null},{"value":false}]`},
		{"utf8_view", arrow.BinaryTypes.StringView, `[{"value":"unused"},{"value":"a longer value requiring a backing buffer"},{"value":null},{"value":"世界"}]`},
		{"utf8", arrow.BinaryTypes.String, `[{"value":"unused"},{"value":"hello"},{"value":null},{"value":"世界"}]`},
		{"list", arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: meta}), `[{"value":[0]},{"value":[1,null,3]},{"value":null},{"value":[]}]`},
		{"struct", arrow.StructOf(arrow.Field{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: meta}), `[{"value":{"text":"unused"}},{"value":{"text":"hello"}},{"value":null},{"value":{"text":null}}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: tc.dt, Nullable: true, Metadata: meta}}, &meta)
			record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(tc.json))
			if err != nil {
				t.Fatal(err)
			}
			defer record.Release()
			slice := record.NewSlice(1, 4)
			defer slice.Release()
			checkCallbackRoundTrip(t, slice)
		})
	}
	t.Run("dictionary", func(t *testing.T) {
		indices := array.NewInt8Builder(memory.DefaultAllocator)
		indices.AppendValues([]int8{0, 1, 0, 1}, []bool{true, true, false, true})
		idx := indices.NewArray()
		indices.Release()
		defer idx.Release()
		values := array.NewStringBuilder(memory.DefaultAllocator)
		values.AppendValues([]string{"first", "second"}, nil)
		vals := values.NewArray()
		values.Release()
		defer vals.Release()
		dict := array.NewDictionaryArray(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}, idx, vals)
		defer dict.Release()
		slice := array.NewSlice(dict, 1, 4)
		defer slice.Release()
		record := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "value", Type: dict.DataType(), Nullable: true}}, nil), []arrow.Array{slice}, 3)
		defer record.Release()
		checkCallbackRoundTrip(t, record)
	})
}

func checkCallbackRoundTrip(t *testing.T, input arrow.RecordBatch) {
	t.Helper()
	ctx := context.Background()
	s := sessionForTest(t)
	if err := s.RegisterTableProvider(ctx, "foreign_arrays", benchmarkProvider{[]arrow.RecordBatch{input}}); err != nil {
		t.Fatal(err)
	}
	var retained arrow.Array
	defer func() {
		if retained != nil {
			retained.Release()
		}
	}()
	if err := s.RegisterScalarFunction(ctx, ScalarFunction{
		Name: "retain_identity", Arguments: []arrow.DataType{input.Column(0).DataType()}, ReturnType: input.Column(0).DataType(),
		Evaluate: func(_ context.Context, args []arrow.Array, _ int) (arrow.Array, error) {
			args[0].Retain()
			retained = args[0] // also keep the imported native argument beyond Evaluate
			args[0].Retain()
			return args[0], nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"SELECT value FROM foreign_arrays", "SELECT retain_identity(value) FROM foreign_arrays"} {
		r, err := s.QueryArrowContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := r.Read()
		if err != nil {
			closeNoError(t, r)
			t.Fatal(err)
		}
		_, err = r.Read()
		if err != io.EOF {
			batch.Release()
			closeNoError(t, r)
			t.Fatalf("expected EOF: %v", err)
		}
		closeNoError(t, r)
		runtime.GC()
		if !array.Equal(input.Column(0), batch.Column(0)) {
			t.Errorf("%s: got %s, want %s", query, batch.Column(0), input.Column(0))
		}
		batch.Release()
	}
	closeNoError(t, s)
	runtime.GC()
	if retained == nil || !array.Equal(input.Column(0), retained) {
		t.Fatal("retained UDF argument lost data after session close")
	}
}

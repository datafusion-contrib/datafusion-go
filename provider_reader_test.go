package datafusion

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/endian"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Track input consumption and ownership independently from wrapper ownership.
type trackedProviderReader struct {
	array.RecordReader
	nexts, releases int
	failure         error
}

func (r *trackedProviderReader) Next() bool { r.nexts++; return r.RecordReader.Next() }
func (r *trackedProviderReader) Release()   { r.releases++; r.RecordReader.Release() }
func (r *trackedProviderReader) Err() error { return r.failure }

func utilityFixture(t *testing.T) *trackedProviderReader {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	metadata := arrow.NewMetadata([]string{"source"}, []string{"fixture"})
	schema := arrow.NewSchemaWithEndian([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Metadata: metadata},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, &metadata, endian.BigEndian)
	ids := array.NewInt64Builder(mem)
	defer ids.Release()
	ids.AppendValues([]int64{10, 20, 30}, nil)
	names := array.NewStringBuilder(mem)
	defer names.Release()
	names.AppendValues([]string{"a", "bb", "ccc"}, nil)
	a, b := ids.NewArray(), names.NewArray()
	defer a.Release()
	defer b.Release()
	record := array.NewRecordBatchWithMetadata(schema, []arrow.Array{a, b}, 3, metadata)
	defer record.Release()
	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{record, record})
	if err != nil {
		t.Fatal(err)
	}
	return &trackedProviderReader{RecordReader: reader}
}

func TestProjectReaderOwnershipAndMetadata(t *testing.T) {
	inner := utilityFixture(t)
	indices := []int{1, 0, 1}
	reader, err := ProjectReader(inner, indices)
	if err != nil {
		t.Fatal(err)
	}
	indices[0] = 0
	if !reader.Next() {
		t.Fatal(reader.Err())
	}
	record := reader.RecordBatch()
	if record.Column(0).(*array.String).Value(2) != "ccc" || record.Column(1).(*array.Int64).Value(1) != 20 || record.Column(2) != record.Column(0) {
		t.Fatal("incorrect projection")
	}
	if record.Column(0) != inner.RecordBatch().Column(1) {
		t.Fatal("projection copied arrays")
	}
	schema := reader.Schema()
	if schema.Endianness() != endian.BigEndian || !schema.Metadata().Equal(inner.Schema().Metadata()) || !schema.Field(1).Metadata.Equal(inner.Schema().Field(0).Metadata) {
		t.Fatal("schema metadata lost")
	}
	if !record.(arrow.RecordBatchWithMetadata).Metadata().Equal(inner.RecordBatch().(arrow.RecordBatchWithMetadata).Metadata()) {
		t.Fatal("batch metadata lost")
	}
	reader.Retain()
	reader.Release()
	if inner.releases != 0 || reader.RecordBatch() != record {
		t.Fatal("nonfinal release freed reader")
	}
	record.Retain()
	if !reader.Next() {
		t.Fatal(reader.Err())
	}
	reader.Release()
	if inner.releases != 1 || record.Column(0).(*array.String).Value(2) != "ccc" {
		t.Fatal("retained batch invalid")
	}
	record.Release()
}

func TestProviderReaderProjectionAndLimits(t *testing.T) {
	for _, projection := range []struct {
		name    string
		indices []int
		cols    int
	}{{"all", nil, 2}, {"empty", []int{}, 0}} {
		for _, limit := range []int64{-1, 0, 2, 4, 6, 20} {
			t.Run(fmt.Sprintf("%s/%d", projection.name, limit), func(t *testing.T) {
				inner := utilityFixture(t)
				projected, err := ProjectReader(inner, projection.indices)
				if err != nil {
					t.Fatal(err)
				}
				reader, err := LimitReader(projected, limit)
				if err != nil {
					projected.Release()
					t.Fatal(err)
				}
				defer reader.Release()
				var rows int64
				for reader.Next() {
					r := reader.RecordBatch()
					rows += r.NumRows()
					if r.NumCols() != int64(projection.cols) {
						t.Fatal("wrong column count")
					}
					if !r.(arrow.RecordBatchWithMetadata).Metadata().Equal(inner.Schema().Metadata()) {
						t.Fatal("slice metadata lost")
					}
				}
				if reader.Err() != nil {
					t.Fatal(reader.Err())
				}
				want := int64(6)
				if limit >= 0 && limit < want {
					want = limit
				}
				if rows != want {
					t.Fatalf("got %d rows, want %d", rows, want)
				}
				calls := inner.nexts
				if reader.Next() || inner.nexts != calls {
					t.Fatal("read after EOF")
				}
				if limit == 0 && calls != 0 {
					t.Fatal("zero limit consumed input")
				}
				if limit == 2 && calls != 1 || limit == 4 && calls != 2 {
					t.Fatal("read past limit")
				}
			})
		}
	}
}

func TestProviderReaderValidationAndErrors(t *testing.T) {
	inner := utilityFixture(t)
	defer inner.Release()
	for _, indices := range [][]int{{-1}, {2}} {
		if _, err := ProjectReader(inner, indices); err == nil {
			t.Fatal("accepted invalid index")
		}
	}
	if _, err := LimitReader(inner, -2); err == nil {
		t.Fatal("accepted invalid limit")
	}
	if inner.releases != 0 {
		t.Fatal("validation took ownership")
	}
	var typedNil *trackedProviderReader
	for _, reader := range []array.RecordReader{nil, typedNil} {
		if _, err := ProjectReader(reader, nil); err == nil {
			t.Fatal("accepted nil")
		}
		if _, err := LimitReader(reader, 0); err == nil {
			t.Fatal("accepted nil")
		}
	}
	inner.failure = errors.New("read failed")
	inner.Retain()
	reader, err := LimitReader(inner, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	for reader.Next() {
	}
	if !errors.Is(reader.Err(), inner.failure) {
		t.Fatal(reader.Err())
	}
}

type malformedProviderReader struct {
	array.RecordReader
	schema *arrow.Schema
	record arrow.RecordBatch
}

func (r *malformedProviderReader) Schema() *arrow.Schema          { return r.schema }
func (r *malformedProviderReader) RecordBatch() arrow.RecordBatch { return r.record }

func TestProviderReaderMalformedInput(t *testing.T) {
	inner := utilityFixture(t)
	defer inner.Release()
	malformed := &malformedProviderReader{RecordReader: inner}
	if _, err := ProjectReader(malformed, nil); err == nil {
		t.Fatal("accepted nil schema")
	}
	malformed.schema = inner.Schema()
	inner.Retain()
	reader, err := LimitReader(malformed, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if reader.Next() || reader.Err() == nil {
		t.Fatal("accepted nil batch")
	}
}

func TestProviderReaderChangedSchemaAndRetainedSlice(t *testing.T) {
	t.Run("changed schema", func(t *testing.T) {
		inner := utilityFixture(t)
		wrong := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 1)
		defer wrong.Release()
		malformed := &malformedProviderReader{RecordReader: inner, schema: inner.Schema(), record: wrong}
		reader, err := ProjectReader(malformed, nil)
		if err != nil {
			inner.Release()
			t.Fatal(err)
		}
		defer reader.Release()
		if reader.Next() || reader.Err() == nil {
			t.Fatal("accepted changed batch schema")
		}
	})
	t.Run("retained slice", func(t *testing.T) {
		inner := utilityFixture(t)
		reader, err := LimitReader(inner, 2)
		if err != nil {
			inner.Release()
			t.Fatal(err)
		}
		if !reader.Next() {
			t.Fatal(reader.Err())
		}
		batch := reader.RecordBatch()
		batch.Retain()
		defer batch.Release()
		reader.Release()
		if batch.NumRows() != 2 || batch.Column(1).(*array.String).Value(1) != "bb" {
			t.Fatal("retained slice invalid")
		}
	})
}

package datafusion

import (
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ProjectReader returns a reader selecting columns in the requested order,
// without copying their buffers. Nil selects all columns; an empty non-nil slice
// selects zero columns while preserving row counts. Duplicates are allowed.
// Schema, field and batch metadata are preserved. The indices are copied.
//
// On success ownership of reader transfers to the result. On error ownership
// stays with the caller. Retain/Release are concurrency-safe; other reader methods
// must not run concurrently. Retain a batch to keep it after Next or Release.
func ProjectReader(reader array.RecordReader, indices []int) (array.RecordReader, error) {
	schema, err := providerReaderSchema(reader)
	if err != nil {
		return nil, err
	}
	if indices == nil {
		return newProviderReader(reader, schema, schema, nil, -1), nil
	}
	projection := append([]int{}, indices...)
	fields := make([]arrow.Field, len(projection))
	for i, index := range projection {
		if index < 0 || index >= schema.NumFields() {
			return nil, fmt.Errorf("datafusion projection index %d outside schema with %d fields", index, schema.NumFields())
		}
		fields[i] = schema.Field(index)
	}
	metadata := schema.Metadata()
	return newProviderReader(reader, schema, arrow.NewSchemaWithEndian(fields, &metadata, schema.Endianness()), projection, -1), nil
}

// LimitReader returns at most limit rows, slicing the last batch without copying
// buffers. A limit of -1 is unbounded; zero does not read the input. Other negative
// values are invalid. This helper is suitable for ScanOptions.Limit after any
// source-side filtering; it does not evaluate ScanOptions.Filters.
// Ownership and concurrency follow ProjectReader. Release always releases the
// owned input, including when the limit is reached before input EOF.
func LimitReader(reader array.RecordReader, limit int64) (array.RecordReader, error) {
	schema, err := providerReaderSchema(reader)
	if err != nil {
		return nil, err
	}
	if limit < -1 {
		return nil, errors.New("datafusion reader limit must be -1 or nonnegative")
	}
	return newProviderReader(reader, schema, schema, nil, limit), nil
}

func providerReaderSchema(reader array.RecordReader) (*arrow.Schema, error) {
	if reader == nil || (reflect.ValueOf(reader).Kind() == reflect.Pointer && reflect.ValueOf(reader).IsNil()) {
		return nil, errors.New("datafusion reader is nil")
	}
	schema := reader.Schema()
	if schema == nil {
		return nil, errors.New("datafusion reader schema is nil")
	}
	return schema, nil
}

type providerReader struct {
	refs                atomic.Int64
	inner               array.RecordReader
	inputSchema, schema *arrow.Schema
	projection          []int
	remaining           int64
	current             arrow.RecordBatch
	err                 error
	done                bool
}

func newProviderReader(inner array.RecordReader, inputSchema, schema *arrow.Schema, projection []int, limit int64) *providerReader {
	r := &providerReader{inner: inner, inputSchema: inputSchema, schema: schema, projection: projection, remaining: limit}
	r.refs.Store(1)
	return r
}
func (r *providerReader) Schema() *arrow.Schema          { return r.schema }
func (r *providerReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *providerReader) Record() arrow.RecordBatch      { return r.current }
func (r *providerReader) Err() error                     { return r.err }
func (r *providerReader) Retain()                        { r.refs.Add(1) }
func (r *providerReader) Release() {
	if r.refs.Add(-1) == 0 {
		if r.current != nil {
			r.current.Release()
			r.current = nil
		}
		r.inner.Release()
	}
}
func (r *providerReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.done || r.remaining == 0 {
		return false
	}
	if !r.inner.Next() {
		r.done = true
		r.err = r.inner.Err()
		return false
	}
	record := r.inner.RecordBatch()
	if record == nil || !r.inputSchema.Equal(record.Schema()) {
		r.done = true
		r.err = errors.New("datafusion input reader returned a nil batch or changed schema")
		return false
	}
	if r.projection != nil {
		cols := make([]arrow.Array, len(r.projection))
		for i, index := range r.projection {
			cols[i] = record.Column(index)
		}
		var metadata arrow.Metadata
		if withMetadata, ok := record.(arrow.RecordBatchWithMetadata); ok {
			metadata = withMetadata.Metadata()
		}
		r.current = array.NewRecordBatchWithMetadata(r.schema, cols, record.NumRows(), metadata)
	} else {
		record.Retain()
		r.current = record
	}
	if r.remaining >= 0 {
		if r.current.NumRows() > r.remaining {
			limited := r.current.NewSlice(0, r.remaining)
			r.current.Release()
			r.current = limited
		}
		r.remaining -= r.current.NumRows()
	}
	return true
}

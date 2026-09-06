//go:build cgo

package native

import (
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// panicSafeRecordReader catches producer panics before Arrow exports a batch
// or error. Next fetches both inside its recovery boundary.
//
// Registration borrows inner synchronously; the caller owns its reference.
// Retain/Release are no-ops to keep producer cleanup out of Rust callbacks.
// Exported batches retain their own buffers.
type panicSafeRecordReader struct {
	inner  array.RecordReader
	schema *arrow.Schema
	record arrow.RecordBatch
	err    error
	done   bool
}

func newPanicSafeRecordReader(inner array.RecordReader) (r *panicSafeRecordReader, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("datafusion-go arrow reader Schema panicked: %v", p)
		}
	}()
	schema := inner.Schema()
	if schema == nil {
		return nil, errors.New("datafusion-go arrow reader schema is nil")
	}
	return &panicSafeRecordReader{inner: inner, schema: schema}, nil
}

func (r *panicSafeRecordReader) Next() (ok bool) {
	r.record = nil
	if r.done {
		return false
	}
	defer func() {
		if p := recover(); p != nil {
			r.err = fmt.Errorf("datafusion-go arrow reader callback panicked: %v", p)
			r.done = true
		}
	}()
	if !r.inner.Next() {
		r.done = true
		if err := r.inner.Err(); err != nil {
			r.err = errors.New(err.Error())
		}
		return false
	}
	r.record = r.inner.RecordBatch()
	if r.record == nil {
		r.err = errors.New("datafusion-go arrow reader returned a nil batch after Next")
		r.done = true
		return false
	}
	return true
}

func (r *panicSafeRecordReader) RecordBatch() arrow.RecordBatch { return r.record }
func (r *panicSafeRecordReader) Record() arrow.RecordBatch      { return r.record }
func (r *panicSafeRecordReader) Schema() *arrow.Schema          { return r.schema }
func (r *panicSafeRecordReader) Err() error                     { return r.err }
func (r *panicSafeRecordReader) Retain()                        {}
func (r *panicSafeRecordReader) Release()                       {}

var _ array.RecordReader = (*panicSafeRecordReader)(nil)

//go:build cgo

package native

/*
#include "datafusion_go.h"
*/
import "C"

import (
	"context"
	"errors"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
)

var callbackAllocator memory.Allocator = mallocator.NewMallocator()

// Concatenate normalizes sliced offsets and copies these layouts into the
// supplied allocator. Dictionary and binary-view concatenation can retain source
// buffers, so those types (including nested occurrences) keep the IPC path.
func canCopyCallbackType(dt arrow.DataType) bool {
	switch dt.(type) {
	case *arrow.DictionaryType, arrow.ExtensionType, arrow.BinaryViewDataType:
		return false
	case arrow.FixedWidthDataType, arrow.BinaryDataType, *arrow.NullType, *arrow.BooleanType:
		return true
	case *arrow.ListType, *arrow.LargeListType, *arrow.FixedSizeListType, *arrow.StructType, *arrow.MapType:
		for _, field := range dt.(arrow.NestedType).Fields() {
			if !canCopyCallbackType(field.Type) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func exportCallbackBatch(batch arrow.RecordBatch, out *C.struct_dfgo_arrow_exchange) ([]byte, error) {
	// Small multi-column batches spend more on individual C allocations than
	// serialization. Keep the combined IPC allocation for that measured case.
	smallColumns := batch.NumCols() > 1
	for _, col := range batch.Columns() {
		if !canCopyCallbackType(col.DataType()) {
			return encodeIPC(batch.Schema(), batch)
		}
		smallColumns = smallColumns && col.Data().SizeInBytes() <= 2048
	}
	if smallColumns {
		return encodeIPC(batch.Schema(), batch)
	}
	cols := make([]arrow.Array, batch.NumCols())
	defer func() {
		for _, col := range cols {
			if col != nil {
				col.Release()
			}
		}
	}()
	for i, col := range batch.Columns() {
		copied, err := array.Concatenate([]arrow.Array{col}, callbackAllocator)
		if err != nil {
			return nil, err
		}
		cols[i] = copied
	}
	copied := array.NewRecordBatch(batch.Schema(), cols, batch.NumRows())
	defer copied.Release()
	cdata.ExportArrowRecordBatch(copied,
		(*cdata.CArrowArray)(unsafe.Pointer(&out.output)),
		(*cdata.CArrowSchema)(unsafe.Pointer(&out.output_schema)))
	return nil, nil
}

// The exchange belongs to the blocking Rust closure. Rust drops all remaining
// C Data owners on success, error, panic or an abandoned query future.
func dispatchArrowCallback(obj any, ctx context.Context, opcode int, data []byte) ([]byte, error) {
	if len(data) != int(C.sizeof_struct_dfgo_arrow_exchange) {
		return nil, errors.New("invalid Arrow callback exchange")
	}
	out := (*C.struct_dfgo_arrow_exchange)(unsafe.Pointer(&data[0]))
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opcode == 12 {
		r := obj.(*callbackReader).reader
		if !r.Next() {
			return nil, r.Err()
		}
		batch := r.RecordBatch()
		if batch == nil {
			return nil, errors.New("go reader returned nil batch")
		}
		return exportCallbackBatch(batch, out)
	}
	f := obj.(ScalarFunction)
	rec, err := cdata.ImportCRecordBatch(
		(*cdata.CArrowArray)(unsafe.Pointer(&out.input)),
		(*cdata.CArrowSchema)(unsafe.Pointer(&out.input_schema)))
	if err != nil {
		return nil, err
	}
	defer rec.Release()
	result, err := f.Evaluate(ctx, rec.Columns(), int(rec.NumRows()))
	if result != nil {
		defer result.Release()
	}
	if err != nil {
		return nil, err
	}
	if result == nil || result.Len() != int(rec.NumRows()) || !arrow.TypeEqual(result.DataType(), f.ReturnType) {
		return nil, errors.New("go UDF returned wrong length or type")
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: f.ReturnType, Nullable: true}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{result}, rec.NumRows())
	defer batch.Release()
	return exportCallbackBatch(batch, out)
}

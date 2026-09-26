//go:build cgo

package native

/*
#include "datafusion_go.h"
#include <stdlib.h>
*/
import "C"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/cgo"
	"strconv"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type callbackContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}
type callbackReader struct{ reader array.RecordReader }

func encodeIPC(schema *arrow.Schema, batch arrow.RecordBatch) ([]byte, error) {
	var out bytes.Buffer
	w := ipc.NewWriter(&out, ipc.WithSchema(schema))
	if batch != nil {
		if err := w.Write(batch); err != nil {
			_ = w.Close()
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

//export dfgoGoInvoke
func dfgoGoInvoke(handle C.uint64_t, operation C.uint64_t, opcode C.int32_t, input *C.uint8_t, inputLen C.int64_t, output **C.uint8_t, outputLen *C.int64_t, outputHandle *C.uint64_t) (status C.int) {
	// Only C-owned bytes or Arrow buffers cross a callback return. All panics (including
	// reader methods and user cleanup) stop here, before unwinding across Rust.
	defer func() {
		if p := recover(); p != nil {
			status = callbackError(output, outputLen, fmt.Errorf("go callback panicked: %v", p))
		}
	}()
	if inputLen < 0 || uint64(inputLen) > uint64(^uint(0)>>1) {
		return callbackError(output, outputLen, errors.New("invalid callback input length"))
	}
	var data []byte
	if inputLen > 0 {
		data = unsafe.Slice((*byte)(unsafe.Pointer(input)), int(inputLen))
	}
	if opcode == 0 {
		defer cgo.Handle(handle).Delete()
	}
	obj := cgo.Handle(handle).Value()
	ctx := context.Background()
	if operation != 0 {
		ctx = cgo.Handle(operation).Value().(*callbackContext).ctx
	}
	result, child, err := dispatchCallback(obj, ctx, int(opcode), data)
	if err != nil {
		return callbackError(output, outputLen, err)
	}
	if len(result) > 0 {
		*output = (*C.uint8_t)(C.CBytes(result))
		*outputLen = C.int64_t(len(result))
	}
	if child != nil {
		*outputHandle = C.uint64_t(cgo.NewHandle(child))
	}
	return 0
}
func callbackError(output **C.uint8_t, outputLen *C.int64_t, err error) C.int {
	data := []byte(err.Error())
	*output = (*C.uint8_t)(C.CBytes(data))
	*outputLen = C.int64_t(len(data))
	return 1
}

func dispatchCallback(obj any, ctx context.Context, opcode int, data []byte) ([]byte, any, error) {
	switch opcode {
	case 0:
		switch v := obj.(type) {
		case *callbackContext:
			v.cancel()
		case *callbackReader:
			v.reader.Release()
		}
		return nil, nil, nil
	case 1:
		child, cancel := context.WithCancel(ctx)
		return nil, &callbackContext{child, cancel}, nil
	case 2:
		obj.(*callbackContext).cancel()
		return nil, nil, nil
	case 3: // provider schema and capabilities
		p := obj.(TableProvider)
		schema := p.Schema()
		if schema == nil {
			return nil, nil, errors.New("go provider returned nil schema")
		}
		b, err := encodeIPC(schema, nil)
		return b, nil, err
	case 4: // open scan
		var req struct {
			Projection []int           `json:"projection"`
			Filters    json.RawMessage `json:"filters"`
			Limit      int64           `json:"limit"`
		}
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, nil, err
		}
		p := obj.(TableProvider)
		var reader array.RecordReader
		var err error
		if pd, ok := p.(PushdownTableProvider); ok {
			var filters []Expr
			if len(req.Filters) > 0 && string(req.Filters) != "null" {
				filters, err = decodeFilters(req.Filters)
				if err != nil {
					return nil, nil, err
				}
			}
			reader, err = pd.ScanWithOptions(ctx, ScanOptions{req.Projection, filters, req.Limit})
		} else {
			reader, err = p.Scan(ctx)
		}

		transferred := false
		if reader != nil {
			defer func() {
				if !transferred {
					reader.Release()
				}
			}()
		}
		if err != nil {
			return nil, nil, err
		}
		if reader == nil {
			return nil, nil, errors.New("go provider returned nil reader")
		}
		schema := reader.Schema()
		if schema == nil {
			return nil, nil, errors.New("go provider reader has nil schema")
		}
		b, err := encodeIPC(schema, nil)
		if err != nil {
			return nil, nil, err
		}
		transferred = true
		return b, &callbackReader{reader}, nil

	case 5: // next batch
		r := obj.(*callbackReader).reader
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if !r.Next() {
			return nil, nil, r.Err()
		}
		rec := r.RecordBatch()
		if rec == nil {
			return nil, nil, errors.New("go reader returned nil batch")
		}
		b, err := encodeIPC(r.Schema(), rec)
		return b, nil, err
	case 6: // UDF signature, return field last
		f := obj.(ScalarFunction)
		fields := make([]arrow.Field, 0, len(f.Arguments)+1)
		for i, dt := range f.Arguments {
			fields = append(fields, arrow.Field{Name: fmt.Sprintf("arg%d", i), Type: dt, Nullable: true})
		}
		fields = append(fields, arrow.Field{Name: "result", Type: f.ReturnType, Nullable: true})
		b, err := encodeIPC(arrow.NewSchema(fields, nil), nil)
		return b, nil, err
	case 8:
		var ref struct {
			Schema string `json:"schema"`
			Table  string `json:"table"`
		}
		if err := json.Unmarshal(data, &ref); err != nil {
			return nil, nil, err
		}
		provider, err := obj.(CatalogProvider).ResolveTable(ctx, ref.Schema, ref.Table)
		if err != nil || provider == nil {
			return nil, nil, err
		}
		capability := 0
		if _, ok := provider.(PushdownTableProvider); ok {
			capability |= 1
		}
		if _, ok := provider.(WritableTableProvider); ok {
			capability |= 2
		}
		return []byte(strconv.Itoa(capability)), provider, nil
	case 9, 10, 11:
		if len(data) != int(C.sizeof_struct_ArrowArrayStream) {
			return nil, nil, errors.New("invalid insert stream")
		}
		// The opcode contract lends a mutable native stream struct. Import moves
		// its callbacks; retained batches own native buffers independently.
		imported, err := cdata.ImportCRecordReader((*cdata.CArrowArrayStream)(unsafe.Pointer(&data[0])), nil)
		if err != nil {
			return nil, nil, err
		}
		reader := imported.(array.RecordReader)
		defer reader.Release()
		written, err := obj.(WritableTableProvider).InsertInto(ctx, InsertOp(opcode-9), reader)
		if err == nil {
			err = reader.Err()
		}
		return []byte(strconv.FormatUint(written, 10)), nil, err
	case 7:
		f := obj.(ScalarFunction)
		r, err := ipc.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, nil, err
		}
		defer r.Release()
		if !r.Next() {
			return nil, nil, errors.New("missing UDF argument batch")
		}
		rec := r.RecordBatch()
		result, err := f.Evaluate(ctx, rec.Columns(), int(rec.NumRows()))
		if result != nil {
			defer result.Release()
		}
		if err != nil {
			return nil, nil, err
		}
		if result == nil || result.Len() != int(rec.NumRows()) || !arrow.TypeEqual(result.DataType(), f.ReturnType) {
			return nil, nil, errors.New("go UDF returned wrong length or type")
		}
		schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: f.ReturnType, Nullable: true}}, nil)
		batch := array.NewRecordBatch(schema, []arrow.Array{result}, rec.NumRows())
		defer batch.Release()
		b, err := encodeIPC(schema, batch)
		return b, nil, err
	case 12, 13:
		b, err := dispatchArrowCallback(obj, ctx, opcode, data)
		return b, nil, err
	}
	return nil, nil, fmt.Errorf("unsupported Go callback operation %d", opcode)
}

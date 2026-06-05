package native

/*
#cgo CFLAGS: -I${SRCDIR}/../../rust/include
#include "datafusion_go.h"
#include <stdlib.h>

static struct ArrowArrayStream *dfgo_arrow_stream_alloc(void) {
	return (struct ArrowArrayStream *)calloc(1, sizeof(struct ArrowArrayStream));
}

static struct ArrowArray *dfgo_arrow_array_alloc(void) {
	return (struct ArrowArray *)calloc(1, sizeof(struct ArrowArray));
}

static int dfgo_arrow_stream_get_schema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	if (stream == NULL || stream->get_schema == NULL) {
		return -1;
	}
	return stream->get_schema(stream, out);
}

static int dfgo_arrow_stream_get_next(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	if (stream == NULL || stream->get_next == NULL) {
		return -1;
	}
	return stream->get_next(stream, out);
}

static const char *dfgo_arrow_stream_get_last_error(struct ArrowArrayStream *stream) {
	if (stream == NULL || stream->get_last_error == NULL) {
		return "arrow stream is closed";
	}
	return stream->get_last_error(stream);
}

static void dfgo_arrow_stream_release(struct ArrowArrayStream *stream) {
	if (stream != NULL && stream->release != NULL) {
		stream->release(stream);
	}
}

static int dfgo_arrow_array_is_released(struct ArrowArray *array) {
	return array == NULL || array->release == NULL;
}

static void dfgo_arrow_array_release(struct ArrowArray *array) {
	if (array != NULL && array->release != NULL) {
		array->release(array);
	}
}

static void dfgo_arrow_schema_release(struct ArrowSchema *schema) {
	if (schema != NULL && schema->release != NULL) {
		schema->release(schema);
	}
}
*/
import "C"

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/arrio"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

const stateOK = 0

type Error struct {
	Kind    string
	Message string
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.Message
}

func (e *Error) Is(target error) bool {
	return e != nil && e.Kind == "cancelled" && target == context.Canceled
}

func (e *Error) NativeErrorKind() string {
	if e == nil {
		return ""
	}
	return e.Kind
}

type Database struct {
	ptr *C.dfgo_database
}

type Connection struct {
	ptr *C.dfgo_connection
}

type Statement struct {
	ptr *C.dfgo_statement
}

type cancelToken struct {
	mu  sync.Mutex
	ptr *C.dfgo_cancel_token
}

type resultReader struct {
	mu     sync.Mutex
	ctx    context.Context
	stream *C.struct_ArrowArrayStream
	array  *C.struct_ArrowArray
	schema *arrow.Schema
	result *C.dfgo_result_stream
	token  *cancelToken
	done   chan struct{}
	closed bool
}

func OpenDatabase(dsn string) (*Database, error) {
	if err := checkNativeVersion(); err != nil {
		return nil, err
	}

	cdsn := C.CString(dsn)
	defer C.free(unsafe.Pointer(cdsn))

	var db *C.dfgo_database
	var cerr *C.dfgo_error
	if C.dfgo_database_open(cdsn, &db, &cerr) != stateOK {
		return nil, takeError(cerr)
	}
	if db == nil {
		return nil, errors.New("datafusion-go native open returned nil database")
	}

	return &Database{ptr: db}, nil
}

func checkNativeVersion() error {
	if got := int(C.dfgo_abi_version()); got != abiVersion {
		return fmt.Errorf("datafusion-go native ABI version mismatch: got %d, want %d", got, abiVersion)
	}
	version := C.dfgo_datafusion_version()
	if version == nil {
		return errors.New("datafusion-go native DataFusion version is null")
	}
	if got := C.GoString(version); got != dataFusionVersion {
		return fmt.Errorf("datafusion-go native DataFusion version mismatch: got %s, want %s", got, dataFusionVersion)
	}
	return nil
}

func (db *Database) Close() {
	if db == nil || db.ptr == nil {
		return
	}
	C.dfgo_database_close(db.ptr)
	db.ptr = nil
}

func (db *Database) Connect(shared bool) (*Connection, error) {
	if db == nil || db.ptr == nil {
		return nil, errors.New("datafusion-go database is closed")
	}

	var conn *C.dfgo_connection
	var cerr *C.dfgo_error
	if shared {
		if C.dfgo_connection_open_shared(db.ptr, &conn, &cerr) != stateOK {
			return nil, takeError(cerr)
		}
	} else {
		if C.dfgo_connection_open_isolated(db.ptr, &conn, &cerr) != stateOK {
			return nil, takeError(cerr)
		}
	}
	if conn == nil {
		return nil, errors.New("datafusion-go native connect returned nil connection")
	}

	return &Connection{ptr: conn}, nil
}

func (conn *Connection) Close() {
	if conn == nil || conn.ptr == nil {
		return
	}
	C.dfgo_connection_close(conn.ptr)
	conn.ptr = nil
}

func (conn *Connection) RegisterArrowIPC(name string, data []byte) error {
	if conn == nil || conn.ptr == nil {
		return errors.New("datafusion-go connection is closed")
	}

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	var ptr *C.uint8_t
	if len(data) != 0 {
		ptr = (*C.uint8_t)(unsafe.Pointer(&data[0]))
	}

	var cerr *C.dfgo_error
	if C.dfgo_connection_register_arrow_ipc(conn.ptr, cname, ptr, C.int64_t(len(data)), &cerr) != stateOK {
		return takeError(cerr)
	}
	return nil
}

func (conn *Connection) RegisterArrowReaderZeroCopy(name string, reader array.RecordReader) error {
	if conn == nil || conn.ptr == nil {
		return errors.New("datafusion-go connection is closed")
	}
	if reader == nil {
		return errors.New("datafusion-go arrow reader is nil")
	}

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	stream := C.dfgo_arrow_stream_alloc()
	if stream == nil {
		return errors.New("datafusion-go could not allocate Arrow stream")
	}
	cdata.ExportRecordReader(reader, (*cdata.CArrowArrayStream)(unsafe.Pointer(stream)))

	var cerr *C.dfgo_error
	errno := C.dfgo_connection_register_arrow_stream(conn.ptr, cname, stream, &cerr)
	// Rust moves the stream callbacks out of this allocation and owns their
	// release path. The allocation itself still belongs to this cgo wrapper.
	C.free(unsafe.Pointer(stream))
	if errno != stateOK {
		return takeError(cerr)
	}
	return nil
}

func (conn *Connection) Prepare(query string) (*Statement, error) {
	if conn == nil || conn.ptr == nil {
		return nil, errors.New("datafusion-go connection is closed")
	}

	cquery := C.CString(query)
	defer C.free(unsafe.Pointer(cquery))

	var stmt *C.dfgo_statement
	var cerr *C.dfgo_error
	if C.dfgo_prepare(conn.ptr, cquery, &stmt, &cerr) != stateOK {
		return nil, takeError(cerr)
	}
	if stmt == nil {
		return nil, errors.New("datafusion-go native prepare returned nil statement")
	}

	return &Statement{ptr: stmt}, nil
}

func (stmt *Statement) Close() {
	if stmt == nil || stmt.ptr == nil {
		return
	}
	C.dfgo_statement_close(stmt.ptr)
	stmt.ptr = nil
}

func (stmt *Statement) NumInput() int {
	if stmt == nil || stmt.ptr == nil {
		return -1
	}
	return int(C.dfgo_statement_num_params(stmt.ptr))
}

func (stmt *Statement) Serializes() bool {
	if stmt == nil || stmt.ptr == nil {
		return false
	}
	return C.dfgo_statement_serializes(stmt.ptr) != 0
}

func (stmt *Statement) ExecuteArrow(ctx context.Context, args []driver.NamedValue) (arrio.Reader, error) {
	if stmt == nil || stmt.ptr == nil {
		return nil, errors.New("datafusion-go statement is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := stmt.checkArgCount(args); err != nil {
		return nil, err
	}

	if err := stmt.bindArgs(args); err != nil {
		return nil, err
	}

	token, err := newCancelToken()
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			token.Cancel()
		case <-done:
		}
	}()

	var result *C.dfgo_result_stream
	var cerr *C.dfgo_error
	if C.dfgo_statement_execute(stmt.ptr, token.ptr, &result, &cerr) != stateOK {
		close(done)
		token.Close()
		return nil, contextError(ctx, takeError(cerr))
	}

	stream := C.dfgo_arrow_stream_alloc()
	if stream == nil {
		close(done)
		C.dfgo_result_close(result)
		token.Close()
		return nil, errors.New("datafusion-go could not allocate Arrow stream")
	}

	if C.dfgo_result_export_arrow_stream(result, stream, &cerr) != stateOK {
		close(done)
		C.free(unsafe.Pointer(stream))
		C.dfgo_result_close(result)
		token.Close()
		return nil, contextError(ctx, takeError(cerr))
	}

	reader, err := newResultReader(ctx, result, token, stream, done)
	if err != nil {
		close(done)
		C.dfgo_arrow_stream_release(stream)
		C.free(unsafe.Pointer(stream))
		C.dfgo_result_close(result)
		token.Close()
		return nil, err
	}

	return reader, nil
}

func (stmt *Statement) checkArgCount(args []driver.NamedValue) error {
	want := stmt.NumInput()
	if want < 0 || len(args) == want {
		return nil
	}

	plural := "s"
	if want == 1 {
		plural = ""
	}
	return fmt.Errorf("datafusion-go SQL statement expects %d argument%s, got %d; pass exactly one argument for each ?, $1/$2, or distinct $name placeholder", want, plural, len(args))
}

func (stmt *Statement) bindArgs(args []driver.NamedValue) error {
	var cerr *C.dfgo_error
	if C.dfgo_statement_clear_bindings(stmt.ptr, &cerr) != stateOK {
		return takeError(cerr)
	}

	for i, arg := range args {
		ordinal := arg.Ordinal
		if ordinal == 0 {
			ordinal = i + 1
		}
		if ordinal <= 0 {
			return fmt.Errorf("parameter ordinal must be positive, got %d", ordinal)
		}
		index := C.int64_t(ordinal)

		if arg.Name != "" {
			cname := C.CString(arg.Name)
			if C.dfgo_statement_set_param_name(stmt.ptr, index, cname, &cerr) != stateOK {
				C.free(unsafe.Pointer(cname))
				return takeError(cerr)
			}
			C.free(unsafe.Pointer(cname))
		}

		if err := stmt.bindValue(index, arg.Value); err != nil {
			return err
		}
	}

	return nil
}

func (stmt *Statement) bindValue(index C.int64_t, value driver.Value) error {
	var cerr *C.dfgo_error

	switch value := value.(type) {
	case nil:
		if C.dfgo_statement_bind_null(stmt.ptr, index, &cerr) != stateOK {
			return takeError(cerr)
		}
	case bool:
		cvalue := C.int(0)
		if value {
			cvalue = 1
		}
		if C.dfgo_statement_bind_bool(stmt.ptr, index, cvalue, &cerr) != stateOK {
			return takeError(cerr)
		}
	case int64:
		if C.dfgo_statement_bind_int64(stmt.ptr, index, C.int64_t(value), &cerr) != stateOK {
			return takeError(cerr)
		}
	case UInt64Parameter:
		if C.dfgo_statement_bind_uint64(stmt.ptr, index, C.uint64_t(value.Value), &cerr) != stateOK {
			return takeError(cerr)
		}
	case float64:
		if C.dfgo_statement_bind_float64(stmt.ptr, index, C.double(value), &cerr) != stateOK {
			return takeError(cerr)
		}
	case DateParameter:
		if C.dfgo_statement_bind_date32(stmt.ptr, index, C.int32_t(value.Days), &cerr) != stateOK {
			return takeError(cerr)
		}
	case TimeParameter:
		if C.dfgo_statement_bind_time64_ns(stmt.ptr, index, C.int64_t(value.Nanoseconds), &cerr) != stateOK {
			return takeError(cerr)
		}
	case TimestampParameter:
		if err := stmt.bindTimestamp(index, value, &cerr); err != nil {
			return err
		}
	case DurationParameter:
		if C.dfgo_statement_bind_duration_ns(stmt.ptr, index, C.int64_t(value.Nanoseconds), &cerr) != stateOK {
			return takeError(cerr)
		}
	case DecimalParameter:
		if err := stmt.bindDecimal(index, value, &cerr); err != nil {
			return err
		}
	case NullParameter:
		if err := stmt.bindTypedNull(index, value, &cerr); err != nil {
			return err
		}
	case string:
		cvalue := C.CString(value)
		if C.dfgo_statement_bind_string(stmt.ptr, index, cvalue, C.int64_t(len(value)), &cerr) != stateOK {
			C.free(unsafe.Pointer(cvalue))
			return takeError(cerr)
		}
		C.free(unsafe.Pointer(cvalue))
	case []byte:
		var ptr *C.uint8_t
		if len(value) != 0 {
			ptr = (*C.uint8_t)(unsafe.Pointer(&value[0]))
		}
		if C.dfgo_statement_bind_binary(stmt.ptr, index, ptr, C.int64_t(len(value)), &cerr) != stateOK {
			return takeError(cerr)
		}
	default:
		return fmt.Errorf("unsupported parameter type %T", value)
	}

	return nil
}

func (stmt *Statement) bindTimestamp(index C.int64_t, value TimestampParameter, cerr **C.dfgo_error) error {
	ctimezone := C.CString(value.TimeZone)
	defer C.free(unsafe.Pointer(ctimezone))
	if C.dfgo_statement_bind_timestamp_ns_tz(stmt.ptr, index, C.int64_t(value.Nanoseconds), ctimezone, C.int64_t(len(value.TimeZone)), cerr) != stateOK {
		return takeError(*cerr)
	}
	return nil
}

func (stmt *Statement) bindDecimal(index C.int64_t, value DecimalParameter, cerr **C.dfgo_error) error {
	cvalue := C.CString(value.Value)
	defer C.free(unsafe.Pointer(cvalue))
	if C.dfgo_statement_bind_decimal128(stmt.ptr, index, cvalue, C.int64_t(len(value.Value)), C.uint8_t(value.Precision), C.int8_t(value.Scale), cerr) != stateOK {
		return takeError(*cerr)
	}
	return nil
}

func (stmt *Statement) bindTypedNull(index C.int64_t, value NullParameter, cerr **C.dfgo_error) error {
	ctimezone := C.CString(value.TimeZone)
	defer C.free(unsafe.Pointer(ctimezone))
	if C.dfgo_statement_bind_typed_null(stmt.ptr, index, C.int32_t(value.Type), C.uint8_t(value.Precision), C.int8_t(value.Scale), ctimezone, C.int64_t(len(value.TimeZone)), cerr) != stateOK {
		return takeError(*cerr)
	}
	return nil
}

func newCancelToken() (*cancelToken, error) {
	var token *C.dfgo_cancel_token
	var cerr *C.dfgo_error
	if C.dfgo_cancel_token_create(&token, &cerr) != stateOK {
		return nil, takeError(cerr)
	}
	if token == nil {
		return nil, errors.New("datafusion-go native cancel token returned nil")
	}

	return &cancelToken{ptr: token}, nil
}

func (token *cancelToken) Cancel() {
	if token == nil {
		return
	}

	token.mu.Lock()
	defer token.mu.Unlock()
	if token.ptr == nil {
		return
	}
	C.dfgo_cancel_token_cancel(token.ptr)
}

func (token *cancelToken) Close() {
	if token == nil {
		return
	}

	token.mu.Lock()
	defer token.mu.Unlock()
	if token.ptr == nil {
		return
	}
	C.dfgo_cancel_token_close(token.ptr)
	token.ptr = nil
}

func newResultReader(ctx context.Context, result *C.dfgo_result_stream, token *cancelToken, stream *C.struct_ArrowArrayStream, done chan struct{}) (*resultReader, error) {
	array := C.dfgo_arrow_array_alloc()
	if array == nil {
		return nil, errors.New("datafusion-go could not allocate Arrow array")
	}

	var cschema C.struct_ArrowSchema
	if errno := C.dfgo_arrow_stream_get_schema(stream, &cschema); errno != 0 {
		C.free(unsafe.Pointer(array))
		return nil, streamError(stream, errno)
	}
	defer C.dfgo_arrow_schema_release(&cschema)

	schema, err := cdata.ImportCArrowSchema((*cdata.CArrowSchema)(unsafe.Pointer(&cschema)))
	if err != nil {
		C.free(unsafe.Pointer(array))
		return nil, err
	}

	reader := &resultReader{
		ctx:    ctx,
		stream: stream,
		array:  array,
		schema: schema,
		result: result,
		token:  token,
		done:   done,
	}
	runtime.SetFinalizer(reader, (*resultReader).finalize)
	return reader, nil
}

func (r *resultReader) Read() (arrow.RecordBatch, error) {
	if err := r.ctx.Err(); err != nil {
		r.Cancel()
		_ = r.Close()
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, io.EOF
	}

	if errno := C.dfgo_arrow_stream_get_next(r.stream, r.array); errno != 0 {
		err := contextError(r.ctx, streamError(r.stream, errno))
		if r.ctx.Err() != nil {
			r.closeLocked()
		}
		return nil, err
	}
	if C.dfgo_arrow_array_is_released(r.array) != 0 {
		r.closeLocked()
		return nil, io.EOF
	}

	rec, err := cdata.ImportCRecordBatchWithSchema((*cdata.CArrowArray)(unsafe.Pointer(r.array)), r.schema)
	if err != nil {
		return nil, err
	}
	return rec, nil
}

func (r *resultReader) Schema() *arrow.Schema {
	if r == nil {
		return nil
	}
	return r.schema
}

func (r *resultReader) Cancel() {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.result != nil {
		C.dfgo_result_cancel(r.result)
	}
	if r.token != nil {
		r.token.Cancel()
	}
}

func (r *resultReader) Close() error {
	if r == nil {
		return nil
	}
	runtime.SetFinalizer(r, nil)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.closeLocked()
	return nil
}

func (r *resultReader) finalize() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closeLocked()
}

func (r *resultReader) closeLocked() {
	if r.closed {
		return
	}
	r.closed = true
	close(r.done)

	if r.result != nil {
		C.dfgo_result_cancel(r.result)
	}
	if r.array != nil {
		C.dfgo_arrow_array_release(r.array)
		C.free(unsafe.Pointer(r.array))
		r.array = nil
	}
	if r.stream != nil {
		C.dfgo_arrow_stream_release(r.stream)
		C.free(unsafe.Pointer(r.stream))
		r.stream = nil
	}
	if r.result != nil {
		C.dfgo_result_close(r.result)
		r.result = nil
	}
	if r.token != nil {
		r.token.Close()
		r.token = nil
	}
}

func streamError(stream *C.struct_ArrowArrayStream, errno C.int) error {
	msg := C.dfgo_arrow_stream_get_last_error(stream)
	if msg == nil {
		return fmt.Errorf("arrow stream failed with errno %d", int(errno))
	}
	return fmt.Errorf("arrow stream failed with errno %d: %s", int(errno), C.GoString(msg))
}

func contextError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

func takeError(cerr *C.dfgo_error) error {
	if cerr == nil {
		return errors.New("datafusion-go native call failed without an error message")
	}
	defer C.dfgo_error_free(cerr)

	msg := C.dfgo_error_message(cerr)
	if msg == nil {
		return errors.New("datafusion-go native call failed without an error message")
	}

	var kind string
	if ckind := C.dfgo_error_kind(cerr); ckind != nil {
		kind = C.GoString(ckind)
	}
	return &Error{
		Kind:    kind,
		Message: C.GoString(msg),
	}
}

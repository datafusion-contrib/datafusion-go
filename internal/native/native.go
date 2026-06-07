package native

/*
#cgo linux LDFLAGS: -ldl
#cgo CFLAGS: -I${SRCDIR}/../../rust/include
#ifndef DFGO_DIRECT_LINK
#define DFGO_NO_FUNCTION_PROTOTYPES
#endif
#include "datafusion_go.h"
#ifdef DFGO_DIRECT_LINK
static int dfgo_native_uses_dynamic_loader(void) {
	return 0;
}

static int dfgo_native_load_library(const char *path) {
	(void)path;
	return 0;
}

static const char *dfgo_native_load_error(void) {
	return "";
}
#else
#include "dynamic_loader.h"
#endif
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
	if err := ensureNativeLibraryLoaded(); err != nil {
		return nil, err
	}
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

var nativeLoad struct {
	once sync.Once
	err  error
}

func ensureNativeLibraryLoaded() error {
	nativeLoad.once.Do(func() {
		if C.dfgo_native_uses_dynamic_loader() == 0 {
			return
		}
		path, err := resolveNativeLibrary()
		if err != nil {
			nativeLoad.err = err
			return
		}
		cpath := C.CString(path)
		defer C.free(unsafe.Pointer(cpath))
		if C.dfgo_native_load_library(cpath) != 0 {
			nativeLoad.err = fmt.Errorf("could not load datafusion-go native library %q: %s", path, C.GoString(C.dfgo_native_load_error()))
		}
	})
	return nativeLoad.err
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

	params, cleanup, err := statementParams(args)
	if err != nil {
		return nil, err
	}
	defer cleanup()

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
	var paramsPtr *C.dfgo_parameter
	if len(params) != 0 {
		paramsPtr = &params[0]
	}
	if C.dfgo_statement_execute_with_params(stmt.ptr, paramsPtr, C.int64_t(len(params)), token.ptr, &result, &cerr) != stateOK {
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

func statementParams(args []driver.NamedValue) ([]C.dfgo_parameter, func(), error) {
	params := make([]C.dfgo_parameter, len(args))
	var allocs []unsafe.Pointer
	cleanup := func() {
		for _, ptr := range allocs {
			C.free(ptr)
		}
	}
	for i, arg := range args {
		ordinal := arg.Ordinal
		if ordinal == 0 {
			ordinal = i + 1
		}
		if ordinal <= 0 {
			cleanup()
			return nil, nil, fmt.Errorf("parameter ordinal must be positive, got %d", ordinal)
		}

		param := &params[i]
		param.index = C.int64_t(ordinal)
		if arg.Name != "" {
			param.name = cParamString(&allocs, arg.Name)
			param.name_len = C.int64_t(len(arg.Name))
		}

		if err := setStatementParam(param, arg.Value, &allocs); err != nil {
			cleanup()
			return nil, nil, err
		}
	}

	return params, cleanup, nil
}

func setStatementParam(param *C.dfgo_parameter, value driver.Value, allocs *[]unsafe.Pointer) error {
	switch value := value.(type) {
	case nil:
		param.is_null = C.int32_t(1)
	case bool:
		param.type_code = C.int32_t(ParameterBool)
		if value {
			param.int64_value = C.int64_t(1)
		}
	case int64:
		param.type_code = C.int32_t(ParameterInt64)
		param.int64_value = C.int64_t(value)
	case UInt64Parameter:
		param.type_code = C.int32_t(ParameterUInt64)
		param.uint64_value = C.uint64_t(value.Value)
	case float64:
		param.type_code = C.int32_t(ParameterFloat64)
		param.float64_value = C.double(value)
	case DateParameter:
		param.type_code = C.int32_t(ParameterDate)
		param.int64_value = C.int64_t(value.Days)
	case TimeParameter:
		param.type_code = C.int32_t(ParameterTime)
		param.int64_value = C.int64_t(value.Nanoseconds)
	case TimestampParameter:
		param.type_code = C.int32_t(ParameterTimestamp)
		param.int64_value = C.int64_t(value.Nanoseconds)
		param.timezone = cParamString(allocs, value.TimeZone)
		param.timezone_len = C.int64_t(len(value.TimeZone))
	case DurationParameter:
		param.type_code = C.int32_t(ParameterDuration)
		param.int64_value = C.int64_t(value.Nanoseconds)
	case DecimalParameter:
		param.type_code = C.int32_t(ParameterDecimal)
		param.data = cParamData(allocs, []byte(value.Value))
		param.data_len = C.int64_t(len(value.Value))
		param.precision = C.uint8_t(value.Precision)
		param.scale = C.int8_t(value.Scale)
	case NullParameter:
		param.type_code = C.int32_t(value.Type)
		param.is_null = C.int32_t(1)
		param.precision = C.uint8_t(value.Precision)
		param.scale = C.int8_t(value.Scale)
		param.timezone = cParamString(allocs, value.TimeZone)
		param.timezone_len = C.int64_t(len(value.TimeZone))
	case string:
		param.type_code = C.int32_t(ParameterString)
		param.data = cParamData(allocs, []byte(value))
		param.data_len = C.int64_t(len(value))
	case []byte:
		param.type_code = C.int32_t(ParameterBinary)
		param.data = cParamData(allocs, value)
		param.data_len = C.int64_t(len(value))
	default:
		return fmt.Errorf("unsupported parameter type %T", value)
	}

	return nil
}

func cParamData(allocs *[]unsafe.Pointer, data []byte) *C.uint8_t {
	if len(data) == 0 {
		return nil
	}
	ptr := C.CBytes(data)
	*allocs = append(*allocs, ptr)
	return (*C.uint8_t)(ptr)
}

func cParamString(allocs *[]unsafe.Pointer, value string) *C.char {
	if value == "" {
		return nil
	}
	return (*C.char)(unsafe.Pointer(cParamData(allocs, []byte(value))))
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

//go:build datafusion_test_sqllogic && cgo

package native

/*
#cgo linux LDFLAGS: -ldl
#include "sqllogictest.h"
*/
import "C"

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime/cgo"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

var sqlLogicSymbols struct {
	sync.Once
	err error
}

// SetupSQLLogicTest installs upstream fixtures on a fresh, idle connection.
// The returned cleanup must run after the connection has been closed.
func loadSQLLogicSymbols() error {
	sqlLogicSymbols.Do(func() {
		library := C.CString(os.Getenv(nativeLibraryEnv))
		defer C.free(unsafe.Pointer(library))
		if C.dfgo_slt_load(library) == 0 {
			sqlLogicSymbols.err = fmt.Errorf("SQLLogicTest symbols are missing; build with the test-sqllogictest Cargo feature")
		}
	})
	return sqlLogicSymbols.err
}

func (conn *Connection) SetupSQLLogicTest(path string) (func(), error) {
	if err := loadSQLLogicSymbols(); err != nil {
		return nil, err
	}
	if conn.ptr == nil {
		return nil, fmt.Errorf("SQLLogicTest connection is closed")
	}
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	var fixture unsafe.Pointer
	var cerr *C.dfgo_error
	if C.dfgo_slt_setup(conn.ptr, cpath, &fixture, &cerr) != 0 {
		return nil, takeError(cerr)
	}
	return sync.OnceFunc(func() { C.dfgo_slt_context_free(fixture) }), nil
}

type SQLLogicReport struct {
	Statements int `json:"statements"`
	Queries    int `json:"queries"`
	Eligible   int `json:"eligible"`
	Passed     int `json:"passed"`
	Skipped    []struct {
		Location   string `json:"location"`
		Conditions string `json:"conditions"`
	} `json:"skipped"`
	Errors  []string         `json:"errors"`
	Records []SQLLogicRecord `json:"records"`
}

// SQLLogicRecord preserves the assertion outcome and its original SQL for
// coverage witnesses. Expected SQL errors are distinct from value assertions.
type SQLLogicRecord struct {
	Location     string   `json:"location"`
	SQL          string   `json:"sql"`
	Kind         string   `json:"kind"`
	Passed       bool     `json:"passed"`
	Skipped      bool     `json:"skipped"`
	ExpectsError bool     `json:"expects_error"`
	Functions    []string `json:"functions"`
	ParseError   string   `json:"parse_error,omitempty"`
}

// SQLLogicHarnessError cannot satisfy an upstream expected-SQL-error record.
type SQLLogicHarnessError struct{ Err error }

func (e *SQLLogicHarnessError) Error() string { return e.Err.Error() }
func (e *SQLLogicHarnessError) Unwrap() error { return e.Err }

// RunSQLLogicTest uses upstream's parser and assertions. The query callback
// executes SQL through the public Go API and returns consumed Arrow batches.
func RunSQLLogicTest(path, workspace string, query func(string) (array.RecordReader, error)) (SQLLogicReport, error) {
	var report SQLLogicReport
	if err := loadSQLLogicSymbols(); err != nil {
		return report, err
	}
	cpath, cworkspace := C.CString(path), C.CString(workspace)
	defer C.free(unsafe.Pointer(cpath))
	defer C.free(unsafe.Pointer(cworkspace))
	handle := cgo.NewHandle(query)
	defer handle.Delete()
	var output *C.char
	var cerr *C.dfgo_error
	if C.dfgo_slt_run(cpath, cworkspace, C.uintptr_t(handle), &output, &cerr) != 0 {
		return report, takeError(cerr)
	}
	defer C.dfgo_slt_free(output)
	if err := json.Unmarshal([]byte(C.GoString(output)), &report); err != nil {
		return report, fmt.Errorf("decode SQLLogicTest report: %w", err)
	}
	return report, nil
}

//export dfgoGoSLTQuery
func dfgoGoSLTQuery(handle C.uintptr_t, query *C.char, length C.size_t, data **C.uint8_t, size *C.size_t) (status C.int) {
	var result []byte
	var stream *C.struct_ArrowArrayStream
	defer func() {
		if recovered := recover(); recovered != nil {
			result = []byte(fmt.Sprintf("Go SQLLogicTest callback panicked: %v", recovered))
			status = 2
		}
		if status != 0 {
			if stream != nil {
				C.dfgo_slt_stream_free(stream)
			}
			*data = (*C.uint8_t)(C.CBytes(result))
			*size = C.size_t(len(result))
		}
	}()
	callback := cgo.Handle(handle).Value().(func(string) (array.RecordReader, error))
	sql := string(unsafe.Slice((*byte)(unsafe.Pointer(query)), int(length)))
	// The upstream runner owns a Rust executor on this callback's OS thread.
	// Execute SQL on its own Go goroutine while the assertion executor waits
	// for the result. The callback thread only transfers the returned batches.
	type answer struct {
		reader array.RecordReader
		err    error
	}
	completed := make(chan answer, 1)
	go func() {
		var result answer
		defer func() {
			if recovered := recover(); recovered != nil {
				result.err = &SQLLogicHarnessError{Err: fmt.Errorf("go SQLLogicTest callback panicked: %v", recovered)}
			}
			completed <- result
		}()
		result.reader, result.err = callback(sql)
	}()
	resultAnswer := <-completed
	reader, err := resultAnswer.reader, resultAnswer.err
	if err != nil {
		result = []byte(err.Error())
		var harnessError *SQLLogicHarnessError
		if errors.As(err, &harnessError) {
			return 2
		}
		return 1
	}
	defer reader.Release()
	stream = (*C.struct_ArrowArrayStream)(C.calloc(1, C.sizeof_struct_ArrowArrayStream))
	if stream == nil {
		result = []byte("allocate SQLLogicTest Arrow stream")
		return 2
	}
	cdata.ExportRecordReader(reader, (*cdata.CArrowArrayStream)(unsafe.Pointer(stream)))
	*data = (*C.uint8_t)(unsafe.Pointer(stream))
	*size = C.sizeof_struct_ArrowArrayStream
	return 0
}

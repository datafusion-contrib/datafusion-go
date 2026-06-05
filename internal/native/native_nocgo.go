//go:build !cgo

package native

import (
	"context"
	"database/sql/driver"
	"errors"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/arrio"
)

var errCgoDisabled = errors.New("datafusion-go requires cgo; rebuild with CGO_ENABLED=1")

type Database struct{}
type Connection struct{}
type Statement struct{}

func OpenDatabase(string) (*Database, error) {
	return nil, errCgoDisabled
}

func (db *Database) Close() {}

func (db *Database) Connect(bool) (*Connection, error) {
	return nil, errCgoDisabled
}

func (conn *Connection) Close() {}

func (conn *Connection) RegisterArrowIPC(string, []byte) error {
	return errCgoDisabled
}

func (conn *Connection) RegisterArrowReaderZeroCopy(string, array.RecordReader) error {
	return errCgoDisabled
}

func (conn *Connection) Prepare(string) (*Statement, error) {
	return nil, errCgoDisabled
}

func (stmt *Statement) Close() {}

func (stmt *Statement) NumInput() int {
	return -1
}

func (stmt *Statement) Serializes() bool {
	return false
}

func (stmt *Statement) ExecuteArrow(context.Context, []driver.NamedValue) (arrio.Reader, error) {
	return nil, errCgoDisabled
}

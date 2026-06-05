package datafusion

import (
	"context"
	"database/sql/driver"
	"sync"

	"github.com/datafusion-contrib/datafusion-go/internal/native"
)

// Stmt is a prepared DataFusion statement.
type Stmt struct {
	stmt      *native.Statement
	connector *Connector
	query     string

	mu     sync.Mutex
	closed bool
}

// Close releases the native prepared statement handle.
func (s *Stmt) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	s.stmt.Close()
	return nil
}

// NumInput returns the number of SQL parameters found while preparing the statement.
func (s *Stmt) NumInput() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return -1
	}
	return s.stmt.NumInput()
}

// CheckNamedValue normalizes DataFusion-specific parameter wrapper types.
func (s *Stmt) CheckNamedValue(nv *driver.NamedValue) error {
	return checkNamedValue(nv)
}

// Exec executes the statement with positional driver values.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	named := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		value, err := normalizeParameterValue(arg)
		if err != nil {
			return nil, err
		}
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
	}
	return s.ExecContext(context.Background(), named)
}

// ExecContext executes the statement with normalized named values.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	named, err := normalizeNamedValueSlice(args)
	if err != nil {
		return nil, err
	}

	unlock, err := s.lockSerializedStatement(ctx)
	if err != nil {
		return nil, err
	}

	reader, err := s.executeArrow(ctx, named)
	if err != nil {
		if unlock != nil {
			unlock()
		}
		return nil, err
	}
	return execResult(newSerializedArrowReader(reader, unlock))
}

// Query executes the statement with positional driver values.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	named := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		value, err := normalizeParameterValue(arg)
		if err != nil {
			return nil, err
		}
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
	}
	return s.QueryContext(context.Background(), named)
}

// QueryContext executes the statement with normalized named values.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	named, err := normalizeNamedValueSlice(args)
	if err != nil {
		return nil, err
	}

	unlock, err := s.lockSerializedStatement(ctx)
	if err != nil {
		return nil, err
	}

	reader, err := s.executeArrow(ctx, named)
	if err != nil {
		if unlock != nil {
			unlock()
		}
		return nil, err
	}
	arrowReader := newSerializedArrowReader(reader, unlock)

	rows, err := newRows(arrowReader)
	if err != nil {
		closeReader(arrowReader)
		return nil, driverError(ErrorScan, "could not create DataFusion rows", err)
	}
	return rows, nil
}

func (s *Stmt) executeArrow(ctx context.Context, named []driver.NamedValue) (ArrowReader, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, driverError(ErrorClosed, "datafusion statement is closed", nil)
	}

	reader, err := s.stmt.ExecuteArrow(ctx, named)
	s.mu.Unlock()
	if err != nil {
		return nil, driverError(ErrorExecute, "could not execute DataFusion statement", err)
	}

	arrowReader, ok := reader.(ArrowReader)
	if !ok {
		closeReader(reader)
		return nil, driverError(ErrorNative, "DataFusion Arrow reader is not closeable", nil)
	}
	return arrowReader, nil
}

func (s *Stmt) lockSerializedStatement(ctx context.Context) (func(), error) {
	if s.connector == nil {
		return nil, nil
	}
	s.mu.Lock()
	serializes := !s.closed && s.stmt.Serializes()
	s.mu.Unlock()
	return s.connector.lockSerializedStatement(ctx, serializes)
}

var _ driver.Stmt = (*Stmt)(nil)
var _ driver.NamedValueChecker = (*Stmt)(nil)
var _ driver.StmtExecContext = (*Stmt)(nil)
var _ driver.StmtQueryContext = (*Stmt)(nil)

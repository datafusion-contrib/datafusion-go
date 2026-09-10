package datafusion

import (
	"context"
	"database/sql/driver"
	"sync"

	"github.com/datafusion-contrib/datafusion-go/internal/native"
)

// Stmt is a prepared DataFusion statement.
type Stmt struct {
	stmt       *native.Statement
	conn       *Conn
	query      string
	numInput   int
	serializes bool

	mu     sync.Mutex
	closed bool
}

// Close releases the native prepared statement handle.
func (s *Stmt) Close() error {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	if s.stmt != nil {
		s.stmt.Close()
		s.stmt = nil
	}
	delete(s.conn.statements, s)
	return nil
}

// NumInput returns the number of SQL parameters found while preparing the statement.
func (s *Stmt) NumInput() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return -1
	}
	return s.numInput
}

// CheckNamedValue normalizes DataFusion-specific parameter wrapper types.
func (s *Stmt) CheckNamedValue(nv *driver.NamedValue) error {
	return checkNamedValue(nv)
}

// Exec executes the statement with positional driver values.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), positionalNamedValues(args))
}

// ExecContext executes the statement with normalized named values.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	reader, err := runArrowQuery(ctx, args, s.prepareOperation)
	if err != nil {
		return nil, err
	}
	return execResult(reader)
}

// Query executes the statement with positional driver values.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), positionalNamedValues(args))
}

// QueryContext executes the statement with normalized named values.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	reader, err := runArrowQuery(ctx, args, s.prepareOperation)
	if err != nil {
		return nil, err
	}
	return queryRows(reader)
}

func (s *Stmt) executeArrow(ctx context.Context, named []driver.NamedValue) (native.RecordReader, error) {
	s.conn.mu.Lock()
	s.mu.Lock()
	if s.closed || s.conn.closed {
		s.mu.Unlock()
		s.conn.mu.Unlock()
		return nil, driverError(ErrorClosed, "datafusion statement is closed", nil)
	}
	if s.stmt == nil {
		stmt, err := s.conn.conn.Prepare(s.query)
		if err != nil {
			s.mu.Unlock()
			s.conn.mu.Unlock()
			return nil, driverError(ErrorPrepare, "could not reprepare DataFusion statement after session reset", err)
		}
		s.stmt = stmt
	}
	s.conn.mu.Unlock()

	defer s.mu.Unlock()
	return executeNativeStatement(ctx, s.stmt, named)
}

func (s *Stmt) prepareOperation() (queryOperation, error) {
	s.mu.Lock()
	serializes := !s.closed && s.serializes
	s.mu.Unlock()
	return queryOperation{
		connector:  s.conn.connector,
		serializes: serializes,
		execute:    s.executeArrow,
	}, nil
}

var _ driver.Stmt = (*Stmt)(nil)
var _ driver.NamedValueChecker = (*Stmt)(nil)
var _ driver.StmtExecContext = (*Stmt)(nil)
var _ driver.StmtQueryContext = (*Stmt)(nil)

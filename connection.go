package datafusion

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"

	"github.com/datafusion-contrib/datafusion-go/internal/native"
)

// Conn is a single database/sql driver connection to a DataFusion SessionContext.
type Conn struct {
	conn      *native.Connection
	connector *Connector
	// Driver.Open owns its private connector; pooled connections do not.
	ownsConnector bool

	mu         sync.Mutex
	closed     bool
	statements map[*Stmt]struct{}
}

func newConn(conn *native.Connection, connector *Connector) *Conn {
	return &Conn{conn: conn, connector: connector, statements: make(map[*Stmt]struct{})}
}

// Prepare validates and prepares query using a background context.
func (conn *Conn) Prepare(query string) (driver.Stmt, error) {
	return conn.PrepareContext(context.Background(), query)
}

// PrepareContext validates and prepares query.
func (conn *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.closed {
		return nil, driverError(ErrorClosed, "datafusion connection is closed", nil)
	}

	stmt, err := conn.conn.Prepare(query)
	if err != nil {
		return nil, driverError(ErrorPrepare, "could not prepare DataFusion statement", err)
	}

	s := &Stmt{stmt: stmt, conn: conn, query: query, numInput: stmt.NumInput(), serializes: stmt.Serializes()}
	conn.statements[s] = struct{}{}
	return s, nil
}

// Close releases the native connection handle.
func (conn *Conn) Close() error {
	conn.mu.Lock()

	if conn.closed {
		conn.mu.Unlock()
		return nil
	}
	conn.closed = true
	conn.invalidateStatementsLocked()
	if conn.conn != nil {
		conn.conn.Close()
		conn.conn = nil
	}
	owned := conn.ownsConnector
	conn.mu.Unlock()
	if owned {
		return conn.connector.Close()
	}
	return nil
}

// The caller holds conn.mu. Always acquire statement locks after conn.mu.
// Cached database/sql statements keep their SQL and metadata but must no
// longer retain the session that is being closed or reset.
func (conn *Conn) invalidateStatementsLocked() {
	for stmt := range conn.statements {
		stmt.mu.Lock()
		if stmt.stmt != nil {
			stmt.stmt.Close()
			stmt.stmt = nil
		}
		stmt.mu.Unlock()
	}
}

// Begin returns an unsupported error because DataFusion transactions are not supported.
func (conn *Conn) Begin() (driver.Tx, error) {
	return conn.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx returns an unsupported error after honoring an already-canceled context.
func (conn *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := conn.checkOpen(); err != nil {
		return nil, err
	}
	_ = opts
	return nil, driverError(ErrorUnsupported, "DataFusion transactions are not supported", nil)
}

// Ping validates that the connection is open.
func (conn *Conn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return conn.checkOpen()
}

// ResetSession resets isolated sessions and validates shared sessions for pool reuse.
func (conn *Conn) ResetSession(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	conn.mu.Lock()
	if conn.closed || conn.conn == nil {
		conn.mu.Unlock()
		return driver.ErrBadConn
	}
	connector := conn.connector
	conn.mu.Unlock()

	if connector == nil {
		return driver.ErrBadConn
	}
	if connector.sharedSession {
		connector.mu.Lock()
		closed := connector.closed
		connector.mu.Unlock()
		if closed {
			return driver.ErrBadConn
		}
		return nil
	}

	nc, err := connector.connectNative(ctx)
	if err != nil {
		_ = conn.Close()
		return errors.Join(driver.ErrBadConn, err)
	}

	replacement := newConn(nc, connector)
	if connector.initFn != nil {
		if err := connector.initFn(ctx, replacement); err != nil {
			_ = replacement.Close()
			_ = conn.Close()
			return errors.Join(driver.ErrBadConn, err)
		}
	}

	conn.mu.Lock()
	if conn.closed || conn.conn == nil {
		conn.mu.Unlock()
		_ = replacement.Close()
		return driver.ErrBadConn
	}
	old := conn.conn
	conn.invalidateStatementsLocked()
	conn.conn = replacement.conn
	replacement.conn = nil
	conn.mu.Unlock()

	old.Close()
	return nil
}

// IsValid reports whether the connection can be reused by database/sql.
func (conn *Conn) IsValid() bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	return !conn.closed && conn.conn != nil
}

// CheckNamedValue normalizes DataFusion-specific parameter wrapper types.
func (conn *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	return checkNamedValue(nv)
}

// ExecContext executes a statement and returns a database/sql result.
func (conn *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	reader, err := conn.QueryArrowContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return execResult(reader)
}

// QueryContext executes a query and adapts Arrow record batches to database/sql rows.
func (conn *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	reader, err := conn.QueryArrowContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return queryRows(reader)
}

// QueryArrowContext executes a query and returns Arrow record batches.
func (conn *Conn) QueryArrowContext(ctx context.Context, query string, args []driver.NamedValue) (ArrowReader, error) {
	return runArrowQuery(ctx, args, func() (queryOperation, error) {
		return conn.prepareOperation(query)
	})
}

func (conn *Conn) prepareOperation(query string) (queryOperation, error) {
	// A temporary statement owns its session reference and executes outside conn.mu.
	var stmt *native.Statement
	if err := conn.withNative(func(nc *native.Connection) error {
		prepared, err := nc.Prepare(query)
		if err != nil {
			return driverError(ErrorPrepare, "could not prepare DataFusion statement", err)
		}
		stmt = prepared
		return nil
	}); err != nil {
		return queryOperation{}, err
	}
	return queryOperation{
		connector:  conn.connector,
		serializes: stmt.Serializes(),
		execute: func(ctx context.Context, args []driver.NamedValue) (native.RecordReader, error) {
			return executeNativeStatement(ctx, stmt, args)
		},
		close: stmt.Close,
	}, nil
}

func (conn *Conn) checkOpen() error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.closed {
		return driverError(ErrorClosed, "datafusion connection is closed", nil)
	}
	return nil
}

// withNative prevents Close or ResetSession from freeing the native handle
// during fn. All access to conn.conn must hold conn.mu.
func (conn *Conn) withNative(fn func(*native.Connection) error) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.closed || conn.conn == nil {
		return driverError(ErrorClosed, "datafusion connection is closed", nil)
	}
	return fn(conn.conn)
}

var _ driver.Conn = (*Conn)(nil)
var _ driver.ConnPrepareContext = (*Conn)(nil)
var _ driver.ConnBeginTx = (*Conn)(nil)
var _ driver.Pinger = (*Conn)(nil)
var _ driver.SessionResetter = (*Conn)(nil)
var _ driver.Validator = (*Conn)(nil)
var _ driver.NamedValueChecker = (*Conn)(nil)
var _ driver.ExecerContext = (*Conn)(nil)
var _ driver.QueryerContext = (*Conn)(nil)

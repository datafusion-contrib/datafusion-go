package datafusion

import (
	"context"
	"database/sql/driver"
	"sync"

	"github.com/datafusion-contrib/datafusion-go/internal/native"
)

// Conn is a single database/sql driver connection to a DataFusion SessionContext.
type Conn struct {
	conn      *native.Connection
	connector *Connector

	mu     sync.Mutex
	closed bool
}

func newConn(conn *native.Connection, connector *Connector) *Conn {
	return &Conn{conn: conn, connector: connector}
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
	if err := conn.checkOpen(); err != nil {
		return nil, err
	}

	stmt, err := conn.conn.Prepare(query)
	if err != nil {
		return nil, driverError(ErrorPrepare, "could not prepare DataFusion statement", err)
	}

	return &Stmt{stmt: stmt, connector: conn.connector, query: query}, nil
}

// Close releases the native connection handle.
func (conn *Conn) Close() error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.closed {
		return nil
	}
	conn.closed = true
	if conn.conn != nil {
		conn.conn.Close()
		conn.conn = nil
	}
	return nil
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
		return err
	}

	replacement := newConn(nc, connector)
	if connector.initFn != nil {
		if err := connector.initFn(ctx, replacement); err != nil {
			replacement.Close()
			return err
		}
	}

	conn.mu.Lock()
	if conn.closed || conn.conn == nil {
		conn.mu.Unlock()
		replacement.Close()
		return driver.ErrBadConn
	}
	old := conn.conn
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
	rows, err := newRows(reader)
	if err != nil {
		closeReader(reader)
		return nil, driverError(ErrorScan, "could not create DataFusion rows", err)
	}
	return rows, nil
}

// QueryArrowContext executes a query and returns Arrow record batches.
func (conn *Conn) QueryArrowContext(ctx context.Context, query string, args []driver.NamedValue) (ArrowReader, error) {
	reader, unlock, err := conn.queryArrowContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return newSerializedArrowReader(reader, unlock), nil
}

func (conn *Conn) queryArrowContext(ctx context.Context, query string, args []driver.NamedValue) (ArrowReader, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if err := conn.checkOpen(); err != nil {
		return nil, nil, err
	}
	named, err := normalizeNamedValueSlice(args)
	if err != nil {
		return nil, nil, err
	}

	stmt, err := conn.conn.Prepare(query)
	if err != nil {
		return nil, nil, driverError(ErrorPrepare, "could not prepare DataFusion statement", err)
	}
	defer stmt.Close()

	unlock, err := conn.lockSerializedStatement(ctx, stmt.Serializes())
	if err != nil {
		return nil, nil, err
	}

	reader, err := stmt.ExecuteArrow(ctx, named)
	if err != nil {
		if unlock != nil {
			unlock()
		}
		return nil, nil, driverError(ErrorExecute, "could not execute DataFusion statement", err)
	}

	arrowReader, ok := reader.(ArrowReader)
	if !ok {
		closeReader(reader)
		if unlock != nil {
			unlock()
		}
		return nil, nil, driverError(ErrorNative, "DataFusion Arrow reader is not closeable", nil)
	}
	return arrowReader, unlock, nil
}

func (conn *Conn) lockSerializedStatement(ctx context.Context, serializes bool) (func(), error) {
	if conn.connector == nil {
		return nil, nil
	}
	return conn.connector.lockSerializedStatement(ctx, serializes)
}

func (conn *Conn) checkOpen() error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.closed {
		return driverError(ErrorClosed, "datafusion connection is closed", nil)
	}
	return nil
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

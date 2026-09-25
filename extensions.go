package datafusion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/datafusion-contrib/datafusion-go/internal/native"
)

// TableProvider supplies a fresh, owned Arrow reader per scan. Implementations
// must be concurrency-safe and honor cancellation. The engine owns each returned
// reader and releases it after use. Go buffers are copied before native retention.
type TableProvider = native.TableProvider

// PushdownTableProvider can avoid source I/O using exact projection and advisory filters.
type PushdownTableProvider = native.PushdownTableProvider

// ScanOptions specifies projection, conservative predicates and a safe limit hint.
type ScanOptions = native.ScanOptions

// ScalarFunction defines a typed, context-aware, vectorized Go UDF.
type ScalarFunction = native.ScalarFunction

// Volatility declares a function's optimizer semantics; zero is Volatile.
type Volatility = native.Volatility

const (
	Volatile  = native.Volatile
	Stable    = native.Stable
	Immutable = native.Immutable
)

// Expr describes a typed predicate supplied to a pushdown provider.
type Expr = native.Expr

// Column describes a typed predicate supplied to a pushdown provider.
type Column = native.Column

// CompareOp describes a typed predicate supplied to a pushdown provider.
type CompareOp = native.CompareOp

// LiteralType describes a typed predicate supplied to a pushdown provider.
type LiteralType = native.LiteralType

// TimeUnit describes a typed predicate supplied to a pushdown provider.
type TimeUnit = native.TimeUnit

// Literal describes a typed predicate supplied to a pushdown provider.
type Literal = native.Literal

// Compare describes a typed predicate supplied to a pushdown provider.
type Compare = native.Compare

// IsNull describes a typed predicate supplied to a pushdown provider.
type IsNull = native.IsNull

// Between describes a typed predicate supplied to a pushdown provider.
type Between = native.Between

// InList describes a typed predicate supplied to a pushdown provider.
type InList = native.InList

// And describes a typed predicate supplied to a pushdown provider.
type And = native.And

// Or describes a typed predicate supplied to a pushdown provider.
type Or = native.Or

// Not describes a typed predicate supplied to a pushdown provider.
type Not = native.Not

const CompareEq = native.CompareEq
const CompareNeq = native.CompareNeq
const CompareLt = native.CompareLt
const CompareLtEq = native.CompareLtEq
const CompareGt = native.CompareGt
const CompareGtEq = native.CompareGtEq
const LiteralBool = native.LiteralBool
const LiteralInt8 = native.LiteralInt8
const LiteralInt16 = native.LiteralInt16
const LiteralInt32 = native.LiteralInt32
const LiteralInt64 = native.LiteralInt64
const LiteralUint8 = native.LiteralUint8
const LiteralUint16 = native.LiteralUint16
const LiteralUint32 = native.LiteralUint32
const LiteralUint64 = native.LiteralUint64
const LiteralFloat32 = native.LiteralFloat32
const LiteralFloat64 = native.LiteralFloat64
const LiteralUtf8 = native.LiteralUtf8
const LiteralBinary = native.LiteralBinary
const LiteralDate32 = native.LiteralDate32
const LiteralDate64 = native.LiteralDate64
const LiteralTimestamp = native.LiteralTimestamp
const TimeUnitSecond = native.TimeUnitSecond
const TimeUnitMillisecond = native.TimeUnitMillisecond
const TimeUnitMicrosecond = native.TimeUnitMicrosecond
const TimeUnitNanosecond = native.TimeUnitNanosecond

func validateProvider(p TableProvider) error {
	if p == nil {
		return errors.New("datafusion table provider is nil")
	}
	value := reflect.ValueOf(p)
	if (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) && value.IsNil() {
		return errors.New("datafusion table provider is nil")
	}
	return nil
}
func registerProvider(ctx context.Context, conn *Conn, name string, p TableProvider) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateProvider(p); err != nil {
		return err
	}
	kind := 0
	if _, ok := p.(PushdownTableProvider); ok {
		kind |= 1
	}
	if _, ok := p.(WritableTableProvider); ok {
		kind |= 2
	}
	return conn.withExtension(ctx, func(nc *native.Connection) error { return nc.RegisterGo(name, p, kind) })
}

// RegisterTableProvider registers a lazy Go source on a SQL connection's session.
// Registration retains the provider; each scan receives query cancellation.
// Ordinary Go Arrow allocations are supported: batches are copied through IPC
// before native code retains them. Duplicate names return an error.
func RegisterTableProvider(ctx context.Context, conn *sql.Conn, name string, p TableProvider) (*RegisteredTable, error) {
	if conn == nil {
		return nil, errors.New("datafusion sql connection is nil")
	}
	if err := withDataFusionConn(conn, func(c *Conn) error { return registerProvider(ctx, c, name, p) }); err != nil {
		return nil, err
	}
	return &RegisteredTable{sqlConn: conn, name: name}, nil
}

// RegisterTableProvider registers a lazy Go source in this session.
func (s *Session) RegisterTableProvider(ctx context.Context, name string, p TableProvider) error {
	return registerProvider(ctx, s.conn, name, p)
}

func registerScalarFunction(ctx context.Context, conn *Conn, f ScalarFunction) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.Name == "" || f.ReturnType == nil || f.Evaluate == nil {
		return errors.New("datafusion function requires a name, return type and evaluator")
	}
	if f.Volatility < Volatile || f.Volatility > Immutable {
		return errors.New("datafusion function has invalid volatility")
	}
	for i, t := range f.Arguments {
		if t == nil {
			return fmt.Errorf("datafusion function argument %d has nil type", i)
		}
	}
	// Snapshot the signature; callers may reuse or modify their configuration slice.
	f.Arguments = append(f.Arguments[:0:0], f.Arguments...)
	return conn.withExtension(ctx, func(nc *native.Connection) error { return nc.RegisterGo(f.Name, f, 10+int(f.Volatility)) })
}

// RegisterScalarFunction registers a batch-oriented Go function. The signature is
// exact (including nested field metadata). Arguments are borrowed; the returned
// array transfers ownership to the engine. Callbacks must honor ctx cancellation.
// Built-in and already registered names cannot be replaced by this function.
func RegisterScalarFunction(ctx context.Context, conn *sql.Conn, f ScalarFunction) error {
	if conn == nil {
		return errors.New("datafusion sql connection is nil")
	}
	return withDataFusionConn(conn, func(c *Conn) error { return registerScalarFunction(ctx, c, f) })
}

// RegisterScalarFunction registers a batch-oriented function in this session.
func (s *Session) RegisterScalarFunction(ctx context.Context, f ScalarFunction) error {
	return registerScalarFunction(ctx, s.conn, f)
}

// CatalogProvider resolves remote tables once per query for planning. It does not
// enumerate remote schemas or guarantee transactions across a changing catalog.
// Return nil, nil for missing tables; honor the supplied context for remote I/O.
type CatalogProvider = native.CatalogProvider

func registerCatalog(ctx context.Context, conn *Conn, name string, catalog CatalogProvider) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if catalog == nil {
		return errors.New("datafusion catalog is nil")
	}
	return conn.withExtension(ctx, func(nc *native.Connection) error { return nc.RegisterGo(name, catalog, 20) })
}

// RegisterCatalog adds a lazily resolved catalog. Existing names cannot be replaced.
func RegisterCatalog(ctx context.Context, conn *sql.Conn, name string, catalog CatalogProvider) error {
	if conn == nil {
		return errors.New("datafusion sql connection is nil")
	}
	return withDataFusionConn(conn, func(c *Conn) error { return registerCatalog(ctx, c, name, catalog) })
}

// RegisterCatalog adds a lazy catalog to this session.
func (s *Session) RegisterCatalog(ctx context.Context, name string, catalog CatalogProvider) error {
	return registerCatalog(ctx, s.conn, name, catalog)
}

// Registration's duplicate check and publication are one session operation.
// Waiting for this lock never holds Conn.mu; shared pooled connections use the
// same lock as SQL mutations while isolated sessions stay independent.
func (conn *Conn) withExtension(ctx context.Context, fn func(*native.Connection) error) error {
	lock := &conn.ddlMu
	if conn.connector.sharedSession {
		lock = &conn.connector.ddlMu
	}
	if err := lock.Lock(ctx); err != nil {
		return err
	}
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return conn.withNative(fn)
}

// WritableTableProvider optionally accepts INSERT with a bounded input stream.
type WritableTableProvider = native.WritableTableProvider

// InsertOp identifies append, overwrite, or replace semantics.
type InsertOp = native.InsertOp

const (
	InsertAppend    = native.InsertAppend
	InsertOverwrite = native.InsertOverwrite
	InsertReplace   = native.InsertReplace
)

package native

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// TableProvider supplies an independent reader for each scan. Readers transfer
// ownership to the engine; buffers are copied before native code retains them.
// Implementations must support concurrent calls and honor context cancellation.
type TableProvider interface {
	Schema() *arrow.Schema
	Scan(context.Context) (array.RecordReader, error)
}

// ScanOptions describes exact projection and advisory pruning. A nil Projection
// means all columns; an empty non-nil slice means zero columns, preserving rows.
// Filters are ANDed and rechecked by DataFusion. Limit is -1 without a safe hint.
type ScanOptions struct {
	Projection []int
	Filters    []Expr
	Limit      int64
}

// PushdownTableProvider optionally accepts projection and conservative pruning.
// It must return precisely the projected schema, including field order.
type PushdownTableProvider interface {
	TableProvider
	ScanWithOptions(context.Context, ScanOptions) (array.RecordReader, error)
}

// Volatility controls optimizer assumptions for an explicitly registered UDF.
type Volatility int

const (
	Volatile  Volatility = iota // default: every call may return a different value
	Stable                      // identical arguments produce identical values within one query
	Immutable                   // identical arguments always produce identical values
)

// ScalarFunction is a typed, batch-oriented Go function. Evaluate owns its
// returned array; the bridge releases it. Arguments are borrowed for the call.
// rows is supplied separately so zero-argument functions can produce a batch.
// Calls may run concurrently. Blocking implementations must honor ctx.
type ScalarFunction struct {
	Name       string
	Arguments  []arrow.DataType
	ReturnType arrow.DataType
	Volatility Volatility
	Evaluate   func(ctx context.Context, args []arrow.Array, rows int) (arrow.Array, error)
}

// CatalogProvider lazily resolves tables into a per-query snapshot. Return nil,
// nil for a missing table. Remote I/O must honor ctx. Repeated references share
// one resolved provider within a query; subsequent queries resolve afresh.
// Discovery/listing is deliberately separate from query-time resolution.
type CatalogProvider interface {
	ResolveTable(ctx context.Context, schema, table string) (TableProvider, error)
}

// DiscoverableCatalogProvider optionally lists schemas and tables for SQL
// metadata queries. Names are literal identifiers, not SQL fragments. Listings
// are refreshed for each metadata query; ordinary table queries only resolve
// referenced tables. Implementations must support concurrent calls and honor ctx.
// A nil list is empty. Duplicate names are ignored; empty/NUL names are errors.
// The reserved information_schema schema is supplied by DataFusion.
type DiscoverableCatalogProvider interface {
	CatalogProvider
	SchemaNames(ctx context.Context) ([]string, error)
	TableNames(ctx context.Context, schema string) ([]string, error)
}

// InsertOp describes a requested write. Providers must reject unsupported modes.
type InsertOp int

const (
	InsertAppend InsertOp = iota
	InsertOverwrite
	InsertReplace
)

// WritableTableProvider can consume INSERT input. The reader is borrowed for this
// call and released by the bridge. Retain batches to keep them beyond the call.
// Implementations must check reader.Err(), honor cancellation, and finish their
// commit/rollback before returning. Errors are never retried by the bridge.
type WritableTableProvider interface {
	TableProvider
	InsertInto(context.Context, InsertOp, array.RecordReader) (uint64, error)
}

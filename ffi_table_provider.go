package datafusion

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"unsafe"

	"github.com/datafusion-contrib/datafusion-go/internal/native"
)

// RegisteredTable is a handle to a table registered on a connection by
// RegisterFFITableProvider. The handle is safe for concurrent use.
//
// The table's lifetime follows the session it was registered on, not this
// handle. Closing sqlConn returns its physical connection to the pool, and
// does not guarantee table release in either session mode. An isolated session
// is discarded when that connection is reset or physically closed; a shared
// session can outlive every individual sqlConn. Deregister explicitly while
// sqlConn is open when deterministic catalog removal is needed.
//
// Catalog removal does not invalidate already-created query plans, views,
// readers, or Arrow batches. Before unloading the producing library, callers
// must stop new queries, remove every registration and dependent view, close
// all dependent readers, and release their batches. The library must remain
// loaded until all foreign objects (including the original provider) are freed.
//
// Dropping the handle never deregisters the table: a caller may legitimately
// keep querying it after dropping the handle. Deregistration is therefore
// always explicit.
type RegisteredTable struct {
	sqlConn *sql.Conn
	name    string

	mu   sync.Mutex
	done bool
}

// RegisterFFITableProvider registers a foreign datafusion-ffi FFI_TableProvider,
// produced by another library, as a queryable table named tableName on the
// DataFusion connection backing sqlConn. Once registered, SQL executed on
// sqlConn can scan the table, and projection/filter predicates are pushed down
// into the foreign provider.
//
// The table is registered on sqlConn's session, following the usual session
// semantics: with WithSharedSession it is registered on the shared session and
// is therefore visible to every connection sharing it, not just sqlConn.
//
// provider must point to a valid, initialized datafusion-ffi FFI_TableProvider
// that is owned by the producing foreign library — memory allocated by that
// library (C/Rust), not Go heap memory. Registration clones callback pointers
// out of the provider and native code invokes them long after this call
// returns, so passing a Go-allocated pointer would violate cgo's pointer-passing
// rules and risk corruption once Go's garbage collector moves or frees it.
// providerDataFusionVersion is the datafusion version the library that produced
// provider was built against; obtain it from that library (not from
// DataFusionVersion). Registration fails with an error if it does not equal this
// package's DataFusionVersion — the two sides must link the same datafusion
// version for the provider's memory layout to be compatible. This check happens
// before provider is dereferenced, so a version mismatch is a clean error rather
// than a crash; it is cooperative and cannot detect a mislabeled provider.
//
// Lifetime: registration clones the provider (bumping its internal refcount), so
// the caller still owns the original FFI_TableProvider pointer and may free it
// through its producing library once this call returns. The producing library
// itself must stay loaded until every registration and dependent foreign object
// is released, including query plans, views, readers, and returned Arrow batches
// (see RegisteredTable). Deregister or closing sqlConn alone does not establish
// that lifetime. Unloading earlier leaves foreign callbacks dangling. The
// DataFusion session backing the provider should also outlive all dependent
// queries; if it does not, queries fail with an error.
func RegisterFFITableProvider(ctx context.Context, sqlConn *sql.Conn, tableName string, provider unsafe.Pointer, providerDataFusionVersion string) (*RegisteredTable, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sqlConn == nil {
		return nil, fmt.Errorf("datafusion sql connection is nil")
	}
	if tableName == "" {
		return nil, fmt.Errorf("datafusion ffi table name is empty")
	}
	if provider == nil {
		return nil, fmt.Errorf("datafusion ffi table provider is nil")
	}
	if providerDataFusionVersion == "" {
		return nil, fmt.Errorf("datafusion ffi provider datafusion version is empty")
	}

	if err := withDataFusionConn(sqlConn, func(conn *Conn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return conn.withNative(func(nc *native.Connection) error {
			return nc.RegisterFFITableProvider(tableName, provider, providerDataFusionVersion)
		})
	}); err != nil {
		return nil, err
	}

	return &RegisteredTable{sqlConn: sqlConn, name: tableName}, nil
}

// Name returns the table name this handle refers to.
func (t *RegisteredTable) Name() string {
	return t.name
}

// Deregister removes the table from the session it was registered on. Under
// WithSharedSession that is the shared session, so the table is removed for
// every connection sharing it, not just sqlConn. Calling it more than once is a
// no-op that returns nil, and deregistering a name that is no longer registered
// is not an error.
//
// Deregister removes the catalog entry in either session mode. Existing views,
// query plans, readers, and batches can retain foreign objects afterward, so
// the producing library must still outlive them. Deregister runs on sqlConn,
// which must remain open; a closed connection returns sql.ErrConnDone.
func (t *RegisteredTable) Deregister(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := withDataFusionConn(t.sqlConn, func(conn *Conn) error {
		return conn.withNative(func(nc *native.Connection) error {
			return nc.DeregisterTable(t.name)
		})
	}); err != nil {
		return err
	}

	t.done = true
	return nil
}

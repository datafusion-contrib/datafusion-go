# Architecture and invariants

The Go package implements `database/sql` and an Arrow API over the same query
execution path. Rust owns DataFusion sessions and execution. The C ABI is the
ownership boundary between them.

## Module map

| Area | Responsibility |
|---|---|
| `datafusion.go`, `connection.go`, `statement.go` | Connector, connection, and prepared-statement lifecycles; Go driver interfaces |
| `execution.go` | Argument normalization, DDL serialization, temporary versus cached statement ownership, native errors |
| `rows.go`, `result.go`, `arrow.go` | SQL rows, affected-row counts, and Arrow result adapters |
| `parameters.go` | Public parameter types and validation |
| `internal/native` | cgo calls, Arrow import/export, cancellation, library discovery and loading |
| `rust/src/abi.rs` | C entry points, opaque handles, raw-pointer validation and transfers |
| `rust/src/session.rs` | Session configuration and shared runtime ownership |
| `rust/src/query.rs` | SQL placeholders, parameter metadata, and statements requiring serialization |
| `rust/src/parameters.rs` | Copy borrowed C arguments into owned DataFusion scalars |
| `rust/src/registration.rs` | Import Arrow batches and register materialized tables |
| `rust/src/stream.rs` | Execution, cancellation, pull callbacks, and result lifetime |
| `rust/src/error.rs` | Error conversion and containment at the C boundary |

Internal Rust modules have crate visibility. Only the C entry points and opaque
ABI types are re-exported. Tests live beside the module they exercise; the
independent DataFusion oracle lives in `rust/tests`.

## Query execution

`Conn.QueryArrowContext` and prepared statements create a private
`queryOperation`. `runArrowQuery` normalizes arguments once, acquires any
connector serialization lock, executes, and transfers reader ownership to the
caller. A direct query closes its temporary native statement before returning.
A prepared query retains its cached statement and executes under `Stmt.mu`.
DataFusion's result retains its own session references in either case.

Readers expose a schema before the first read, including for empty results.
`Read` transfers one batch reference to the caller, which must release it.
`Close` is idempotent and cancels pending work. The SQL adapter validates the
schema immediately and closes the reader if adaptation fails.

## Ownership

| Object | Owner and release rule |
|---|---|
| Native database | Connector; close after all application use. Connections own independent references to its runtime and session. |
| Native connection | Go `Conn`; access under `Conn.mu`. Closing releases its reference, while active results retain theirs. |
| Native statement | Temporary query operation or Go `Stmt`; arguments belong to an execution, not the statement. |
| Cancellation token | Execution and result share an `Arc`; cancellation is idempotent and wakes blocked polling. |
| Native result | Arrow reader; exporting its stream transfers that stream exactly once. |
| Returned Arrow batch | Caller; may outlive the reader, statement, table registration, connection, and connector. |
| Registration input | Caller retains the reader. Copy registration creates Rust-owned buffers; zero-copy registration retains exported foreign buffers. |
| FFI table provider | Caller owns the original provider. DataFusion holds a clone; its producing library must outlive every dependent plan, stream, and batch. |
| Native error | Rust allocates it; the Go/C caller reads the borrowed strings and frees the error once. |

The C header contains the detailed ABI ownership contract. Finalizers provide
a fallback; callers must close resources explicitly for predictable release.

## Lock ordering and reset

`Conn.mu` precedes `Stmt.mu` whenever both are required. Neither is held while
waiting for the connector's DDL lock. The DDL lock transfers to the result
reader and stays held until close; failures before that transfer release it.
This includes reader-construction errors and context cancellation.

The shared-session initializer has a separate context-aware lock. Shared
sessions preserve their catalog across pool resets. An isolated reset closes
cached native statements, replaces the native connection, and runs its
initializer again. A Go prepared statement retains its SQL and prepares again
against the new session on its next execution. Failed reset makes the
connection unusable so `database/sql` can replace it.

The native reader mutex protects Arrow callback state. Cancellation can happen
before waiting for that mutex, so close can interrupt a blocked read. The
reader keeps the Rust runtime and session alive throughout each callback.

## Sources of truth

`versions.toml` remains the only human-maintained release/version source.
`rust/include/datafusion_go.h` is the source for C function signatures and
parameter layout. `internal/tools/genabi` derives the dynamic loader's function
list and Rust contract tests from this header. `make generate.check` rejects
drift. Rust tests compile function-pointer assignments for every declaration,
then compile a C probe to compare sizes, alignments, and parameter field offsets
with Rust on the current platform. The ordinary CI matrix runs this check on
each supported architecture.

The generator deliberately handles only this header's small declaration
grammar. Unsupported declarations fail generation and need explicit support;
the project does not maintain another ABI schema or expose a public plugin
interface for testing.

See [testing.md](testing.md) for regression oracles and verification commands.

# datafusion-go
DataFusion binding for the Go language

This repository currently pins the Rust `datafusion` crate at `53.1.0`.
Release tags encode that DataFusion version as `v0.530100.<driver-patch>`.

## Requirements

- Go with cgo enabled for normal use.
- A C toolchain for linking.
- Rust for local source builds and for rebuilding bundled static libraries.
- On Windows, use a MinGW/GNU C toolchain. The bundled Windows build targets Rust's `x86_64-pc-windows-gnu` ABI so Go cgo and Rust produce compatible static objects.

`CGO_ENABLED=0` is not supported; the package returns a clear `datafusion-go requires cgo` error in that mode.

## Development

Build and bundle the Rust shim before running Go tests:

```sh
make test
```

That target copies the generated native archive to `internal/native/lib/<goos>-<goarch>/libdatafusion_go.a`, which is the default static-link path. Native archives are build outputs and are not committed to Git.

To link directly from `rust/target/release` during local development:

```sh
make test.source
```

To verify a downloaded/bundled release artifact without rebuilding it into
`internal/native/lib`, run:

```sh
make verify.release.downloaded
```

The driver registers with Go's `database/sql` package as `datafusion`:

```go
db, err := sql.Open("datafusion", "")
```

## Linking Modes

Default builds use a generated static library selected from `internal/native/lib/<goos>-<goarch>`. Source checkouts should run `make bundle` or `make test` before invoking `go test` directly.

Development modes:

```sh
make rust
go test -tags=datafusion_use_source ./...
go test -tags=datafusion_use_static_lib ./...
```

`datafusion_use_source` and `datafusion_use_static_lib` link from `rust/target/release`, so run `make rust` first. On `windows-amd64`, `make rust` targets `x86_64-pc-windows-gnu` and the link path is `rust/target/x86_64-pc-windows-gnu/release`.

Shared-library mode links with `-ldatafusion_go` and requires `libdatafusion_go` to be on the system linker path:

```sh
CGO_LDFLAGS="-L/path/to/lib" go test -tags=datafusion_use_lib ./...
```

Runnable examples live under `examples/` for basic queries, typed parameters, and Arrow reader usage.

## DSN Semantics

Supported DSNs are empty string, `:memory:`, `?<options>`, `:memory:?<options>`, and `datafusion://memory?<options>`.

Query parameters are passed to DataFusion as session configuration options:

```text
:memory:?datafusion.execution.batch_size=8192
```

Driver-owned options use the `datafusion.go.` prefix and are stripped before the remaining options are passed to DataFusion. Connections from one `sql.DB` share a DataFusion `SessionContext` by default, so catalog changes such as `CREATE VIEW` are visible across pooled connections. Set `datafusion.go.shared_session=false` to restore isolated-per-connection session state:

```text
:memory:?datafusion.go.shared_session=false
```

File paths and other URL forms are rejected. DataFusion is not treated as a file-backed embedded database by this driver.

## SQL Parameters

The driver supports DataFusion SQL parameters through `database/sql`:

```go
row := db.QueryRowContext(ctx, "select ? + 1, ?", int64(41), "x")
```

Supported parameter values are `nil`, `bool`, signed integer types that fit `int64`, unsigned integer types as DataFusion `UInt64`, floating-point values as `float64`, `string`, `[]byte`, `time.Time` as `Timestamp[ns]`, and `time.Duration` as `Duration[ns]`. `time.Time` preserves loadable IANA locations such as `America/New_York`; fixed-offset, local, or otherwise non-loadable locations bind as UTC unless `TimestampWithTimeZone` is used. `float32` values are promoted to DataFusion `Float64` through `database/sql` conversion. Other values are rejected by `CheckNamedValue` before native execution.

Bare `nil` binds as DataFusion's untyped null. Use `NullOf`, `NullDecimal`, or `NullTimestamp` when DataFusion needs a concrete type, for example `select $1 + 1` with `NullOf(ParameterInt64)`.

Use typed parameter wrappers when inference would be ambiguous or when exact Arrow/DataFusion types matter:

```go
row := db.QueryRowContext(
	ctx,
	"select $1, $2, $3, $4, $5",
	datafusion.DateFromTime(day),
	datafusion.TimeFromTime(clock),
	datafusion.DurationFromTime(2*time.Second),
	datafusion.DecimalString("123.45", 10, 2),
	datafusion.NullOf(datafusion.ParameterInt64),
)
```

Available wrappers cover `UInt64`, `Date`, `Time`, `Timestamp`, `Duration`, `Decimal`, and typed nulls through `NullOf`, `NullDecimal`, and `NullTimestamp`. Wrappers expose accessor methods for logging and tests; validating constructors such as `NewTimeNanos` and `NewDecimalString` are available when callers want errors before query execution.

Prepared statements report `Stmt.NumInput` for `?` placeholders, `$1`/`$2` positional parameters, and distinct `$name` parameters. Repeated named parameters count once; each `?` occurrence counts as a separate positional parameter. Mixed question-mark, dollar-numbered, and named parameter styles are rejected during prepare.

Named statements must be executed with the matching `sql.Named` arguments. Positional arguments for named statements, named arguments for positional statements, missing names, extra names, and duplicate supplied names are rejected before query execution is handed to DataFusion.

## Arrow Reader

Arrow-native callers can use a closeable reader:

```go
reader, err := datafusion.QueryArrowContext(ctx, conn, "select $1", int64(42))
defer reader.Close()
```

Records returned by `Read` must be released by the caller.

The Arrow reader owns native stream resources and must be closed. Prefer `defer reader.Close()` immediately after checking the error from `QueryArrowContext`. A finalizer is installed as a leak safety net, but finalizers are not prompt cleanup and should not be relied on for normal resource management.

Arrow record readers can also be registered as in-memory DataFusion tables:

```go
rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{batch})
if err != nil {
	return err
}
defer rdr.Release()

if err := datafusion.RegisterArrowReader(ctx, conn, "events", rdr); err != nil {
	return err
}
```

`RegisterArrowReader` consumes the remaining batches from the reader, serializes them as an Arrow IPC stream, and registers decoded Rust-owned batches. The copy is intentional: a registered table can outlive the cgo call, while ordinary Go Arrow arrays can contain Go-owned buffers that native code must not retain.

`RegisterArrowReaderZeroCopy` skips the IPC copy by exporting the reader through the Arrow C Stream Interface. Use it only when every exported Arrow buffer is valid for native code to retain until the table is dropped or the owning session/connector closes, for example data built with Arrow Go's `memory/mallocator` package or another C/foreign allocator. Do not use the zero-copy API with ordinary Go-allocated Arrow buffers unless you fully own that lifetime and cgo pointer-safety tradeoff.

## Type Conversion

`database/sql` row conversion currently supports:

| Arrow type family | Go value |
| --- | --- |
| Null | `nil` |
| Bool | `bool` |
| Signed integers | `int64` |
| Unsigned integers | `int64` when in range |
| Float16/Float32/Float64 | `float64` |
| Utf8/LargeUtf8/StringView | `string` |
| Binary/LargeBinary/FixedSizeBinary/BinaryView | `[]byte` |
| Date/Time/Timestamp | `time.Time` |
| Duration | `int64` nanoseconds |
| Decimal | `string` |
| Intervals | `string` |

Column metadata is exposed through `database/sql` where Arrow schema information is precise: nullable columns use typed `sql.Null*` scan types where practical, fixed-size binary columns report length, decimal columns report precision and scale, and temporal/interval database type names include their Arrow unit or interval subtype. Variable-width string and binary columns do not report declared lengths because DataFusion's Arrow result schema does not preserve SQL declarations such as `VARCHAR(32)`.

Time-only values are returned as UTC `time.Time` values on the Unix epoch date. Duration values are returned as `int64` nanoseconds because `time.Duration` is not a legal `database/sql/driver.Value`. Interval values are returned as strings that preserve their month, day, millisecond, and nanosecond components.

Nested and complex Arrow values such as lists, structs, maps, unions, dictionaries, extensions, and run-end encoded values are rejected from `database/sql` row conversion when schema information is available; use the Arrow-native reader for exact batch data.

## Semantics

- `PrepareContext` validates SQL syntax with DataFusion's parser.
- A prepared query must contain exactly one SQL statement. Multiple result sets are not supported; `Rows.NextResultSet` reports no additional result sets. Execute migration scripts one statement at a time.
- `ExecStatements` executes an already-split statement slice for setup or migration-style code. The driver does not split SQL scripts.
- `Stmt.NumInput` reports parser-derived parameter counts for question-mark, dollar-numbered, or named parameters.
- Connections from one connector share a DataFusion `SessionContext` by default, matching normal `database/sql` expectations that catalog changes made on one pooled connection are visible to another. Use `datafusion.go.shared_session=false` or `WithSharedSession(false)` for isolated connection sessions.
- Shared sessions intentionally share session-scoped catalog/config mutations, including `CREATE VIEW`, `DROP VIEW`, and `SET`, across all connections from the same connector. Non-query statements are serialized at the connector level across `ExecContext`, `QueryContext`, and `QueryArrowContext`; queries can still observe normal ordering effects if they run concurrently with DDL.
- Pooled reuse implements `driver.SessionResetter`. Shared sessions validate the connection and preserve shared session state; isolated sessions recreate the underlying `SessionContext` and rerun the connector initialization callback when one was provided.
- Connector initialization can use `NewConnectorWithInitContext` when setup work needs the connection context or connector options such as `WithSharedSession(false)`; `NewConnector` keeps the legacy contextless callback form. In shared-session mode the initialization callback runs once per connector, and in isolated mode it runs for each connection/reset.
- Connections implement `driver.Validator`; closed connections are reported invalid before they return to the pool.
- Context cancellation is bridged to a native cancellation token and checked during planning, stream creation, and record batch reads.
- Native errors carry machine-readable error kinds across the C ABI and are exposed on `*datafusion.Error.NativeKind`.
- `errors.Is` can match native sentinel errors: `ErrNativeCancelled`, `ErrNativeInvalidArgument`, `ErrNativeFailure`, and `ErrNativePanic`.
- `RowsAffected` returns `0` by default and reports the sum of a single integer `count`, `rows_affected`, or `rowsaffected` output column when DataFusion emits one.
- `LastInsertId` returns `0, nil`; DataFusion does not expose insert IDs through this driver.
- `RegisterArrowReader` registers Go Arrow record readers as DataFusion in-memory tables through a safe IPC copy. `RegisterArrowReaderZeroCopy` is available for callers that can guarantee native-safe Arrow buffer lifetimes.
- Prepared statement handles are reused when callers use `db.Prepare` or `conn.PrepareContext`, but DataFusion still plans and executes each run; the driver does not cache DataFusion physical plans.
- `Close` is idempotent for connectors, connections, statements, rows, and Arrow readers.
- Transactions are unsupported and return explicit unsupported errors. `BeginTx` still honors an already-canceled context before returning the unsupported transaction error.

## Current Distribution Status

Native archives are generated by the build and intentionally kept out of Git because the static archives exceed GitHub's normal file-size limits. Release automation builds archives for `darwin-arm64`, `darwin-amd64`, `linux-amd64`, `linux-arm64`, and `windows-amd64`, writes `internal/native/lib/SHA256SUMS`, verifies it without rebundling the current runner's local archive, and can run as a dry run before publishing. When `publish` is enabled, it tags the source commit and uploads the generated archives plus checksums to the GitHub release.

Windows arm64 is not currently a bundled target. Before a public release, run the release workflow once with `publish=false`, verify that all release-runner labels are available in GitHub Actions, and confirm the generated native archives link from a clean consumer module on each target platform.

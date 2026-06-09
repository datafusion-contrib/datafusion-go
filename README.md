# datafusion-go

[![Go Reference](https://pkg.go.dev/badge/github.com/datafusion-contrib/datafusion-go.svg)](https://pkg.go.dev/github.com/datafusion-contrib/datafusion-go)
[![CI](https://github.com/datafusion-contrib/datafusion-go/actions/workflows/ci.yml/badge.svg)](https://github.com/datafusion-contrib/datafusion-go/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/datafusion-contrib/datafusion-go)](https://goreportcard.com/report/github.com/datafusion-contrib/datafusion-go)

`datafusion-go` is a Go `database/sql` driver and Arrow bridge backed by Apache DataFusion's in-process query engine.

Use it when you want analytic SQL from Go without running a database server, while still having access to Arrow record batches for callers that need exact Arrow schemas or complex values.

## Status

- Current module version: `v0.530100.1`.
- Bundled Apache DataFusion version: `53.1.0`.
- Minimum Go version: `1.24`.
- Normal execution requires cgo and a C toolchain.
- Transactions are not supported.
- The DSN opens an in-process DataFusion session, not a remote host or file-backed embedded database.
- Release automation publishes native libraries for `darwin-arm64`, `darwin-amd64`, `linux-amd64`, `linux-arm64`, and `windows-amd64`. Windows arm64 is not currently bundled.

Release metadata is maintained in [versions.toml](versions.toml). Release tags are derived as `v<major>.<encoded-datafusion-version>.<patch>`, where DataFusion `53.1.0` encodes as `530100`.

## Why datafusion-go?

Apache DataFusion is a Rust query engine built around Arrow. `datafusion-go` wraps it for Go applications that already use `database/sql`, but it does not force every result through scalar row conversion.

The package has two main paths:

- `database/sql` for ordinary SQL execution, prepared statements, connection pooling, and scalar row scanning.
- Arrow-native APIs for streaming `arrow.RecordBatch` values and registering Go Arrow readers as DataFusion tables.

The driver is intentionally in-process. It is useful for local analytic execution, tests, tools, embedded query features, and services that want DataFusion inside the Go process. It is not a network database client, and it does not make DataFusion behave like a persistent file database.

## Install

```sh
go get github.com/datafusion-contrib/datafusion-go
```

Install from a tagged release for normal consumer use. Pseudo-versions from `@main` are development snapshots and may not have matching GitHub Release assets for the native runtime downloader.

Normal builds require:

- Go with `CGO_ENABLED=1`.
- A C toolchain for the target platform.
- A compatible `libdatafusion_go` native library at runtime.

Default builds use cgo but do not link DataFusion at Go link time. At runtime, the driver looks for a shared native library in this order:

1. `DATAFUSION_GO_LIBRARY`, if set.
2. `internal/native/lib/<goos>-<goarch>/` in a source checkout.
3. A checksum-verified library downloaded from the matching GitHub Release into the user cache.

Set `DATAFUSION_GO_NO_DOWNLOAD=1` to disable automatic release-asset downloads. Set `DATAFUSION_GO_DOWNLOAD_BASE` to override the release download base URL.

`CGO_ENABLED=0` is not supported for normal use; the package returns a clear `datafusion-go requires cgo` error in that mode.

## Quick Start

Import the driver for registration and open the `datafusion` driver with an empty DSN:

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/datafusion-contrib/datafusion-go"
)

func main() {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var one int64
	if err := db.QueryRowContext(context.Background(), "select 1").Scan(&one); err != nil {
		log.Fatal(err)
	}

	fmt.Println(one)
}
```

Runnable examples are available in:

- [examples/simple](examples/simple)
- [examples/parameters](examples/parameters)
- [examples/arrow](examples/arrow)

## Core Concepts

### Driver and Sessions

The package registers itself with `database/sql` as `datafusion`.

```go
db, err := sql.Open("datafusion", "")
```

A `sql.DB` opened by one connector shares a DataFusion `SessionContext` by default. This matches typical `database/sql` expectations: catalog changes such as `CREATE VIEW` are visible across pooled connections from the same `sql.DB`.

Use isolated sessions when you want each physical connection to have its own DataFusion session state:

```go
db, err := sql.Open("datafusion", "?datafusion.go.shared_session=false")
```

Or configure it directly on a connector:

```go
connector, err := datafusion.NewConnectorWithInitContext(
	"",
	nil,
	datafusion.WithSharedSession(false),
)
```

### DSNs

Supported DSNs are:

- `""`
- `?<options>`
- `datafusion://`
- `datafusion://?<options>`

Query parameters are passed to DataFusion as session configuration options:

```text
?datafusion.execution.batch_size=8192
```

Driver-owned options use the `datafusion.go.` prefix and are stripped before the remaining options are passed to DataFusion:

```text
?datafusion.go.shared_session=false
```

File paths, hosts, and other URL forms are rejected. The URL form exists only to carry session options.

### Query Paths

Use ordinary `database/sql` calls when rows contain scalar Arrow values that can be represented as `database/sql/driver.Value` values:

```go
rows, err := db.QueryContext(ctx, "select 1 as value")
```

Use `QueryArrowContext` when you need exact Arrow schemas, record-batch streaming, or complex Arrow values that `database/sql` cannot scan:

```go
conn, err := db.Conn(ctx)
if err != nil {
	return err
}
defer conn.Close()

reader, err := datafusion.QueryArrowContext(ctx, conn, "select 1 as value")
if err != nil {
	return err
}
defer reader.Close()
```

Records returned by `Read` must be released by the caller.

## Common Usage

### Initialization

Use `NewConnector` or `NewConnectorWithInitContext` when a pooled database needs setup SQL before use.

```go
connector, err := datafusion.NewConnectorWithInitContext(
	"",
	func(ctx context.Context, exec driver.ExecerContext) error {
		_, err := exec.ExecContext(ctx, "create view nums as select 1 as n", nil)
		return err
	},
)
if err != nil {
	return err
}
defer connector.Close()

db := sql.OpenDB(connector)
defer db.Close()
```

In shared-session mode, the initialization callback runs once per connector. In isolated-session mode, it runs for each connection and reset.

### Multiple Setup Statements

DataFusion prepares one SQL statement at a time. Split migration or setup scripts before calling the driver, then execute the statements in order:

```go
err := datafusion.ExecStatements(ctx, db, []string{
	"create view one as select 1 as n",
	"create view two as select 2 as n",
})
```

The helper skips blank statements and wraps errors with the statement index.

### Context Cancellation

Query contexts are bridged to native cancellation. Cancellation is checked during planning, stream creation, and record-batch reads.

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

rows, err := db.QueryContext(ctx, "select * from some_large_table")
```

Native cancellation errors can be matched with `errors.Is(err, datafusion.ErrNativeCancelled)`.

## SQL Parameters

The driver supports DataFusion SQL parameters through `database/sql`:

```go
row := db.QueryRowContext(ctx, "select ? + 1, ?", int64(41), "x")
```

Supported ordinary parameter values are:

- `nil`
- `bool`
- signed integer types that fit `int64`
- unsigned integer types as DataFusion `UInt64`
- floating-point values as `float64`
- `string`
- `[]byte`
- `time.Time` as `Timestamp[ns]`
- `time.Duration` as `Duration[ns]`

`time.Time` preserves loadable IANA locations such as `America/New_York`. Fixed-offset, local, or otherwise non-loadable locations bind as UTC unless `TimestampWithTimeZone` is used. `float32` values are promoted to DataFusion `Float64` through `database/sql` conversion.

Other values are rejected by `CheckNamedValue` before native execution.

Use typed wrappers when inference would be ambiguous or when exact Arrow/DataFusion types matter:

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

Available wrappers cover `UInt64`, `Date`, `Time`, `Timestamp`, `Duration`, `Decimal`, and typed nulls through `NullOf`, `NullDecimal`, and `NullTimestamp`.

Bare `nil` binds as DataFusion's untyped null. Use `NullOf`, `NullDecimal`, or `NullTimestamp` when DataFusion needs a concrete type, for example `select $1 + 1` with `NullOf(ParameterInt64)`.

Prepared statements report `Stmt.NumInput` for `?` placeholders, `$1`/`$2` positional parameters, and distinct `$name` parameters. Repeated named parameters count once; each `?` occurrence counts as a separate positional parameter. Mixed question-mark, dollar-numbered, and named parameter styles are rejected during prepare.

Named statements must be executed with matching `sql.Named` arguments. Positional arguments for named statements, named arguments for positional statements, missing names, extra names, and duplicate supplied names are rejected before query execution is handed to DataFusion.

## Arrow Native Usage

### Query Arrow Batches

`QueryArrowContext` executes SQL on a `*sql.Conn` and returns a closeable Arrow reader:

```go
reader, err := datafusion.QueryArrowContext(ctx, conn, "select $1", int64(42))
if err != nil {
	return err
}
defer reader.Close()

for {
	record, err := reader.Read()
	if err == io.EOF {
		break
	}
	if err != nil {
		return err
	}
	// Use record.
	record.Release()
}
```

The Arrow reader owns native stream resources and must be closed. A finalizer is installed as a leak safety net, but finalizers are not prompt cleanup and should not be relied on for normal resource management.

### Register Arrow Tables

Arrow record readers can be registered as in-memory DataFusion tables:

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

`RegisterArrowReaderZeroCopy` skips the IPC copy by exporting the reader through the Arrow C Stream Interface. Use it only when every exported Arrow buffer is valid for native code to retain until the table is dropped or the owning session/connector closes, for example data built with Arrow Go's `memory/mallocator` package or another C/foreign allocator.

Do not use the zero-copy API with ordinary Go-allocated Arrow buffers unless you fully own that lifetime and cgo pointer-safety tradeoff.

## API Overview

Important exported APIs:

- `Driver`: the `database/sql/driver.Driver` registered as `datafusion`.
- `Connector`: owns the native DataFusion database handle used to open pooled connections.
- `NewConnector`: creates a connector with an optional initialization callback.
- `NewConnectorWithInitContext`: creates a connector whose initialization callback receives a context.
- `WithSharedSession`: controls whether connections from one connector share a DataFusion `SessionContext`.
- `ExecStatements`: executes an already-split slice of SQL statements.
- `QueryArrowContext`: streams Arrow record batches from a `*sql.Conn`.
- `RegisterArrowReader`: safely registers a Go Arrow reader as a DataFusion table through IPC copy.
- `RegisterArrowReaderZeroCopy`: registers Arrow data without IPC copy when native buffer lifetime is guaranteed.
- `Error`: structured driver errors with operation type and native error kind.
- `DataFusionVersion` and `DataFusionGoVersion`: generated version constants.

See the [Go package documentation](https://pkg.go.dev/github.com/datafusion-contrib/datafusion-go) for the complete API reference.

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

Column metadata is exposed through `database/sql` where Arrow schema information is precise: nullable columns use typed `sql.Null*` scan types where practical, fixed-size binary columns report length, decimal columns report precision and scale, and temporal/interval database type names include their Arrow unit or interval subtype.

Variable-width string and binary columns do not report declared lengths because DataFusion's Arrow result schema does not preserve SQL declarations such as `VARCHAR(32)`.

Time-only values are returned as UTC `time.Time` values on the Unix epoch date. Duration values are returned as `int64` nanoseconds because `time.Duration` is not a legal `database/sql/driver.Value`. Interval values are returned as strings that preserve their month, day, millisecond, and nanosecond components.

Nested and complex Arrow values such as lists, structs, maps, unions, dictionaries, extensions, and run-end encoded values are rejected from `database/sql` row conversion when schema information is available. Use the Arrow-native reader for exact batch data.

## Semantics and Limits

- `PrepareContext` validates SQL syntax with DataFusion's parser.
- A prepared query must contain exactly one SQL statement. Multiple result sets are not supported; `Rows.NextResultSet` reports no additional result sets.
- `Stmt.NumInput` reports parser-derived parameter counts for question-mark, dollar-numbered, or named parameters.
- Connections from one connector share a DataFusion `SessionContext` by default.
- Shared sessions intentionally share session-scoped catalog/config mutations, including `CREATE VIEW`, `DROP VIEW`, and `SET`, across all connections from the same connector.
- Non-query statements are serialized at the connector level across `ExecContext`, `QueryContext`, and `QueryArrowContext`; queries can still observe normal ordering effects if they run concurrently with DDL.
- Pooled reuse implements `driver.SessionResetter`. Shared sessions validate the connection and preserve shared session state; isolated sessions recreate the underlying `SessionContext` and rerun the connector initialization callback when one was provided.
- Connections implement `driver.Validator`; closed connections are reported invalid before they return to the pool.
- Native errors carry machine-readable error kinds across the C ABI and are exposed on `*datafusion.Error.NativeKind`.
- `errors.Is` can match native sentinel errors: `ErrNativeCancelled`, `ErrNativeInvalidArgument`, `ErrNativeFailure`, and `ErrNativePanic`.
- `RowsAffected` returns `0` by default and reports the sum of a single integer `count`, `rows_affected`, or `rowsaffected` output column when DataFusion emits one.
- `LastInsertId` returns `0, nil`; DataFusion does not expose insert IDs through this driver.
- Prepared statement handles are reused when callers use `db.Prepare` or `conn.PrepareContext`, but DataFusion still plans and executes each run; the driver does not cache DataFusion physical plans.
- `Close` is idempotent for connectors, connections, statements, rows, and Arrow readers.
- Transactions are unsupported and return explicit unsupported errors. `BeginTx` still honors an already-canceled context before returning the unsupported transaction error.

## Linking and Native Runtime

Default builds dynamically load a shared native library at runtime. This keeps ordinary Go builds from linking the DataFusion native library directly.

Source checkouts should run `make bundle` or `make test` before invoking Go tests that open the driver directly:

```sh
make test
```

`make test` builds the Rust shim, copies the generated native archive and shared library to `internal/native/lib/<goos>-<goarch>/`, and runs the dynamic and bundled Go test paths.

Development link modes:

```sh
make rust
go test -tags=datafusion_use_bundled ./...
go test -tags=datafusion_use_source ./...
go test -tags=datafusion_use_static_lib ./...
```

- `datafusion_use_bundled` links the static archive from `internal/native/lib/<goos>-<goarch>`.
- `datafusion_use_source` and `datafusion_use_static_lib` link from `rust/target/release`, so run `make rust` first.
- On `windows-amd64`, `make rust` targets Rust's `x86_64-pc-windows-gnu` ABI so Go cgo and Rust produce compatible static objects.

Shared-library link mode links with `-ldatafusion_go` and requires `libdatafusion_go` to be on the system linker path:

```sh
CGO_LDFLAGS="-L/path/to/lib" go test -tags=datafusion_use_lib ./...
```

## Compatibility and Versioning

`versions.toml` is the only human-maintained release/version source.

After changing it, regenerate derived Go/Rust version files and the Rust lockfile:

```sh
make generate
make generate.check
```

Do not hand-edit generated version constants in:

- [version.go](version.go)
- [internal/native/version_generated.go](internal/native/version_generated.go)
- [rust/src/generated.rs](rust/src/generated.rs)
- generated version/dependency fields in [rust/Cargo.toml](rust/Cargo.toml)

Generated Go/Rust files and [rust/Cargo.lock](rust/Cargo.lock) updates should be committed with the `versions.toml` change.

## Troubleshooting

### `datafusion-go requires cgo`

Enable cgo for normal execution:

```sh
CGO_ENABLED=1 go test ./...
```

### Native library not found

If automatic download is disabled or the release does not publish an asset for your platform, either set `DATAFUSION_GO_LIBRARY` or build a local library:

```sh
make bundle
DATAFUSION_GO_LIBRARY="$PWD/internal/native/lib/$(go env GOOS)-$(go env GOARCH)/libdatafusion_go.so" go test ./...
```

On macOS, use `libdatafusion_go.dylib`. On Windows, use `datafusion_go.dll`.

### Local checkout tests fail before opening the driver

Run the project build/test target so `internal/native/lib/<goos>-<goarch>/` is populated:

```sh
make test
```

### DSN rejected

Use an empty DSN or a session-options DSN. Paths, hosts, and file URLs are intentionally unsupported.

```go
sql.Open("datafusion", "")
sql.Open("datafusion", "?datafusion.execution.batch_size=8192")
sql.Open("datafusion", "datafusion://?datafusion.go.shared_session=false")
```

### `database/sql` cannot scan a result column

The `database/sql` path rejects complex Arrow values. Use `QueryArrowContext` to read exact Arrow record batches.

### Windows build or test failures

Use a MinGW/GNU C toolchain. The bundled Windows build targets Rust's `x86_64-pc-windows-gnu` ABI.

## Development

Run linting:

```sh
make lint
```

Run the default Go test path:

```sh
make test
```

Run alternate Go link-mode tests:

```sh
make test.source
make test.static
```

Run Rust tests:

```sh
cargo test --manifest-path rust/Cargo.toml --release
```

For release-sensitive changes, prefer the full release verification targets documented in [CONTRIBUTING.md](CONTRIBUTING.md).

## Distribution

Native libraries are generated by the build and intentionally kept out of Git because the static archives exceed GitHub's normal file-size limits and Go module zips have a 500 MiB source limit.

Release automation builds static archives and shared libraries for `darwin-arm64`, `darwin-amd64`, `linux-amd64`, `linux-arm64`, and `windows-amd64`, verifies them without rebundling the current runner's local output, writes the release-asset `SHA256SUMS` manifest into the tagged tree, and uploads the generated native files plus checksums to the GitHub release.

Before a public release, run the release workflow once with `publish=false` and verify that all release-runner labels are available in GitHub Actions.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, testing, version-bump, and release instructions.

## License

This project is licensed under the terms in [LICENSE](LICENSE).

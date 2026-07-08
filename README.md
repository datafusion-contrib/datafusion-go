# datafusion-go

[![Go Reference](https://pkg.go.dev/badge/github.com/datafusion-contrib/datafusion-go.svg)](https://pkg.go.dev/github.com/datafusion-contrib/datafusion-go)
[![CI](https://github.com/datafusion-contrib/datafusion-go/actions/workflows/ci.yml/badge.svg)](https://github.com/datafusion-contrib/datafusion-go/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/datafusion-contrib/datafusion-go)](https://goreportcard.com/report/github.com/datafusion-contrib/datafusion-go)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## What is datafusion-go?

datafusion-go is in-process analytic SQL for Go, powered by [Apache DataFusion](https://datafusion.apache.org/) — an extensible query engine written in Rust that uses [Apache Arrow](https://arrow.apache.org/) as its in-memory format. It ships as a standard `database/sql` driver with an Arrow-native escape hatch: no server, no network connection, nothing else to run.

Out of the box, datafusion-go offers the familiar `database/sql` interface, analytic SQL over CSV and Parquet files, Arrow record batches in and out, and prebuilt native runtimes for macOS, Linux, and Windows that install themselves on first use. DataFusion's columnar, multi-threaded, vectorized execution engine does the heavy lifting; the driver's job is to make it feel like Go.

That makes datafusion-go great for building data-heavy tools and CLIs, tests for analytic pipelines, services that want embedded query features, and more. It is not a network database client, and it does not turn DataFusion into a persistent file database: catalog and session state live in memory and last only as long as the process.

Here are links to some important information:

- [Go package documentation](https://pkg.go.dev/github.com/datafusion-contrib/datafusion-go)
- [Runnable examples](examples)
- [Apache DataFusion documentation](https://datafusion.apache.org/) and its [SQL reference](https://datafusion.apache.org/user-guide/sql/index.html)
- [Contributor guide](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)

datafusion-go is a community-maintained binding in the [datafusion-contrib](https://github.com/datafusion-contrib) organization, alongside other bindings and extensions built around Apache DataFusion.

## How do I install and use it?

### Install

```sh
go get github.com/datafusion-contrib/datafusion-go
```

Requires Go 1.24+ with cgo enabled (the default) and a C toolchain. On supported platforms — `darwin-arm64`, `darwin-amd64`, `linux-amd64`, `linux-arm64`, and `windows-amd64` — there is no other setup: the driver downloads the matching `libdatafusion_go` native library from the module's GitHub release and checksum-verifies it on first use. See [Native Runtime](#native-runtime) to override resolution or disable downloads.

Install from a tagged release for normal consumer use. Pseudo-versions from `@main` are development snapshots and may not have matching GitHub Release assets for the native runtime downloader; source checkouts build the library locally with `make bundle` instead.

### Quick Start

Give it a CSV file:

```sh
printf 'city,trips\nnyc,3\nnyc,5\nsf,2\n' > trips.csv
```

Register the file as a table and run analytic SQL against it:

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
	ctx := context.Background()

	db, err := sql.Open("datafusion", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, `create external table trips
		stored as csv location 'trips.csv'
		options ('format.has_header' 'true')`)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, `select city, sum(trips) as total
		from trips group by city order by total desc`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var city string
		var total int64
		if err := rows.Scan(&city, &total); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\t%d\n", city, total)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
```

```text
nyc	8
sf	2
```

The same pattern works for Parquet (`stored as parquet`).

### Examples

More runnable examples live in [examples/](examples): [simple](examples/simple), [parameters](examples/parameters), and [arrow](examples/arrow). Try one without cloning anything:

```sh
go run github.com/datafusion-contrib/datafusion-go/examples/simple@latest
```

From a source checkout, run `make bundle` once, then `go run ./examples/simple`.

**Finding your way:** [Loading Data](#loading-data) covers most use beyond the quick start. Arrow interop is under [Arrow-Native Usage](#arrow-native-usage), exact scanning behavior under [Type Conversion](#type-conversion) and [Semantics and Limits](#semantics-and-limits), and building from source under [Native Runtime](#native-runtime) and [CONTRIBUTING.md](CONTRIBUTING.md).

## Philosophy

- **In-process by design.** The driver embeds a query engine in your process rather than connecting to one somewhere else. It suits local analytic execution, tests, tools, embedded query features, and services that want DataFusion inside the Go process — and it deliberately refuses DSNs that look like hosts or file paths rather than pretending to be a client.
- **Standard interfaces first.** Everything that `database/sql` can express goes through `database/sql`: pooling, prepared statements, parameters, cancellation. Arrow-native APIs exist as an escape hatch for what it cannot express — exact schemas, complex types, record-batch streaming — not as a parallel API surface.
- **Zero setup, no surprises.** Prebuilt native libraries download and checksum-verify themselves on first use, and every step of that is overridable: point at your own library, disable downloads, or build from source.
- **A query engine, not a database.** There is no persistence, no transactions, and no insert IDs. Where DataFusion doesn't provide a behavior, the driver returns an explicit unsupported error instead of emulating it. Files are the durable layer: `CREATE EXTERNAL TABLE` reads them, `COPY ... TO` writes them.
- **Track upstream faithfully.** Each release bundles a specific DataFusion version, encoded in the module version itself — see [Versioning](#versioning).

## Features

- **Standard `database/sql` driver** — registered as `datafusion`, with connection pooling, prepared statements, SQL parameters, and context cancellation.
- **Files as tables** — `CREATE EXTERNAL TABLE` over CSV and Parquet files, `COPY ... TO` to write results back out.
- **Arrow-native APIs** — stream results as `arrow.RecordBatch` values and register Go Arrow readers as DataFusion tables, with a zero-copy option.
- **Typed SQL parameters** — `?`, `$1`, and `$name` styles, plus typed wrappers for dates, times, timestamps, durations, decimals, and typed nulls.
- **Context cancellation end to end** — Go contexts bridge to native cancellation during planning, stream creation, and record-batch reads.
- **Structured errors** — machine-readable native error kinds, matchable with `errors.Is`.
- **Zero-setup native runtime** — prebuilt libraries for macOS, Linux, and Windows are downloaded and checksum-verified automatically on first use.

## User Guide

### Loading Data

Data reaches a session two ways: SQL DDL over files, or Arrow registration from Go memory. Both are session-scoped — see [Driver and Sessions](#driver-and-sessions) for how catalog state is shared across pooled connections.

#### Files

`CREATE EXTERNAL TABLE` registers a file as a queryable table. CSV and Parquet work out of the box:

```go
_, err := db.ExecContext(ctx, `create external table events
	stored as parquet location '/data/events.parquet'`)
```

`COPY` writes query results back to files:

```go
_, err := db.ExecContext(ctx, `copy (select * from events where kind = 'click')
	to '/data/clicks.parquet' stored as parquet`)
```

File paths appear only in SQL; the DSN never carries them — see [DSNs](#dsns).

#### Go Memory

Register any Go Arrow record reader as an in-memory table with `RegisterArrowReader`, or skip the defensive copy with `RegisterArrowReaderZeroCopy` when buffer lifetimes allow it. See [Register Arrow Tables](#register-arrow-tables).

### Driver and Sessions

The package registers itself with `database/sql` as `datafusion`.

```go
db, err := sql.Open("datafusion", "")
```

A `sql.DB` opened by one connector shares a DataFusion `SessionContext` by default. This matches typical `database/sql` expectations: catalog changes such as `CREATE VIEW` or `CREATE EXTERNAL TABLE` are visible across pooled connections from the same `sql.DB`.

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

The DSN opens an in-process DataFusion session, not a remote host or file-backed embedded database. Query parameters are passed to DataFusion as session configuration options:

```text
?datafusion.execution.batch_size=8192
```

Driver-owned options use the `datafusion.go.` prefix and are stripped before the remaining options are passed to DataFusion:

```text
?datafusion.go.shared_session=false
```

File paths, hosts, and other URL forms are rejected. The URL form exists only to carry session options; files are queried through SQL instead — see [Loading Data](#loading-data).

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

### SQL Parameters

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

### Arrow-Native Usage

#### Query Arrow Batches

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

#### Register Arrow Tables

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

### API Overview

The most important exported APIs:

- `NewConnector` / `NewConnectorWithInitContext`: pooled databases with setup SQL and connector options such as `WithSharedSession`.
- `QueryArrowContext`: streams Arrow record batches from a `*sql.Conn`.
- `RegisterArrowReader` / `RegisterArrowReaderZeroCopy`: register Go Arrow readers as DataFusion tables.
- `ExecStatements`: executes an already-split slice of SQL statements.
- `Error` and the `ErrNative*` sentinels: structured driver errors with operation type and native error kind.

See the [Go package documentation](https://pkg.go.dev/github.com/datafusion-contrib/datafusion-go) for the complete API reference.

### Type Conversion

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

### Semantics and Limits

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

### Native Runtime

Default builds use cgo but do not link DataFusion at Go link time. At runtime, the driver loads a shared `libdatafusion_go` library, resolved in this order:

1. `DATAFUSION_GO_LIBRARY`, if set.
2. `internal/native/lib/<goos>-<goarch>/` in a source checkout.
3. A checksum-verified library downloaded from the matching GitHub Release into the user cache.

Environment variables:

- `DATAFUSION_GO_LIBRARY` — explicit path to the shared library.
- `DATAFUSION_GO_NO_DOWNLOAD=1` — disable automatic release-asset downloads.
- `DATAFUSION_GO_DOWNLOAD_BASE` — override the release download base URL.

Prebuilt native libraries are published for `darwin-arm64`, `darwin-amd64`, `linux-amd64`, `linux-arm64`, and `windows-amd64`. Windows arm64 is not currently bundled.

`CGO_ENABLED=0` is not supported for normal use; the package returns a clear `datafusion-go requires cgo` error in that mode.

Source checkouts should run `make bundle` or `make test` before invoking Go tests or examples that open the driver directly; both build the Rust shim and copy the native archive and shared library into `internal/native/lib/<goos>-<goarch>/`. Alternative link modes (bundled static archive, source, static-lib, and system shared-library linking) are documented in [CONTRIBUTING.md](CONTRIBUTING.md).

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

Use an empty DSN or a session-options DSN. Paths, hosts, and file URLs are intentionally unsupported; register files through SQL instead — see [Loading Data](#loading-data).

```go
sql.Open("datafusion", "")
sql.Open("datafusion", "?datafusion.execution.batch_size=8192")
sql.Open("datafusion", "datafusion://?datafusion.go.shared_session=false")
```

### `database/sql` cannot scan a result column

The `database/sql` path rejects complex Arrow values. Use `QueryArrowContext` to read exact Arrow record batches.

### Windows build or test failures

Use a MinGW/GNU C toolchain. The bundled Windows build targets Rust's `x86_64-pc-windows-gnu` ABI.

## Developing

Run `make lint` and `make test` before sending changes. Setup, the full test matrix, native link modes, version bumps, and the release process are documented in [CONTRIBUTING.md](CONTRIBUTING.md). Questions, bug reports, and pull requests are welcome on GitHub.

## Versioning

Release tags encode the bundled DataFusion version as `v<major>.<encoded-datafusion-version>.<patch>`: DataFusion `53.1.0` encodes as `530100`, so `v0.530100.1` bundles DataFusion `53.1.0`. Release metadata is maintained in [versions.toml](versions.toml); the version-bump and release workflow is documented in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

# datafusion-go

[![Go Reference](https://pkg.go.dev/badge/github.com/datafusion-contrib/datafusion-go.svg)](https://pkg.go.dev/github.com/datafusion-contrib/datafusion-go)
[![CI](https://github.com/datafusion-contrib/datafusion-go/actions/workflows/ci.yml/badge.svg)](https://github.com/datafusion-contrib/datafusion-go/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/datafusion-contrib/datafusion-go)](https://goreportcard.com/report/github.com/datafusion-contrib/datafusion-go)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

datafusion-go provides a `database/sql` driver and Arrow APIs for [Apache DataFusion](https://datafusion.apache.org/). It is an unofficial community binding in [datafusion-contrib](https://github.com/datafusion-contrib).

[Go API reference](https://pkg.go.dev/github.com/datafusion-contrib/datafusion-go) |
[User guide](#user-guide) |
[Releases](https://github.com/datafusion-contrib/datafusion-go/releases) |
[Changelog](CHANGELOG.md) |
[Contributing](CONTRIBUTING.md)

## Install

The package requires Go 1.25 or newer, a C toolchain, and cgo enabled. On Windows, use a MinGW/GNU toolchain for the `x86_64-pc-windows-gnu` Rust ABI.

Prebuilt native libraries support these platforms:

| Operating system | Go platforms |
| --- | --- |
| macOS | `darwin-arm64`, `darwin-amd64` |
| Linux | `linux-amd64`, `linux-arm64` |
| Windows | `windows-amd64` |

The shell examples use POSIX shell syntax.

If you do not have a Go module, run these commands in order:

```sh
mkdir datafusion-quickstart
cd datafusion-quickstart
go mod init example.com/datafusion-quickstart
```

From your module directory, install the package:

```sh
go get github.com/datafusion-contrib/datafusion-go
```

On first use, the driver downloads `libdatafusion_go` from the GitHub release for the installed module version. It verifies the library checksum. For library selection and download controls, see [Native Runtime](#native-runtime).

Use a tagged release for applications. Development snapshots from `@main` can lack native libraries with the same version.

## Quick Start

Use the module directory from [Install](#install) for this example.

1. Create `trips.csv`:

	```sh
	printf 'city,trips\nnyc,3\nnyc,5\nsf,2\n' > trips.csv
	```

2. Save this code as `main.go` in the same directory:

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

3. Run the program:

	```sh
	go run .
	```

	The output is:

	```text
	nyc	8
	sf	2
	```

More examples: [simple queries](examples/simple), [parameters](examples/parameters), and [Arrow](examples/arrow).

## User Guide

This guide describes the Go binding. For SQL syntax and session options, see DataFusion's [SQL reference](https://datafusion.apache.org/user-guide/sql/index.html) and [configuration reference](https://datafusion.apache.org/user-guide/configs.html).

| Go API | Guide |
| --- | --- |
| `sql.Open` / `sql.OpenDB` | [Sessions](#driver-and-sessions), [DSNs](#dsns), and [initialization](#initialization) |
| `QueryContext` / `QueryRowContext` | [Parameters](#sql-parameters) and [type conversion](#type-conversion) |
| `QueryArrowContext` | [Arrow batches](#query-arrow-batches) |
| `RegisterArrowReader` / `RegisterArrowReaderZeroCopy` | [Arrow tables and buffer ownership](#register-arrow-tables) |
| `RegisterFFITableProvider` | [Foreign table providers](#register-foreign-ffi-table-providers) |
| `ExecStatements` | [Multiple setup statements](#multiple-setup-statements) |

For driver limits and deployment, see [Semantics and Limits](#semantics-and-limits) and [Native Runtime](#native-runtime).

### Driver and Sessions

By default, connections from one connector share a DataFusion `SessionContext`. Catalog and configuration changes apply across pooled connections from the same `sql.DB`.

If each physical connection must have its own session state, use isolated sessions:

```go
db, err := sql.Open("datafusion", "?datafusion.go.shared_session=false")
```

To select isolated sessions on a connector, use `WithSharedSession(false)`:

```go
connector, err := datafusion.NewConnectorWithInitContext(
	"",
	nil,
	datafusion.WithSharedSession(false),
)
```

When you close a `*sql.Conn`, its physical connection returns to the pool. On reuse, shared sessions keep their state. The driver resets isolated sessions and prepares cached statements again. Connection closure does not immediately release registered tables or Arrow batches that callers still hold.

By default, the memory pool for DataFusion queries has no limit. Set a memory budget in the connector initializer:

```go
connector, err := datafusion.NewConnectorWithInitContext("",
	func(ctx context.Context, exec driver.ExecerContext) error {
		_, err := exec.ExecContext(ctx,
			"SET datafusion.runtime.memory_limit = '512M'", nil)
		return err
	},
)
// After checking err:
db := sql.OpenDB(connector)
```

The initializer applies the budget before queries run and after isolated-session resets. The budget limits only the query-execution memory that DataFusion tracks.

Set different budgets for registered in-memory tables, Arrow batches that callers keep, and Go allocations.

### DSNs

The driver accepts these data source name (DSN) forms:

- An empty string, `""`
- Options after a question mark, `?<options>`
- The URL form, `datafusion://`
- The URL form with options, `datafusion://?<options>`.

The driver passes query parameters to DataFusion as session configuration options:

```text
?datafusion.execution.batch_size=8192
```

Driver-owned options use the `datafusion.go.` prefix. The driver removes these options before it passes the remaining options to DataFusion:

```text
?datafusion.go.shared_session=false
```

The driver rejects file paths, hosts, and other URL forms. For file tables, put the path in [`CREATE EXTERNAL TABLE`](https://datafusion.apache.org/user-guide/sql/ddl.html#create-external-table) SQL.

### Initialization

If setup SQL is necessary before you use a pooled database, use `NewConnector` or `NewConnectorWithInitContext`:

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

In shared-session mode, the initialization callback runs one time for each connector. In isolated-session mode, it runs for each connection and reset.

### Multiple Setup Statements

DataFusion prepares one SQL statement at a time.

1. Split migration or setup scripts into individual SQL statements.
2. Execute the statements in order with `ExecStatements`:

	```go
	err := datafusion.ExecStatements(ctx, db, []string{
		"create view one as select 1 as n",
		"create view two as select 2 as n",
	})
	```

The helper skips blank statements. If a statement fails, the helper includes its index in the error.

### Context Cancellation

The driver connects Go query contexts to native cancellation. It checks for cancellation during query planning, stream creation, and record-batch reads:

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

rows, err := db.QueryContext(ctx, "select * from some_large_table")
```

To identify native cancellation errors, use `errors.Is(err, datafusion.ErrNativeCancelled)`.

### SQL Parameters

The driver supports DataFusion SQL parameters through `database/sql`:

```go
row := db.QueryRowContext(ctx, "select ? + 1, ?", int64(41), "x")
```

The driver accepts these ordinary parameter values:

- A null value, `nil`
- A Boolean value, `bool`
- Signed integer types in the `int64` range
- Unsigned integer types as DataFusion `UInt64`
- Floating-point values as `float64`
- A string, `string`
- A byte slice, `[]byte`
- A time value, `time.Time`, as `Timestamp[ns]`
- A duration, `time.Duration`, as `Duration[ns]`.

The `time.Time` conversion keeps loadable IANA locations, such as `America/New_York`. Fixed-offset, local, and other non-loadable locations bind as UTC. To supply an explicit Arrow time zone, use `TimestampWithTimeZone`.

The `database/sql` conversion promotes `float32` values to DataFusion `Float64`. Before native execution, `CheckNamedValue` rejects other value types.

If type inference is ambiguous or exact Arrow/DataFusion types are necessary, use typed wrappers:

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

The wrappers support `UInt64`, `Date`, `Time`, `Timestamp`, `Duration`, and `Decimal`. Bare `nil` binds as an untyped DataFusion null. If a concrete null type is necessary, use `NullOf`, `NullDecimal`, or `NullTimestamp`. For example, `select $1 + 1` accepts `NullOf(ParameterInt64)`.

Prepared statements report `Stmt.NumInput` for these parameter styles:

- Each `?` occurrence counts as a separate positional parameter.
- Dollar-numbered parameters, such as `$1` and `$2`, use positional counts.
- Each name, such as `$name`, counts as one parameter regardless of the number of occurrences.

The driver rejects statements that mix question-mark, dollar-numbered, and named parameter styles during preparation.

Positional statements require positional arguments. Named statements require `sql.Named` arguments with matching names. The driver rejects missing, extra, or duplicate supplied names before query execution.

### Arrow-Native Usage

#### Query Arrow Batches

For exact schemas or values that `database/sql` cannot scan, use `QueryArrowContext` on a `*sql.Conn`:

```go
conn, err := db.Conn(ctx)
if err != nil {
	return err
}
defer conn.Close()

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

Call `Release` on each record after use. Call `Close` on the reader to release its native stream resources. The finalizer does not guarantee immediate cleanup.

#### Register Arrow Tables

To register an Arrow record reader as an in-memory DataFusion table, use `RegisterArrowReader`:

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

`RegisterArrowReader` consumes the remaining batches from the reader. It serializes the batches as an Arrow IPC stream, then registers decoded Rust-owned batches. The copy lets the table outlive the cgo call. Ordinary Go Arrow arrays can contain Go-owned buffers that native code must not keep after that call.

`RegisterArrowReaderZeroCopy` exports the reader through the Arrow C Stream Interface without an IPC copy. Each exported buffer must stay valid for native use until table removal or closure of its session or connector.

Before you use `RegisterArrowReaderZeroCopy`, make sure that native code can keep all exported buffers for this period. Examples include buffers from Arrow Go's `memory/mallocator` package and other C or foreign allocators.

For Go-allocated buffers, obey the [cgo pointer rules](https://pkg.go.dev/cmd/cgo#hdr-Passing_pointers) during the full period of native use. Keep these buffers valid for this period. If you cannot satisfy these requirements, use `RegisterArrowReader`.

#### Register Foreign FFI Table Providers

To register a `datafusion-ffi` `FFI_TableProvider` from a foreign library, use `RegisterFFITableProvider`:

```go
// providerPtr is an *FFI_TableProvider handed to you by the producing library,
// and providerVersion is the datafusion version that library was built against.
table, err := datafusion.RegisterFFITableProvider(ctx, conn, "t", providerPtr, providerVersion)
if err != nil {
	return err
}
defer table.Deregister(ctx)

rows, err := db.QueryContext(ctx, `SELECT ... FROM t WHERE ...`)
```

**Version and ownership**

1. Get `providerVersion` from the library that supplies the provider.
2. Make sure that `providerVersion` equals this package's `DataFusionVersion`.

Do not substitute this package's `DataFusionVersion` for the version from the foreign library. The driver compares versions before it dereferences the provider pointer. A mismatch returns an error.

Exact version equality is stricter than the major-version ABI contract of datafusion-ffi.

The provider pointer must refer to memory that the foreign C or Rust library owns. Do not supply a pointer to Go heap memory. Native code retains cloned callback pointers after registration returns.

Registration clones the provider and increases its reference count. You retain ownership of the original pointer. After registration returns, you can free the original pointer through its library.

**Library lifetime**

The foreign library must stay loaded while registrations or dependent foreign objects exist. These objects include views, query plans, readers, and returned Arrow batches. They can invoke foreign callbacks after deregistration.

Before you unload the library, complete these steps:

1. Stop new queries.
2. Deregister the tables.
3. Remove dependent views.
4. Close the readers.
5. Release the Arrow batches.
6. Free the original provider through its library.
7. Release all other dependent foreign objects.

**Deregistration**

`RegisterFFITableProvider` returns a `*RegisteredTable` handle. While its `*sql.Conn` is open, call `Deregister` to remove the catalog entry.

In shared and isolated modes, connection closure normally returns the physical connection to the pool. It does not guarantee table release. Queries can retain foreign objects even after the connector closes. The driver does not deregister a table when you discard its registration handle.

### Type Conversion

The `database/sql` row conversion supports these types:

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

Where the Arrow schema provides precise information, the driver exposes column metadata through `database/sql`:

- Nullable columns use typed `sql.Null*` scan types where practical.
- Fixed-size binary columns report length.
- Decimal columns report precision and scale.
- Temporal and interval database type names include their Arrow unit or interval subtype.

Variable-width string and binary columns do not report declared lengths. The Arrow result schema does not preserve SQL declarations such as `VARCHAR(32)`.

The driver converts time-only values to UTC `time.Time` values on the Unix epoch date. It converts durations to `int64` nanoseconds because `database/sql/driver.Value` does not accept `time.Duration`. Interval strings keep the month, day, millisecond, and nanosecond components.

When schema information is available, row conversion rejects lists, structs, maps, unions, dictionaries, extensions, and run-end encoded values. For these types or exact batch data, use [`QueryArrowContext`](#query-arrow-batches).

### Semantics and Limits

- `PrepareContext` validates SQL syntax with the DataFusion parser. A prepared query must contain exactly one SQL statement.
- The driver does not support multiple result sets. `Rows.NextResultSet` reports no additional result sets.
- The connector serializes non-query statements across `ExecContext`, `QueryContext`, and `QueryArrowContext`. Concurrent queries and DDL can still have ordering effects.
- Connections implement `driver.Validator`. The driver reports closed connections as invalid before they return to the pool.
- Native errors carry machine-readable kinds across the C ABI. The driver exposes these kinds on `*datafusion.Error.NativeKind`.
- `errors.Is` matches the native sentinels `ErrNativeCancelled`, `ErrNativeInvalidArgument`, `ErrNativeFailure`, and `ErrNativePanic`.
- `RowsAffected` returns `0` by default. If DataFusion emits a single integer output column named `count`, `rows_affected`, or `rowsaffected`, the driver reports its sum.
- `LastInsertId` returns `0, nil`. DataFusion does not expose insert IDs through this driver.
- The driver reuses statement handles for `db.Prepare` and `conn.PrepareContext`. DataFusion plans and executes each run. The driver does not cache physical plans.
- `Close` is idempotent for connectors, connections, statements, rows, and Arrow readers.
- Transactions return explicit unsupported errors. For an already-canceled context, `BeginTx` returns the context error.

SQL executes with the filesystem and network permissions of the host process. Isolated sessions have independent catalogs. They do not sandbox hostile SQL or native FFI providers.

Run untrusted workloads in a different process with restricted operating-system permissions and resource limits.

### Native Runtime

Default builds use cgo but do not link DataFusion at Go link time. At runtime, the driver searches for a shared `libdatafusion_go` library in this order:

1. The path in `DATAFUSION_GO_LIBRARY`, if set
2. The source-checkout directory `internal/native/lib/<goos>-<goarch>/`
3. The user cache, with automatic download from the GitHub release for the installed module version.

These environment variables control library selection and downloads:

| Variable | Effect |
| --- | --- |
| `DATAFUSION_GO_LIBRARY` | Selects an explicit absolute path to a trusted shared library. |
| `DATAFUSION_GO_NO_DOWNLOAD=1` | Disables automatic release-asset downloads. |
| `DATAFUSION_GO_DOWNLOAD_BASE` | Selects an alternative HTTPS base URL for release downloads. Redirects must also use HTTPS. |

Automatic source and cache resolution checks ownership and write permissions for the library and its ancestor directories. Keep these directories private to the service account or system administrators. An explicit library path is trusted configuration and bypasses automatic permission checks.

For setuid, setgid, or Linux file-capability processes, use `datafusion_use_bundled` or `datafusion_use_source`. These processes must link the library at build time. The driver disables runtime resolution for them.

Before you run driver examples or Go tests from a source checkout, run `make bundle` or `make test`. Each target builds the Rust shim and copies the native archive and shared library into `internal/native/lib/<goos>-<goarch>/`.

The package also has these link modes:

| Build tag | Library source |
| --- | --- |
| `datafusion_use_bundled` | The static archive in `internal/native/lib/<goos>-<goarch>/`. |
| `datafusion_use_source` | The static archive from the Rust release build in the source checkout. |
| `datafusion_use_static_lib` | The same static archive as `datafusion_use_source`. |
| `datafusion_use_lib` | A system library that the linker finds through `-ldatafusion_go`. |

Select one link mode with `go build -tags=<build-tag>`. For `datafusion_use_lib`, use `CGO_LDFLAGS` to add the library directory with `-L`. Configure the runtime loader to find the shared library on your operating system.

For native-build setup and tests, see [CONTRIBUTING.md](CONTRIBUTING.md#native-libraries).

## Troubleshooting

| Problem | Action |
| --- | --- |
| `datafusion-go requires cgo` | Install a C toolchain. Set `CGO_ENABLED=1`. |
| Native library not found | Set `DATAFUSION_GO_LIBRARY` to a local library. For a source checkout, run `make bundle`. See [Native Runtime](#native-runtime). |
| Local checkout tests fail before the driver opens | Run `make test` from the repository root to build the native library before the Go tests. |
| DSN rejected | Use an empty DSN or [session options](#dsns). |
| `database/sql` cannot scan a result column | Use [`QueryArrowContext`](#query-arrow-batches) for complex Arrow values. |
| Windows build or test failures | Use a MinGW/GNU C toolchain for the `x86_64-pc-windows-gnu` Rust ABI. |

## Developing

For setup, test modes, version changes, and the release process, see [CONTRIBUTING.md](CONTRIBUTING.md).

Before you submit code changes, run these commands from the repository root:

```sh
make lint
make test
make test.source
make rust.test
```

For questions and ordinary bug reports, use [GitHub Issues](https://github.com/datafusion-contrib/datafusion-go/issues). Code and documentation contributions are welcome through [pull requests](https://github.com/datafusion-contrib/datafusion-go/pulls).

## Versioning

Release tags encode the bundled DataFusion version as `v<major>.<encoded-datafusion-version>.<patch>`: DataFusion `53.1.0` encodes as `530100`, so `v0.530100.1` bundles DataFusion `53.1.0`.

[versions.toml](versions.toml) contains the release metadata. For version changes and the release workflow, see [CONTRIBUTING.md](CONTRIBUTING.md#version-bumps).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

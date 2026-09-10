# Changelog

All notable changes to datafusion-go are documented here.

## v0.550000.2 - 2026-09-09

- Consolidated query and statement execution and simplified native ownership and cleanup across Go and Rust.
- Fixed statement preparation to honor the configured SQL dialect and preserve parameters owned by SQL `PREPARE` and `CREATE FUNCTION` statements, including under `EXPLAIN`.
- Upgraded Arrow Go to v18.8.0 for correct fractional-hour timezone offsets.
- Added the pinned DataFusion SQLLogicTest corpus, SQL documentation coverage reports, reproducible datasets, and CI runs on Linux, macOS, and Windows. Replaced platform-dependent trigonometric snapshots with portable assertions. Two zero-length fixed-size-list assertions remain disabled pending Arrow Go support, and parallel spill stress tests run as nonblocking diagnostics.
- Expanded lifecycle, installation, conformance, fuzzing, and native ABI checks, and clarified setup and usage documentation.

## v0.550000.1 - 2026-09-06

- Fixed Arrow reader callback panics leaking imported buffers, premature reader finalization during active reads, and native connection access racing with close or session reset.
- Hardened automatic native library loading with canonical path, ownership, permission, and platform ACL checks. Explicit library paths must now be absolute; downloads and redirects require HTTPS, and native asset sizes are bounded. Restricted Windows DLL dependency searches and disabled runtime loading for privileged processes.
- Rejected embedded NULs before C-string conversion and oversized parameter ordinals before allocation. Sanitized NULs in streamed execution errors to prevent native process aborts.
- Added memory, concurrency, callback, and loader security regression tests, and documented process isolation requirements for untrusted SQL and native providers.

## v0.550000.0 - 2026-09-05

- Upgraded bundled Apache DataFusion to 55.0.0 and Arrow Rust to 59.3.0. Foreign table providers must be built with `datafusion-ffi` 55.0.0; the exact version handshake rejects older providers before dereferencing their pointers.
- Fixed parameterized `CREATE TABLE` and `CREATE VIEW` statements by binding their logical plans before executing DDL, preserving existing tables when parameter validation fails.
- Fixed cached prepared statements retaining and querying old isolated sessions, failed session initialization exposing previous session state, and native runtimes leaking after direct `Driver.Open`/`Close` calls.
- Made Arrow reader cancellation interrupt active reads and made DDL and shared-initialization waits honor context deadlines. Added native lifetime and allocation regression tests.
- Removed an unnecessary full IPC buffer copy during safe Arrow registration. Documented query memory budgets and the complete foreign-provider/library lifetime contract, including pooled connections and retained batches.
- Updated vulnerable dependencies and added Go/Rust advisory scans and dependency monitoring. The module now requires Go 1.25+, recommends the patched Go 1.27.1 toolchain, and requires Rust 1.94+ for source builds.

## v0.540100.0 - 2026-07-31

- Upgraded the bundled Apache DataFusion to 54.1.0. No driver API changed.
- `datafusion-ffi` 54 builds its stable ABI on `stabby` instead of `abi_stable`, so foreign table providers passed to `RegisterFFITableProvider` must come from `datafusion-ffi` 54.1.0. The exact `DataFusionVersion` handshake rejects providers built against any other version before their pointer is dereferenced.

## v0.530100.2 - 2026-07-19

- Added foreign FFI table provider support: register a `datafusion-ffi` `FFI_TableProvider` produced by another library with `RegisterFFITableProvider` and query it with projection and filter pushdown reaching the provider. Returns a `*RegisteredTable` handle for explicit `Deregister`, with an exact `DataFusionVersion` handshake checked before the provider pointer is dereferenced.
- Restructured the README in Apache DataFusion style.

## v0.530100.1 - 2026-06-05

Initial release for Apache DataFusion 53.1.0.

- Added a `database/sql` driver backed by an in-process DataFusion `SessionContext`.
- Added bundled native static-library build and release automation for darwin-amd64, darwin-arm64, linux-amd64, linux-arm64, and windows-amd64.
- Added source, bundled, static-library, and no-cgo link-mode test coverage.
- Added SQL parameter binding for common scalar, temporal, decimal, binary, and typed-null values.
- Added Arrow-native query streaming through `QueryArrowContext`.
- Added Arrow record-reader table registration through safe IPC copy and native zero-copy materialized registration.
- Added shared-session and isolated-session connection modes.
- Added native cancellation, native error kinds, and panic containment across the Rust/C/Go boundary.

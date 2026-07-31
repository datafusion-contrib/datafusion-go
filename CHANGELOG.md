# Changelog

All notable changes to datafusion-go are documented here.

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

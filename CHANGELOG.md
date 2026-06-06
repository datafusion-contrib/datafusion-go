# Changelog

All notable changes to datafusion-go are documented here.

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

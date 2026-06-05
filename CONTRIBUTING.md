# Contributing

## Requirements

- Go with cgo enabled for normal development and tests.
- A C toolchain for the target platform.
- Rust stable for rebuilding the DataFusion FFI shim.

`CGO_ENABLED=0` is intentionally unsupported for normal execution; tests cover that the package returns a clear error in that mode.

## Test Workflow

Run the default bundled-library path:

```sh
make test
```

Run the alternate link modes:

```sh
make test.source
make test.static
```

Run the release verification path for already-bundled/downloaded artifacts:

```sh
make verify.release.downloaded
```

Run linting:

```sh
make lint
```

## Native Libraries

The Rust crate under `rust/` builds `libdatafusion_go.a`. The default Go build links the platform-specific archive from `internal/native/lib/<goos>-<goarch>/libdatafusion_go.a`.

Use `make bundle` only when you intend to copy the current host build into `internal/native/lib`. Release verification uses `make verify.release.downloaded` so downloaded matrix artifacts are not overwritten by the release runner.

## Release Dry Run

Before publishing, run the `Release` GitHub Actions workflow with `publish=false`. That builds the native matrix, downloads all archives into one checkout, verifies checksums, runs Go/Rust/no-cgo tests, and runs a clean consumer-module smoke test without committing, tagging, or creating a GitHub release.

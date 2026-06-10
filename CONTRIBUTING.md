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

Run the source link mode (`datafusion_use_static_lib` links identically and needs
no separate run):

```sh
make test.source
```

Run a single suite against an already-built native library (CI uses these
directly; each is one `go test` invocation):

```sh
make go.test.dynamic
make go.test.bundled
make go.test.race
```

Run the release verification path for already-bundled/downloaded artifacts:

```sh
make stage.release.assets
make release.verify
```

Run linting:

```sh
make lint
```

On macOS, `go test -race` may emit a non-fatal Apple linker warning about a
malformed `LC_DYSYMTAB` when cgo links the native DataFusion archive. Normal
tests and non-race builds are quiet, and forcing `-Wl,-ld_classic` only trades
that warning for a deprecated-linker warning. Treat this as a known macOS
toolchain caveat unless it becomes fatal or appears outside race-enabled native
tests.

## Version Bumps

`versions.toml` is the human-maintained source of truth for release metadata:

- `datafusion.version` pins the Rust `datafusion` and `datafusion-sql` crate versions.
- `datafusion_go.major` and `datafusion_go.patch` produce the Go module tag `v<major>.<encoded-datafusion-version>.<patch>`.
- `abi.version` is the native ABI expected by Rust, C, and Go.

Do not hand-edit generated version constants in Go or Rust. To bump a release:

```sh
$EDITOR versions.toml
make generate
make generate.check
```

`make generate` updates `rust/Cargo.toml`, `rust/Cargo.lock`, `version.go`,
`internal/native/version_generated.go`, and `rust/src/generated.rs`. Commit those
mechanical outputs with the `versions.toml` change.

Increment `abi.version` only when the C ABI in `rust/include/datafusion_go.h`
changes incompatibly with the Go native wrapper.

## Native Libraries

The Rust crate under `rust/` builds a static archive and a shared library. The default Go build loads the platform-specific shared library at runtime from `DATAFUSION_GO_LIBRARY`, from `internal/native/lib/<goos>-<goarch>` in source checkouts, or from the checksum-verified release-asset cache. The explicit `datafusion_use_bundled` mode links the platform-specific static archive from `internal/native/lib/<goos>-<goarch>/libdatafusion_go.a`.

Use `make bundle` only when you intend to copy the current host build into `internal/native/lib`. Release verification uses `make release.verify` so downloaded CI artifacts are not overwritten by the release runner.

## Release Automation

Before publishing, update `versions.toml`, run `make generate`, update
`CHANGELOG.md`, and merge the release PR into `main` after CI passes. Releases
are cut only by GitHub Actions after the `CI` workflow completes successfully on
`main`.

The `Release` workflow derives the tag from `versions.toml`. If that tag already
exists, the workflow exits without publishing. Otherwise, it downloads the
native libraries that the triggering `CI` run built on the pinned release
runners, verifies them against the build-time checksum manifests uploaded
alongside them, stages release assets using the exact filenames the runtime
downloader requests, smoke-tests the downloaded libraries (bundled link plus a
race run against the shared library), commits the checksum manifest to `main`,
tags that commit with the derived release tag, and uploads the native libraries
plus checksums. It never compiles Rust or rebuilds artifacts. The tag must
point at the commit containing release asset names in
`internal/native/lib/SHA256SUMS`; otherwise `go get` consumers cannot verify or
download native libraries automatically.

The workflow also supports `workflow_dispatch` with a `ci_run_id` input and a
`dry_run` flag (default true) to rehearse download, checksum verification,
staging, and smoke-testing without committing, tagging, or publishing. A
non-dry-run dispatch refuses to proceed unless the supplied run is a successful
`CI` run whose head SHA equals the SHA being tagged, so manual releases cannot
ship bytes built from a different commit.

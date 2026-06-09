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
make stage.release.assets
make verify.release.downloaded
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

Use `make bundle` only when you intend to copy the current host build into `internal/native/lib`. Release verification uses `make verify.release.downloaded` so downloaded matrix artifacts are not overwritten by the release runner.

## Release Dry Run

Before publishing, update `versions.toml`, run `make generate`, update
`CHANGELOG.md`, and run the `Release` GitHub Actions workflow with
`publish=false`. The workflow derives the tag from `versions.toml`, builds the
native matrix, downloads all archives into one checkout, stages release assets
using the exact filenames the runtime downloader requests, embeds the
release-asset checksum manifest, and runs Go/Rust/no-cgo tests without tagging
or creating a GitHub release.

When the dry run succeeds, rerun the same workflow with `publish=true`. It tags
commits the release-asset checksum manifest, tags that commit with the derived
release tag, and uploads the generated native libraries plus checksums. The tag
must point at the commit containing release asset names in
`internal/native/lib/SHA256SUMS`; otherwise `go get` consumers cannot verify or
download native libraries automatically.

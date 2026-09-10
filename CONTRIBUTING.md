# Contributing

## Requirements

- A supported, patched Go toolchain with cgo enabled. `go.mod` recommends
  Go 1.27.1; Go automatically selects it for development in this module.
  The module requires Go 1.25 or newer.
- A C toolchain for the target platform.
- Rust 1.94 or newer for rebuilding the DataFusion FFI shim.

`CGO_ENABLED=0` is intentionally unsupported for normal execution; tests cover that the package returns a clear error in that mode.

## Test Workflow

The [architecture guide](docs/architecture.md) records module responsibilities,
ownership, and lock ordering. The [testing guide](docs/testing.md) explains the
shared query corpus, deterministic failure tests, installation tests, fuzzing,
native instrumentation, and coverage gates.

After `make bundle`, use `make test.quick` for local iteration. Before handing
off changes, run `make lint`, `make test`, `make test.source`, and
`make rust.test`. Extended checks are available with `make test.extended`.

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

Run dependency vulnerability checks:

```sh
make go.vuln
cargo install cargo-audit --version 0.22.2 --locked
make rust.audit
```

CI runs both scans on a current Go toolchain. Native lifecycle tests check
session/runtime destruction with Rust weak references and Arrow buffer release
with a checked C allocator, including cancellation, reset, and registration
failures. Go's race detector supplements these allocation checks.

Security regression tests cover panicking Arrow reader callbacks with a checked C allocator, GC during an active read, concurrent connection close/reset, and writable native-library paths (including macOS and Windows ACLs). On Linux, `TestSecureExecutionWithCapabilitiesWithoutProc` additionally exercises a non-root executable with a file capability after chroot removes `/proc`. That fixture requires root and `setcap`; run it in a disposable container. It skips on ordinary non-root test runs.

The Rust build, test, and lint targets use the same macOS deployment settings
so switching targets does not invalidate native dependency builds.

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

It also derives the native loader declarations and ABI contract tests from
`rust/include/datafusion_go.h`. Edit that header for an ABI change, then run
`make generate` and `make generate.check`; do not hand-edit ABI generated files.

Increment `abi.version` only when the C ABI in `rust/include/datafusion_go.h`
changes incompatibly with the Go native wrapper.

CI compares each pull request with its base commit, and each push to `main`
with the previous commit. Changes to shipped Go or Rust code, native headers
and libraries, dependency manifests, the Makefile, release workflow, or Git
archive attributes require a version bump and a non-empty changelog entry.
Documentation, examples, test fixtures, test-only modules, and development
tooling can keep the existing version. Tests embedded in a production source
file still count as a change to that file.

For the same DataFusion version, increment `datafusion_go.patch` by one. For
a newer DataFusion version, reset it to zero. The module major can stay the
same or increment by one; DataFusion and ABI versions cannot decrease. New
release tags must not already exist. Generated-file checks also run in CI.
Two PRs proposing the same next version must update their branches after the
first merges so their version check runs against the new base.

To run the version check locally after fetching `main` and the release tags:

```sh
git fetch origin main --tags
go run ./internal/tools/genversions -base-ref origin/main
```

## Native Libraries

The Rust crate under `rust/` builds a static archive and a shared library. The default Go build loads the platform-specific shared library at runtime from `DATAFUSION_GO_LIBRARY`, from `internal/native/lib/<goos>-<goarch>` in source checkouts, or from the checksum-verified release-asset cache. The explicit `datafusion_use_bundled` mode links the platform-specific static archive from `internal/native/lib/<goos>-<goarch>/libdatafusion_go.a`.

Use `make bundle` only when you intend to copy the current host build into `internal/native/lib`. Release verification uses `make release.verify` so downloaded CI artifacts are not overwritten by the release runner.

## Release Automation

Before publishing, update `versions.toml`, run `make generate`, update
`CHANGELOG.md`, and merge the release PR into `main` after CI passes. The
`CHANGELOG.md` entry must use a heading of exactly `## <tag>` or
`## <tag> - <date>` (for example `## v0.530100.1 - 2026-06-05`) with a
non-empty body; `make changelog.check` validates this and runs in CI, and the
release workflow extracts the release notes from that entry. Releases
are cut only by GitHub Actions after the `CI` workflow completes successfully on
`main`, using the default workflow token; no extra credentials are required.

The `Release` workflow derives the tag from `versions.toml`. If that tag already
exists, the workflow exits without publishing. Otherwise, it downloads the
native libraries that the triggering `CI` run built on the pinned release
runners, verifies them against the build-time checksum manifests uploaded
alongside them, stages release assets using the exact filenames the runtime
downloader requests, and smoke-tests the downloaded libraries (bundled link plus
a race run against the shared library). If the staged checksum manifest differs
from the committed placeholder, the workflow creates a release commit on top of
the CI commit containing the refreshed `internal/native/lib/SHA256SUMS` and
pushes it **only as the release tag — never to `main`**, so branch protections
stay intact and no follow-up CI run fires. The release commit is reachable only
from the tag; GitHub labels it "does not belong to any branch", which is
expected. `main` keeps a placeholder manifest, so runtime download of native
libraries works only for tagged releases — source checkouts load from
`internal/native/lib/<goos>-<goarch>` or `DATAFUSION_GO_LIBRARY` instead. The
workflow never compiles Rust or rebuilds artifacts. The tag must point at the
commit containing release asset names in `internal/native/lib/SHA256SUMS`;
otherwise `go get` consumers cannot verify or download native libraries
automatically.

The workflow also supports `workflow_dispatch` with a `ci_run_id` input and a
`dry_run` flag (default true) to rehearse download, checksum verification,
staging, and smoke-testing without committing, tagging, or publishing. A
non-dry-run dispatch refuses to proceed
unless the supplied run is a successful push-triggered `CI` run whose head SHA
equals the current tip of `main` (the SHA being tagged), so manual releases
cannot ship bytes built from a different commit. Older commits cannot be
released manually.

### When a release stalls

A green `Release` run is not necessarily a published release; each run's
summary records the gate decision. The workflow skips when the tag already
exists with a published release, or when `main` has advanced past the CI run's
commit ("a newer run will release"). If a run pushed the tag but failed before
publishing the release, the next run for the same artifacts resumes publication
automatically. If the newer run's CI fails — or `versions.toml` changed in
between, so the newer run computes a different tag — the skipped version is
never published. To recover:

- Re-run the failed or skipped `Release` run: it reuses its recorded CI run,
  whose artifacts are retained for 30 days.
- Dispatch `Release` from `main` with `ci_run_id` set to a successful `CI` run
  for the current tip of `main` and `dry_run: false`.
- Otherwise push an empty commit (`git commit --allow-empty`) or any normal
  change to `main` to force a fresh CI build and release attempt.

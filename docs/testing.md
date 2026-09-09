# Correctness testing

The test strategy borrows SQLite's use of independent oracles, failure
injection, deterministic replay, and instrumented native execution. It does
not claim SQLite's test volume, complete branch coverage, or a proof that no
regressions are possible.

## Local and required checks

Run `make bundle` once, then `make test.quick` for normal iteration. Quick tests
reuse the native library and skip installation subprocesses. They still run
the query corpus, lifecycle tests, and saved Go fuzz seeds. A native source
change requires rebuilding before this target. Native-linked tests use
`-count=1` because the Go test cache does not hash external libraries.

Before handing off changes, run all four repository gates:

```sh
make lint
make test
make test.source
make rust.test
```

`make go.test.race`, `make go.test.nocgo`, and `make consumer.smoke` add checks
for Go concurrency, unsupported builds, and a separate consuming module.
The existing release verification and platform matrix remain in place.

## Shared query corpus

`testdata/conformance/*.json` records SQL, optional setup, typed parameters,
column names/types/nullability, decimal metadata, expected values, and errors.
Null cells are JSON null; other cells use strings to preserve large integers.
Tests assert metadata even when no rows are returned. Numeric decimal values
are compared without insignificant trailing zeroes, while precision and scale
are checked separately.

Every case runs through Go's direct SQL, prepared SQL, and Arrow APIs, in both
shared and isolated sessions. Rust runs the same case directly through
DataFusion, bypassing the bridge and its placeholder rewriting. `native_sql`
supplies the equivalent numbered placeholders when a fixture uses `?`.
`make test.sqlite` runs only cases explicitly marked `sqlite: true`; this
additional engine compares portable values and names, not DataFusion schemas
or engine-specific syntax.

Add a minimized fixture for each query regression. Fix the expected result
from an independently understood example before changing the implementation.
Do not regenerate expected values from the driver being tested.

## Complete upstream SQL corpus

`make test.sqllogic` runs the unchanged SQLLogicTest corpus from the DataFusion
release in `versions.toml`, through the public Go Arrow API. A separate native
test library supplies upstream fixtures and result comparisons. SQL execution
and Arrow batch consumption happen in Go. The test dependencies and callbacks
are absent from release libraries.

The target verifies pinned fixture and dataset hashes, generates deterministic
TPC-H data, tests the assertion harness, and writes per-file, per-record, and
documentation coverage reports. It fails on missing cases or unsupported
results. Comment-only Spark stubs and postgres-only records have explicit
counts, rather than appearing as passing DataFusion SQL. The documented
function and SQL section inventory keeps gaps visible beyond the upstream suite.

See [the corpus instructions](../testdata/sqllogictest/README.md) for requirements,
focused runs, corpus updates, and the limits of each coverage metric. CI runs
this target on Linux, macOS, and Windows.
After failures, `make test.sqllogic.oracle` compares the failed files with native
DataFusion and retains separate diagnostic logs. CI runs this comparison with a
ten-minute limit; its outcomes never replace the Go coverage results.

## Lifecycle and failure sequences

`TestLifecycleSequences` models catalog visibility and retained batch values.
It mixes registration, direct/prepared queries, retained Arrow results, reset,
deregistration, cancellation, and release under copy and zero-copy modes.
Checked C allocators must return to zero after the entire owning graph closes.
Rust tests independently observe runtime and session destruction with weak
references and require terminal readers to stop polling.

`TestRegistrationFailureAtEveryBatch` injects an input-reader error after each
batch, checks that no partial table was published and no buffers remain, and
then checks successful reuse. Rust applies the same strategy to result polling
and cancellation. These use real Arrow and DataFusion objects. They are not a
global allocator OOM simulation or an exhaustive enumeration of thread schedules.

Replay or extend a sequence with:

```sh
DFGO_TEST_SEED=20260909 DFGO_TEST_STEPS=10000 make test.sequences
```

Failures log the seed, operation list, and failing step. Preserve a minimized
sequence as an ordinary regression test rather than relying on a future random
run to rediscover it.

## Installation in fresh processes

`make test.install` builds a `-trimpath` test consumer and launches a fresh
process per scenario. This tests the real dynamic loader and its process-wide
initialization: explicit override, source discovery, download, valid/corrupt
cache, interrupted response, offline response, disabled download, missing
manifest/file, bad checksum, and incompatible ABI/DataFusion versions.
Local TLS fixtures avoid external network dependencies. Test-only cache and
transport substitutions are confined to each child process. Test cleanup
checks temporary files and cache publication.

## Extended runs

Install the pinned development tools (the default Rust toolchain is unchanged):

```sh
rustup toolchain install nightly-2026-06-10 --profile minimal --component rust-src
rustup component add llvm-tools-preview
cargo install cargo-fuzz --version 0.13.2 --locked
cargo install cargo-llvm-cov --version 0.9.1 --locked
```

Clang and its matching `llvm-cov` and `llvm-profdata` are also required. On
macOS these come from Xcode command-line tools. Linux CI installs `clang llvm`.
`DFGO_CLANG`, `DFGO_LLVM_COV`, and `DFGO_LLVM_PROFDATA` override their paths.

```sh
make test.fuzz FUZZ_TIME=120s
make rust.fuzz RUST_FUZZ_FLAGS=--dev RUST_FUZZ_SECONDS=120
make test.native.asan
make test.coverage
```

Go fuzzing checks bound-value round trips against the original inputs through
SQL and Arrow, and version parsing with malformed inputs. Rust's `prepare`
target exercises parsing, metadata, and diagnostics through allocated C inputs;
it prepares SQL without executing it. Checked-in Rust seeds are read-only.
New fuzz inputs go to `rust/target/fuzz-corpus`; failures go to
`rust/fuzz/artifacts`. Go records failures under `testdata/fuzz`. Keep a minimized
reproducer, plus a clear assertion of the desired behavior.

The Rust ASan target rebuilds dependencies and the standard library in an
isolated target directory and runs Rust unit/ABI/oracle tests. The C target
uses address and undefined-behavior sanitizers around a real dynamic-loader
consumer and Arrow callback lifecycle. These are separate instrumentation
runs, not a claim that the Go race detector observes Rust memory. LeakSanitizer
is enabled on Linux; Apple's runtime does not support it, so macOS also relies
on checked allocations and Rust lifetime assertions. The implementation follows
the [Rust sanitizer guidance](https://doc.rust-lang.org/unstable-book/compiler-flags/sanitizer.html).

Coverage runs the Go suite against an instrumented Rust shared library, merges
the installation subprocesses into Go's profile, and produces separate C
coverage. A test-only build tag flushes the LLVM counters explicitly because Go
does not run C's exit hooks. The hook is absent from release libraries; a failed
flush or missing native profile fails the check. `coverage/summary.md`,
`coverage/go.out`, `coverage/rust.lcov`, and
`coverage/c-coverage/` remain local artifacts. Go percentages measure statements;
Rust and C percentages measure executable lines. They are not interchangeable.
Thresholds in `testdata/coverage_minimums.json` prevent loss of the measured
baseline. Raise them as coverage improves; a reduction needs a documented
reason in the change description. Missing coverage fails the check.

`Extended correctness` runs these longer checks nightly and on manual dispatch,
retaining reports and fuzz artifacts for 30 days. Normal pull-request CI keeps
the complete existing OS/link/race matrix and adds the SQLite oracle. Extended
checks must pass before merging changes to ownership or the ABI; they remain
available locally through `make test.extended`.

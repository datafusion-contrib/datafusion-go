# DataFusion SQLLogicTest corpus

`test_files/` is the pinned SQLLogicTest corpus from the DataFusion release
in `versions.toml`. `upstream.json` records its source commit, the hash of every
fixture, and the exact commits and archive checksums of the upstream datasets.
The included Apache license and notice apply to these upstream files.

Two assertions in `arrow_typeof.slt` are temporarily commented out because
Arrow Go v18.8.0 cannot import `FixedSizeList(0, Null)`. Restore them when the
Arrow Go dependency supports zero-length fixed-size lists, then update the
fixture checksum in `upstream.json`. Case G's two queries in
`aggregate_memory_spill.slt` are also commented out and run separately as
diagnostics. These four assertions are excluded from required execution and
coverage totals.

Run `make test.sqllogic` to execute the complete corpus through Go. This builds
a separate native test library, downloads the pinned datasets into `.cache/`,
and produces per-file results and a summary in
`.cache/sqllogictest/reports/current/`. The initial download is approximately
125 MB. Python 3.11 or newer, Make, and a C compiler are required. Subsequent
runs use checksum-verified cached archives. The pinned TPC-H generator produces
SF 0.1 data locally; `tpch.json` verifies all eight generated files. Explicit
signed wrapping and LF line endings make generation reproducible across
compilers and operating systems.

`run.json` records the repository revision and tracked changes, driver fixture
manifest checksum, Arrow Go version and module replacement, native library
checksum, and Tokio worker setting. CI uses four Tokio workers;
`TOKIO_WORKER_THREADS=4 make test.sqllogic` reproduces that setting locally.

The Rust SQLLogicTest runner parses records, expands includes, and compares
results using DataFusion's published test support crate. It calls back into Go
to execute SQL through `QueryArrowContext`. Go consumes and retains the batches,
then exports an Arrow C stream for the upstream result comparator. The Go
query runs on its own goroutine, outside the Rust runner's executor thread.
The test library also
installs upstream's custom fixture tables, functions, and deterministic session
settings. It enables the Avro, Parquet encryption, and Spark test dependencies
required by the upstream suite. These fixture dependencies and C callbacks are
absent from production builds.

Every `.slt` file is discovered. Missing files, changed fixture contents, absent
datasets, unavailable fixture setup, parsing errors, and adapter failures fail
the run. An adapter error cannot satisfy a test that expects a SQL error.
The upstream DataFusion/postgres conditions are recorded in the report;
unknown conditions, named connections, retries, and unsupported directives
fail instead of silently changing the execution scope. A filtered run
retains the complete file denominator and is never reported as full coverage.

For a focused rerun:

```sh
make test.sqllogic SQLLOGIC_RUN='^TestSQLLogic$$/^aggregate[.]slt$$'
```

Use `make sqllogic.sync` when updating the corpus to the version in
`versions.toml`, then review and commit the fixture and manifest changes.
`make sqllogic.check` verifies that no reviewed fixtures were changed or lost.
A sync restores upstream fixtures. Reapply the temporary Arrow Go and parallel
spill exclusions and the portable trigonometric assertions until the relevant
dependency or upstream tests are fixed, then update their manifest checksums.
Keep driver-specific regression fixtures outside `test_files/`; do not change
upstream expected values to accommodate a driver failure. Put additional
assertions in `driver/`, then run `make sqllogic.driver.sync` to update their
reviewed checksums. These are reported separately from the upstream file count.

DataFusion 55.0.0 supplies 504 `.slt` files. Of these, 146 are comment-only Spark
stubs and 358 contain executable assertions. With four temporary exclusions and
two added portable trigonometric queries, includes expand to 24,867 records;
eight are explicitly postgres-only. The driver must execute all 24,859 eligible
records. Comment-only files are inventoried but
never counted as passing SQL.

`sql/` contains the pinned release's SQL documentation. `coverage.json`
inventories its 327 documented functions (including aliases) and 188 sections.
The generated coverage report attaches passing query locations and SQL hashes
to functions, operators, and SELECT clauses found by the SQL parser. A function
or operator witness must come from a SELECT query that returned rows. Expected
errors, EXPLAIN plans (including those preceded by comments), comments, strings,
empty results, and failed assertions cannot supply these witnesses.
Spark's alternate function registry contributes to corpus totals but does not
supply witnesses for the default DataFusion SQL guide.
The parser is conservative: syntax it cannot parse stays visible in the report.

`coverage-map.json` records additional reviewed section witnesses by location and
SQL hash. Sections without parsed or reviewed witnesses remain gaps. Witnesses measure examples,
not every overload, input, branch, or combination. Full corpus execution and
documentation witnesses are separate metrics; neither proves every possible
SQL program correct. `complete` means the corpus passed;
`sql_surface_complete` additionally requires witnesses for all inventory entries.
Grouping headings can use all their child sections' witnesses; the report
retains those child identifiers so the relationship can be checked.
Mappings record discrepancies in the pinned guide, including the removed
`COMPRESSION TYPE` spelling and illustrative struct field orders. Additional
tests check the supported syntax and field values while preserving upstream
documentation unchanged.

`SQLLogicTest` CI runs the full target on Linux, macOS, and Windows and retains
reports even on failure. The temporary exclusions described above also apply
in CI.

After a failed run, `make test.sqllogic.oracle` replays each failed SQL file with
DataFusion's own Rust executor. It verifies that the report references the
current corpus and driver fixtures, then writes separate logs and outcomes
under the report's `native/` directory. CI runs this diagnostic after failures,
with a ten-minute limit. Native results do not change the Go assertion outcomes
or count as Go coverage.

The original `spark/math/csc.slt` and `spark/math/sec.slt` snapshots differed
on Windows near trigonometric poles. For example, the `csc(pi())` snapshot
expected `8165619676597685`, while Windows returned `8165889364191922`.
The Spark functions use reciprocals of Rust's `f64::sin` and `f64::cos`, whose
[precision can vary by platform](https://doc.rust-lang.org/std/primitive.f64.html#method.sin).
The fixtures now check known values away from the poles with an absolute
tolerance of `1e-12`. Ordinary-input, NULL, NaN, and infinity checks remain.
The suite's comparison rules are unchanged.

Case G's four-partition aggregation in `aggregate_memory_spill.slt` can time
out or exhaust its 1 MiB pool. `make test.sqllogic.spill` runs its result and
`EXPLAIN ANALYZE` assertions separately, requiring both the expected count and
sum and a nonzero `spill_count` in the final aggregation. It attempts 25
repetitions through Go and in each of two native Rust execution modes. A failure
stops that mode; the other modes still run. Each Go run has a two-minute limit,
and Rust operations have five-second timeouts.

CI runs this diagnostic on all three platforms with `continue-on-error` and a
ten-minute step limit. Logs and exit codes are retained under
`.cache/sqllogictest/reports/current/spill/`, separately from required corpus
results. Local diagnostic failures return a nonzero exit status. Cases A–F and
the ordinary single-partition Go spill regression remain required. Restore Case
G's corpus assertions after repeated native and Go runs establish reliability.

The ignored Rust diagnostic
`upstream_spill_with_blocking_pulls` in `rust/tests/sqllogictest_oracle.rs`
reproduces failures without Go. It compares separate blocking stream
pulls against creating and collecting the query in one async operation, selected
with `DFGO_SQLLOGICTEST_ASYNC_COLLECT=1`. The async control initially passed 25
repetitions, but a subsequent run reached its five-second timeout. The blocking
mode has failed with memory exhaustion. Both modes can fail in native DataFusion;
changing how the Go bridge polls is not an established fix.
The ordinary Go regression checks a single-partition aggregation and verifies
both returned values and a nonzero `spill_count` in `EXPLAIN ANALYZE` output.

# Go callback transport performance

Measured on 2026-09-26, Apple M1 Pro, macOS arm64, Go 1.27.1,
Rust 1.96.0, DataFusion 55.1.0, Arrow Rust 59.3.0 and Arrow Go 18.8.0.
The benchmark fixes the native runtime at four workers; Go uses ten logical CPUs.

The C Data transport materially improves large provider scans and scalar UDFs.
The implementation preserves ordinary Go allocator support: Go-produced buffers
are copied once into C-owned allocations before export. Native UDF arguments
cross without serialization. This is not a fully zero-copy Go-to-Rust bridge.

## Method

Both test executables use **the same release-built native library**. The baseline
Go executable was built from commit `1ae9d1c` plus `callback_bench_test.go`, so it
advertises callback table v1 and exercises the preserved IPC implementation.
The candidate advertises v2. This controls for engine version, engine settings,
query planning, async scheduling and result consumption; it does not compare
against datafusion-golang's different engine/version/execution architecture.

Five independent samples per case used `-test.benchtime=300ms`. Each round ran
both implementations, alternating order between rounds. No builds or other test
suites ran concurrently with the reported measurements. The table reports medians,
not statistical significance or a universal performance guarantee.

Each query consumes 32 batches of either 128 or 8,192 rows. Shapes are one int64
column, 16 int64 columns, or one UTF-8 column containing 64-byte strings. Providers
repeat the same immutable Go batch to isolate transport from storage I/O; both
paths therefore see identical hot source data. UDF inputs are
registered in native memory before timing; an identity UDF accepts all columns
and returns the first. The engine may coalesce small input batches before UDF
execution. Results are fully drained and row counts checked. These cheap workloads
make transport cost visible; expensive user code or remote I/O will reduce the
end-to-end speedup. Setup and native-table registration are outside timing.

## Results

| Workload / shape / rows per source batch | IPC ms/query | C Data ms/query | Speedup | Go bytes/query, IPC → C Data | Go allocations/query, IPC → C Data |
| --- | ---: | ---: | ---: | ---: | ---: |
| provider / int64 / 128 | 0.555 | 0.553 | 1.00× | 258,641 → 146,951 | 2,182 → 1,963 |
| udf / int64 / 128 | 0.190 | 0.180 | 1.06× | 94,134 → 10,714 | 153 → 143 |
| provider / int64 / 8192 | 1.061 | 0.680 | 1.56× | 2,574,144 → 146,948 | 2,186 → 1,963 |
| udf / int64 / 8192 | 2.381 | 0.747 | 3.19× | 5,036,500 → 271,815 | 3,791 → 3,494 |
| provider / wide16 / 128 | 1.871 | 1.868 | 1.00× | 2,951,069 → 2,951,106 | 6,701 → 6,701 |
| udf / wide16 / 128 | 0.515 | 0.425 | 1.21× | 605,657 → 31,922 | 292 → 300 |
| provider / wide16 / 8192 | 13.283 | 3.557 | 3.73× | 74,123,303 → 1,368,390 | 6,734 → 12,660 |
| udf / wide16 / 8192 | 4.592 | 1.556 | 2.95× | 37,388,366 → 941,736 | 8,262 → 8,516 |
| provider / utf8 / 128 | 0.659 | 0.586 | 1.12× | 534,311 → 156,422 | 2,213 → 2,153 |
| udf / utf8 / 128 | 0.304 | 0.209 | 1.45× | 604,257 → 11,119 | 156 → 150 |
| provider / utf8 / 8192 | 4.848 | 1.333 | 3.64× | 19,603,082 → 156,463 | 2,220 → 2,153 |
| udf / utf8 / 8192 | 4.932 | 1.092 | 4.52× | 37,810,127 → 286,492 | 3,889 → 3,728 |

Go allocation figures **exclude C/Rust allocations** and are not total-memory or
peak-RSS measurements. Wide batches can create more small Go ownership objects
while eliminating tens of megabytes of serialization staging. A process peak-RSS
measurement was attempted, but macOS sandbox restrictions blocked the required
`sysctl`; no total-memory reduction is claimed. Separate lifetime tests verify
that C callback allocations return to their starting value after retained batches
are released and after a cancelled callback returns late.

The initial all-C-Data prototype made 128-row, 16-column scans approximately 8%
slower (1.961 → 2.123 ms). The final implementation retains IPC for multi-column
outputs where every column has at most 2 KiB of backing buffers. The final small
wide scan is effectively unchanged. This internal heuristic uses backing-buffer
size, so sliced arrays may overestimate their payload and take the C Data path.

Dictionary, extension, binary/list-view, union and run-end-encoded output layouts
retain IPC, including nested occurrences. Arrow Go concatenation can retain source
buffers for some layouts; exporting those buffers directly would break the
ordinary-Go-allocation guarantee. Supported nested arrays are normalized before
export to preserve sliced offsets and avoid copying unrelated backing data.
Registration schemas still use IPC. Public APIs and ownership contracts are unchanged.

Raw samples: [IPC](benchmarks/callback-ipc-darwin-arm64.txt) and
[C Data](benchmarks/callback-cdata-darwin-arm64.txt).

## Reproduce

From this checkout, build the release library and both benchmark executables:

```sh
make bundle
git worktree add --detach /tmp/dfgo-ipc 1ae9d1c
cp callback_bench_test.go /tmp/dfgo-ipc/
(cd /tmp/dfgo-ipc && go test -c -o /tmp/dfgo-ipc.test .)
go test -c -o /tmp/dfgo-cdata.test .
export DATAFUSION_GO_LIBRARY="$PWD/internal/native/lib/darwin-arm64/libdatafusion_go.dylib"
```

Adjust the native library path for other platforms. Run five rounds without
concurrent builds, reversing executable order on even rounds:

```sh
for round in 1 2 3 4 5; do
    order="ipc cdata"
    case "$round" in 2|4) order="cdata ipc" ;; esac
    for transport in $order; do
        /tmp/dfgo-$transport.test -test.run='^$' \
            -test.bench=BenchmarkCallbackTransfer -test.benchmem \
            -test.benchtime=300ms -test.count=1 >> /tmp/dfgo-$transport.txt
    done
done
```

Use a POSIX shell for the loop, and start with empty output files for a fresh run.
The benchmark also exercises v1 callbacks against the updated native library.
The [Arrow C Data ownership specification](https://arrow.apache.org/docs/format/CDataInterface.html#memory-management)
and [Arrow Go export contract](https://pkg.go.dev/github.com/apache/arrow-go/v18/arrow/cdata#ExportArrowRecordBatch)
describe the buffer-lifetime constraints behind the copied-output design.

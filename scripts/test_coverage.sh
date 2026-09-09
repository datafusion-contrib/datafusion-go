#!/bin/sh
set -eu
cd "$(dirname "$0")/.."

library_name=$1
native_library=$2
export CARGO_LLVM_COV_TARGET_DIR="$PWD/rust/target/llvm-cov-target"
export CARGO_PROFILE_DEV_DEBUG=0
mkdir -p coverage
rm -f coverage/install/*.out
case "$(uname -s)" in
  Darwin)
    export CGO_CFLAGS="${CGO_CFLAGS:-} -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET:-13.0}"
    export CGO_LDFLAGS="${CGO_LDFLAGS:-} -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET:-13.0}"
    ;;
esac
cargo llvm-cov clean --manifest-path rust/Cargo.toml --profraw-only
cargo llvm-cov --manifest-path rust/Cargo.toml --locked --lib --tests --no-report

# Cargo builds cdylibs under deps for tests. Run the Go suite against that
# instrumented library so native coverage includes real calls across cgo.
instrumented="$CARGO_LLVM_COV_TARGET_DIR/debug/deps/$library_name"
test -f "$instrumented"
# TestMain explicitly flushes the profiling runtime before Go exits.
export LLVM_PROFILE_FILE="$CARGO_LLVM_COV_TARGET_DIR/go-%p-%m.profraw"
export DATAFUSION_GO_LIBRARY="$instrumented"
export DFGO_TEST_COVERAGE_DIR="$PWD/coverage/install"
go test -tags=datafusion_test_coverage -count=1 -covermode=atomic -coverpkg=./... -coverprofile=coverage/go.out ./...
set -- "$CARGO_LLVM_COV_TARGET_DIR"/go-*.profraw
test -f "$1" # Fail if the Go-to-Rust coverage path produced no native profile.
cargo llvm-cov report --manifest-path rust/Cargo.toml --lcov --output-path coverage/rust.lcov
# Clang's profiler runtime can differ from rustc's LLVM version. Keep their
# profiles and native libraries separate instead of mixing runtimes in C.
python3 scripts/test_native.py coverage "$native_library"
python3 scripts/coverage_summary.py
go tool cover -func=coverage/go.out > coverage/go.txt

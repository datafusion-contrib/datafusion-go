#!/bin/sh
set -eu

# Explicit target keeps sanitizer flags out of host build scripts/proc macros.
# build-std instruments the standard library as recommended by the Rust book.
cd "$(dirname "$0")/.."
toolchain=${RUST_ASAN_TOOLCHAIN:-nightly-2026-06-10}
target=$(rustc "+$toolchain" -vV | sed -n 's/^host: //p')
case "$target" in
  *-apple-darwin) leaks=0 ;;
  *-unknown-linux-gnu) leaks=1 ;;
  *) echo "Rust ASan target is unsupported by this test target: $target" >&2; exit 2 ;;
esac
export CARGO_TARGET_DIR="$PWD/rust/target/asan"
export CARGO_PROFILE_DEV_DEBUG=1
export RUSTFLAGS="-Zsanitizer=address"
export RUSTDOCFLAGS="$RUSTFLAGS"
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=$leaks:halt_on_error=1}"
cargo "+$toolchain" test --manifest-path rust/Cargo.toml --locked \
  -Zbuild-std --target "$target" --lib --tests

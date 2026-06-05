GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
MACOSX_DEPLOYMENT_TARGET ?= 13.0
NATIVE_PLATFORM := $(GOOS)-$(GOARCH)
NATIVE_LIB_DIR := internal/native/lib/$(NATIVE_PLATFORM)
NATIVE_LIB := $(NATIVE_LIB_DIR)/libdatafusion_go.a
CARGO_BUILD_TARGET ?=

ifeq ($(GOOS)-$(GOARCH),windows-amd64)
CARGO_BUILD_TARGET := $(or $(CARGO_BUILD_TARGET),x86_64-pc-windows-gnu)
endif

ifneq ($(strip $(CARGO_BUILD_TARGET)),)
RUST_TARGET_FLAG := --target $(CARGO_BUILD_TARGET)
RUST_TARGET_RELEASE_DIR := rust/target/$(CARGO_BUILD_TARGET)/release
else
RUST_TARGET_FLAG :=
RUST_TARGET_RELEASE_DIR := rust/target/release
endif

ifeq ($(GOOS),darwin)
RUST_BUILD_ENV := MACOSX_DEPLOYMENT_TARGET=$(MACOSX_DEPLOYMENT_TARGET) CFLAGS="$(strip $(CFLAGS) -mmacosx-version-min=$(MACOSX_DEPLOYMENT_TARGET))"
endif

.PHONY: generate generate.check rust bundle checksums verify.checksums test test.bundled test.source test.static consumer.smoke lint verify.release verify.release.downloaded clean

generate:
	go run ./internal/tools/genversions
	cargo update --manifest-path rust/Cargo.toml -p datafusion-go -p datafusion -p datafusion-sql

generate.check:
	go run ./internal/tools/genversions -check
	cargo metadata --manifest-path rust/Cargo.toml --locked --format-version 1 >/dev/null

rust: generate.check
	$(RUST_BUILD_ENV) cargo build --manifest-path rust/Cargo.toml --release $(RUST_TARGET_FLAG)

bundle: rust
	mkdir -p $(NATIVE_LIB_DIR)
	cp $(RUST_TARGET_RELEASE_DIR)/libdatafusion_go.a $(NATIVE_LIB)

checksums:
	mkdir -p internal/native/lib
	cd internal/native/lib && find . -name libdatafusion_go.a -print | sed 's#^\./##' | sort | while read -r file; do shasum -a 256 "$$file"; done > SHA256SUMS

verify.checksums:
	test -s internal/native/lib/SHA256SUMS
	cd internal/native/lib && shasum -a 256 -c SHA256SUMS

test: bundle
	$(MAKE) test.bundled

test.bundled:
	go test ./...

test.source: rust
	go test -tags=datafusion_use_source ./...

test.static: rust
	go test -tags=datafusion_use_static_lib ./...

consumer.smoke:
	@tmpdir=$$(mktemp -d); \
	trap 'rm -rf "$$tmpdir"' EXIT; \
	cd "$$tmpdir"; \
	go mod init example.com/datafusion-smoke >/dev/null; \
	go mod edit -replace github.com/datafusion-contrib/datafusion-go=$(CURDIR); \
	go get github.com/datafusion-contrib/datafusion-go >/dev/null; \
	printf '%s\n' \
		'package main' \
		'import (' \
		'	"context"' \
		'	"database/sql"' \
		'	"fmt"' \
		'	_ "github.com/datafusion-contrib/datafusion-go"' \
		')' \
		'func main() {' \
		'	db, err := sql.Open("datafusion", "")' \
		'	if err != nil { panic(err) }' \
		'	defer db.Close()' \
		'	var value int64' \
		'	if err := db.QueryRowContext(context.Background(), "select 1").Scan(&value); err != nil { panic(err) }' \
		'	if value != 1 { panic(fmt.Sprintf("got %d, want 1", value)) }' \
		'}' > main.go; \
	go run .

lint: generate.check
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run
	cargo clippy --manifest-path rust/Cargo.toml --all-targets -- -D warnings
	cargo fmt --manifest-path rust/Cargo.toml -- --check

verify.release: test test.source test.static
	go test -race ./...
	go vet ./...
	cargo test --manifest-path rust/Cargo.toml --release
	CGO_ENABLED=0 go test ./...
	$(MAKE) checksums
	$(MAKE) verify.checksums

verify.release.downloaded: verify.checksums test.bundled consumer.smoke test.source test.static
	go test -race ./...
	go vet ./...
	cargo test --manifest-path rust/Cargo.toml --release
	CGO_ENABLED=0 go test ./...
	$(MAKE) verify.checksums

clean:
	cargo clean --manifest-path rust/Cargo.toml
	go clean ./...

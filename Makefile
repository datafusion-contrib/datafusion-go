GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
MACOSX_DEPLOYMENT_TARGET ?= 13.0
DIST_DIR ?= dist
NATIVE_PLATFORM := $(GOOS)-$(GOARCH)
NATIVE_LIB_DIR := internal/native/lib/$(NATIVE_PLATFORM)
NATIVE_LIB := $(NATIVE_LIB_DIR)/libdatafusion_go.a
CARGO_BUILD_TARGET ?=

ifeq ($(GOOS),windows)
NATIVE_SHARED_NAME := datafusion_go.dll
else ifeq ($(GOOS),darwin)
NATIVE_SHARED_NAME := libdatafusion_go.dylib
else
NATIVE_SHARED_NAME := libdatafusion_go.so
endif

NATIVE_SHARED := $(NATIVE_LIB_DIR)/$(NATIVE_SHARED_NAME)

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

RUST_SHARED_LIB := $(RUST_TARGET_RELEASE_DIR)/$(NATIVE_SHARED_NAME)

ifeq ($(GOOS),darwin)
RUST_BUILD_ENV := MACOSX_DEPLOYMENT_TARGET=$(MACOSX_DEPLOYMENT_TARGET) CFLAGS="$(strip $(CFLAGS) -mmacosx-version-min=$(MACOSX_DEPLOYMENT_TARGET))"
STRIP_SHARED := strip -x
else ifeq ($(GOOS),linux)
STRIP_SHARED := strip --strip-unneeded
else ifeq ($(GOOS),windows)
STRIP_SHARED := strip --strip-unneeded
endif

.PHONY: generate generate.check rust.build rust.test rust.lint bundle checksum.native checksums verify.checksums stage.release.assets verify.release.assets go.lint go.vet go.test.dynamic go.test.bundled go.test.race go.test.source go.test.nocgo test test.source lint consumer.smoke release.verify clean

generate:
	go run ./internal/tools/genversions
	cargo update --manifest-path rust/Cargo.toml -p datafusion-go -p datafusion -p datafusion-sql

generate.check:
	go run ./internal/tools/genversions -check
	cargo metadata --manifest-path rust/Cargo.toml --locked --format-version 1 >/dev/null

rust.build: generate.check
	$(RUST_BUILD_ENV) cargo build --manifest-path rust/Cargo.toml --release $(RUST_TARGET_FLAG)

rust.test:
	cargo test --manifest-path rust/Cargo.toml --release $(RUST_TARGET_FLAG)

rust.lint:
	cargo clippy --manifest-path rust/Cargo.toml --all-targets -- -D warnings
	cargo fmt --manifest-path rust/Cargo.toml -- --check

bundle: rust.build
	mkdir -p $(NATIVE_LIB_DIR)
	cp $(RUST_TARGET_RELEASE_DIR)/libdatafusion_go.a $(NATIVE_LIB)
	cp $(RUST_SHARED_LIB) $(NATIVE_SHARED)
	if [ -n "$(STRIP_SHARED)" ]; then $(STRIP_SHARED) $(NATIVE_SHARED); fi

checksum.native:
	cd $(NATIVE_LIB_DIR) && shasum -a 256 *datafusion_go* > SHA256SUMS-$(NATIVE_PLATFORM)

checksums:
	mkdir -p internal/native/lib
	cd internal/native/lib && find . -type f \( -name libdatafusion_go.a -o -name libdatafusion_go.so -o -name libdatafusion_go.dylib -o -name datafusion_go.dll \) -print | sed 's#^\./##' | sort | while read -r file; do shasum -a 256 "$$file"; done > SHA256SUMS

verify.checksums:
	test -s internal/native/lib/SHA256SUMS
	cd internal/native/lib && shasum -a 256 -c SHA256SUMS

stage.release.assets:
	@metadata=$$(mktemp); \
	go run ./internal/tools/genversions -github-output "$$metadata"; \
	. "$$metadata"; \
	rm -f "$$metadata"; \
	rm -rf "$(DIST_DIR)"; \
	mkdir -p "$(DIST_DIR)"; \
	find internal/native/lib -mindepth 2 -maxdepth 2 -type f \( -name 'libdatafusion_go.a' -o -name 'libdatafusion_go.so' -o -name 'libdatafusion_go.dylib' -o -name 'datafusion_go.dll' \) -print | sort | while IFS= read -r file; do \
		platform="$$(basename "$$(dirname "$$file")")"; \
		base="$$(basename "$$file")"; \
		cp "$$file" "$(DIST_DIR)/datafusion-go-$${release_tag}-$${platform}-$${base}"; \
	done
	cd "$(DIST_DIR)" && shasum -a 256 datafusion-go-* > SHA256SUMS
	cd "$(DIST_DIR)" && shasum -a 256 -c SHA256SUMS
	cp "$(DIST_DIR)/SHA256SUMS" internal/native/lib/SHA256SUMS

verify.release.assets:
	@metadata=$$(mktemp); \
	go run ./internal/tools/genversions -github-output "$$metadata"; \
	. "$$metadata"; \
	rm -f "$$metadata"; \
	cmp "$(DIST_DIR)/SHA256SUMS" internal/native/lib/SHA256SUMS; \
	(cd "$(DIST_DIR)" && shasum -a 256 -c SHA256SUMS); \
	for asset in \
		"datafusion-go-$${release_tag}-darwin-arm64-libdatafusion_go.dylib" \
		"datafusion-go-$${release_tag}-darwin-amd64-libdatafusion_go.dylib" \
		"datafusion-go-$${release_tag}-linux-amd64-libdatafusion_go.so" \
		"datafusion-go-$${release_tag}-linux-arm64-libdatafusion_go.so" \
		"datafusion-go-$${release_tag}-windows-amd64-datafusion_go.dll"; do \
		test -f "$(DIST_DIR)/$$asset"; \
		grep -F "  $$asset" "$(DIST_DIR)/SHA256SUMS" >/dev/null; \
	done

go.lint: generate.check
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run

go.vet:
	go vet ./...

# -count=1 on every native-linked suite: Go's build cache does not hash
# external cgo archives (golang/go#28019), so cached test results could
# report success without exercising the native library currently on disk.
go.test.dynamic:
	DATAFUSION_GO_LIBRARY=$(CURDIR)/$(NATIVE_SHARED) go test -count=1 ./...

go.test.bundled:
	go test -count=1 -tags=datafusion_use_bundled ./...

go.test.race:
	DATAFUSION_GO_LIBRARY=$(CURDIR)/$(NATIVE_SHARED) go test -race -count=1 ./...

go.test.source:
	go test -count=1 -tags=datafusion_use_source ./...

go.test.nocgo:
	CGO_ENABLED=0 go test -count=1 ./...

test: bundle
	$(MAKE) go.test.dynamic
	$(MAKE) go.test.bundled

test.source: rust.build
	$(MAKE) go.test.source

lint: go.lint rust.lint

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
		DATAFUSION_GO_LIBRARY=$(CURDIR)/$(NATIVE_SHARED) go run .

release.verify: verify.release.assets
	$(MAKE) go.test.bundled
	$(MAKE) go.test.race

clean:
	cargo clean --manifest-path rust/Cargo.toml
	go clean ./...

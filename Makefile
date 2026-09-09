GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
MACOSX_DEPLOYMENT_TARGET ?= 13.0
DIST_DIR ?= dist
NATIVE_PLATFORM := $(GOOS)-$(GOARCH)
NATIVE_LIB_DIR := internal/native/lib/$(NATIVE_PLATFORM)
NATIVE_LIB := $(NATIVE_LIB_DIR)/libdatafusion_go.a
CARGO_BUILD_TARGET ?=
FUZZ_TIME ?= 30s
RUST_FUZZ_SECONDS ?= 30
RUST_FUZZ_FLAGS ?=
RUST_ASAN_TOOLCHAIN ?= nightly-2026-06-10
SQLLOGIC_TARGET_DIR ?= rust/target/sqllogictest
SQLLOGIC_RUN ?= ^TestSQLLogic$$

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

.PHONY: generate generate.check rust.build rust.test rust.lint rust.audit bundle checksum.native checksums verify.checksums changelog.check stage.release.assets verify.release.assets go.lint go.vet go.vuln go.test.dynamic go.test.bundled go.test.race go.test.source go.test.nocgo test test.source lint consumer.smoke release.verify clean

generate:
	go run ./internal/tools/genversions
	go run ./internal/tools/genabi
	cargo update --manifest-path rust/Cargo.toml -p datafusion-go -p datafusion -p datafusion-ffi -p datafusion-sql

generate.check:
	go run ./internal/tools/genversions -check
	go run ./internal/tools/genabi -check
	cargo metadata --manifest-path rust/Cargo.toml --locked --format-version 1 >/dev/null

rust.build: generate.check
	$(RUST_BUILD_ENV) cargo build --manifest-path rust/Cargo.toml --release $(RUST_TARGET_FLAG)

rust.test:
	$(RUST_BUILD_ENV) cargo test --manifest-path rust/Cargo.toml --release $(RUST_TARGET_FLAG)

rust.lint:
	$(RUST_BUILD_ENV) cargo clippy --manifest-path rust/Cargo.toml --all-targets --features test-sqllogictest -- -D warnings
	$(RUST_BUILD_ENV) cargo clippy --manifest-path rust/fuzz/Cargo.toml --all-targets -- -D warnings
	cargo fmt --manifest-path rust/Cargo.toml --all -- --check

rust.audit:
	cargo audit --file rust/Cargo.lock --deny unsound

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

# The release workflow extracts notes for the derived tag with the same
# heading contract: exactly "## <tag>" or "## <tag> - <date>", non-empty body.
changelog.check:
	@set -eu; \
	metadata=$$(mktemp); \
	go run ./internal/tools/genversions -github-output "$$metadata"; \
	. "$$metadata"; \
	rm -f "$$metadata"; \
	awk -v tag="$$release_tag" '$$0 == "## " tag || index($$0, "## " tag " - ") == 1 { found = 1; next }; found && /^## / { exit }; found { print }; END { if (!found) exit 1 }' CHANGELOG.md \
		| grep -q '[^[:space:]]' \
		|| { echo "CHANGELOG.md is missing a non-empty '## $$release_tag' or '## $$release_tag - <date>' entry" >&2; exit 1; }

stage.release.assets:
	@set -eu; \
	metadata=$$(mktemp); \
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
	@set -eu; \
	metadata=$$(mktemp); \
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
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2 run
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2 run --build-tags=datafusion_test_coverage
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2 run --build-tags=datafusion_test_sqllogic

go.vet:
	go vet ./...

go.vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...

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
	@set -eu; \
	tmpdir=$$(mktemp -d); \
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

# Fast feedback after make bundle; release gates above remain unchanged.
.PHONY: test.quick test.install test.sqlite test.sequences test.fuzz rust.fuzz rust.test.asan test.native.asan test.native.coverage test.coverage test.extended
test.quick: generate.check
	DATAFUSION_GO_LIBRARY=$(CURDIR)/$(NATIVE_SHARED) go test -short -count=1 ./...

test.install:
	DATAFUSION_GO_LIBRARY=$(CURDIR)/$(NATIVE_SHARED) go test -count=1 -run '^TestLibraryProcesses$$' ./internal/native

test.sqlite:
	python3 scripts/test_sqlite.py

test.sequences:
	DATAFUSION_GO_LIBRARY=$(CURDIR)/$(NATIVE_SHARED) DFGO_TEST_STEPS=$${DFGO_TEST_STEPS:-2000} go test -race -count=1 -run 'TestLifecycleSequences|TestRegistrationFailureAtEveryBatch' .

test.fuzz:
	DATAFUSION_GO_LIBRARY=$(CURDIR)/$(NATIVE_SHARED) go test -run '^$$' -fuzz '^FuzzQueryBindings$$' -fuzztime=$(FUZZ_TIME) -parallel=2 .
	go test -run '^$$' -fuzz '^FuzzParseSemver$$' -fuzztime=$(FUZZ_TIME) -parallel=2 ./internal/tools/genversions

rust.fuzz:
	mkdir -p rust/target/fuzz-corpus/prepare
	$(RUST_BUILD_ENV) CARGO_PROFILE_DEV_DEBUG=0 sh -c 'cd rust && cargo +$(RUST_ASAN_TOOLCHAIN) fuzz run $(RUST_FUZZ_FLAGS) prepare target/fuzz-corpus/prepare fuzz/corpus/prepare -- -max_total_time=$(RUST_FUZZ_SECONDS) -max_len=8192'

rust.test.asan:
	$(RUST_BUILD_ENV) RUST_ASAN_TOOLCHAIN=$(RUST_ASAN_TOOLCHAIN) sh scripts/test_rust_asan.sh

test.native.asan: rust.test.asan
	python3 scripts/test_native.py asan $(NATIVE_SHARED)

test.native.coverage:
	python3 scripts/test_native.py coverage $(NATIVE_SHARED)

test.coverage:
	$(RUST_BUILD_ENV) sh scripts/test_coverage.sh $(NATIVE_SHARED_NAME) $(NATIVE_SHARED)

test.extended: test.sqlite test.install test.sequences test.fuzz rust.fuzz test.native.asan test.coverage

# The optional upstream fixture dependencies and test callbacks live in their
# own build directory. Release and source-link artifacts remain independent.
.PHONY: sqllogic.sync sqllogic.driver.sync sqllogic.check sqllogic.tools.test rust.sqllogic test.sqllogic test.sqllogic.oracle
sqllogic.sync:
	python3 scripts/sqllogictest.py sync

sqllogic.driver.sync:
	python3 scripts/sqllogictest.py sync-driver

sqllogic.check:
	python3 scripts/sqllogictest.py check

sqllogic.tools.test:
	python3 -m unittest discover -s scripts -p 'sqllogictest_tools_test.py' -v

rust.sqllogic: generate.check sqllogic.check
	$(RUST_BUILD_ENV) cargo build --manifest-path rust/Cargo.toml --release $(RUST_TARGET_FLAG) --target-dir $(SQLLOGIC_TARGET_DIR) --features test-sqllogictest --locked

test.sqllogic: rust.sqllogic sqllogic.tools.test
	$(RUST_BUILD_ENV) cargo test --manifest-path rust/Cargo.toml --release $(RUST_TARGET_FLAG) --target-dir $(SQLLOGIC_TARGET_DIR) --features test-sqllogictest --locked --lib sqllogictest::
	DATAFUSION_GO_LIBRARY=$(abspath $(SQLLOGIC_TARGET_DIR))/$(if $(CARGO_BUILD_TARGET),$(CARGO_BUILD_TARGET)/)release/$(NATIVE_SHARED_NAME) go test -count=1 -tags=datafusion_test_sqllogic -run '^TestSQLLogicHarness$$' .
	DATAFUSION_GO_LIBRARY=$(abspath $(SQLLOGIC_TARGET_DIR))/$(if $(CARGO_BUILD_TARGET),$(CARGO_BUILD_TARGET)/)release/$(NATIVE_SHARED_NAME) python3 scripts/sqllogictest.py run --run '$(SQLLOGIC_RUN)'

test.sqllogic.oracle:
	$(RUST_BUILD_ENV) python3 scripts/sqllogictest.py oracle --target-dir $(SQLLOGIC_TARGET_DIR)

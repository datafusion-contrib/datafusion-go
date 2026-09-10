package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestVersionBumpPolicy(t *testing.T) {
	version := func(df string, major, patch, abi int) config {
		t.Helper()
		data := fmt.Sprintf("[datafusion]\nversion = %q\n[datafusion_go]\nmajor = %d\npatch = %d\n[abi]\nversion = %d\n", df, major, patch, abi)
		cfg, err := parseConfig("test", []byte(data))
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	base := version("55.0.0", 0, 1, 1)
	for _, test := range []struct {
		name    string
		base    config
		current config
		shipped bool
		wantErr string
	}{
		{"docs only", base, base, false, ""},
		{"forgotten bump", base, base, true, "require a new release version"},
		{"next patch", base, version("55.0.0", 0, 2, 1), true, ""},
		{"release-only PR", base, version("55.0.0", 0, 2, 1), false, ""},
		{"skipped patch", base, version("55.0.0", 0, 3, 1), true, "patch must be 2"},
		{"patch rollback", base, version("55.0.0", 0, 0, 1), true, "patch must be 2"},
		{"new engine major", base, version("56.0.0", 0, 0, 1), true, ""},
		{"new engine minor", base, version("55.1.0", 0, 0, 1), true, ""},
		{"new engine patch", base, version("55.0.1", 0, 0, 1), true, ""},
		{"new engine without reset", base, version("56.0.0", 0, 2, 1), true, "patch must be 0"},
		{"engine rollback", base, version("54.1.0", 0, 0, 1), true, "datafusion.version must not decrease"},
		{"module major", base, version("55.0.0", 1, 2, 1), true, ""},
		{"module major skips", base, version("55.0.0", 2, 2, 1), true, "major must stay"},
		{"module major rollback", version("55.0.0", 1, 1, 1), version("55.0.0", 0, 2, 1), true, "major must stay"},
		{"ABI without release", base, version("55.0.0", 0, 1, 2), false, "require a new release version"},
		{"ABI rollback", base, version("55.0.0", 0, 2, 0), true, "abi.version must not decrease"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var files []string
			if test.shipped {
				files = []string{"connection.go"}
			}
			err := validateVersionBump(test.base, test.current, files)
			assertBumpError(t, err, test.wantErr)
		})
	}
}

func TestShippedFiles(t *testing.T) {
	for _, name := range []string{
		"connection.go", "version.go", "shim.c", "shim.h", "go.mod", "go.sum", "Makefile", ".gitattributes",
		".github/workflows/release.yml", "internal/native/native.go", "internal/native/dynamic_loader.h",
		"internal/native/lib/SHA256SUMS", "internal/newpackage/feature.go", "newpackage/api.go", "rust/Cargo.toml", "rust/Cargo.lock",
		"rust/build.rs", "rust/include/datafusion_go.h", "rust/src/query.rs", "rust/src/query/new_module.rs",
	} {
		if !isShippedFile(name) {
			t.Errorf("shipped file ignored: %s", name)
		}
	}
	for _, name := range []string{
		"", "versions.toml", "README.md", "CHANGELOG.md", "docs/testing.md", "examples/simple/main.go",
		"connection_test.go", "sqllogictest_test.go", "testdata/new.slt", "internal/native/native_test.go",
		"internal/native/test_coverage.go", "internal/native/sqllogictest.go", "internal/native/sqllogictest.h",
		"internal/native/README.md",
		"internal/native/testdata/fixture.c", "internal/conformance/corpus.go", "internal/tools/genversions/main.go",
		"rust/src/query/tests.rs", "rust/src/query/tests/prepare.rs", "rust/src/coverage_tests.rs",
		"rust/src/sqllogictest.rs", "rust/src/sqllogictest/coverage.rs", "rust/src/abi/contract_generated.rs",
		"rust/tests/conformance.rs", "rust/fuzz/Cargo.toml", "scripts/sqllogictest.py", ".github/workflows/ci.yml",
	} {
		if isShippedFile(name) {
			t.Errorf("development file requires a release: %s", name)
		}
	}
}

func TestReleaseNotesHeading(t *testing.T) {
	for _, test := range []struct {
		text string
		want bool
	}{
		{"## v0.550000.2\n\n- Fixed a bug.\n", true},
		{"## v0.550000.2 - 2026-09-09\n\nRelease notes.\n", true},
		{"## v0.550000.2\n\n", false},
		{"## v0.550000.2\n\n## v0.550000.1\nOld notes.\n", false},
		{"## v0.550000.20\nOther release.\n", false},
		{"## v0.550000.2-rc1\nPrerelease.\n", false},
		{"## v0.550000.1\nMentions v0.550000.2.\n", false},
	} {
		if got := hasReleaseNotes(test.text, "v0.550000.2"); got != test.want {
			t.Errorf("hasReleaseNotes(%q) = %t, want %t", test.text, got, test.want)
		}
	}
}

func TestVersionBumpAgainstGitBase(t *testing.T) {
	t.Chdir(t.TempDir())
	git := func(args ...string) string {
		t.Helper()
		output, err := gitOutput(args...)
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(output))
	}
	git("init", "-q")
	writeFixture(t, "versions.toml", sampleConfig)
	writeFixture(t, "CHANGELOG.md", "## v0.530102.3\n\nOld notes.\n")
	writeFixture(t, "driver.go", "package driver\n")
	writeFixture(t, "README.md", "Documentation.\n")
	git("add", ".")
	git("-c", "user.name=Version test", "-c", "user.email=version-test@example.invalid",
		"-c", "commit.gpgsign=false", "-c", "core.hooksPath=.git/disabled-hooks", "commit", "-qm", "base")
	base := git("rev-parse", "HEAD")
	git("tag", "v0.530102.3")
	writeFixture(t, "README.md", "Updated documentation.\n")
	assertBumpError(t, checkVersionBump(base), "")

	// A deletion or rename out of the package still requires a release.
	git("mv", "driver.go", "example.txt")
	assertBumpError(t, checkVersionBump(base), "require a new release version")
	git("mv", "example.txt", "driver.go")
	writeFixture(t, "driver.go", "package driver\n// Shipped change.\n")
	assertBumpError(t, checkVersionBump(base), "require a new release version")

	writeFixture(t, "versions.toml", strings.Replace(sampleConfig, "patch = 3", "patch = 4", 1))
	assertBumpError(t, checkVersionBump(base), "CHANGELOG.md needs a non-empty")
	writeFixture(t, "CHANGELOG.md", "## v0.530102.4\n\nNew release notes.\n")
	assertBumpError(t, checkVersionBump(base), "")
	git("tag", "v0.530102.4")
	assertBumpError(t, checkVersionBump(base), "already exists")
	assertBumpError(t, checkVersionBump("missing-base"), "git rev-parse")
	if err := os.WriteFile("versions.toml", []byte("broken config"), 0o644); err != nil {
		t.Fatal(err)
	}
	assertBumpError(t, checkVersionBump(base), "key outside section")
}

func assertBumpError(t *testing.T, err error, want string) {
	t.Helper()
	if want == "" {
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

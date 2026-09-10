package main

import (
	"bytes"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const sampleConfig = `[datafusion]
version = "53.1.2" # pinned upstream
[datafusion_go]
major = 0
patch = 3
[abi]
version = 1
`

func writeFixture(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestReadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "versions.toml")
	writeFixture(t, path, sampleConfig)
	cfg, err := readConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.releaseTag(); got != "v0.530102.3" {
		t.Fatalf("release tag %q", got)
	}
	for name, bad := range map[string]string{
		"missing":           strings.ReplaceAll(sampleConfig, "patch = 3", ""),
		"negative":          strings.ReplaceAll(sampleConfig, "patch = 3", "patch = -1"),
		"unknown key":       strings.ReplaceAll(sampleConfig, "patch = 3", "patc = 3"),
		"unknown section":   strings.ReplaceAll(sampleConfig, "[abi]", "[abii]"),
		"outside section":   "patch = 3\n" + sampleConfig,
		"duplicate key":     strings.ReplaceAll(sampleConfig, "patch = 3", "patch = 3\npatch = 4"),
		"duplicate section": sampleConfig + "[abi]\nversion = 1\n",
		"missing equals":    strings.ReplaceAll(sampleConfig, "patch = 3", "patch 3"),
		"unquoted":          strings.ReplaceAll(sampleConfig, `"53.1.2"`, "53.1.2"),
		"empty string":      strings.ReplaceAll(sampleConfig, `"53.1.2"`, `""`),
		"escape":            strings.ReplaceAll(sampleConfig, `"53.1.2"`, `"53.1.\q"`),
		"prerelease":        strings.ReplaceAll(sampleConfig, "53.1.2", "53.1.2-rc1"),
		"minor width":       strings.ReplaceAll(sampleConfig, "53.1.2", "53.100.2"),
		"patch width":       strings.ReplaceAll(sampleConfig, "53.1.2", "53.1.100"),
		"semver overflow":   strings.ReplaceAll(sampleConfig, "53.1.2", "999999999999999999999999999.1.2"),
		"integer overflow":  strings.ReplaceAll(sampleConfig, "patch = 3", "patch = 999999999999999999999999999"),
		"abi overflow":      strings.ReplaceAll(sampleConfig, "version = 1\n", "version = 2147483648\n"),
	} {
		t.Run(name, func(t *testing.T) {
			writeFixture(t, path, bad)
			if _, err := readConfig(path); err == nil {
				t.Fatalf("accepted invalid config:\n%s", bad)
			}
		})
	}
}

func TestQuotedComments(t *testing.T) {
	for input, want := range map[string]string{
		`version = "a#b" # comment`:   `version = "a#b" `,
		`version = "a\"#b" # comment`: `version = "a\"#b" `,
		"# comment":                   "",
		"patch = 0":                   "patch = 0",
	} {
		if got := stripComment(input); got != want {
			t.Errorf("stripComment(%q) = %q, want %q", input, got, want)
		}
	}
}

const cargoFixture = `[package]
name = "datafusion-go"
version = "old"
edition = "2024"

[dependencies]
datafusion = "=old"
datafusion-ffi = "=old"
datafusion-sql = "=old"
tokio = { version = "1", features = ["rt"] }

[dependencies.datafusion-sqllogictest]
version = "=old"
optional = true

[dev-dependencies]
serde_json = "1"
`

func TestGenerationAndDrift(t *testing.T) {
	t.Chdir(t.TempDir())
	writeFixture(t, "versions.toml", sampleConfig)
	writeFixture(t, "rust/Cargo.toml", cargoFixture)
	if err := run(true, ""); err == nil {
		t.Fatal("check accepted missing outputs")
	}
	if _, err := os.Stat("version.go"); !os.IsNotExist(err) {
		t.Fatalf("check wrote output: %v", err)
	}
	if err := run(false, ""); err != nil {
		t.Fatal(err)
	}
	if err := run(true, ""); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"version.go", "internal/native/version_generated.go"} {
		if _, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors); err != nil {
			t.Fatal(err)
		}
	}
	cargo, err := os.ReadFile("rust/Cargo.toml")
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Replace(cargoFixture, `version = "old"`, `version = "0.530102.3"`, 1)
	want = strings.ReplaceAll(want, `"=old"`, `"=53.1.2"`)
	if string(cargo) != want {
		t.Fatalf("unexpected Cargo rewrite:\n%s", cargo)
	}
	if err := run(false, ""); err != nil {
		t.Fatal(err)
	}
	again, err := os.ReadFile("rust/Cargo.toml")
	if err != nil || !bytes.Equal(cargo, again) {
		t.Fatalf("generation is not idempotent: %v", err)
	}
	writeFixture(t, "version.go", "stale")
	if err := run(true, ""); err == nil {
		t.Fatal("check accepted stale output")
	}
	if err := run(false, "output.txt"); err != nil {
		t.Fatal(err)
	}
	out, err := os.ReadFile("output.txt")
	if err != nil || string(out) != "release_tag=v0.530102.3\ndatafusion_version=53.1.2\ndatafusion_go_version=0.530102.3\n" {
		t.Fatalf("GitHub output: %q, %v", out, err)
	}
}

func TestCargoMissingFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "Cargo.toml")
	for _, field := range []string{`version = "old"`, `datafusion = "=old"`, `datafusion-ffi = "=old"`, `datafusion-sql = "=old"`, `version = "=old"`} {
		writeFixture(t, path, strings.Replace(cargoFixture, field, "", 1))
		if _, err := updateCargoToml(path, config{}); err == nil {
			t.Errorf("accepted missing field %s", field)
		}
	}
}

func FuzzParseSemver(f *testing.F) {
	for _, seed := range []string{"53.1.2", "0.0.0", "999999999999999999999.0.0", "bad"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, version string) {
		major, minor, patch, err := parseSemver(version)
		if err == nil && (major < 0 || minor < 0 || patch < 0) {
			t.Fatal("successful parse produced a negative component")
		}
	})
}

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseRelevantPath(t *testing.T) {
	t.Parallel()

	tests := map[string]bool{
		"datafusion.go":                      true,
		"ffi_table_provider.go":              true,
		"ffi_table_provider_test.go":         false,
		"go.mod":                             true,
		"internal/native/native.go":          true,
		"internal/native/lib/SHA256SUMS":     false,
		"internal/tools/genversions/main.go": true,
		"rust/Cargo.toml":                    true,
		"rust/src/lib.rs":                    true,
		"versions.toml":                      true,
		".github/workflows/release.yml":      false,
		"docs/architecture.md":               false,
		"examples/simple/main.go":            false,
		"CHANGELOG.md":                       false,
		"CONTRIBUTING.md":                    false,
		"README.md":                          false,
	}

	for path, want := range tests {
		path, want := path, want
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			if got := releaseRelevantPath(path); got != want {
				t.Fatalf("releaseRelevantPath(%q) = %v, want %v", path, got, want)
			}
		})
	}
}

func TestCheckReleaseNovelty(t *testing.T) {
	repo := t.TempDir()
	runGit(t, repo, "init")
	runGit(t, repo, "config", "user.name", "Release Test")
	runGit(t, repo, "config", "user.email", "release-test@example.com")

	writeTestFile(t, repo, "datafusion.go", "package datafusion\n")
	runGit(t, repo, "add", "datafusion.go")
	runGit(t, repo, "commit", "-m", "initial")

	cfg := config{
		DataFusionVersion: "53.1.0",
		GoMajor:           0,
		GoPatch:           1,
	}
	runGit(t, repo, "tag", cfg.releaseTag())

	restore := chdir(t, repo)
	defer restore()

	if err := checkReleaseNovelty(cfg); err != nil {
		t.Fatalf("checkReleaseNovelty() at tag: %v", err)
	}

	writeTestFile(t, repo, "README.md", "documentation\n")
	if err := checkReleaseNovelty(cfg); err != nil {
		t.Fatalf("checkReleaseNovelty() with documentation change: %v", err)
	}

	writeTestFile(t, repo, "datafusion.go", "package datafusion\n\nconst changed = true\n")
	err := checkReleaseNovelty(cfg)
	if err == nil {
		t.Fatal("checkReleaseNovelty() with code change succeeded, want error")
	}
	if message := err.Error(); !strings.Contains(message, cfg.releaseTag()) || !strings.Contains(message, "datafusion.go") {
		t.Fatalf("checkReleaseNovelty() error = %q, want tag and changed path", message)
	}

	cfg.GoPatch = 2
	if err := checkReleaseNovelty(cfg); err != nil {
		t.Fatalf("checkReleaseNovelty() with a new tag: %v", err)
	}
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
}

func writeTestFile(t *testing.T, root, name, contents string) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}

func chdir(t *testing.T, dir string) func() {
	t.Helper()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	return func() {
		if err := os.Chdir(previous); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	}
}

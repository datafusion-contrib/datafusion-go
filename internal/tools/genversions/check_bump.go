package main

import (
	"cmp"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
)

func checkVersionBump(baseRef string) error {
	commit, err := gitOutput("rev-parse", "--verify", "--end-of-options", baseRef+"^{commit}")
	if err != nil {
		return err
	}
	baseCommit := strings.TrimSpace(string(commit))
	data, err := gitOutput("show", baseCommit+":versions.toml")
	if err != nil {
		return err
	}
	base, err := parseConfig(baseCommit+":versions.toml", data)
	if err != nil {
		return err
	}
	current, err := readConfig("versions.toml")
	if err != nil {
		return err
	}
	// Include both sides of renames so moving a shipped file cannot bypass the check.
	diff, err := gitOutput("diff", "--name-only", "--no-renames", "-z", baseCommit, "--")
	if err != nil {
		return err
	}
	var shipped []string
	for _, name := range strings.Split(string(diff), "\x00") {
		if isShippedFile(name) {
			shipped = append(shipped, name)
		}
	}
	if err := validateVersionBump(base, current, shipped); err != nil {
		return err
	}
	if current.releaseTag() == base.releaseTag() {
		fmt.Println("No shipped package changes require a release version bump.")
		return nil
	}
	tags, err := gitOutput("tag", "--list", current.releaseTag())
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(tags))) != 0 {
		return fmt.Errorf("release tag %s already exists; choose the next unreleased version", current.releaseTag())
	}
	changelog, err := os.ReadFile("CHANGELOG.md")
	if err != nil {
		return err
	}
	if !hasReleaseNotes(string(changelog), current.releaseTag()) {
		return fmt.Errorf("CHANGELOG.md needs a non-empty '## %s' or '## %s - <date>' entry", current.releaseTag(), current.releaseTag())
	}
	fmt.Printf("Release version %s -> %s and changelog entry verified.\n", base.releaseTag(), current.releaseTag())
	return nil
}

func gitOutput(args ...string) ([]byte, error) {
	output, err := exec.Command("git", args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w\n%s", strings.Join(args, " "), err, output)
	}
	return output, nil
}

func validateVersionBump(base, current config, shipped []string) error {
	if current.ABIVersion < base.ABIVersion {
		return fmt.Errorf("abi.version must not decrease (%d -> %d)", base.ABIVersion, current.ABIVersion)
	}
	if current.releaseTag() == base.releaseTag() {
		if len(shipped) != 0 || current.ABIVersion != base.ABIVersion {
			return fmt.Errorf("shipped package changes require a new release version (still %s); update versions.toml, run make generate, and add release notes in CHANGELOG.md; changed files: %v", current.releaseTag(), shipped)
		}
		return nil
	}
	if current.GoMajor != base.GoMajor && current.GoMajor != base.GoMajor+1 {
		return fmt.Errorf("datafusion_go.major must stay at %d or increment to %d", base.GoMajor, base.GoMajor+1)
	}
	comparison := compareDataFusion(current, base)
	if comparison < 0 {
		return fmt.Errorf("datafusion.version must not decrease (%s -> %s)", base.DataFusionVersion, current.DataFusionVersion)
	}
	wantPatch := 0
	if comparison == 0 {
		wantPatch = base.GoPatch + 1
	}
	if current.GoPatch != wantPatch {
		return fmt.Errorf("datafusion_go.patch must be %d for DataFusion %s (got %d): use 0 for a new DataFusion version, otherwise increment the previous patch", wantPatch, current.DataFusionVersion, current.GoPatch)
	}
	return nil
}

func compareDataFusion(a, b config) int {
	for _, pair := range [][2]int{{a.DataFusionMajor, b.DataFusionMajor}, {a.DataFusionMinor, b.DataFusionMinor}, {a.DataFusionPatch, b.DataFusionPatch}} {
		if comparison := cmp.Compare(pair[0], pair[1]); comparison != 0 {
			return comparison
		}
	}
	return 0
}

func isShippedFile(name string) bool {
	switch name {
	case "go.mod", "go.sum", "rust/Cargo.toml", "rust/Cargo.lock", "rust/build.rs",
		"Makefile", ".gitattributes", ".github/workflows/release.yml":
		return true
	}
	base := path.Base(name)
	if strings.HasSuffix(base, "_test.go") || strings.HasSuffix(base, ".md") || strings.Contains(name, "/testdata/") {
		return false
	}
	for _, prefix := range []string{"docs/", "examples/", "testdata/", "scripts/", ".agents/", ".codex/", ".github/", "internal/tools/", "internal/conformance/"} {
		if strings.HasPrefix(name, prefix) {
			return false
		}
	}
	if !strings.Contains(name, "/") {
		return strings.HasSuffix(name, ".go") || strings.HasSuffix(name, ".c") || strings.HasSuffix(name, ".h")
	}
	if strings.HasPrefix(name, "internal/") {
		return !strings.HasPrefix(base, "test_") && !strings.HasPrefix(base, "sqllogictest.")
	}
	if strings.HasSuffix(name, ".go") {
		return true
	}
	if strings.HasPrefix(name, "rust/src/") {
		return base != "tests.rs" && !strings.HasSuffix(base, "_tests.rs") &&
			!strings.Contains(name, "/tests/") && name != "rust/src/sqllogictest.rs" &&
			!strings.HasPrefix(name, "rust/src/sqllogictest/") &&
			name != "rust/src/abi/contract_generated.rs"
	}
	return strings.HasPrefix(name, "rust/include/")
}

func hasReleaseNotes(changelog, tag string) bool {
	heading := "## " + tag
	inRelease := false
	for _, line := range strings.Split(changelog, "\n") {
		if line == heading || strings.HasPrefix(line, heading+" - ") {
			inRelease = true
			continue
		}
		if inRelease && strings.HasPrefix(line, "## ") {
			return false
		}
		if inRelease && strings.TrimSpace(line) != "" {
			return true
		}
	}
	return false
}

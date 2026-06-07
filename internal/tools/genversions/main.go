package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type config struct {
	DataFusionVersion string
	DataFusionMajor   int
	DataFusionMinor   int
	DataFusionPatch   int
	GoMajor           int
	GoPatch           int
	ABIVersion        int
}

func main() {
	check := flag.Bool("check", false, "check generated files without writing")
	githubOutput := flag.String("github-output", "", "append computed release values to a GitHub Actions output file")
	flag.Parse()

	cfg, err := readConfig("versions.toml")
	if err != nil {
		fatal(err)
	}

	if *githubOutput != "" {
		if err := appendGitHubOutput(*githubOutput, cfg); err != nil {
			fatal(err)
		}
		return
	}

	updates, err := plannedUpdates(cfg)
	if err != nil {
		fatal(err)
	}

	var stale []string
	for path, want := range updates {
		current, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			fatal(err)
		}
		if bytes.Equal(current, want) {
			continue
		}
		if *check {
			stale = append(stale, path)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			fatal(err)
		}
		if err := os.WriteFile(path, want, 0o644); err != nil {
			fatal(err)
		}
	}

	if len(stale) != 0 {
		fatal(fmt.Errorf("generated version files are stale; run `make generate`: %s", strings.Join(stale, ", ")))
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func readConfig(path string) (config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return config{}, err
	}

	values := map[string]string{}
	section := ""
	for lineNo, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(stripComment(raw))
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(line, "["), "]"))
			switch section {
			case "datafusion", "datafusion_go", "abi":
			default:
				return config{}, fmt.Errorf("%s:%d: unknown section %q", path, lineNo+1, section)
			}
			continue
		}
		if section == "" {
			return config{}, fmt.Errorf("%s:%d: key outside section", path, lineNo+1)
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return config{}, fmt.Errorf("%s:%d: expected key = value", path, lineNo+1)
		}
		name := section + "." + strings.TrimSpace(key)
		switch name {
		case "datafusion.version", "datafusion_go.major", "datafusion_go.patch", "abi.version":
		default:
			return config{}, fmt.Errorf("%s:%d: unknown key %q", path, lineNo+1, name)
		}
		values[name] = strings.TrimSpace(value)
	}

	version, err := requiredString(values, "datafusion.version")
	if err != nil {
		return config{}, err
	}
	major, minor, patch, err := parseSemver(version)
	if err != nil {
		return config{}, err
	}
	if minor > 99 || patch > 99 {
		return config{}, fmt.Errorf("datafusion.version %q cannot be encoded with two-digit minor and patch components", version)
	}

	goMajor, err := requiredInt(values, "datafusion_go.major")
	if err != nil {
		return config{}, err
	}
	goPatch, err := requiredInt(values, "datafusion_go.patch")
	if err != nil {
		return config{}, err
	}
	abiVersion, err := requiredInt(values, "abi.version")
	if err != nil {
		return config{}, err
	}

	return config{
		DataFusionVersion: version,
		DataFusionMajor:   major,
		DataFusionMinor:   minor,
		DataFusionPatch:   patch,
		GoMajor:           goMajor,
		GoPatch:           goPatch,
		ABIVersion:        abiVersion,
	}, nil
}

func stripComment(line string) string {
	inQuote := false
	escaped := false
	for i, r := range line {
		switch {
		case escaped:
			escaped = false
		case r == '\\' && inQuote:
			escaped = true
		case r == '"':
			inQuote = !inQuote
		case r == '#' && !inQuote:
			return line[:i]
		}
	}
	return line
}

func requiredString(values map[string]string, key string) (string, error) {
	value, ok := values[key]
	if !ok {
		return "", fmt.Errorf("versions.toml missing %s", key)
	}
	if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
		return "", fmt.Errorf("%s must be a quoted string", key)
	}
	unquoted, err := strconv.Unquote(value)
	if err != nil {
		return "", fmt.Errorf("%s is not a valid quoted string: %w", key, err)
	}
	if unquoted == "" {
		return "", fmt.Errorf("%s must not be empty", key)
	}
	return unquoted, nil
}

func requiredInt(values map[string]string, key string) (int, error) {
	value, ok := values[key]
	if !ok {
		return 0, fmt.Errorf("versions.toml missing %s", key)
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return n, nil
}

func parseSemver(version string) (int, int, int, error) {
	match := regexp.MustCompile(`^([0-9]+)\.([0-9]+)\.([0-9]+)$`).FindStringSubmatch(version)
	if match == nil {
		return 0, 0, 0, fmt.Errorf("datafusion.version must be major.minor.patch, got %q", version)
	}
	major, _ := strconv.Atoi(match[1])
	minor, _ := strconv.Atoi(match[2])
	patch, _ := strconv.Atoi(match[3])
	return major, minor, patch, nil
}

func (cfg config) encodedDataFusionVersion() string {
	return fmt.Sprintf("%d%02d%02d", cfg.DataFusionMajor, cfg.DataFusionMinor, cfg.DataFusionPatch)
}

func (cfg config) dataFusionGoVersion() string {
	return fmt.Sprintf("%d.%s.%d", cfg.GoMajor, cfg.encodedDataFusionVersion(), cfg.GoPatch)
}

func (cfg config) releaseTag() string {
	return "v" + cfg.dataFusionGoVersion()
}

func plannedUpdates(cfg config) (map[string][]byte, error) {
	cargo, err := updateCargoToml("rust/Cargo.toml", cfg)
	if err != nil {
		return nil, err
	}

	return map[string][]byte{
		"rust/Cargo.toml":                      cargo,
		"rust/src/generated.rs":                []byte(rustGenerated(cfg)),
		"version.go":                           []byte(goPublicVersion(cfg)),
		"internal/native/version_generated.go": []byte(goNativeVersion(cfg)),
	}, nil
}

func updateCargoToml(path string, cfg config) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var out strings.Builder
	section := ""
	packageVersionSet := false
	datafusionSet := false
	datafusionSQLSet := false
	for _, raw := range strings.SplitAfter(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")
		}

		switch {
		case section == "package" && strings.HasPrefix(line, "version = "):
			fmt.Fprintf(&out, "version = %q\n", cfg.dataFusionGoVersion())
			packageVersionSet = true
		case section == "dependencies" && strings.HasPrefix(line, "datafusion = "):
			fmt.Fprintf(&out, "datafusion = %q\n", "="+cfg.DataFusionVersion)
			datafusionSet = true
		case section == "dependencies" && strings.HasPrefix(line, "datafusion-sql = "):
			fmt.Fprintf(&out, "datafusion-sql = %q\n", "="+cfg.DataFusionVersion)
			datafusionSQLSet = true
		default:
			out.WriteString(raw)
		}
	}

	var missing []string
	if !packageVersionSet {
		missing = append(missing, "[package].version")
	}
	if !datafusionSet {
		missing = append(missing, "[dependencies].datafusion")
	}
	if !datafusionSQLSet {
		missing = append(missing, "[dependencies].datafusion-sql")
	}
	if len(missing) != 0 {
		return nil, fmt.Errorf("%s missing expected fields: %s", path, strings.Join(missing, ", "))
	}

	return []byte(out.String()), nil
}

func rustGenerated(cfg config) string {
	return fmt.Sprintf(`// Code generated by internal/tools/genversions; DO NOT EDIT.

pub(crate) const DFGO_ABI_VERSION: i32 = %d;
pub(crate) const DATAFUSION_VERSION: &[u8] = b"%s\0";
`, cfg.ABIVersion, cfg.DataFusionVersion)
}

func goPublicVersion(cfg config) string {
	return fmt.Sprintf(`// Code generated by internal/tools/genversions; DO NOT EDIT.

package datafusion

const (
	// DataFusionVersion is the exact Rust datafusion crate version pinned by this release.
	DataFusionVersion = %q

	// DataFusionVersionEncoded is used in Go module tags: v<major>.<encoded-datafusion-version>.<patch>.
	DataFusionVersionEncoded = %q

	// DataFusionGoMajor is the major component of datafusion-go release tags.
	DataFusionGoMajor = %d

	// DataFusionGoPatch is the patch component of datafusion-go release tags.
	DataFusionGoPatch = %d

	// DataFusionGoVersion is the full datafusion-go module version without the leading v.
	DataFusionGoVersion = %q
)
`, cfg.DataFusionVersion, cfg.encodedDataFusionVersion(), cfg.GoMajor, cfg.GoPatch, cfg.dataFusionGoVersion())
}

func goNativeVersion(cfg config) string {
	return fmt.Sprintf(`// Code generated by internal/tools/genversions; DO NOT EDIT.
//go:build cgo

package native

const abiVersion = %d
const dataFusionVersion = %q
const dataFusionGoVersion = %q
`, cfg.ABIVersion, cfg.DataFusionVersion, cfg.dataFusionGoVersion())
}

func appendGitHubOutput(path string, cfg config) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}

	if _, err = fmt.Fprintf(
		f,
		"release_tag=%s\ndatafusion_version=%s\ndatafusion_go_version=%s\n",
		cfg.releaseTag(),
		cfg.DataFusionVersion,
		cfg.dataFusionGoVersion(),
	); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

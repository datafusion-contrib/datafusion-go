//go:build cgo && !datafusion_use_bundled && !datafusion_use_source && !datafusion_use_static_lib && !datafusion_use_lib

package native

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Each scenario gets a fresh process because the C loader and sync.Once are
// process-wide. The trimpath executable exercises installed-consumer discovery
// without a source checkout accidentally satisfying the request.
func TestLibraryProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("fresh-process installation tests; run make test.install")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	name, err := nativeSharedLibraryName()
	if err != nil {
		t.Fatal(err)
	}
	library := os.Getenv(nativeLibraryEnv)
	if library == "" {
		library = filepath.Join(root, "internal/native/lib", nativePlatform(), name)
	}
	if _, err := os.Stat(library); err != nil {
		t.Fatalf("build the host library with make bundle: %v", err)
	}
	exe := buildLibraryProcess(t, filepath.Join(root, "internal/native"), true)
	for _, mode := range []string{"explicit", "download", "cache", "corrupt-cache", "interrupted", "offline", "no-download", "missing-manifest", "bad-checksum", "missing-file", "abi-mismatch", "datafusion-mismatch"} {
		t.Run(mode, func(t *testing.T) {
			fixture := library
			if strings.HasSuffix(mode, "mismatch") {
				fixture = buildVersionFixture(t, root, name, mode)
			}
			runLibraryProcess(t, exe, mode, fixture)
		})
	}
	// The untrimmed executable can find the source library. It still runs in a
	// fresh process with an empty explicit override and an unavailable network.
	t.Run("source", func(t *testing.T) {
		// Compile from a private source fixture: CI workspaces can live on a
		// volume whose owner the loader correctly does not trust (e.g. D:\).
		source := libraryProcessDir(t)
		paths := []string{"go.mod", "go.sum", "rust/include/datafusion_go.h", "internal/native/lib/SHA256SUMS"}
		entries, err := os.ReadDir(filepath.Join(root, "internal/native"))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if !entry.IsDir() && (strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), ".h")) {
				paths = append(paths, filepath.Join("internal/native", entry.Name()))
			}
		}
		for _, path := range paths {
			copyLibraryFixture(t, filepath.Join(root, path), filepath.Join(source, path))
		}
		copyLibraryFixture(t, library, filepath.Join(source, "internal/native/lib", nativePlatform(), name))
		current := buildLibraryProcess(t, filepath.Join(source, "internal/native"), false)
		runLibraryProcess(t, current, "source", library)
	})
}

func buildLibraryProcess(t *testing.T, dir string, trimpath bool) string {
	t.Helper()
	exe := filepath.Join(t.TempDir(), "native-install.test")
	if runtime.GOOS == "windows" {
		exe += ".exe"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	args := []string{"test", "-c", "-o", exe}
	if trimpath {
		args = append(args, "-trimpath")
	}
	if os.Getenv("DFGO_TEST_COVERAGE_DIR") != "" {
		args = append(args, "-covermode=atomic", "-coverpkg=.")
	}
	cmd := exec.CommandContext(ctx, "go", append(args, ".")...)
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build installed-consumer test: %v\n%s", err, output)
	}
	return exe
}

func libraryProcessDir(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "windows" {
		return t.TempDir()
	}
	// Use the same trusted profile tree as a normal installation. Windows CI
	// can place its default temporary directory on an untrusted data volume.
	base, err := os.UserCacheDir()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(base, "datafusion-go-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	return dir
}

func copyLibraryFixture(t *testing.T, source, dest string) {
	t.Helper()
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func runLibraryProcess(t *testing.T, exe, mode, fixture string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestLibraryProcessHelper$", "-test.v")
	if dir := os.Getenv("DFGO_TEST_COVERAGE_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		cmd.Args = append(cmd.Args, "-test.coverprofile="+filepath.Join(dir, mode+".out"))
	}
	// Setenv restores the parent environment and replaces inherited settings
	// without duplicate keys (including Windows' case-insensitive environment).
	t.Setenv("DFGO_TEST_LIBRARY_MODE", mode)
	t.Setenv("DFGO_TEST_LIBRARY_FIXTURE", fixture)
	// Windows keeps loaded DLLs locked until the child exits. The parent owns
	// the cache directory so its cleanup runs after that process has stopped.
	t.Setenv("DFGO_TEST_LIBRARY_CACHE", libraryProcessDir(t))
	t.Setenv(nativeLibraryEnv, "")
	t.Setenv(nativeNoDownloadEnv, "")
	t.Setenv(nativeDownloadBaseEnv, "https://unavailable.invalid")
	cmd.Env = os.Environ()
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s: %v\n%s", mode, err, output)
	}
}

func buildVersionFixture(t *testing.T, root, name, mode string) string {
	t.Helper()
	dir := t.TempDir()
	source := filepath.Join(dir, "fixture.c")
	code := `#define DFGO_NO_FUNCTION_PROTOTYPES
#include "datafusion_go.h"
#include "abi_generated.h"
#define dfgo_abi_version unused_abi_version
#define dfgo_datafusion_version unused_datafusion_version
#define STUB(result, name, args, values, ret) result name args { ret (result)0; }
DFGO_FUNCTIONS(STUB)
#undef dfgo_abi_version
#undef dfgo_datafusion_version
int32_t dfgo_abi_version(void) { return FIXTURE_ABI; }
const char *dfgo_datafusion_version(void) { return "0.0.0-fixture"; }
`
	if err := os.WriteFile(source, []byte(code), 0o600); err != nil {
		t.Fatal(err)
	}
	version := abiVersion
	if mode == "abi-mismatch" {
		version++
	}
	compiler := os.Getenv("CC")
	if compiler == "" {
		compiler = "cc"
	}
	args := []string{"-shared", "-I" + filepath.Join(root, "rust/include"), "-I" + filepath.Join(root, "internal/native"), "-DFIXTURE_ABI=" + strconv.Itoa(version), source, "-o", filepath.Join(dir, name)}
	if runtime.GOOS != "windows" {
		args = append([]string{"-fPIC"}, args...)
	}
	if output, err := exec.Command(compiler, args...).CombinedOutput(); err != nil {
		t.Fatalf("compile version fixture: %v\n%s", err, output)
	}
	return filepath.Join(dir, name)
}

func TestLibraryProcessHelper(t *testing.T) {
	mode := os.Getenv("DFGO_TEST_LIBRARY_MODE")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	fixture := os.Getenv("DFGO_TEST_LIBRARY_FIXTURE")
	if mode == "explicit" || strings.HasSuffix(mode, "mismatch") {
		t.Setenv(nativeLibraryEnv, fixture)
	}
	if mode == "missing-file" {
		t.Setenv(nativeLibraryEnv, filepath.Join(t.TempDir(), "absent"))
	}
	if mode == "no-download" {
		t.Setenv(nativeNoDownloadEnv, "1")
	}
	cache := os.Getenv("DFGO_TEST_LIBRARY_CACHE")
	if cache == "" {
		t.Fatal("subprocess cache directory was not provided")
	}
	nativeUserCacheDir = func() (string, error) { return cache, nil }
	asset, err := nativeAssetName()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(fixture)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("%x", hash.Sum(nil))
	nativeChecksumManifest = want + "  " + asset + "\n"
	if mode == "missing-manifest" {
		nativeChecksumManifest = "# source placeholder\n"
	}
	if mode == "bad-checksum" {
		nativeChecksumManifest = strings.Repeat("0", 64) + "  " + asset + "\n"
	}
	installed := filepath.Join(cache, nativeDownloadCacheName, "v"+dataFusionGoVersion, asset)
	if mode == "cache" || mode == "corrupt-cache" {
		if err := os.MkdirAll(filepath.Dir(installed), 0o700); err != nil {
			t.Fatal(err)
		}
		data := []byte("interrupted old contents")
		if mode == "cache" {
			data, err = os.ReadFile(fixture)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(installed, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/"+asset {
			t.Errorf("unexpected asset request: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if mode == "interrupted" {
			w.Header().Set("Content-Length", "1000000")
			_, _ = w.Write([]byte("partial download"))
			return
		}
		if mode == "offline" || mode == "cache" || mode == "source" {
			http.Error(w, "offline", http.StatusServiceUnavailable)
			return
		}
		http.ServeFile(w, r, fixture)
	}))
	defer server.Close()
	t.Setenv(nativeDownloadBaseEnv, server.URL)
	// Only the child test process trusts this local TLS server. Production
	// defaults and other tests retain the normal transport and certificate store.
	http.DefaultTransport = server.Client().Transport
	db, loadErr := OpenDatabase("")
	expectError := map[string]string{
		"interrupted": "unexpected EOF", "offline": "503", "no-download": "native library was not found",
		"missing-manifest": "checksums do not include", "bad-checksum": "checksum verification",
		"missing-file": "could not load", "abi-mismatch": "ABI version mismatch", "datafusion-mismatch": "DataFusion version mismatch",
	}[mode]
	if expectError != "" {
		if loadErr == nil || !strings.Contains(loadErr.Error(), expectError) {
			t.Fatalf("got %v, want %q", loadErr, expectError)
		}
		// A failed process-wide load must stay failed; changing the override
		// cannot silently change which native implementation this process uses.
		t.Setenv(nativeLibraryEnv, fixture)
		if _, second := OpenDatabase(""); second == nil || second.Error() != loadErr.Error() {
			t.Fatalf("load result changed within a process: first=%v second=%v", loadErr, second)
		}
	} else {
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		db.Close()
		// Check the once path too, after a successful load.
		db, err = OpenDatabase("")
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
	wantRequests := int32(0)
	if mode == "download" || mode == "corrupt-cache" || mode == "interrupted" || mode == "offline" || mode == "bad-checksum" {
		wantRequests = 1
	}
	if got := requests.Load(); got != wantRequests {
		t.Fatalf("got %d download requests, want %d", got, wantRequests)
	}
	leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(installed), "*.tmp"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("download left temporary files: %v, %v", leftovers, err)
	}
	if mode == "download" || mode == "cache" || mode == "corrupt-cache" {
		if err := verifyFileSHA256(installed, want); err != nil {
			t.Fatal(err)
		}
	} else if expectError != "" {
		if _, err := os.Stat(installed); !os.IsNotExist(err) {
			t.Fatalf("failed installation published a cache file: %v", err)
		}
	}
}

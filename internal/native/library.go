//go:build cgo

package native

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	_ "embed"
)

const (
	nativeLibraryEnv        = "DATAFUSION_GO_LIBRARY"
	nativeNoDownloadEnv     = "DATAFUSION_GO_NO_DOWNLOAD"
	nativeDownloadBaseEnv   = "DATAFUSION_GO_DOWNLOAD_BASE"
	nativeDownloadCacheName = "datafusion-go"
)

//go:embed lib/SHA256SUMS
var nativeChecksumManifest string

func resolveNativeLibrary() (string, error) {
	if path := os.Getenv(nativeLibraryEnv); path != "" {
		return path, nil
	}
	if path, ok := localNativeLibrary(); ok {
		return path, nil
	}
	if os.Getenv(nativeNoDownloadEnv) != "" {
		return "", fmt.Errorf("datafusion-go native library was not found; unset %s or set %s to a libdatafusion_go shared library", nativeNoDownloadEnv, nativeLibraryEnv)
	}
	return downloadNativeLibrary()
}

func localNativeLibrary() (string, bool) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", false
	}
	name, err := nativeSharedLibraryName()
	if err != nil {
		return "", false
	}
	path := filepath.Join(filepath.Dir(file), "lib", nativePlatform(), name)
	if regularFile(path) {
		return path, true
	}
	return "", false
}

func downloadNativeLibrary() (string, error) {
	asset, err := nativeAssetName()
	if err != nil {
		return "", err
	}
	want, ok := nativeAssetChecksum(asset)
	if !ok {
		return "", fmt.Errorf("datafusion-go release checksums do not include %s; set %s to a compatible libdatafusion_go shared library", asset, nativeLibraryEnv)
	}

	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("could not locate user cache directory for datafusion-go native library: %w", err)
	}
	dir := filepath.Join(cacheDir, nativeDownloadCacheName, "v"+dataFusionGoVersion)
	path := filepath.Join(dir, asset)
	if err := verifyFileSHA256(path, want); err == nil {
		return path, nil
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("could not create datafusion-go native cache directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, asset+".*.tmp")
	if err != nil {
		return "", fmt.Errorf("could not create datafusion-go native download file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	url := nativeDownloadURL(asset)
	if err := downloadFile(tmp, url); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("could not close datafusion-go native download: %w", err)
	}
	if err := verifyFileSHA256(tmpPath, want); err != nil {
		return "", fmt.Errorf("downloaded datafusion-go native library failed checksum verification: %w", err)
	}
	if err := os.Chmod(tmpPath, 0o755); err != nil {
		return "", fmt.Errorf("could not mark datafusion-go native library executable: %w", err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("could not replace cached datafusion-go native library: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return "", fmt.Errorf("could not install datafusion-go native library in cache: %w", err)
	}
	return path, nil
}

func nativeDownloadURL(asset string) string {
	base := os.Getenv(nativeDownloadBaseEnv)
	if base == "" {
		base = "https://github.com/datafusion-contrib/datafusion-go/releases/download/v" + dataFusionGoVersion
	}
	return strings.TrimRight(base, "/") + "/" + asset
}

func downloadFile(dst *os.File, url string) error {
	client := http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("could not download datafusion-go native library %s: %w", url, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("could not download datafusion-go native library %s: HTTP %s", url, resp.Status)
	}
	if _, err := io.Copy(dst, resp.Body); err != nil {
		return fmt.Errorf("could not write datafusion-go native library download: %w", err)
	}
	return nil
}

func nativeAssetName() (string, error) {
	name, err := nativeSharedLibraryName()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("datafusion-go-v%s-%s-%s", dataFusionGoVersion, nativePlatform(), name), nil
}

func nativePlatform() string {
	return runtime.GOOS + "-" + runtime.GOARCH
}

func nativeSharedLibraryName() (string, error) {
	switch runtime.GOOS {
	case "darwin":
		return "libdatafusion_go.dylib", nil
	case "linux":
		return "libdatafusion_go.so", nil
	case "windows":
		return "datafusion_go.dll", nil
	default:
		return "", fmt.Errorf("datafusion-go does not publish a native shared library for %s", nativePlatform())
	}
}

func nativeAssetChecksum(asset string) (string, bool) {
	for _, line := range strings.Split(nativeChecksumManifest, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		if fields[1] == asset {
			return strings.ToLower(fields[0]), true
		}
	}
	return "", false
}

func verifyFileSHA256(path string, want string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	got := hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(got, want) {
		return fmt.Errorf("sha256 mismatch for %s: got %s, want %s", path, got, want)
	}
	return nil
}

func regularFile(path string) bool {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	return err == nil && info.Mode().IsRegular()
}

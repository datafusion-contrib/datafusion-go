//go:build cgo

package native

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

// Kept as a dependency so installation tests can use a private cache in a
// fresh process without changing the user's home directory or platform rules.
var nativeUserCacheDir = os.UserCacheDir

func resolveNativeLibrary() (string, error) {
	return resolveNativeLibraryForExecution(secureExecution())
}

func resolveNativeLibraryForExecution(privileged bool) (string, error) {
	// User-owned source and cache paths are not a trust boundary for setgid or
	// file-capability programs, even if their effective UID is unchanged.
	if privileged {
		return "", errors.New("datafusion-go runtime library loading is disabled for privileged execution; use datafusion_use_bundled or datafusion_use_source")
	}
	if path := os.Getenv(nativeLibraryEnv); path != "" {
		if !filepath.IsAbs(path) {
			return "", fmt.Errorf("%s must be an absolute path, got %q", nativeLibraryEnv, path)
		}
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
	// -trimpath produces relative paths; probing those would trust the CWD.
	if !ok || !filepath.IsAbs(file) {
		return "", false
	}
	name, err := nativeSharedLibraryName()
	if err != nil {
		return "", false
	}
	path := filepath.Join(filepath.Dir(file), "lib", nativePlatform(), name)
	if resolved, err := trustedNativePath(path, false); err == nil {
		return resolved, true
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

	cacheDir, err := nativeUserCacheDir()
	if err != nil {
		return "", fmt.Errorf("could not locate user cache directory for datafusion-go native library: %w", err)
	}
	dir := filepath.Join(cacheDir, nativeDownloadCacheName, "v"+dataFusionGoVersion)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("could not create datafusion-go native cache directory: %w", err)
	}
	dir, err = trustedNativePath(dir, true)
	if err != nil {
		return "", fmt.Errorf("refusing to use datafusion-go native cache directory: %w", err)
	}
	path := filepath.Join(dir, asset)
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return "", fmt.Errorf("cached native library is not a regular file: %s", path)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if resolved, err := trustedNativePath(path, false); err == nil {
		path = resolved
		if err := verifyFileSHA256(path, want); err == nil {
			return path, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("refusing to use cached native library: %w", err)
	}
	tmp, err := os.CreateTemp(dir, asset+".*.tmp")
	if err != nil {
		return "", fmt.Errorf("could not create datafusion-go native download file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	// Inherited ACLs can make a newly created file writable even in a
	// directory whose own permissions passed. Check before writing any bytes.
	if _, err := trustedNativePath(tmpPath, false); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("unsafe native download file: %w", err)
	}
	downloadURL, err := nativeDownloadURL(asset)
	if err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := downloadFile(tmp, downloadURL); err != nil {
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
	if err := os.Rename(tmpPath, path); err != nil {
		return "", fmt.Errorf("could not install datafusion-go native library in cache: %w", err)
	}
	return path, nil
}

func nativeDownloadURL(asset string) (string, error) {
	base := os.Getenv(nativeDownloadBaseEnv)
	if base == "" {
		base = "https://github.com/datafusion-contrib/datafusion-go/releases/download/v" + dataFusionGoVersion
	} else {
		parsed, err := url.Parse(base)
		if err != nil {
			return "", fmt.Errorf("invalid %s %q: %w", nativeDownloadBaseEnv, base, err)
		}
		if parsed.Scheme != "https" || parsed.Host == "" {
			return "", fmt.Errorf("%s must be an https:// URL with a host, got %q", nativeDownloadBaseEnv, base)
		}
	}
	return strings.TrimRight(base, "/") + "/" + asset, nil
}

// Bound disk use before an oversized download can fail checksum verification.
const maxNativeAssetSize = 512 << 20

func downloadFile(dst *os.File, url string) error {
	client := http.Client{
		Timeout: 10 * time.Minute,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if req.URL.Scheme != "https" {
				return errors.New("native library download redirect must use HTTPS")
			}
			if len(via) >= 10 {
				return errors.New("too many native library download redirects")
			}
			return nil
		},
	}
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
	if resp.ContentLength > maxNativeAssetSize {
		return fmt.Errorf("datafusion-go native library download is %d bytes, above the %d byte limit", resp.ContentLength, maxNativeAssetSize)
	}
	written, err := io.Copy(dst, io.LimitReader(resp.Body, maxNativeAssetSize+1))
	if err != nil {
		return fmt.Errorf("could not write datafusion-go native library download: %w", err)
	}
	if written > maxNativeAssetSize {
		return fmt.Errorf("datafusion-go native library download exceeds the %d byte limit", maxNativeAssetSize)
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
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > maxNativeAssetSize {
		return fmt.Errorf("native library must be a regular file no larger than %d bytes: %s", maxNativeAssetSize, path)
	}
	hash := sha256.New()
	// Bound reads too, in case a cache file grows after Stat.
	if _, err := io.Copy(hash, io.LimitReader(file, maxNativeAssetSize+1)); err != nil {
		return err
	}
	got := hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(got, want) {
		return fmt.Errorf("sha256 mismatch for %s: got %s, want %s", path, got, want)
	}
	return nil
}

//go:build cgo

package native

import (
	"fmt"
	"os"
	"path/filepath"
)

// trustedNativePath resolves symlinks and checks the resulting path from the
// root down. Returning the resolved path is essential: loading via the original
// symlink would let its owner redirect the load after verification.
func trustedNativePath(path string, directory bool) (string, error) {
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("native path must be absolute: %q", path)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	var ancestors []string
	for p := resolved; ; p = filepath.Dir(p) {
		ancestors = append(ancestors, p)
		if filepath.Dir(p) == p {
			break
		}
	}
	for i := len(ancestors) - 1; i >= 0; i-- {
		p := ancestors[i]
		info, err := os.Lstat(p)
		if err != nil {
			return "", err
		}
		wantDir := i != 0 || directory
		if (wantDir && !info.IsDir()) || (!wantDir && !info.Mode().IsRegular()) {
			return "", fmt.Errorf("native path has unexpected file type: %s", p)
		}
		libraryDirectory := wantDir && ((directory && i == 0) || (!directory && i == 1))
		if err := checkNativePathPermissions(p, info, libraryDirectory); err != nil {
			return "", err
		}
	}
	return resolved, nil
}

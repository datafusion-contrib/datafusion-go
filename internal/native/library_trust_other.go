//go:build cgo && !unix && !windows

package native

import (
	"fmt"
	"os"
)

func checkNativePathPermissions(path string, _ os.FileInfo, _ bool) error {
	return fmt.Errorf("native path permission checks are unsupported on this platform: %s", path)
}

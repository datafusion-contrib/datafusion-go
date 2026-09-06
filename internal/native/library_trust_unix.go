//go:build cgo && unix

package native

import (
	"fmt"
	"os"
	"syscall"
)

func checkNativePathPermissions(path string, info os.FileInfo, libraryDirectory bool) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("could not inspect ownership of %s", path)
	}
	if uid := stat.Uid; uid != uint32(os.Geteuid()) && uid != 0 {
		return fmt.Errorf("native path is owned by another user: %s (uid %d)", path, uid)
	}
	// Root-owned sticky ancestors protect existing children from replacement.
	// The library's own directory must also prevent new dependency files.
	stickyRoot := !libraryDirectory && info.IsDir() && stat.Uid == 0 && info.Mode()&os.ModeSticky != 0
	if info.Mode().Perm()&0o022 != 0 && !stickyRoot {
		return fmt.Errorf("native path is writable by other users: %s (mode %04o)", path, info.Mode().Perm())
	}
	return checkNativeACLPermissions(path, info.IsDir(), libraryDirectory)
}

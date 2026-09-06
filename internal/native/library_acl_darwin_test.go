//go:build cgo && darwin

package native

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativePathRejectsExtendedACLWrites(t *testing.T) {
	for _, target := range []string{"parent", "file"} {
		t.Run(target, func(t *testing.T) {
			parent := filepath.Join(t.TempDir(), "cache")
			dir := filepath.Join(parent, "version")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(dir, "native")
			if err := os.WriteFile(file, []byte("library"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := trustedNativePath(file, false); err != nil {
				t.Fatal(err)
			}
			path, permission := parent, "delete_child"
			if target == "file" {
				path, permission = file, "write"
			}
			if out, err := exec.Command("chmod", "+a", "everyone allow "+permission, path).CombinedOutput(); err != nil {
				t.Fatalf("set ACL: %v: %s", err, out)
			}
			t.Cleanup(func() {
				if out, err := exec.Command("chmod", "-N", path).CombinedOutput(); err != nil {
					t.Errorf("remove ACL: %v: %s", err, out)
				}
			})
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm()&0022 != 0 {
				t.Fatal("fixture unexpectedly changed POSIX write bits")
			}
			if _, err := trustedNativePath(file, false); err == nil || !strings.Contains(err.Error(), "ACL") {
				t.Fatalf("accepted writable ACL on %s: %v", target, err)
			}
		})
	}
}

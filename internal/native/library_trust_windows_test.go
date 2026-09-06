//go:build cgo && windows

package native

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestNativePathRejectsWritableWindowsACL(t *testing.T) {
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
			path := parent
			if target == "file" {
				path = file
			}
			original, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
			if err != nil {
				t.Fatal(err)
			}
			originalACL, _, err := original.DACL()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION, nil, nil, originalACL, nil); err != nil {
					t.Error(err)
				}
			})
			user, err := windows.GetCurrentProcessToken().GetTokenUser()
			if err != nil {
				t.Fatal(err)
			}
			sd, err := windows.SecurityDescriptorFromString("D:P(A;;FA;;;" + user.User.Sid.String() + ")(A;;FA;;;WD)")
			if err != nil {
				t.Fatal(err)
			}
			acl, _, err := sd.DACL()
			if err != nil {
				t.Fatal(err)
			}
			if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, acl, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := trustedNativePath(file, false); err == nil || !strings.Contains(err.Error(), "writable") {
				t.Fatalf("accepted writable %s: %v", target, err)
			}
		})
	}
}

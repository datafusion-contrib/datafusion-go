//go:build cgo && windows

package native

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestNativePathPermissionsRejectWritableWindowsACL(t *testing.T) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	privateDACL := "D:P(A;;FA;;;" + user.User.Sid.String() + ")"
	for _, tc := range []struct {
		name             string
		file             bool
		libraryDirectory bool
		mask             windows.ACCESS_MASK
		wantRejected     bool
	}{
		{name: "ancestor_delete_child", mask: 0x40, wantRejected: true},
		{name: "ancestor_create_file", mask: windows.FILE_WRITE_DATA},
		{name: "library_directory_create_file", libraryDirectory: true, mask: windows.FILE_WRITE_DATA, wantRejected: true},
		{name: "file_write", file: true, mask: windows.FILE_WRITE_DATA, wantRejected: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "native")
			var err error
			if tc.file {
				err = os.WriteFile(path, []byte("library"), 0600)
			} else {
				err = os.Mkdir(path, 0700)
			}
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			// Control this path's DACL without assuming its host ancestors are trusted.
			setNativeTestDACL(t, path, privateDACL)
			t.Cleanup(func() { setNativeTestDACL(t, path, privateDACL) })
			if err := checkNativePathPermissions(path, info, tc.libraryDirectory); err != nil {
				t.Fatal(err)
			}
			setNativeTestDACL(t, path, privateDACL+fmt.Sprintf("(A;;0x%08x;;;WD)", tc.mask))
			err = checkNativePathPermissions(path, info, tc.libraryDirectory)
			if tc.wantRejected {
				if err == nil || !strings.Contains(err.Error(), "writable") {
					t.Fatalf("accepted unsafe ACL: %v", err)
				}
			} else if err != nil {
				t.Fatalf("rejected safe ancestor permissions: %v", err)
			}
		})
	}
}

func setNativeTestDACL(t *testing.T, path, sddl string) {
	t.Helper()
	sd, err := windows.SecurityDescriptorFromString(sddl)
	if err != nil {
		t.Fatal(err)
	}
	acl, _, err := sd.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, acl, nil); err != nil {
		t.Fatal(err)
	}
}

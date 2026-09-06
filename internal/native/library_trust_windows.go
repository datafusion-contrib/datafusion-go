//go:build cgo && windows

package native

import (
	"fmt"
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/windows"
)

func checkNativePathPermissions(path string, info os.FileInfo, libraryDirectory bool) error {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return err
	}
	sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return fmt.Errorf("could not inspect native path %s: %w", path, err)
	}
	// TrustedInstaller can own the volume root and system directories.
	trustedInstaller, _, _, err := windows.LookupSID("", `NT SERVICE\TrustedInstaller`)
	if err != nil {
		return fmt.Errorf("could not resolve TrustedInstaller: %w", err)
	}
	// Owner and ACE SIDs point into sd's storage.
	defer runtime.KeepAlive(sd)
	trustedSID := func(sid *windows.SID) bool {
		return sid != nil && (sid.Equals(user.User.Sid) || sid.Equals(trustedInstaller) ||
			sid.IsWellKnown(windows.WinLocalSystemSid) ||
			sid.IsWellKnown(windows.WinBuiltinAdministratorsSid))
	}
	owner, _, err := sd.Owner()
	if err != nil {
		return err
	}
	if !trustedSID(owner) {
		return fmt.Errorf("native path is owned by another user: %s", path)
	}
	acl, _, err := sd.DACL()
	if err != nil {
		return err
	}
	if acl == nil {
		return fmt.Errorf("native path has no restrictive DACL: %s", path)
	}
	// DELETE_CHILD bypasses a child's DACL. Allow creation only in ancestors;
	// the library directory must prevent planted dependencies.
	const deleteChild windows.ACCESS_MASK = 0x40
	writes := windows.ACCESS_MASK(windows.DELETE | windows.WRITE_DAC | windows.WRITE_OWNER | windows.GENERIC_WRITE | windows.GENERIC_ALL)
	if info.IsDir() {
		writes |= deleteChild
		if libraryDirectory {
			writes |= windows.FILE_WRITE_DATA | windows.FILE_APPEND_DATA
		}
	} else {
		writes |= windows.FILE_WRITE_DATA | windows.FILE_APPEND_DATA
	}
	for i := uint32(0); i < uint32(acl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(acl, i, &ace); err != nil {
			return err
		}
		if ace.Header.AceFlags&windows.INHERIT_ONLY_ACE != 0 {
			continue
		}
		switch ace.Header.AceType {
		case windows.ACCESS_DENIED_ACE_TYPE:
			// Ignoring denies is conservative: an ambiguous ACL is rejected.
			continue
		case windows.ACCESS_ALLOWED_ACE_TYPE:
			sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
			if ace.Mask&writes != 0 && !trustedSID(sid) {
				return fmt.Errorf("native path is writable by other users: %s", path)
			}
		default:
			return fmt.Errorf("native path has an unsupported ACL entry: %s", path)
		}
	}
	return nil
}

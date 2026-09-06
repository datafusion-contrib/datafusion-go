//go:build cgo && darwin

package native

/*
#include <errno.h>
#include <membership.h>
#include <stdlib.h>
#include <sys/acl.h>
#include <unistd.h>

static int dfgo_check_native_acl(const char *path, int directory, int library_directory) {
    acl_t acl = acl_get_file(path, ACL_TYPE_EXTENDED);
    if (acl == NULL) return (errno == ENOTSUP || errno == ENOENT) ? 0 : errno;
    int result = 0;
    acl_entry_t entry;
    int entry_id = ACL_FIRST_ENTRY;
    while (acl_get_entry(acl, entry_id, &entry) == 0) {
        entry_id = ACL_NEXT_ENTRY;
        acl_tag_t tag;
        acl_permset_t perms;
        acl_flagset_t flags;
        if (acl_get_tag_type(entry, &tag) != 0 ||
            acl_get_permset(entry, &perms) != 0 ||
            acl_get_flagset_np(entry, &flags) != 0) { result = EACCES; break; }
        if (tag == ACL_EXTENDED_DENY || acl_get_flag_np(flags, ACL_ENTRY_ONLY_INHERIT) == 1) continue;
        if (tag != ACL_EXTENDED_ALLOW) { result = EACCES; break; }
        // Adding new children alone does not replace existing protected ones.
        int writes = acl_get_perm_np(perms, ACL_DELETE) ||
            acl_get_perm_np(perms, ACL_WRITE_SECURITY) ||
            acl_get_perm_np(perms, ACL_CHANGE_OWNER);
        if (directory) {
            writes |= acl_get_perm_np(perms, ACL_DELETE_CHILD);
            if (library_directory) writes |= acl_get_perm_np(perms, ACL_ADD_FILE) || acl_get_perm_np(perms, ACL_ADD_SUBDIRECTORY);
        }
        else writes |= acl_get_perm_np(perms, ACL_WRITE_DATA) || acl_get_perm_np(perms, ACL_APPEND_DATA);
        if (!writes) continue;
        uuid_t *qualifier = (uuid_t *)acl_get_qualifier(entry);
        id_t id = 0;
        int type = -1;
        int trusted = qualifier != NULL && mbr_uuid_to_id(*qualifier, &id, &type) == 0 &&
            type == ID_TYPE_UID && (id == 0 || id == geteuid());
        if (qualifier != NULL) acl_free(qualifier);
        if (!trusted) { result = EACCES; break; }
    }
    acl_free(acl);
    return result;
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

func checkNativeACLPermissions(path string, directory, libraryDirectory bool) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	var dir, private C.int
	if directory {
		dir = 1
	}
	if libraryDirectory {
		private = 1
	}
	if errno := C.dfgo_check_native_acl(cpath, dir, private); errno != 0 {
		return fmt.Errorf("native path has an unsafe or unreadable ACL: %s (errno %d)", path, int(errno))
	}
	return nil
}

//go:build cgo && unix && !darwin

package native

// POSIX access ACLs are constrained by the group-class permission bits already
// checked in checkNativePathPermissions. macOS extended ACLs need a separate
// check because they can grant writes without changing those bits.
func checkNativeACLPermissions(string, bool, bool) error { return nil }

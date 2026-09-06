//go:build cgo

package native

import "os"

// secureExecution detects privileges gained through setuid, setgid, or file
// capabilities; runtime library loading is disabled in this state.
func secureExecution() bool {
	if os.Geteuid() != os.Getuid() || os.Getegid() != os.Getgid() {
		return true
	}
	return secureExecutionPlatform()
}

//go:build cgo && darwin

package native

/*
#include <unistd.h>
*/
import "C"

func secureExecutionPlatform() bool {
	// issetugid stays true for the process lifetime once the exec was tainted,
	// even after the effective IDs are reset to the real IDs.
	return C.issetugid() != 0
}

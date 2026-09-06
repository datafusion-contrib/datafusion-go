//go:build cgo && linux

package native

/*
#include <errno.h>
#include <sys/auxv.h>

static int dfgo_secure_execution(void) {
    errno = 0;
    unsigned long secure = getauxval(AT_SECURE);
    // The libc auxiliary vector survives chroot and does not require /proc.
    // If the flag cannot be read, refuse runtime loading.
    return secure != 0 || errno != 0;
}
*/
import "C"

func secureExecutionPlatform() bool {
	return C.dfgo_secure_execution() != 0
}

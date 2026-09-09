//go:build datafusion_test_coverage && cgo && (darwin || linux)

package native

/*
#cgo linux LDFLAGS: -ldl
#include <dlfcn.h>
#include <stdlib.h>

static int dfgo_flush_test_coverage(const char *path) {
    void *handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL) return -1;
    int (*flush)(void) = (int (*)(void))dlsym(handle, "dfgo_test_write_coverage");
    int rc = flush == NULL ? -2 : flush();
    dlclose(handle);
    return rc;
}
*/
import "C"

import (
	"fmt"
	"os"
	"unsafe"
)

// WriteTestCoverage is present only in instrumented test builds. Go does not
// run C's exit hooks, so tests must flush the loaded Rust library explicitly.
func WriteTestCoverage() error {
	path := os.Getenv(nativeLibraryEnv)
	if path == "" {
		return nil
	}
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	if rc := C.dfgo_flush_test_coverage(cpath); rc != 0 {
		return fmt.Errorf("native test coverage flush failed for %s: code %d", path, int(rc))
	}
	return nil
}

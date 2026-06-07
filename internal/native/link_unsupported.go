//go:build cgo && datafusion_use_bundled && !((darwin && arm64) || (darwin && amd64) || (linux && amd64) || (linux && arm64) || (windows && amd64))

package native

/*
#error "datafusion-go does not bundle a native static library for this GOOS/GOARCH; use datafusion_use_source, datafusion_use_static_lib, or datafusion_use_lib"
*/
import "C"

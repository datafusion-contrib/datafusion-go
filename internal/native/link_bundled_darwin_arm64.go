//go:build !datafusion_use_static_lib && !datafusion_use_lib && !datafusion_use_source && darwin && arm64

package native

/*
#cgo LDFLAGS: -L${SRCDIR}/lib/darwin-arm64 -ldatafusion_go
*/
import "C"

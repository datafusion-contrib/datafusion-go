//go:build !datafusion_use_static_lib && !datafusion_use_lib && !datafusion_use_source && linux && arm64

package native

/*
#cgo LDFLAGS: -L${SRCDIR}/lib/linux-arm64 -ldatafusion_go -ldl -lm -lpthread
*/
import "C"

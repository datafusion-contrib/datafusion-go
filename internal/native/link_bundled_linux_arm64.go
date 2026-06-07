//go:build datafusion_use_bundled && linux && arm64

package native

/*
#cgo CFLAGS: -DDFGO_DIRECT_LINK
#cgo LDFLAGS: ${SRCDIR}/lib/linux-arm64/libdatafusion_go.a -ldl -lm -lpthread
*/
import "C"

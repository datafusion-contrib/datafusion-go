//go:build datafusion_use_bundled && darwin && arm64

package native

/*
#cgo CFLAGS: -DDFGO_DIRECT_LINK
#cgo LDFLAGS: ${SRCDIR}/lib/darwin-arm64/libdatafusion_go.a
*/
import "C"

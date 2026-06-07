//go:build datafusion_use_bundled && darwin && amd64

package native

/*
#cgo CFLAGS: -DDFGO_DIRECT_LINK
#cgo LDFLAGS: ${SRCDIR}/lib/darwin-amd64/libdatafusion_go.a
*/
import "C"

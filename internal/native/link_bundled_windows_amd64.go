//go:build datafusion_use_bundled && windows && amd64

package native

/*
#cgo CFLAGS: -DDFGO_DIRECT_LINK
#cgo LDFLAGS: ${SRCDIR}/lib/windows-amd64/libdatafusion_go.a -lws2_32 -luserenv -lbcrypt -ladvapi32 -lntdll
*/
import "C"

//go:build !datafusion_use_static_lib && !datafusion_use_lib && !datafusion_use_source && windows && amd64

package native

/*
#cgo LDFLAGS: -L${SRCDIR}/lib/windows-amd64 -ldatafusion_go -lws2_32 -luserenv -lbcrypt -ladvapi32 -lntdll
*/
import "C"

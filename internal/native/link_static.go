//go:build datafusion_use_static_lib

package native

/*
#cgo CFLAGS: -DDFGO_DIRECT_LINK
#cgo !windows LDFLAGS: ${SRCDIR}/../../rust/target/release/libdatafusion_go.a
#cgo windows,amd64 LDFLAGS: ${SRCDIR}/../../rust/target/x86_64-pc-windows-gnu/release/libdatafusion_go.a
#cgo linux LDFLAGS: -ldl -lm -lpthread
#cgo windows LDFLAGS: -lws2_32 -luserenv -lbcrypt -ladvapi32 -lntdll
*/
import "C"

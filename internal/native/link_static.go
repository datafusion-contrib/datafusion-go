//go:build datafusion_use_static_lib

package native

/*
#cgo !windows LDFLAGS: -L${SRCDIR}/../../rust/target/release -ldatafusion_go
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/../../rust/target/x86_64-pc-windows-gnu/release -ldatafusion_go
#cgo linux LDFLAGS: -ldl -lm -lpthread
#cgo windows LDFLAGS: -lws2_32 -luserenv -lbcrypt -ladvapi32 -lntdll
*/
import "C"

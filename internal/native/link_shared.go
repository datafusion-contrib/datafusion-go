//go:build datafusion_use_lib

package native

/*
#cgo CFLAGS: -DDFGO_DIRECT_LINK
#cgo LDFLAGS: -ldatafusion_go
*/
import "C"

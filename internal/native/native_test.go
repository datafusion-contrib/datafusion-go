//go:build cgo

package native

import (
	"errors"
	"testing"
)

func TestNativeErrorHasKind(t *testing.T) {
	_, err := OpenDatabase(":memory:?datafusion.nope=1")
	if err == nil {
		t.Fatal("expected invalid DataFusion config error")
	}

	var nativeErr *Error
	if !errors.As(err, &nativeErr) {
		t.Fatalf("got %T, want *native.Error", err)
	}
	if nativeErr.Kind == "" {
		t.Fatalf("native error kind is empty for %q", nativeErr.Message)
	}
}

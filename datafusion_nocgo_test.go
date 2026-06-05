//go:build !cgo

package datafusion

import (
	"database/sql"
	"strings"
	"testing"
)

func TestCgoDisabledUnsupported(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err == nil {
		defer db.Close()
		err = db.Ping()
	}
	if err == nil {
		t.Fatal("expected cgo-disabled error")
	}
	if !strings.Contains(err.Error(), "requires cgo") {
		t.Fatalf("got %v, want cgo-disabled error", err)
	}
}

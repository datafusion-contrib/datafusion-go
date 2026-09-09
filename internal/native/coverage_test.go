//go:build datafusion_test_coverage && cgo && (darwin || linux)

package native

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if err := WriteTestCoverage(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

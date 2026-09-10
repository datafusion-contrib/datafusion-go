//go:build datafusion_test_coverage && cgo && (darwin || linux)

package datafusion

import (
	"fmt"
	"os"
	"testing"

	"github.com/datafusion-contrib/datafusion-go/internal/native"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if err := native.WriteTestCoverage(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

package datafusion

import "testing"

type closeErrorer interface {
	Close() error
}

func closeNoError(tb testing.TB, closer closeErrorer) {
	tb.Helper()
	if err := closer.Close(); err != nil {
		tb.Fatalf("close: %v", err)
	}
}

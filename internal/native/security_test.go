//go:build cgo

package native

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestResolveNativeLibraryRejectsRelativeEnvPath(t *testing.T) {
	t.Setenv(nativeLibraryEnv, "relative/libdatafusion_go.so")
	_, err := resolveNativeLibrary()
	if err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("expected absolute-path error, got %v", err)
	}
}

func TestNativeDownloadURLRejectsNonHTTPSBase(t *testing.T) {
	for _, base := range []string{"http://example.com/assets", "ftp://example.com", "example.com/assets", "https://"} {
		t.Setenv(nativeDownloadBaseEnv, base)
		if _, err := nativeDownloadURL("asset"); err == nil {
			t.Errorf("expected error for download base %q", base)
		}
	}
}

func TestNativeDownloadURLAcceptsDefaultAndHTTPSBase(t *testing.T) {
	t.Setenv(nativeDownloadBaseEnv, "")
	url, err := nativeDownloadURL("asset")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(url, "https://github.com/datafusion-contrib/datafusion-go/releases/download/") {
		t.Fatalf("unexpected default download URL %q", url)
	}

	t.Setenv(nativeDownloadBaseEnv, "https://mirror.example.com/datafusion/")
	url, err = nativeDownloadURL("asset")
	if err != nil {
		t.Fatal(err)
	}
	if url != "https://mirror.example.com/datafusion/asset" {
		t.Fatalf("unexpected mirrored download URL %q", url)
	}
}

func TestNULBytesAreRejectedAtTheFFIBoundary(t *testing.T) {
	if _, err := OpenDatabase("\x00?x=y"); err == nil || !strings.Contains(err.Error(), "NUL") {
		t.Errorf("DSN with NUL byte: got %v, want NUL rejection", err)
	}

	db, err := OpenDatabase("")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Prepare("select 1 \x00 -- dropped"); err == nil || !strings.Contains(err.Error(), "NUL") {
		t.Errorf("query with NUL byte: got %v, want NUL rejection", err)
	}
	if err := conn.RegisterArrowIPC("tenant_alice\x00-me", nil); err == nil || !strings.Contains(err.Error(), "NUL") {
		t.Errorf("RegisterArrowIPC name with NUL byte: got %v, want NUL rejection", err)
	}
	if err := conn.DeregisterTable("tenant_alice\x00-me"); err == nil || !strings.Contains(err.Error(), "NUL") {
		t.Errorf("DeregisterTable name with NUL byte: got %v, want NUL rejection", err)
	}
}

func TestOversizedParameterOrdinalIsRejected(t *testing.T) {
	db, err := OpenDatabase("")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stmt, err := conn.Prepare("select $1")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	_, err = stmt.ExecuteArrow(context.Background(), []driver.NamedValue{{
		Ordinal: 1 << 40,
		Value:   int64(1),
	}})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized ordinal: got %v, want ordinal-bound rejection", err)
	}
}

func TestPrivilegedResolutionRejectsAllRuntimePaths(t *testing.T) {
	t.Setenv(nativeLibraryEnv, filepath.Join(t.TempDir(), "native-library"))
	t.Setenv(nativeDownloadBaseEnv, "https://example.com")
	if _, err := resolveNativeLibraryForExecution(true); err == nil || !strings.Contains(err.Error(), "privileged") {
		t.Fatalf("expected privileged execution error before path resolution, got %v", err)
	}
}

func TestDownloadRejectsHTTPRedirect(t *testing.T) {
	var received atomic.Bool
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer destination.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusFound)
	}))
	defer redirect.Close()
	dst, err := os.CreateTemp(t.TempDir(), "download")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	if err := downloadFile(dst, redirect.URL); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("expected HTTPS redirect rejection, got %v", err)
	}
	if received.Load() {
		t.Fatal("followed insecure redirect")
	}
}

func TestDownloadRejectsOversizedContentLength(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(maxNativeAssetSize+1))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	dst, err := os.CreateTemp(t.TempDir(), "download")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	if err := downloadFile(dst, server.URL); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("expected size limit rejection, got %v", err)
	}
	info, err := dst.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatal("oversized download wrote bytes")
	}
}

//go:build cgo && linux

package native

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// Requires a disposable root container with setcap; see CONTRIBUTING.md.
func TestSecureExecutionWithCapabilitiesWithoutProc(t *testing.T) {
	const childEnv = "DFGO_TEST_CAPABILITY_CHROOT"
	if root := os.Getenv(childEnv); root != "" {
		if os.Getuid() == 0 || os.Getuid() != os.Geteuid() {
			t.Fatal("fixture must run with equal non-root IDs")
		}
		if err := syscall.Chroot(root); err != nil {
			t.Fatal(err)
		}
		if err := os.Chdir("/"); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat("/proc/self/auxv"); !os.IsNotExist(err) {
			t.Fatalf("expected absent /proc: %v", err)
		}
		if !secureExecution() {
			t.Fatal("lost AT_SECURE after chroot")
		}
		if _, err := resolveNativeLibrary(); err == nil || !strings.Contains(err.Error(), "privileged") {
			t.Fatalf("privileged resolution: %v", err)
		}
		return
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root in a disposable container")
	}
	setcap, err := exec.LookPath("setcap")
	if err != nil {
		t.Skip("setcap is unavailable")
	}
	root, err := os.MkdirTemp("", "dfgo-capability-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(root, "empty")
	if err := os.Mkdir(empty, 0755); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(exe)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	target := filepath.Join(root, "test")
	dst, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0755)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(dst, source)
	closeErr := dst.Close()
	if copyErr != nil {
		t.Fatal(copyErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if out, err := exec.Command(setcap, "cap_sys_chroot=ep", target).CombinedOutput(); err != nil {
		t.Fatalf("setcap: %v: %s", err, out)
	}
	child := exec.Command(target, "-test.run=^TestSecureExecutionWithCapabilitiesWithoutProc$", "-test.v")
	child.Env = append(os.Environ(), childEnv+"="+empty)
	child.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}}
	if out, err := child.CombinedOutput(); err != nil {
		t.Fatalf("capability subprocess: %v: %s", err, out)
	}
}

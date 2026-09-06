//go:build cgo && unix

package native

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativePathRejectsWritableAncestorsAndFiles(t *testing.T) {
	for _, target := range []string{"parent", "file"} {
		t.Run(target, func(t *testing.T) {
			parent := filepath.Join(t.TempDir(), "cache")
			dir := filepath.Join(parent, "version")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(dir, "native")
			if err := os.WriteFile(file, []byte("library"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := trustedNativePath(file, false); err != nil {
				t.Fatal(err)
			}
			path := parent
			if target == "file" {
				path = file
			}
			if err := os.Chmod(path, 0777); err != nil {
				t.Fatal(err)
			}
			if _, err := trustedNativePath(file, false); err == nil || !strings.Contains(err.Error(), "writable") {
				t.Fatalf("accepted writable %s: %v", target, err)
			}
		})
	}
}

func TestNativePathUsesResolvedSymlinkTarget(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "native")
	link := filepath.Join(root, "link")
	if err := os.WriteFile(target, []byte("verified library"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	resolved, err := trustedNativePath(link, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(link, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(resolved)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "verified library" {
		t.Fatalf("redirected resolved load path: %q", got)
	}
}

func TestNativePathRejectsSpecialFiles(t *testing.T) {
	// A directory where the library should be must fail before opening/hashing.
	if _, err := trustedNativePath(t.TempDir(), false); err == nil {
		t.Fatal("accepted a directory as a native library")
	}
}

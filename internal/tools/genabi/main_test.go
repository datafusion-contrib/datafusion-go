package main

import (
	"os"
	"strings"
	"testing"
)

func TestHeaderContract(t *testing.T) {
	header, err := os.ReadFile("../../../rust/include/datafusion_go.h")
	if err != nil {
		t.Fatal(err)
	}
	abi, err := parse(string(header))
	if err != nil {
		t.Fatal(err)
	}
	if len(abi.functions) != 25 || len(abi.fields) != 14 {
		t.Fatalf("unexpected ABI dimensions: %d functions, %d fields", len(abi.functions), len(abi.fields))
	}
	for _, fixture := range []struct{ from, to string }{
		{"int32_t dfgo_abi_version(void);", "int32_t dfgo_abi_version(void);\nint32_t dfgo_abi_version(void);"},
		{"int32_t dfgo_abi_version(void);", "long dfgo_abi_version(void);"},
		{"int64_t index;", "int64_t index[2];"},
		{"int64_t index;", "int64_t index;\nint64_t index;"},
		{"#ifndef DFGO_NO_FUNCTION_PROTOTYPES", "#ifndef MISSING_BLOCK"},
	} {
		if _, err := parse(strings.Replace(string(header), fixture.from, fixture.to, 1)); err == nil {
			t.Errorf("accepted invalid contract: %s", fixture.to)
		}
	}
}

func TestRustTypes(t *testing.T) {
	for c, want := range map[string]string{
		"const char *":  "*const std::ffi::c_char",
		"dfgo_error **": "*mut *mut super::dfgo_error",
		"const void *":  "*const std::ffi::c_void",
		"int":           "std::ffi::c_int",
		"void":          "()",
	} {
		got, err := rustType(c)
		if err != nil || got != want {
			t.Errorf("%s: got %q, %v; want %q", c, got, err, want)
		}
	}
}

func TestGenerationAndCheck(t *testing.T) {
	header, err := os.ReadFile("../../../rust/include/datafusion_go.h")
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(t.TempDir())
	if err := os.MkdirAll("rust/include", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("rust/include/datafusion_go.h", header, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run(true); err == nil {
		t.Fatal("check accepted missing files")
	}
	if err := run(false); err != nil {
		t.Fatal(err)
	}
	if err := run(true); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("internal/native/abi_generated.h", []byte("stale\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run(true); err == nil {
		t.Fatal("check accepted stale files")
	}
}

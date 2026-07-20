package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCaptureGoroutineStacks(t *testing.T) {
	dump := captureGoroutineStacks()
	if len(dump) == 0 {
		t.Fatal("expected non-empty goroutine dump")
	}
	if !bytes.Contains(dump, []byte("goroutine ")) {
		t.Fatalf("dump does not look like a goroutine dump: %.80q", dump)
	}
}

func TestWriteGoroutineDump(t *testing.T) {
	dir := t.TempDir()

	path, n, dump, err := writeGoroutineDump(dir)
	if err != nil {
		t.Fatalf("writeGoroutineDump: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected positive goroutine count, got %d", n)
	}
	if filepath.Dir(path) != dir {
		t.Fatalf("dump written outside target dir: %s", path)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading dump file: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("dump file is empty")
	}
	if !bytes.Equal(data, dump) {
		t.Fatal("returned dump bytes differ from file contents")
	}
	if !strings.Contains(string(data), "goroutine ") {
		t.Fatal("dump file does not contain goroutine stacks")
	}
	// Reaching this point proves the dump writer returns normally and does not
	// exit the process.
}

func TestWriteGoroutineDumpErrorPath(t *testing.T) {
	// Writing into a non-existent directory makes os.WriteFile fail; the writer
	// must return the error (and still hand back the raw dump) rather than panic
	// or exit.
	_, _, dump, err := writeGoroutineDump(filepath.Join(t.TempDir(), "does-not-exist"))
	if err == nil {
		t.Fatal("expected error writing into a missing directory")
	}
	if len(dump) == 0 {
		t.Fatal("expected dump bytes even when the file write fails")
	}
}

func TestIsTruthy(t *testing.T) {
	for _, v := range []string{"1", "true", "yes"} {
		if !isTruthy(v) {
			t.Errorf("isTruthy(%q) = false, want true", v)
		}
	}
	for _, v := range []string{"", "0", "false", "no", "off", "2", "TRUE"} {
		if isTruthy(v) {
			t.Errorf("isTruthy(%q) = true, want false", v)
		}
	}
}

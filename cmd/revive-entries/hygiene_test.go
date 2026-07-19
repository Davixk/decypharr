package main

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

// TestStdoutContainsOnlyTSV pins the tool's output contract at the file
// descriptor level: every stdout line is either a '#' header/comment line or a
// 6-field TSV row. The production run leaked two non-TSV lines into stdout (a
// "Loading config from ..." fmt.Printf from internal/config and a zerolog INFO
// console line from storage init); run() now reroutes the process-global
// os.Stdout to stderr while the stores are open, which this test exercises the
// same way main does: the TSV writer is the pre-swap stdout handle.
func TestStdoutContainsOnlyTSV(t *testing.T) {
	stateDir := seedUnflipState(t)

	// Force the config singleton to reload inside run so the "Loading config
	// from ..." print actually fires during the captured window (seeding has
	// already latched the singleton by the time this test runs).
	config.Reset()

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open %s: %v", os.DevNull, err)
	}
	defer func() {
		if err := devnull.Close(); err != nil {
			t.Errorf("close devnull: %v", err)
		}
	}()

	oldStdout, oldStderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = stdoutW, devnull
	defer func() { os.Stdout, os.Stderr = oldStdout, oldStderr }()

	// Exactly main's call shape: the stdout writer is the (current) os.Stdout
	// handle, captured before run performs its internal swap.
	code := run(testOptions(stateDir, false), os.Stdout, os.Stderr)

	runRestoredStdout := os.Stdout == stdoutW
	os.Stdout, os.Stderr = oldStdout, oldStderr
	if err := stdoutW.Close(); err != nil {
		t.Fatalf("close pipe write end: %v", err)
	}
	data, err := io.ReadAll(stdoutR)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}
	captured := string(data)

	if code != exitOK {
		t.Fatalf("exit = %d, want %d\ncaptured stdout:\n%s", code, exitOK, captured)
	}
	if !runRestoredStdout {
		t.Error("run did not restore the process-global os.Stdout it found at entry")
	}

	lines := strings.Split(strings.TrimRight(captured, "\n"), "\n")
	if len(lines) < 3 {
		t.Fatalf("suspiciously short stdout (%d lines):\n%s", len(lines), captured)
	}
	sawHeader, sawCensus, sawRow := false, false, false
	for _, line := range lines {
		switch {
		case strings.HasPrefix(line, "# hash\t"):
			sawHeader = true
		case strings.HasPrefix(line, "# census:"):
			sawCensus = true
		case strings.HasPrefix(line, "#"):
			// other trailing comment lines
		default:
			if fields := strings.Split(line, "\t"); len(fields) != 6 {
				t.Errorf("non-TSV line leaked into stdout: %q", line)
			} else {
				sawRow = true
			}
		}
	}
	if !sawHeader || !sawCensus || !sawRow {
		t.Errorf("expected header/rows/census on stdout (header=%v row=%v census=%v):\n%s",
			sawHeader, sawRow, sawCensus, captured)
	}

	// Belt and braces for the two production leaks specifically.
	for _, leak := range []string{"Loading config", "Config file not found", "INF"} {
		if strings.Contains(captured, leak) {
			t.Errorf("known leak %q found on stdout:\n%s", leak, captured)
		}
	}
}

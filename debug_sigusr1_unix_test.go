//go:build unix

package main

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestSIGUSR1ProducesDumpWithoutExiting exercises the full signal path on Unix:
// SIGUSR1 must produce a goroutine dump file and must NOT terminate the process.
func TestSIGUSR1ProducesDumpWithoutExiting(t *testing.T) {
	dir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// signal.Notify runs synchronously inside the installer, so the signal is
	// diverted from its default (terminate) disposition before we send it.
	startDebugDumpHandler(ctx, dir, zerolog.Nop())

	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("sending SIGUSR1: %v", err)
	}

	// Poll for the dump file that the handler goroutine writes.
	deadline := time.Now().Add(3 * time.Second)
	for {
		matches, _ := filepath.Glob(filepath.Join(dir, "goroutine-dump-*.txt"))
		if len(matches) > 0 {
			if info, err := os.Stat(matches[0]); err == nil && info.Size() > 0 {
				return // success — and we are still running, so it did not exit
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for goroutine dump file after SIGUSR1")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

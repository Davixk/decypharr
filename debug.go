package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"time"

	"github.com/rs/zerolog"
)

// startDebugDumpHandler installs a non-fatal, on-demand goroutine-dump handler.
//
// It lets an operator capture a full snapshot of every goroutine's stack from a
// RUNNING instance, without restarting it and without an open debug port:
//
//	docker kill -s USR1 <container>   # sends SIGUSR1 to PID 1 (decypharr)
//
// Unlike SIGQUIT (which the Go runtime turns into a fatal crash), SIGUSR1 is
// caught here and only READS runtime state, so the process keeps running. Each
// signal writes a timestamped dump file under dir (the logs directory) and also
// mirrors the stacks to the structured logger. Unavailable on Windows.
//
// It is deliberately separate from the SIGINT/SIGTERM graceful-shutdown context
// in main(): SIGUSR1 is registered on its own channel and never triggers a
// shutdown.
func startDebugDumpHandler(ctx context.Context, dir string, log zerolog.Logger) {
	sigs := debugDumpSignals()
	if len(sigs) == 0 {
		return // platform without a user-defined dump signal (e.g. Windows)
	}

	ch := make(chan os.Signal, 1)
	// Register synchronously so the signal is diverted from its default
	// (process-terminating) disposition before this function returns.
	signal.Notify(ch, sigs...)

	go func() {
		defer signal.Stop(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				handleDumpSignal(dir, log)
			}
		}
	}()
}

// handleDumpSignal captures all goroutine stacks, writes them to a file and
// mirrors them to the logger. It never exits the process.
//
// Only goroutine stacks are dumped: the manager's internal counters (job-queue
// depth, action-gate inflight, etc.) are not reachable from here without
// coupling to the manager or taking its locks, and the stacks themselves
// already reveal which queue/semaphore waiters are blocked.
func handleDumpSignal(dir string, log zerolog.Logger) {
	path, n, dump, err := writeGoroutineDump(dir)
	if err != nil {
		// File write failed (e.g. read-only dir): still surface the stacks via
		// the logger so the snapshot isn't lost.
		log.Error().Err(err).Int("goroutines", n).
			Msgf("goroutine dump: file write failed, stacks follow:\n%s", dump)
		return
	}
	log.Info().Int("goroutines", n).Str("path", path).
		Msgf("goroutine dump written to %s (%d goroutines)", path, n)
	// Mirror the full stacks into the rotating log too, so they survive even if
	// the standalone dump file is later removed.
	log.Info().Msgf("goroutine stacks:\n%s", dump)
}

// writeGoroutineDump captures ALL goroutine stacks (equivalent to SIGQUIT's
// output, but non-fatal) and writes them to a timestamped file under dir. It
// returns the file path, the goroutine count, and the raw dump bytes so callers
// can also log them.
//
// It only reads runtime state, so it is race-free and safe to call at any time.
func writeGoroutineDump(dir string) (path string, n int, dump []byte, err error) {
	dump = captureGoroutineStacks()
	n = runtime.NumGoroutine()

	name := fmt.Sprintf("goroutine-dump-%s.txt", time.Now().Format("20060102-150405"))
	path = filepath.Join(dir, name)
	if werr := os.WriteFile(path, dump, 0o644); werr != nil {
		return "", n, dump, werr
	}
	return path, n, dump, nil
}

// captureGoroutineStacks returns a dump of every goroutine's stack, growing the
// buffer (starting at 1 MiB, doubling) until runtime.Stack reports it fit
// (returned length < len(buf)).
func captureGoroutineStacks() []byte {
	buf := make([]byte, 1<<20)
	for {
		if written := runtime.Stack(buf, true); written < len(buf) {
			return buf[:written]
		}
		buf = make([]byte, 2*len(buf))
	}
}

// isTruthy reports whether an environment value means "on" (1/true/yes),
// matching the config package's parseBool semantics.
func isTruthy(v string) bool {
	switch v {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

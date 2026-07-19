package manager

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// The breaker must stay closed below the threshold, pause with exponential
// backoff (base -> 2x -> 4x, capped) once tripped, resume only when the canary
// succeeds, and reset its chain on any success.
func TestRestoreCircuitBreakerBackoffAndResume(t *testing.T) {
	prevThreshold, prevBase, prevMax := restoreBreakerThreshold, restoreBreakerBaseDelay, restoreBreakerMaxDelay
	restoreBreakerThreshold = 3
	restoreBreakerBaseDelay = 30 * time.Second
	restoreBreakerMaxDelay = 2 * time.Minute
	t.Cleanup(func() {
		restoreBreakerThreshold, restoreBreakerBaseDelay, restoreBreakerMaxDelay = prevThreshold, prevBase, prevMax
	})

	canaryResults := []error{
		errors.New("still down"),
		errors.New("still down"),
		errors.New("still down"),
		nil, // fourth probe succeeds
	}
	canaryCalls := 0
	var slept []time.Duration

	breaker := newRestoreCircuitBreaker(zerolog.Nop(), func(context.Context) error {
		result := canaryResults[canaryCalls]
		canaryCalls++
		return result
	})
	breaker.sleep = func(_ context.Context, d time.Duration) bool {
		slept = append(slept, d)
		return true
	}

	ctx := context.Background()

	// Below the threshold the breaker never pauses.
	breaker.recordFailure()
	breaker.recordFailure()
	if breaker.tripped() {
		t.Fatal("breaker tripped below the threshold")
	}
	if !breaker.pauseUntilHealthy(ctx) {
		t.Fatal("pauseUntilHealthy returned shutdown without cancellation")
	}
	if canaryCalls != 0 || len(slept) != 0 {
		t.Fatalf("untripped breaker probed canary (calls=%d, sleeps=%v)", canaryCalls, slept)
	}

	// A success resets the chain.
	breaker.recordSuccess()
	breaker.recordFailure()
	breaker.recordFailure()
	if breaker.tripped() {
		t.Fatal("consecutive counter did not reset on success")
	}

	// Third consecutive failure trips the breaker; the pause backs off
	// 30s -> 60s -> 120s -> 120s (cap) while the canary keeps failing.
	breaker.recordFailure()
	if !breaker.tripped() {
		t.Fatal("breaker did not trip at the threshold")
	}
	if !breaker.pauseUntilHealthy(ctx) {
		t.Fatal("pauseUntilHealthy returned shutdown without cancellation")
	}
	wantSleeps := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 2 * time.Minute}
	if len(slept) != len(wantSleeps) {
		t.Fatalf("sleep sequence = %v, want %v", slept, wantSleeps)
	}
	for i, want := range wantSleeps {
		if slept[i] != want {
			t.Fatalf("sleep[%d] = %s, want %s (full sequence %v)", i, slept[i], want, slept)
		}
	}
	if canaryCalls != 4 {
		t.Fatalf("canary calls = %d, want 4", canaryCalls)
	}
	if breaker.tripped() {
		t.Fatal("breaker still tripped after canary success")
	}
	if breaker.delay != 0 {
		t.Fatalf("backoff delay = %s, want reset to 0", breaker.delay)
	}
}

// Shutdown must interrupt a paused restore: a cancelled context makes the
// pause return false instead of spinning on the canary.
func TestRestoreCircuitBreakerRespectsShutdown(t *testing.T) {
	breaker := newRestoreCircuitBreaker(zerolog.Nop(), func(context.Context) error {
		return errors.New("still down")
	})
	for i := 0; i < restoreBreakerThreshold; i++ {
		breaker.recordFailure()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if breaker.pauseUntilHealthy(ctx) {
		t.Fatal("pauseUntilHealthy ignored a cancelled context")
	}
}

// End-to-end: three consecutive infrastructure rebuild failures pause restore
// pass-2; the loop resumes only after the canary succeeds and keeps the
// remaining entries un-poisoned.
func TestRestorePass2CircuitBreakerPausesAndResumes(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	server.Close() // dead substrate for every rebuild

	m, _ := newVerdictTestManager(t, host, port)

	prevThreshold, prevBase, prevMax := restoreBreakerThreshold, restoreBreakerBaseDelay, restoreBreakerMaxDelay
	restoreBreakerThreshold = 3
	restoreBreakerBaseDelay = time.Millisecond
	restoreBreakerMaxDelay = 4 * time.Millisecond
	prevRetryBase := nzbInfraRetryBaseDelay
	nzbInfraRetryBaseDelay = time.Millisecond
	t.Cleanup(func() {
		restoreBreakerThreshold, restoreBreakerBaseDelay, restoreBreakerMaxDelay = prevThreshold, prevBase, prevMax
		nzbInfraRetryBaseDelay = prevRetryBase
	})

	var canaryCalls atomic.Int32
	m.restoreCanaryTestHook = func(context.Context) error {
		if canaryCalls.Add(1) == 1 {
			return errors.New("substrate still down")
		}
		return nil
	}

	hashes := []string{"breaker-entry-1", "breaker-entry-2", "breaker-entry-3", "breaker-entry-4"}
	for _, hash := range hashes {
		newQueuedNZBEntry(t, m, hash)
	}

	m.restoreActiveDownloadJobs()

	// Entries 1-3 fail infra and trip the breaker; before entry 4 the loop
	// pauses, probes the canary (fail once, then succeed) and resumes.
	if got := canaryCalls.Load(); got != 2 {
		t.Fatalf("canary calls = %d, want 2 (one failed probe, one successful)", got)
	}
	for _, hash := range hashes {
		persisted, err := m.storage.GetQueued(hash)
		if err != nil {
			t.Fatalf("GetQueued(%s): %v", hash, err)
		}
		if persisted.State == storage.EntryStateError {
			t.Fatalf("entry %s was marked terminal error during the paused restore", hash)
		}
		if persisted.Status != debridTypes.TorrentStatusQueued {
			t.Fatalf("entry %s status = %q, want queued", hash, persisted.Status)
		}
	}
}

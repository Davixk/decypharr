package manager

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/customerror"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// TestAnsweredTransientAcrossEveryDebridLeavesNoDurableDamage is the regression
// test for the half of the outage problem the ceiling guard did not cover.
//
// A provider that never ANSWERS was already safe: IsBackendTimeout short-circuits
// and the entry is left alone. A provider that answers with a FAILURE was not.
// Every such answer — a submit refused, a status poll that came back incomplete,
// a 503 mid-outage — was recorded as that debrid's permanent failure for this
// entry, and once every debrid had one recorded the entry itself was condemned
// (Bad) and persisted. An hour of provider trouble therefore left durable damage
// across the library that nothing afterwards could distinguish from content that
// had genuinely died.
//
// The cycle count is the point: this runs the whole cascade repeatedly, exactly
// as an outage would, and NOTHING may accumulate.
func TestAnsweredTransientAcrossEveryDebridLeavesNoDurableDamage(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("7", 40)
	snapshot := persistLifecycleEntry(t, m, lifecycleEntry(hash, "alpha", "alpha-old"))

	// Both providers answer, and both answer with an outage-class failure.
	for _, name := range []string{"alpha", "beta"} {
		client := &lifecycleDebridClient{name: name}
		client.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
			return nil, fmt.Errorf("realdebrid API error: Status: 503 || Code: 19")
		}
		m.clients.Store(name, client)
	}
	m.fixer.providerOrder = []string{"alpha", "beta"}

	for cycle := 1; cycle <= 5; cycle++ {
		result, err := m.fixer.FixTorrent(context.Background(), snapshot, false)
		if err == nil || result == nil || result.Success {
			t.Fatalf("cycle %d: FixTorrent = (%+v, %v), want a failed repair", cycle, result, err)
		}
		if result.AttemptsCount != 2 {
			t.Fatalf("cycle %d: attempted %d debrids, want 2 — a marker was written and excluded one",
				cycle, result.AttemptsCount)
		}
		if customerror.IsContentPermanentlyGone(err) {
			t.Fatalf("cycle %d: an outage was reported as a permanent content verdict: %v", cycle, err)
		}
		if !customerror.IsRetriableError(err) {
			t.Fatalf("cycle %d: an outage was reported as non-retryable: %v", cycle, err)
		}
	}

	current, getErr := m.storage.Get(hash)
	if getErr != nil {
		t.Fatalf("Get entry: %v", getErr)
	}
	if current.Bad {
		t.Fatal("repeated outage-class failures condemned the entry; a provider having a bad day says nothing about the content")
	}
	for _, name := range []string{"alpha", "beta"} {
		if m.fixer.IsFailedToReinsert(hash, name) {
			t.Fatalf("outage-class failure wrote the per-debrid marker for %q; that marker excludes a healthy debrid from repairing this entry", name)
		}
	}
}

// TestConfirmedTakedownCondemnsTheEntry is the other side of the split: the
// guarantee above must not be so broad that nothing is ever condemned. A
// provider stating the content itself is legally gone IS a durable verdict and
// must reach the durable flag.
func TestConfirmedTakedownCondemnsTheEntry(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("8", 40)
	snapshot := persistLifecycleEntry(t, m, lifecycleEntry(hash, "alpha", "alpha-old"))

	client := &lifecycleDebridClient{name: "alpha"}
	client.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		return nil, customerror.NewContentTakedownError(fmt.Errorf("infringing_file (code 35)"))
	}
	m.clients.Store("alpha", client)
	m.fixer.providerOrder = []string{"alpha"}

	result, err := m.fixer.FixTorrent(context.Background(), snapshot, false)
	if err == nil || result == nil || result.Success {
		t.Fatalf("FixTorrent = (%+v, %v), want a failed repair", result, err)
	}

	current, getErr := m.storage.Get(hash)
	if getErr != nil {
		t.Fatalf("Get entry: %v", getErr)
	}
	if !current.Bad {
		t.Fatal("a confirmed legal takedown on every debrid did not condemn the entry")
	}
	if !m.fixer.IsFailedToReinsert(hash, "alpha") {
		t.Fatal("a confirmed content verdict did not write the per-debrid marker")
	}
}

// TestResetFailureStateClearsThePerDebridMarkers is the regression test for the
// key-shape mismatch.
//
// Markers are written and read as "infohash:debrid"; ResetFailureState deleted
// the bare "infohash", a key nothing writes and nothing reads. So every marker
// was permanent for the life of the process, and the one event meant to clear
// them — a re-insertion that succeeded, this function's only caller — cleared
// nothing. Worse, the exclusion could never lift itself: clearing required a
// success on a debrid the marker had already removed from the attempt order.
func TestResetFailureStateClearsThePerDebridMarkers(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("9", 40)
	other := strings.Repeat("a", 40)

	for _, name := range []string{"alpha", "beta"} {
		m.fixer.failedToReinsert.Store(failureMarkerKey(hash, name), struct{}{})
	}
	// A different entry's marker must survive: the reset is scoped to one
	// infohash, and a prefix scan that is too eager would wipe the library's.
	m.fixer.failedToReinsert.Store(failureMarkerKey(other, "alpha"), struct{}{})

	m.fixer.ResetFailureState(hash)

	for _, name := range []string{"alpha", "beta"} {
		if m.fixer.IsFailedToReinsert(hash, name) {
			t.Fatalf("ResetFailureState left the %q marker in place; it is keyed %q and the reset deleted something else",
				name, failureMarkerKey(hash, name))
		}
	}
	if !m.fixer.IsFailedToReinsert(other, "alpha") {
		t.Fatal("ResetFailureState cleared an unrelated entry's marker")
	}
}

// TestSuccessfulReinsertionClearsTheMarkers pins the same fix end to end, at the
// call site that actually matters: a repair that SUCCEEDS must wipe the slate,
// or a debrid excluded once stays excluded for the process's lifetime.
func TestSuccessfulReinsertionClearsTheMarkers(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("b", 40)
	snapshot := persistLifecycleEntry(t, m, lifecycleEntry(hash, "alpha", "alpha-old"))

	// Left over from an earlier confirmed verdict against a debrid that is not
	// in this repair's path.
	m.fixer.failedToReinsert.Store(failureMarkerKey(hash, "gamma"), struct{}{})

	client := &lifecycleDebridClient{name: "alpha"}
	client.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		return completedRemote(hash, "alpha", "alpha-new"), nil
	}
	m.clients.Store("alpha", client)
	m.fixer.providerOrder = []string{"alpha"}

	result, err := m.fixer.FixTorrent(context.Background(), snapshot, false)
	if err != nil || result == nil || !result.Success {
		t.Fatalf("FixTorrent = (%+v, %v), want a successful repair", result, err)
	}
	if m.fixer.IsFailedToReinsert(hash, "gamma") {
		t.Fatal("a successful re-insertion did not clear the entry's per-debrid markers")
	}
}

// TestReacquireElsewhereWithNoOtherDebridDoesNotCondemn covers the zero-attempt
// path the takedown route can reach: with a single debrid configured, skipping
// the active one leaves nothing to try. An empty attempt order is not "every
// provider says this is dead", and reading it as such would let the fixer
// condemn an entry without asking anybody anything.
func TestReacquireElsewhereWithNoOtherDebridDoesNotCondemn(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("c", 40)
	snapshot := persistLifecycleEntry(t, m, lifecycleEntry(hash, "alpha", "alpha-old"))

	client := &lifecycleDebridClient{name: "alpha"}
	m.clients.Store("alpha", client)
	m.fixer.providerOrder = []string{"alpha"}

	if err := m.ReacquireEntryElsewhere(context.Background(), snapshot); err == nil {
		t.Fatal("re-acquiring with no other debrid configured reported success")
	}

	current, getErr := m.storage.Get(hash)
	if getErr != nil {
		t.Fatalf("Get entry: %v", getErr)
	}
	if current.Bad {
		t.Fatal("a zero-attempt re-acquisition condemned the entry")
	}
}

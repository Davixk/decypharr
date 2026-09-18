package manager

import (
	"context"
	"fmt"
	"testing"

	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// 🔴 LOSING A PROVIDER IS NOT THE PROVIDER LOSING EVERYTHING.
//
// Removing a placement deletes the ENTRY when it is the only one, so a provider
// that answers 200 with an empty or short list empties the library of everything
// placed on it — and with arr_delete on, spends one indexer search per entry to
// get it back. An outright error is already safe (doRefreshTorrents returns
// before removals are computed); the successful-but-empty answer is the one that
// deletes, and it was guarded only by a debug log.
//
// This matters imminently: an AllDebrid subscription is being cancelled on an
// account holding ~5,000 items. If it lapses while still configured, that is the
// exact shape below.
func TestALapsedProviderCannotEmptyTheLibrary(t *testing.T) {
	// 5,000 held, provider lists none of them.
	if !removalBatchIsImplausible(5000, 5000) {
		t.Fatal("a provider listing NOTHING for 5,000 held placements was believed. Every entry whose " +
			"only placement is that provider would be deleted outright, and each one costs a fresh " +
			"indexer search to recover")
	}
	// Still implausible well short of total: a tenth is already extraordinary.
	if !removalBatchIsImplausible(2700, 26580) {
		t.Fatal("a batch removing >10% of a provider's holdings in one refresh window was believed")
	}
}

// ⚠️ AND ORDINARY CHURN MUST PASS UNTOUCHED, or the guard becomes a brake on
// normal operation and someone turns it off.
func TestOrdinaryChurnIsNotBlocked(t *testing.T) {
	cases := []struct {
		name                string
		removals, heldLocal int
	}{
		{"a handful deleted on the provider", 12, 5000},
		{"a busy day", 99, 1200},
		{"right at the floor", 100, 150},
		{"large account, proportionate churn", 500, 26580},
	}
	for _, tc := range cases {
		if removalBatchIsImplausible(tc.removals, tc.heldLocal) {
			t.Errorf("%s: %d removals against %d held was refused; normal churn must pass or the guard "+
				"gets disabled and protects nothing", tc.name, tc.removals, tc.heldLocal)
		}
	}
}

// A provider we hold nothing on cannot be the subject of a mass removal, and the
// guard must not divide by that zero.
func TestNothingHeldIsNotGuarded(t *testing.T) {
	if removalBatchIsImplausible(500, 0) {
		t.Fatal("guarded a provider we hold nothing on")
	}
}

// 🔑 AND THE GUARD MUST BE WIRED, not merely correct.
//
// The tests above exercise the predicate. Deleting the call site in
// doRefreshTorrents leaves every one of them green, which is precisely the
// shape that has already shipped defects in this codebase — a verdict computed
// and then not acted on. This one drives the real refresh against a provider
// that answers successfully with an empty list, and asserts the entries survive.
func TestAnEmptyEnumerationDeletesNothingThroughTheRealRefresh(t *testing.T) {
	const held = 150 // above massRemovalFloor, so the guard is the only thing that can save them

	m := newProviderLifecycleManager(t)
	// reportProviderOrphans reads the queue on the same pass; the lifecycle
	// fixture does not wire one.
	m.queue = newQueue(m.storage, "")
	client := &lifecycleDebridClient{
		name: "provider",
		// A LAPSED SUBSCRIPTION ANSWERS LIKE THIS: 200 OK, no rows, no error.
		// An outright error is already safe — doRefreshTorrents returns before
		// removals are computed — so success-with-nothing is the dangerous shape.
		getAll: func() ([]*debridTypes.Torrent, error) { return nil, nil },
	}
	m.clients.Store("provider", client)

	hashes := make([]string, 0, held)
	for i := range held {
		hash := fmt.Sprintf("%040x", i)
		entry := lifecycleEntry(hash, "provider", fmt.Sprintf("remote-%d", i))
		persistLifecycleEntry(t, m, entry)
		hashes = append(hashes, hash)
	}

	if err := m.doRefreshTorrents(context.Background(), "provider", client); err != nil {
		t.Fatalf("refresh returned %v", err)
	}

	survived := 0
	for _, hash := range hashes {
		if got, err := m.storage.Get(hash); err == nil && got != nil {
			survived++
		}
	}
	if survived != held {
		t.Fatalf("%d of %d entries survived an empty provider listing. Each entry's only placement was on "+
			"that provider, so removing it deletes the ENTRY — a lapsed subscription or a revoked key "+
			"would empty the library, and with arr_delete on, spend an indexer search per entry to "+
			"recover", survived, held)
	}
}

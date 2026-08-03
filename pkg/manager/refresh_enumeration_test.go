package manager

import (
	"strings"
	"testing"
	"time"

	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// ONE ENUMERATION, TWO VIEWS.
//
// The refresh listed the account with GetTorrents (downloaded only) and then
// treated "not in that listing" as "gone from the provider". Those are not the
// same claim. A torrent that is merely mid-download, queued, or dead is absent
// from the downloaded-only view while still occupying a slot on the account, so
// the refresh removed our placement for it — and, when that was the entry's
// only placement, removeProviderPlacement deletes the entry outright.
//
// A status filter must never be able to mean deletion.

func presenceOf(rows ...*debridTypes.Torrent) providerPresence {
	p := providerPresence{
		hashes: map[string]struct{}{},
		ids:    map[string]struct{}{},
	}
	for _, r := range rows {
		if r == nil {
			continue
		}
		if r.InfoHash != "" {
			p.hashes[strings.ToLower(r.InfoHash)] = struct{}{}
		}
		if r.Id != "" {
			p.ids[r.Id] = struct{}{}
		}
	}
	return p
}

// TestRefreshKeepsPlacementForAnInProgressProviderCopy is the regression test.
// The provider still holds the torrent; it just is not finished.
func TestRefreshKeepsPlacementForAnInProgressProviderCopy(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("a", 40)
	persistLifecycleEntry(t, m, lifecycleEntry(hash, "provider", "live-id"))

	// Downloading on the provider, so it is absent from the LIBRARY view but
	// present in the PRESENCE view.
	inProgress := &debridTypes.Torrent{
		Id:       "live-id",
		InfoHash: hash,
		Debrid:   "provider",
		Status:   debridTypes.TorrentStatusDownloading,
	}

	_, removals, err := m.detectTorrentChanges(
		"provider",
		map[string]*debridTypes.Torrent{},
		map[string]*debridTypes.Torrent{},
		presenceOf(inProgress),
	)
	if err != nil {
		t.Fatalf("detectTorrentChanges: %v", err)
	}
	if len(removals) != 0 {
		t.Fatalf("removals = %d, want 0: the provider still holds this torrent, it is only unfinished. "+
			"Removing the placement here deletes our record of an item we are still paying a slot for.",
			len(removals))
	}
}

// TestRefreshKeepsPlacementForADeadProviderCopy: a dead copy still occupies a
// stored slot. Forgetting it locally does not free anything — culling it is
// ENUMERATE's job, which deletes on the provider rather than only locally.
func TestRefreshKeepsPlacementForADeadProviderCopy(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("b", 40)
	persistLifecycleEntry(t, m, lifecycleEntry(hash, "provider", "dead-id"))

	dead := &debridTypes.Torrent{
		Id:             "dead-id",
		InfoHash:       hash,
		Debrid:         "provider",
		Status:         debridTypes.TorrentStatusError,
		ProviderStatus: "magnet_error",
		ProviderDead:   true,
	}

	_, removals, err := m.detectTorrentChanges(
		"provider",
		map[string]*debridTypes.Torrent{},
		map[string]*debridTypes.Torrent{},
		presenceOf(dead),
	)
	if err != nil {
		t.Fatalf("detectTorrentChanges: %v", err)
	}
	if len(removals) != 0 {
		t.Fatalf("removals = %d, want 0: a dead copy is still on the account", len(removals))
	}
}

// TestRefreshStillRemovesGenuinelyAbsentPlacements is the mirror. Without it,
// an implementation that simply never removes anything would pass the tests
// above and silently retain placements for items the provider really dropped.
func TestRefreshStillRemovesGenuinelyAbsentPlacements(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("c", 40)
	persistLifecycleEntry(t, m, lifecycleEntry(hash, "provider", "gone-id"))

	// The provider answers with a healthy listing that simply does not contain
	// this item — the only condition that justifies removal.
	somethingElse := &debridTypes.Torrent{
		Id:       "other-id",
		InfoHash: strings.Repeat("d", 40),
		Debrid:   "provider",
		Status:   debridTypes.TorrentStatusDownloaded,
	}

	_, removals, err := m.detectTorrentChanges(
		"provider",
		map[string]*debridTypes.Torrent{},
		map[string]*debridTypes.Torrent{},
		presenceOf(somethingElse),
	)
	if err != nil {
		t.Fatalf("detectTorrentChanges: %v", err)
	}
	if len(removals) != 1 {
		t.Fatalf("removals = %d, want 1: this placement is genuinely gone from the provider", len(removals))
	}
}

// TestPresenceMatchesOnEitherIdentity: providers rotate placement IDs and folder
// aliases carry synthetic hashes, so presence must answer on either.
func TestPresenceMatchesOnEitherIdentity(t *testing.T) {
	p := presenceOf(&debridTypes.Torrent{Id: "the-id", InfoHash: "AABBCC"})

	if !p.holds("aabbcc", "") {
		t.Error("presence must match a hash case-insensitively")
	}
	if !p.holds("", "the-id") {
		t.Error("presence must match a placement ID when the hash is unknown")
	}
	if !p.holds("unrelated", "the-id") {
		t.Error("a matching ID alone is a positive sighting")
	}
	if p.holds("unrelated", "unrelated-id") {
		t.Error("presence reported an item the provider does not hold")
	}
	if (providerPresence{}).holds("anything", "anything") {
		t.Error("an empty presence must hold nothing")
	}
}

// TestFillCacheAdoptsAnObservedCount: the refresh has already counted the
// account, so the fill snapshot is free rather than a second enumeration.
func TestFillCacheAdoptsAnObservedCount(t *testing.T) {
	client := &fillClient{count: 7}
	c := newProviderFillCache()
	now := time.Now()
	c.observe("provider", 4998, now)

	count, known := c.fill("provider", client, now)
	if !known || count != 4998 {
		t.Fatalf("fill = (%d, %v), want (4998, true)", count, known)
	}
	client.mu.Lock()
	calls := client.calls
	client.mu.Unlock()
	if calls != 0 {
		t.Fatalf("enumerations = %d, want 0: an observed count is the whole point of not re-listing", calls)
	}

	// Past the TTL the observation ages out like any other snapshot, so a stale
	// count cannot outlive its usefulness.
	if got, _ := c.fill("provider", client, now.Add(providerFillTTL+time.Second)); got != 7 {
		t.Fatalf("post-TTL fill = %d, want 7 from a fresh enumeration", got)
	}
}

// TestObserveRejectsANonCount: "unknown" must never be laundered into a number,
// because a count that is too low reads as "the account has room".
func TestObserveRejectsANonCount(t *testing.T) {
	c := newProviderFillCache()
	now := time.Now()
	c.observe("", 10, now)
	c.observe("provider", -1, now)

	if _, known := c.fill("provider", nil, now); known {
		t.Fatal("a negative count was accepted as a fill observation")
	}
}

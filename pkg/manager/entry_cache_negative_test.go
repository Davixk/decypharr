package manager

import (
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// TestEntryCacheDoesNotPinNegativeResults pins the hardening in cache.go.
//
// A nil `current` is never a fact — it is the failure return of both producers
// (getTorrentChildren when storage.GetEntryItem ERRORS, getEntryChildren when
// ForEachMeta ERRORS). Caching one used to be permanent: the map has no TTL and
// is cleared only by a global EntryCache.Refresh(), so ONE unlucky read pinned
// an entry to PROPFIND-404 for the life of the process while /api/browse, the
// repair sweep and direct reads all still resolved it perfectly.
//
// Note what this test does NOT claim: it is not the cause of entries missing
// from the `__all__` parent listing. Those entries answer their own PROPFIND
// with a valid 207, so nothing is pinned negative for them.
func TestEntryCacheDoesNotPinNegativeResults(t *testing.T) {
	m := newActionLifecycleFixture(t, 0)
	m.initEntryCache()

	const name = "LateArrival"

	// First lookup misses: the entry does not exist yet.
	if current, _ := m.GetTorrentChildren(name); current != nil {
		t.Fatal("lookup of a non-existent entry returned a node")
	}

	now := time.Unix(1_700_000_000, 0).UTC()
	entry := &storage.Entry{
		Protocol:   config.ProtocolTorrent,
		InfoHash:   "late-arrival-hash",
		Name:       name,
		Status:     debridTypes.TorrentStatusDownloaded,
		IsComplete: true,
		AddedOn:    now,
		Files: map[string]*storage.File{
			"movie.mkv": {Name: "movie.mkv", InfoHash: "late-arrival-hash", Size: 4096, AddedOn: now},
		},
	}
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}

	// The SAME cache instance must now resolve it. No global Refresh() in
	// between: that is exactly the crutch the old behaviour depended on.
	current, children := m.GetTorrentChildren(name)
	if current == nil {
		t.Fatal("a negative result pinned the entry: a later successful lookup still returned nothing without a global EntryCache.Refresh()")
	}
	if current.Name() != name {
		t.Fatalf("resolved node name = %q, want %q", current.Name(), name)
	}
	if len(children) != 1 {
		t.Fatalf("children = %d, want 1", len(children))
	}

	// Positive results are still cached — the fix must not disable the cache.
	if _, ok := m.entry.entries.Load(torrentEntryCachePrefix + name); !ok {
		t.Fatal("a successful listing was not cached; every PROPFIND would recompute it")
	}
}

// TestEntryCacheStoresOnlyResolvedListings states the rule directly: a miss is
// served to the caller but never written to the map.
func TestEntryCacheStoresOnlyResolvedListings(t *testing.T) {
	m := newActionLifecycleFixture(t, 0)
	m.initEntryCache()

	const missing = "NeverExisted"
	if _, _ = m.GetTorrentChildren(missing); true {
		if _, ok := m.entry.entries.Load(torrentEntryCachePrefix + missing); ok {
			t.Fatal("a negative lookup was cached; with no TTL and only a global Refresh() to clear it, that is a permanent pin")
		}
	}
}

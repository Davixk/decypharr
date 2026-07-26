package manager

import (
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
)

// GetEntryNode used to synthesise a directory node for ANY name, so a
// one-segment PROPFIND on a group that does not exist rendered as a valid,
// EMPTY collection (207, zero children) instead of 404 — while the same
// nonexistent name one segment deeper had always answered 404 correctly. The
// EntryCache then kept it, because a non-nil `current` looks like a successful
// lookup, so the wrong answer was sticky rather than transient.
//
// These tests pin both halves of the rule: a name that is not a real top-level
// node resolves to nothing, and a name that IS one still resolves even when it
// holds no entries.

func newEntryGroupFixture(t *testing.T) *Manager {
	t.Helper()
	m := newActionLifecycleFixture(t, 0)
	m.initEntryCache()
	// One configured debrid client and one configured custom folder, so the
	// dynamic halves of the top-level name set are exercised too. The client
	// value is never called — only its name is a folder.
	m.clients = xsync.NewMap[string, debrid.Client]()
	m.clients.Store("realdebrid", nil)
	m.customFolders = &CustomFolders{
		filters: map[string][]directoryFilter{"movies": nil},
		folders: []string{"movies"},
	}
	return m
}

// The root listing and the per-name lookup must agree in BOTH directions:
// everything advertised resolves, and nothing else does.
func TestGetEntryNodeResolvesExactlyWhatTheRootAdvertises(t *testing.T) {
	m := newEntryGroupFixture(t)

	advertised := m.GetEntries()
	if len(advertised) == 0 {
		t.Fatal("the mount root advertised nothing")
	}
	for _, entry := range advertised {
		if node := m.GetEntryNode(entry.Name()); node == nil {
			t.Fatalf("the root advertises %q but GetEntryNode returned nil; the listing and the lookup disagree", entry.Name())
		}
	}

	// Near-misses on a built-in, a custom folder and a provider folder; a
	// case-folded version.txt; and an entry name, which is never a top-level
	// group.
	for _, name := range []string{
		"",
		"stale-group",
		"__all___",
		"movies2",
		"realdebrid2",
		"VERSION.TXT",
		"SomeRelease",
	} {
		if node := m.GetEntryNode(name); node != nil {
			t.Fatalf("GetEntryNode(%q) synthesised a phantom group; a one-segment PROPFIND renders that as an existing, empty directory", name)
		}
		if current, children := m.getEntryChildren(name); current != nil || children != nil {
			t.Fatalf("getEntryChildren(%q) = (%s, %d children), want nothing", name, describe(current), len(children))
		}
	}
}

// THE CONSTRAINT THAT MUST NOT REGRESS. Telling "real and empty" apart from
// "does not exist" is the whole point of the fix; collapsing them the other way
// would hide legitimately empty groups.
func TestRealButEmptyGroupStillResolvesWithZeroChildren(t *testing.T) {
	m := newEntryGroupFixture(t)

	for _, group := range []string{
		EntryAllFolder, EntryBadFolder, EntryTorrentFolder, EntryNZBFolder,
		"realdebrid", "movies",
	} {
		current, children := m.getEntryChildren(group)
		if current == nil {
			t.Fatalf("%q is a real group that happens to hold nothing; it must still resolve, not 404", group)
		}
		if current.Name() != group || !current.IsDir() {
			t.Fatalf("%q resolved to %q (dir=%v)", group, current.Name(), current.IsDir())
		}
		if len(children) != 0 {
			t.Fatalf("%q listed %d children in an empty store", group, len(children))
		}
	}
}

// A miss must not be written to the entry cache. The map has no TTL and is
// cleared only by a global EntryCache.Refresh(), so caching one would pin the
// wrong answer for the life of the process — the same defect EntryCache.store
// was already hardened against for torrent lookups, stated here for groups.
func TestGroupMissIsNotCachedAsAnEmptyListing(t *testing.T) {
	m := newActionLifecycleFixture(t, 0)
	m.initEntryCache()
	m.clients = xsync.NewMap[string, debrid.Client]()
	m.customFolders = &CustomFolders{filters: map[string][]directoryFilter{}}

	const group = "movies"

	if current, children := m.GetEntryChildren(group); current != nil || children != nil {
		t.Fatalf("an unknown group resolved to %s with %d children", describe(current), len(children))
	}
	if _, ok := m.entry.entries.Load(group); ok {
		t.Fatal("a group miss was cached; with no TTL and only a global Refresh() to clear it, that is a permanent pin")
	}

	// The group becomes real, exactly as a config reload registering a custom
	// folder makes it. No EntryCache.Refresh() in between: depending on one is
	// the crutch this rule exists to remove.
	m.customFolders = &CustomFolders{
		filters: map[string][]directoryFilter{group: nil},
		folders: []string{group},
	}

	current, children := m.GetEntryChildren(group)
	if current == nil {
		t.Fatal("the earlier miss was served from cache: the group is real now and still resolved to nothing")
	}
	if current.Name() != group {
		t.Fatalf("resolved node name = %q, want %q", current.Name(), group)
	}
	if len(children) != 0 {
		t.Fatalf("a newly real, empty group listed %d children", len(children))
	}
	// Resolved listings ARE still cached — the rule must not disable the cache.
	if _, ok := m.entry.entries.Load(group); !ok {
		t.Fatal("a resolved listing was not cached; every PROPFIND would recompute it")
	}
}

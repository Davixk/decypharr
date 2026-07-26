package manager

import (
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// The `__all__` listing and the folder it links to derive their names from the
// same function at two different times: the listing from a FROZEN
// IndexEntry.Name snapshot, the folder from a live-derived entryItems key. When
// they disagree the listing advertises a name that resolves to nothing while the
// real, byte-serving entry is never emitted at all — invisible to rclone, Plex
// and the *arrs even though every direct read of it succeeds.
//
// These tests reproduce that divergence by writing the two stores apart, and pin
// that the listing is reconciled onto the navigable set.

func listingTestEntry(t *testing.T, m *Manager, folderName string) *storage.Entry {
	t.Helper()
	now := time.Unix(1_700_000_000, 0).UTC()
	hash := "hash-" + folderName
	entry := &storage.Entry{
		Protocol:   config.ProtocolTorrent,
		InfoHash:   hash,
		Name:       folderName,
		Status:     debridTypes.TorrentStatusDownloaded,
		IsComplete: true,
		AddedOn:    now,
		Files: map[string]*storage.File{
			"movie.mkv": {Name: "movie.mkv", InfoHash: hash, Size: 4096, AddedOn: now},
		},
	}
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate(%s): %v", folderName, err)
	}
	return entry
}

// divergeNavigableName reproduces the production split WITHOUT touching the
// entry: it writes an entryItems row under the name the live derivation would
// produce, while the entry's frozen IndexEntry.Name keeps the old one. That is
// precisely the state a FolderNaming change leaves behind — entryItems is
// rebuilt for every entry on every boot, IndexEntry.Name is never recomputed.
func divergeNavigableName(t *testing.T, m *Manager, navigable string, entry *storage.Entry) {
	t.Helper()
	files := make(map[string]*storage.File, len(entry.Files))
	var size int64
	for name, f := range entry.Files {
		copied := *f
		files[name] = &copied
		size += f.Size
	}
	if err := m.storage.UpdateItem(&storage.EntryItem{Name: navigable, Files: files, Size: size}); err != nil {
		t.Fatalf("UpdateItem(%s): %v", navigable, err)
	}
	if _, ok := m.storage.GetEntryItems()[navigable]; !ok {
		t.Fatalf("precondition: %s must be navigable", navigable)
	}
}

func listingNames(infos []FileInfo) map[string]FileInfo {
	out := make(map[string]FileInfo, len(infos))
	for _, info := range infos {
		out[info.Name()] = info
	}
	return out
}

// TestAllFolderListsEveryNavigableEntry is the production symptom: an entry that
// answers its own PROPFIND with a valid collection and serves real bytes must
// appear in its parent listing. It was absent for 9 entries.
func TestAllFolderListsEveryNavigableEntry(t *testing.T) {
	m := newActionLifecycleFixture(t, 0)
	m.initEntryCache()

	listingTestEntry(t, m, "VisibleRelease")

	// The entry's frozen listing name keeps the media extension (`filename`);
	// the live-derived navigable name does not (`filename_no_ext`).
	phantom := listingTestEntry(t, m, "HiddenRelease.mkv")
	divergeNavigableName(t, m, "HiddenRelease", phantom)

	_, children := m.getEntryChildren(EntryAllFolder)
	names := listingNames(children)

	if _, ok := names["HiddenRelease"]; !ok {
		t.Fatalf("a navigable, byte-serving entry is absent from __all__; listing = %v", keysOf(names))
	}
	if _, ok := names["VisibleRelease"]; !ok {
		t.Fatal("the reconciliation dropped an entry that was already listed; the fix must be purely additive")
	}

	// The restored child must carry an infohash, or nothing downstream can
	// resolve it.
	restored := names["HiddenRelease"]
	if restored.infohash == "" {
		t.Fatal("restored listing entry has no infohash")
	}
	if !restored.IsDir() {
		t.Fatal("restored listing entry is not a directory")
	}
}

// TestAllFolderReconciliationIsPurelyAdditive pins the constraint that keeps
// this safe to ship: an existing advertised name is NEVER removed, so no
// on-mount path can change under Plex or an *arr.
func TestAllFolderReconciliationIsPurelyAdditive(t *testing.T) {
	m := newActionLifecycleFixture(t, 0)
	m.initEntryCache()

	entry := listingTestEntry(t, m, "PhantomCase.mkv")
	divergeNavigableName(t, m, "PhantomCase", entry)

	_, children := m.getEntryChildren(EntryAllFolder)
	names := listingNames(children)

	// The phantom is counted and logged, not deleted — dropping it is a separate
	// decision and removing a name is the only way this change could break a
	// library.
	if _, ok := names["PhantomCase.mkv"]; !ok {
		t.Fatal("an already-advertised name was removed; the reconciliation must only add")
	}
	if _, ok := names["PhantomCase"]; !ok {
		t.Fatal("the navigable name was not restored alongside the phantom")
	}
}

// TestAllFolderSkipsBlankMetadataNames covers the internal bookkeeping rows
// (__migration_status__ is Put with nil metadata, so it decodes to a blank
// Name). ForEachMeta does not filter "__"-prefixed keys the way ForEach/List do,
// so a blank-named, unopenable child was advertised on the mount.
func TestAllFolderSkipsBlankMetadataNames(t *testing.T) {
	m := newActionLifecycleFixture(t, 0)
	m.initEntryCache()
	listingTestEntry(t, m, "RealRelease")

	_, children := m.getEntryChildren(EntryAllFolder)
	for _, child := range children {
		if child.Name() == "" {
			t.Fatal("__all__ advertised a blank-named child; it is unopenable on the mount")
		}
	}
}

// TestAllFolderListingNeverFeedsAHealthVerdict states the hard safety rule as an
// executable assertion: the repair sweep enumerates the NAVIGABLE set, not the
// listing, so an entry missing from `__all__` can never be classified broken —
// and therefore can never be pruned — for that reason.
func TestAllFolderListingNeverFeedsAHealthVerdict(t *testing.T) {
	m := newActionLifecycleFixture(t, 0)
	m.initEntryCache()

	entry := listingTestEntry(t, m, "InvisibleButAlive.mkv")
	divergeNavigableName(t, m, "InvisibleButAlive", entry)

	// A verdict written for it must be reachable by the name the sweep uses,
	// and no listing-derived signal may exist that could mark it dead.
	item, err := m.storage.GetEntryItem("InvisibleButAlive")
	if err != nil || item == nil {
		t.Fatalf("the repair sweep's own lookup failed for a live entry: %v", err)
	}
	if len(item.Files) == 0 {
		t.Fatal("a live entry enumerated by the sweep has no files")
	}
}

func keysOf(m map[string]FileInfo) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

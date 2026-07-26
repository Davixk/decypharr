package manager

import (
	"context"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// An entry's folder name is DERIVED from the process-global config.FolderNaming
// every time it is asked for, and the entryItems key set is re-derived under the
// CURRENT setting on every boot. Change the setting and every key moves — but
// the *arr symlinks on disk, and the frozen IndexEntry.Name snapshots the
// `__all__` listing is built from, keep the name the OLD setting produced. Those
// names then matched no key, so every name-addressed lookup missed and the
// library dangled on content that was present and serving bytes the whole time.
//
// These tests build that state the way production did — write under one setting,
// restart under the other — and pin that BOTH names address the same entry.

func aliasTestEntry(name string) *storage.Entry {
	now := time.Unix(1_700_000_000, 0).UTC()
	hash := "hash-" + name
	return &storage.Entry{
		Protocol:   config.ProtocolTorrent,
		InfoHash:   hash,
		Name:       name,
		Status:     debridTypes.TorrentStatusDownloaded,
		IsComplete: true,
		AddedOn:    now,
		Files: map[string]*storage.File{
			"video.mkv":  {Name: "video.mkv", InfoHash: hash, Size: 4096, AddedOn: now},
			"sample.mkv": {Name: "sample.mkv", InfoHash: hash, Size: 512, AddedOn: now},
		},
	}
}

// switchFolderNaming reproduces a FolderNaming change end to end: entries are
// written under `before` (which freezes IndexEntry.Name), the store is closed,
// and it is reopened under `after` (which re-derives every entryItems key in
// reconcileEntryItemsAtStartup). No row is hand-written — the divergence is
// produced by the same code path that produced it in production.
func switchFolderNaming(t *testing.T, before, after config.WebDavFolderNaming, names ...string) *Manager {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	dbDir := t.TempDir()
	cfg := config.Get()

	cfg.FolderNaming = before
	first, err := storage.NewStorage(dbDir)
	if err != nil {
		t.Fatalf("NewStorage(%s): %v", before, err)
	}
	for _, name := range names {
		if err := first.AddOrUpdate(aliasTestEntry(name)); err != nil {
			t.Fatalf("AddOrUpdate(%s): %v", name, err)
		}
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close(%s): %v", before, err)
	}

	cfg.FolderNaming = after
	second, err := storage.NewStorage(dbDir)
	if err != nil {
		t.Fatalf("NewStorage(%s): %v", after, err)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Errorf("Close(%s): %v", after, err)
		}
	})

	m := &Manager{
		storage: second,
		config:  cfg,
		logger:  zerolog.Nop(),
		ctx:     context.Background(),
	}
	m.initEntryCache()
	return m
}

func requireNavigable(t *testing.T, m *Manager, live, legacy string) {
	t.Helper()
	items := m.storage.GetEntryItems()
	if _, ok := items[live]; !ok {
		t.Fatalf("precondition: %q must be the live entryItems key; keys = %v", live, sortedKeys(items))
	}
	if _, ok := items[legacy]; ok {
		t.Fatalf("precondition: %q must NOT be a live key, or the alias is never exercised", legacy)
	}
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func childFingerprints(infos []FileInfo) []string {
	out := make([]string, 0, len(infos))
	for _, info := range infos {
		out = append(out, info.Name()+"|"+info.InfoHash()+"|"+strconv.FormatInt(info.Size(), 10))
	}
	sort.Strings(out)
	return out
}

func describe(info *FileInfo) string {
	if info == nil {
		return "<nil>"
	}
	return info.Name()
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestLegacyFolderNameEnumeratesSameChildrenAsLiveName is the production
// symptom, stated as an assertion: the name an *arr symlink points at must list
// exactly what the live name lists.
func TestLegacyFolderNameEnumeratesSameChildrenAsLiveName(t *testing.T) {
	const (
		legacy = "Movie.2024.SHD13.mkv"
		live   = "Movie.2024.SHD13"
	)
	m := switchFolderNaming(t, config.WebDavUseFileName, config.WebDavUseFileNameNoExt, legacy)
	requireNavigable(t, m, live, legacy)

	liveCurrent, liveChildren := m.getTorrentChildren(live)
	if liveCurrent == nil || len(liveChildren) == 0 {
		t.Fatal("the live name must enumerate; the shipped behaviour has regressed")
	}

	legacyCurrent, legacyChildren := m.getTorrentChildren(legacy)
	if legacyCurrent == nil {
		t.Fatalf("the legacy name %q resolved to nothing; every *arr symlink written under the old setting still dangles", legacy)
	}
	if !equalStrings(childFingerprints(liveChildren), childFingerprints(legacyChildren)) {
		t.Fatalf("the two names enumerate different children:\n live   = %v\n legacy = %v",
			childFingerprints(liveChildren), childFingerprints(legacyChildren))
	}
	if legacyCurrent.Name() != legacy {
		t.Fatalf("the legacy directory advertises itself as %q; the href the client asked for was %q", legacyCurrent.Name(), legacy)
	}
	if legacyCurrent.Size() != liveCurrent.Size() {
		t.Fatalf("aliased directory size %d != live %d", legacyCurrent.Size(), liveCurrent.Size())
	}
	// Children must keep pointing at the row that actually exists, or every
	// mutation path (RemoveTorrentFile, COPY/MOVE) would address a missing key.
	for _, child := range legacyChildren {
		if child.Parent() != live {
			t.Fatalf("child %q of the legacy name has parent %q, want the live key %q", child.Name(), child.Parent(), live)
		}
	}

	// The cached path (EntryCache) must agree with the direct one for both names.
	if cached, children := m.GetTorrentChildren(legacy); cached == nil || !equalStrings(childFingerprints(children), childFingerprints(liveChildren)) {
		t.Fatal("the cached entry path disagrees with the direct one for the legacy name")
	}
}

// TestFileGetAndEnumerationAgreeForBothNames pins the invariant the whole change
// exists to hold: a name either enumerates AND serves, or does neither. A
// directory that lists children it cannot open is what broke the library the
// first time.
func TestFileGetAndEnumerationAgreeForBothNames(t *testing.T) {
	const (
		legacy = "Show.S01E01.1080p.WEB-DL.mkv"
		live   = "Show.S01E01.1080p.WEB-DL"
	)
	m := switchFolderNaming(t, config.WebDavUseFileName, config.WebDavUseFileNameNoExt, legacy)
	requireNavigable(t, m, live, legacy)

	for _, name := range []string{live, legacy} {
		current, children := m.getTorrentChildren(name)
		if current == nil {
			t.Fatalf("%q does not enumerate", name)
		}
		if _, err := m.GetEntryInfo(name); err != nil {
			t.Fatalf("%q enumerates but has no directory metadata: %v", name, err)
		}
		for _, child := range children {
			file, err := m.GetTorrentFile(name, child.Name())
			if err != nil {
				t.Fatalf("%q lists %q but cannot serve it: %v", name, child.Name(), err)
			}
			if file.Size() != child.Size() || file.InfoHash() != child.InfoHash() {
				t.Fatalf("%q/%q served metadata disagrees with the listing", name, child.Name())
			}
			if file.Parent() != live {
				t.Fatalf("%q/%q resolved to parent %q, want the live key %q", name, child.Name(), file.Parent(), live)
			}
		}
	}
}

// TestLegacyStrippedNameResolvesToExtensionBearingKey covers the OTHER
// direction: `filename_no_ext` -> `filename`. The extension cannot be
// reconstructed from the requested name, so this is the direction that needs the
// reverse index rather than a string derivation.
func TestLegacyStrippedNameResolvesToExtensionBearingKey(t *testing.T) {
	const (
		legacy = "Keeps.Extension"
		live   = "Keeps.Extension.mkv"
	)
	m := switchFolderNaming(t, config.WebDavUseFileNameNoExt, config.WebDavUseFileName, live)
	requireNavigable(t, m, live, legacy)

	_, liveChildren := m.getTorrentChildren(live)
	legacyCurrent, legacyChildren := m.getTorrentChildren(legacy)
	if legacyCurrent == nil {
		t.Fatalf("the legacy name %q resolved to nothing", legacy)
	}
	if !equalStrings(childFingerprints(liveChildren), childFingerprints(legacyChildren)) {
		t.Fatal("the two names enumerate different children in the strip -> keep direction")
	}
	if _, err := m.GetTorrentFile(legacy, "video.mkv"); err != nil {
		t.Fatalf("the legacy name enumerates but cannot serve: %v", err)
	}
}

// TestAmbiguousStemRefusesToAlias keeps the resolution exact. Two live keys can
// strip to the same stem (`X.mkv` and `X.mp4`); choosing one would serve some
// other entry's children under this name, so it must resolve to nothing.
func TestAmbiguousStemRefusesToAlias(t *testing.T) {
	m := switchFolderNaming(t, config.WebDavUseFileNameNoExt, config.WebDavUseFileName,
		"Ambiguous.mkv", "Ambiguous.mp4")

	items := m.storage.GetEntryItems()
	for _, live := range []string{"Ambiguous.mkv", "Ambiguous.mp4"} {
		if _, ok := items[live]; !ok {
			t.Fatalf("precondition: %q must be a live key; keys = %v", live, sortedKeys(items))
		}
	}
	if current, children := m.getTorrentChildren("Ambiguous"); current != nil || children != nil {
		t.Fatalf("an ambiguous stem resolved to %s; the alias must never guess between candidates", describe(current))
	}
	if _, err := m.GetEntryInfo("Ambiguous"); err == nil {
		t.Fatal("an ambiguous stem produced directory metadata")
	}
}

// TestUnknownNameResolvesToNothing is the no-false-positives guard. A name that
// belongs to no entry under any derivation must behave exactly as it does today.
func TestUnknownNameResolvesToNothing(t *testing.T) {
	m := switchFolderNaming(t, config.WebDavUseFileName, config.WebDavUseFileNameNoExt, "Movie.2024.SHD13.mkv")

	// "Movie.2024" is the trap: it is a PREFIX of a real entry, and ".2024" is
	// not a media extension, so nothing may strip or extend it into a match.
	for _, name := range []string{"", "NoSuchEntry", "NoSuchEntry.mkv", "Movie.2024", "Movie"} {
		if current, children := m.getTorrentChildren(name); current != nil || children != nil {
			t.Fatalf("%q resolved to a phantom entry %s", name, describe(current))
		}
		if _, err := m.GetEntryInfo(name); err == nil {
			t.Fatalf("%q produced directory metadata for an entry that does not exist", name)
		}
		if _, err := m.GetTorrentFile(name, "video.mkv"); err == nil {
			t.Fatalf("%q served a file for an entry that does not exist", name)
		}
	}
}

// TestAliasIsContainerAgnostic documents the one place the rule is broader than
// "this exact name was once this entry's folder": the alias relates NAMES, not
// name histories, so `<liveKey>.<anyMediaExt>` resolves to `<liveKey>`.
//
// That is deliberate and it is the correct generalisation, because under a
// strip-extension naming several DIFFERENT source names legitimately collapse
// onto one live key — `X.mkv` and `X.mp4` both project to the folder `X` and are
// merged into a single row — so the alias must not try to tell them apart.
// It still never invents anything: the target must be a live, serving key.
func TestAliasIsContainerAgnostic(t *testing.T) {
	m := switchFolderNaming(t, config.WebDavUseFileName, config.WebDavUseFileNameNoExt, "Movie.2024.SHD13.mkv")

	current, children := m.getTorrentChildren("Movie.2024.SHD13.mp4")
	if current == nil || len(children) == 0 {
		t.Fatal("a media-extension spelling of a live key must resolve onto that key")
	}
	for _, child := range children {
		if child.Parent() != "Movie.2024.SHD13" {
			t.Fatalf("resolved onto %q, want the live key", child.Parent())
		}
	}
	// A NON-media suffix is not an extension and must not resolve.
	if current, _ := m.getTorrentChildren("Movie.2024.SHD13.zzz"); current != nil {
		t.Fatalf("a non-media suffix resolved to %s", describe(current))
	}
}

// TestLiveNameWithoutLegacyCounterpartIsUnaffected pins that the ordinary case
// — an entry written under the CURRENT setting, so its frozen name and its live
// key already agree — takes the exact-match path and is untouched.
func TestLiveNameWithoutLegacyCounterpartIsUnaffected(t *testing.T) {
	m := switchFolderNaming(t, config.WebDavUseFileNameNoExt, config.WebDavUseFileNameNoExt, "Fresh.Release.mkv")
	const live = "Fresh.Release"

	if _, err := m.storage.GetEntryItem(live); err != nil {
		t.Fatalf("precondition: %q must resolve by exact key: %v", live, err)
	}
	current, children := m.getTorrentChildren(live)
	if current == nil || len(children) != 2 {
		t.Fatalf("a live name with no legacy counterpart changed behaviour: current=%s children=%d", describe(current), len(children))
	}
	if current.Name() != live {
		t.Fatalf("directory name is %q, want %q", current.Name(), live)
	}
	info, err := m.GetEntryInfo(live)
	if err != nil || info.Name() != live {
		t.Fatalf("GetEntryInfo(%q) = (%s, %v)", live, describe(info), err)
	}
}

// TestAliasIsInvisibleToTheRepairSweep is the hard safety rule as an executable
// assertion. The sweep enumerates the entryItems store directly and resolves
// through storage.GetEntryItem; both must keep seeing EXACTLY the live key set,
// so nothing about listing visibility can ever become evidence of deadness.
func TestAliasIsInvisibleToTheRepairSweep(t *testing.T) {
	const (
		legacy = "Sweep.Case.mkv"
		live   = "Sweep.Case"
	)
	m := switchFolderNaming(t, config.WebDavUseFileName, config.WebDavUseFileNameNoExt, legacy)
	requireNavigable(t, m, live, legacy)

	// The alias resolves for the serving paths...
	if current, _ := m.getTorrentChildren(legacy); current == nil {
		t.Fatal("the legacy name must resolve on the serving path")
	}
	// ...and does NOT exist for the sweep's own lookups.
	if _, err := m.storage.GetEntryItem(legacy); err == nil {
		t.Fatal("storage.GetEntryItem resolved a legacy name; the repair sweep would now see a key it does not enumerate")
	}
	if _, ok := m.storage.GetEntryItems()[legacy]; ok {
		t.Fatal("the legacy name entered the enumerated key set")
	}
	if _, err := m.storage.GetEntryItem(live); err != nil {
		t.Fatalf("the sweep's own lookup failed for a live entry: %v", err)
	}
}

// TestAllFolderStillListsBothNames pins the shipped reconciliation against the
// real post-switch state: the frozen legacy name is still advertised (removing
// it is the only way this could break a library) and the live name is restored
// alongside it. Both are now navigable, so both are usable.
func TestAllFolderStillListsBothNames(t *testing.T) {
	const (
		legacy = "Listed.Both.Ways.mkv"
		live   = "Listed.Both.Ways"
	)
	m := switchFolderNaming(t, config.WebDavUseFileName, config.WebDavUseFileNameNoExt, legacy)
	requireNavigable(t, m, live, legacy)

	_, children := m.getEntryChildren(EntryAllFolder)
	names := listingNames(children)
	for _, want := range []string{legacy, live} {
		if _, ok := names[want]; !ok {
			t.Fatalf("__all__ does not advertise %q; listing = %v", want, keysOf(names))
		}
	}
	for name := range names {
		if current, _ := m.getTorrentChildren(name); current == nil {
			t.Fatalf("__all__ advertises %q but it enumerates nothing", name)
		}
	}
}

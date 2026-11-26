package storage

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/appendstore"
	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"google.golang.org/protobuf/proto"
)

// caseTestEntry is a completed torrent row reading from provider/id.
func caseTestEntry(hash, name, provider, id string) *Entry {
	added := time.Unix(1_700_000_000, 0).UTC()
	entry := &Entry{
		Protocol:         config.ProtocolTorrent,
		InfoHash:         hash,
		Name:             name,
		OriginalFilename: name,
		Size:             100,
		Bytes:            100,
		Status:           debridTypes.TorrentStatusDownloaded,
		IsComplete:       true,
		AddedOn:          added,
		CreatedAt:        added,
		Providers:        map[string]*ProviderEntry{},
		Files: map[string]*File{
			"movie.mkv": {Name: "movie.mkv", Size: 100, InfoHash: hash, AddedOn: added},
		},
	}
	if provider != "" {
		entry.ActiveProvider = provider
		entry.Providers[provider] = &ProviderEntry{
			Provider: provider,
			ID:       id,
			Status:   debridTypes.TorrentStatusDownloaded,
			Files:    map[string]*ProviderFile{"movie.mkv": {Id: "f-" + id, Link: "https://example.invalid/" + id}},
		}
	}
	return entry
}

// openLegacyStore writes rows under their RAW keys, the way builds before
// case-canonical keys did, then opens the directory with NewStorage so startup
// migrations and the legacy-key index run exactly as on a production upgrade.
func openLegacyStore(t *testing.T, rows ...*Entry) (*Storage, string) {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	dir := t.TempDir()
	raw, err := appendstore.Open(filepath.Join(dir, "entries.db"), appendstore.Options{})
	if err != nil {
		t.Fatalf("open raw entries store: %v", err)
	}
	for _, row := range rows {
		data, err := proto.Marshal(EntryToProto(row))
		if err != nil {
			t.Fatalf("marshal legacy row %s: %v", row.InfoHash, err)
		}
		if err := raw.Put(row.InfoHash, data, nil); err != nil {
			t.Fatalf("put legacy row %s: %v", row.InfoHash, err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw entries store: %v", err)
	}
	store, err := NewStorage(dir)
	if err != nil {
		t.Fatalf("NewStorage over legacy rows: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, dir
}

// mainKeys lists every entry key in the main store, sorted by the store.
func mainKeys(t *testing.T, s *Storage) []string {
	t.Helper()
	var keys []string
	if err := s.entries.ForEachMetadata(func(key string, _ *appendstore.Metadata) error {
		if !isInternalMainKey(key) {
			keys = append(keys, key)
		}
		return nil
	}); err != nil {
		t.Fatalf("scan main keys: %v", err)
	}
	return keys
}

// THE DEFECT: an uppercase btih from a magnet, then the provider's lowercase
// hash, made two rows. Now they are one, stored lowercase.
func TestMixedCaseWritesProduceOneRow(t *testing.T) {
	store, _ := openLegacyStore(t)
	lower := strings.Repeat("ab", 20)
	upper := strings.ToUpper(lower)

	if err := store.AddOrUpdate(caseTestEntry(upper, "Movie", "ad", "ad-1")); err != nil {
		t.Fatalf("AddOrUpdate uppercase: %v", err)
	}
	current, err := store.Get(lower)
	if err != nil {
		t.Fatalf("an uppercase write is unreachable by its lowercase hash: %v", err)
	}
	current.Category = "radarr"
	if err := store.AddOrUpdate(current); err != nil {
		t.Fatalf("AddOrUpdate lowercase: %v", err)
	}
	// A brand-new lowercase arrival for the same hash must hit the existing row
	// (and be fenced as a stale create), never become a second one.
	if err := store.AddOrUpdate(caseTestEntry(lower, "Movie (other)", "rd", "rd-1")); !errors.Is(err, ErrStaleEntryGeneration) {
		t.Fatalf("a fresh lowercase create over an existing row returned %v, want ErrStaleEntryGeneration", err)
	}

	keys := mainKeys(t, store)
	if len(keys) != 1 || keys[0] != lower {
		t.Fatalf("main keys = %v, want exactly [%s]", keys, lower)
	}
	stored, err := store.Get(upper)
	if err != nil {
		t.Fatalf("Get(upper): %v", err)
	}
	if stored.InfoHash != lower || stored.Files["movie.mkv"].InfoHash != lower || stored.Category != "radarr" {
		t.Fatalf("stored row = hash %q file hash %q category %q; want lowercase identity and the update",
			stored.InfoHash, stored.Files["movie.mkv"].InfoHash, stored.Category)
	}
}

// Rows written before the fix stay reachable by ANY casing, and an update lands
// in place: no row changes key outside the operator merge.
func TestLegacyUppercaseRowStaysReadableBeforeMerge(t *testing.T) {
	lower := strings.Repeat("cd", 20)
	upper := strings.ToUpper(lower)
	store, _ := openLegacyStore(t, caseTestEntry(upper, "Legacy Movie", "ad", "ad-1"))

	for _, key := range []string{upper, lower, "C" + lower[1:]} {
		entry, err := store.Get(key)
		if err != nil {
			t.Fatalf("Get(%s) on a legacy uppercase row: %v", key, err)
		}
		if entry.InfoHash != upper {
			t.Fatalf("Get(%s) returned hash %s, want the legacy row %s", key, entry.InfoHash, upper)
		}
		if ok, _ := store.Exists(key); !ok {
			t.Fatalf("Exists(%s) = false for a legacy uppercase row", key)
		}
	}

	if _, present, err := store.MutateEntryIfPresent(lower, func(entry *Entry) (bool, error) {
		entry.Category = "sonarr"
		return true, nil
	}); err != nil || !present {
		t.Fatalf("MutateEntryIfPresent(lower) = present %v err %v", present, err)
	}
	keys := mainKeys(t, store)
	if len(keys) != 1 || keys[0] != upper {
		t.Fatalf("main keys after a lowercase update = %v, want the legacy row updated in place [%s]", keys, upper)
	}
	if entry, _ := store.Get(upper); entry == nil || entry.Category != "sonarr" {
		t.Fatalf("the in-place update was not persisted: %+v", entry)
	}
}

// A twin pair before the merge: each key reaches ITS OWN row. Scanners address
// rows by their stored key, so resolving one twin to the other would make them
// stale-fail or write one row's state into the other.
func TestExactKeyWinsForUnmergedTwins(t *testing.T) {
	lower := strings.Repeat("ef", 20)
	upper := strings.ToUpper(lower)
	store, _ := openLegacyStore(t,
		caseTestEntry(lower, "Lower Name", "rd", "rd-1"),
		caseTestEntry(upper, "Upper Name", "ad", "ad-1"),
	)
	snapshot, err := store.Get(upper)
	if err != nil || snapshot.Name != "Upper Name" {
		t.Fatalf("Get(upper) = %+v, %v; want the uppercase row", snapshot, err)
	}
	if entry, err := store.Get(lower); err != nil || entry.Name != "Lower Name" {
		t.Fatalf("Get(lower) = %+v, %v; want the lowercase row", entry, err)
	}
	if _, present, err := store.MutateEntrySnapshot(snapshot, func(entry *Entry) (bool, error) {
		entry.Category = "upper-only"
		return true, nil
	}); err != nil || !present {
		t.Fatalf("MutateEntrySnapshot(upper) = present %v err %v", present, err)
	}
	if entry, _ := store.Get(lower); entry.Category == "upper-only" {
		t.Fatal("a mutation of the uppercase twin was written into the lowercase twin")
	}
	if entry, _ := store.Get(upper); entry.Category != "upper-only" {
		t.Fatalf("the uppercase twin lost its own mutation: %+v", entry)
	}
}

// A deleted legacy row must leave no trace in the fallback index: the next
// lowercase write creates ONE lowercase row, never a resurrected uppercase key.
func TestDeletedLegacyRowIsNotResurrectedByALowercaseWrite(t *testing.T) {
	for _, how := range []string{"delete-api", "behind-the-index"} {
		t.Run(how, func(t *testing.T) {
			lower := strings.Repeat("0a", 20)
			upper := strings.ToUpper(lower)
			store, _ := openLegacyStore(t, caseTestEntry(upper, "Legacy", "ad", "ad-1"))

			if how == "delete-api" {
				if err := store.Delete(lower); err != nil {
					t.Fatalf("Delete(lower) of a legacy row: %v", err)
				}
			} else if err := store.entries.Delete(upper); err != nil {
				// The index still names the key; resolution must re-check it.
				t.Fatalf("raw delete: %v", err)
			}
			if store.entries.Exists(upper) {
				t.Fatal("the legacy row survived its delete")
			}

			if err := store.AddOrUpdate(caseTestEntry(lower, "Fresh", "rd", "rd-1")); err != nil {
				t.Fatalf("AddOrUpdate(lower) after the delete: %v", err)
			}
			keys := mainKeys(t, store)
			if len(keys) != 1 || keys[0] != lower {
				t.Fatalf("main keys = %v, want exactly [%s] with no resurrected %s", keys, lower, upper)
			}
			if store.legacyMainKeyCount() != 0 {
				t.Fatalf("legacy index still holds %d key(s) after the row was deleted", store.legacyMainKeyCount())
			}
		})
	}
}

// Both casings of one hash share ONE mutation stripe, so interleaved writers
// addressing it in different cases never lose an update.
func TestMixedCaseWritersSerialiseOnOneStripe(t *testing.T) {
	store, _ := openLegacyStore(t)
	lower := strings.Repeat("9b", 20)
	if err := store.AddOrUpdate(caseTestEntry(lower, "Movie", "ad", "ad-1")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	const writers = 40
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := range writers {
		key := lower
		if i%2 == 1 {
			key = strings.ToUpper(lower)
		}
		wg.Go(func() {
			_, _, err := store.MutateEntryIfPresent(key, func(entry *Entry) (bool, error) {
				entry.Seeders++
				return true, nil
			})
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("a mixed-case writer failed: %v", err)
		}
	}
	entry, err := store.Get(lower)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if entry.Seeders != writers {
		t.Fatalf("Seeders = %d after %d increments; mixed-case writers raced", entry.Seeders, writers)
	}
}

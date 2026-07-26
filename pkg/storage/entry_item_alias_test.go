package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// LegacyEntryItemName is what makes a folder name written under a previous
// FolderNaming setting keep working. These tests pin its contract directly: it
// follows EXACT alternate derivations only, in both directions, and it invents
// nothing.

func aliasStore(t *testing.T, liveKeys ...string) *Storage {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	now := time.Unix(1_700_000_000, 0).UTC()
	for _, key := range liveKeys {
		item := &EntryItem{
			Name: key,
			Files: map[string]*File{
				"video.mkv": {Name: "video.mkv", InfoHash: "hash-" + key, Size: 4096, AddedOn: now},
			},
		}
		if err := s.UpdateItem(item); err != nil {
			t.Fatalf("UpdateItem(%s): %v", key, err)
		}
	}
	return s
}

func TestLegacyEntryItemNameFollowsExactAlternateDerivations(t *testing.T) {
	s := aliasStore(t,
		"Movie.2024.SHD13",    // live under filename_no_ext
		"Keeps.Extension.mkv", // live under filename
	)

	for _, tc := range []struct {
		name    string
		request string
		want    string
		ok      bool
	}{
		{"keep -> strip", "Movie.2024.SHD13.mkv", "Movie.2024.SHD13", true},
		{"keep -> strip, other container", "Movie.2024.SHD13.mp4", "Movie.2024.SHD13", true},
		{"strip -> keep", "Keeps.Extension", "Keeps.Extension.mkv", true},
		{"unknown name", "NoSuchEntry", "", false},
		{"unknown name with extension", "NoSuchEntry.mkv", "", false},
		{"prefix of a real entry", "Movie.2024", "", false},
		{"non-media suffix is not an extension", "Movie.2024.SHD13.SHD14", "", false},
		{"empty", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := s.LegacyEntryItemName(tc.request)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("LegacyEntryItemName(%q) = (%q, %v), want (%q, %v)", tc.request, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// The reverse index must refuse rather than guess when several live keys strip
// to the same stem: serving one entry's children under another entry's name is
// worse than not resolving at all.
func TestLegacyEntryItemNameRefusesAmbiguousStems(t *testing.T) {
	s := aliasStore(t, "Ambiguous.mkv", "Ambiguous.mp4")

	if got, ok := s.LegacyEntryItemName("Ambiguous"); ok {
		t.Fatalf("LegacyEntryItemName(%q) = %q, want no resolution", "Ambiguous", got)
	}
	// A third container name must not be dragged into the ambiguity either: its
	// only alternate derivation is the stem, and the stem is not a live key.
	if got, ok := s.LegacyEntryItemName("Ambiguous.avi"); ok {
		t.Fatalf("LegacyEntryItemName(%q) = %q, want no resolution", "Ambiguous.avi", got)
	}
}

// The reverse index is cached; a newly created key must still be reachable. This
// pins that the invalidation fires on key-set changes.
func TestLegacyEntryItemNameSeesKeysAddedAfterFirstLookup(t *testing.T) {
	s := aliasStore(t, "Existing.mkv")

	if _, ok := s.LegacyEntryItemName("Later"); ok {
		t.Fatal("resolved a name before its key existed")
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	if err := s.UpdateItem(&EntryItem{
		Name:  "Later.mkv",
		Files: map[string]*File{"video.mkv": {Name: "video.mkv", InfoHash: "hash-later", Size: 1, AddedOn: now}},
	}); err != nil {
		t.Fatalf("UpdateItem: %v", err)
	}
	if got, ok := s.LegacyEntryItemName("Later"); !ok || got != "Later.mkv" {
		t.Fatalf("LegacyEntryItemName(%q) = (%q, %v) after the key was created", "Later", got, ok)
	}
}

// The reverse index is published under a mutex and never mutated afterwards, so
// a reader may hold one across a rebuild. This exercises that under -race:
// resolvers run concurrently with the writes that invalidate them.
func TestLegacyEntryItemNameIsSafeUnderConcurrentInvalidation(t *testing.T) {
	s := aliasStore(t, "Concurrent.Base.mkv")
	now := time.Unix(1_700_000_000, 0).UTC()

	var wg sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				s.LegacyEntryItemName("Concurrent.Base")
				s.LegacyEntryItemName("Concurrent.Base.mkv")
				s.LegacyEntryItemName("Absent.Name")
				s.EntryItemAliases().Resolve("Concurrent.Base")
			}
		}()
	}
	for writer := 0; writer < 2; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				name := fmt.Sprintf("Churn.%d.%d.mkv", writer, i)
				if err := s.UpdateItem(&EntryItem{
					Name:  name,
					Files: map[string]*File{"video.mkv": {Name: "video.mkv", InfoHash: "hash-" + name, Size: 1, AddedOn: now}},
				}); err != nil {
					t.Errorf("UpdateItem(%s): %v", name, err)
					return
				}
			}
		}(writer)
	}
	wg.Wait()

	if got, ok := s.LegacyEntryItemName("Concurrent.Base"); !ok || got != "Concurrent.Base.mkv" {
		t.Fatalf("LegacyEntryItemName(%q) = (%q, %v) after concurrent churn", "Concurrent.Base", got, ok)
	}
}

// GetEntryItem is the repair sweep's resolver. It must stay exact: a legacy name
// that the serving paths alias must NOT become a key the sweep can see.
func TestGetEntryItemStaysExact(t *testing.T) {
	s := aliasStore(t, "Movie.2024.SHD13")

	if _, err := s.GetEntryItem("Movie.2024.SHD13.mkv"); err == nil {
		t.Fatal("GetEntryItem resolved a legacy name; the alias must not be visible to the repair sweep")
	}
	if _, err := s.GetEntryItem("Movie.2024.SHD13"); err != nil {
		t.Fatalf("GetEntryItem(live) = %v", err)
	}
}

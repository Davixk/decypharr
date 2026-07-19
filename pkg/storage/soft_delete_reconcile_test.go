package storage

import (
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// TestStartupReconcileAdoptsLegacyItemOnlySoftDeletes covers state written by
// pre-projection releases, where RemoveTorrentFile recorded a file soft-delete
// only on the derived folder item and never on the authoritative main row.
// The startup reconcile must adopt that flag into the main row instead of
// resurrecting the file in WebDAV.
func TestStartupReconcileAdoptsLegacyItemOnlySoftDeletes(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	dbPath := t.TempDir()
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	entry := atomicTestEntry()
	entry.Files["extra.mkv"] = &File{
		Name:     "extra.mkv",
		Size:     5,
		InfoHash: entry.InfoHash,
		AddedOn:  entry.AddedOn,
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	folder := entry.GetFolder()

	// Simulate the legacy RemoveTorrentFile: mark Deleted on the item only.
	item, err := store.GetEntryItem(folder)
	if err != nil {
		t.Fatalf("GetEntryItem: %v", err)
	}
	item.Files["movie.mkv"].Deleted = true
	if err := store.UpdateItem(item); err != nil {
		t.Fatalf("UpdateItem legacy soft-delete: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	assertDeletedAfterReopen := func(pass string) *Storage {
		t.Helper()
		reopened, err := NewStorage(dbPath)
		if err != nil {
			t.Fatalf("%s reopen: %v", pass, err)
		}
		item, err := reopened.GetEntryItem(folder)
		if err != nil {
			t.Fatalf("%s GetEntryItem: %v", pass, err)
		}
		if _, err := item.GetFile("movie.mkv"); err == nil {
			t.Fatalf("%s: soft-deleted file was resurrected by startup reconcile", pass)
		}
		if _, err := item.GetFile("extra.mkv"); err != nil {
			t.Fatalf("%s: healthy sibling file disappeared: %v", pass, err)
		}
		if got, want := item.GetSize(), int64(5); got != want {
			t.Fatalf("%s: folder size = %d, want %d (deleted file excluded)", pass, got, want)
		}
		return reopened
	}

	reopened := assertDeletedAfterReopen("first")
	main, err := reopened.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("Get main after reconcile: %v", err)
	}
	if file := main.Files["movie.mkv"]; file == nil || !file.Deleted {
		t.Fatalf("main row did not adopt the legacy soft-delete: %+v", file)
	}
	if file := main.Files["extra.mkv"]; file == nil || file.Deleted {
		t.Fatalf("healthy main-row file was wrongly marked deleted: %+v", file)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close after first reopen: %v", err)
	}

	// The flag is durable across further restarts once adopted.
	second := assertDeletedAfterReopen("second")
	if err := second.Close(); err != nil {
		t.Fatalf("Close after second reopen: %v", err)
	}
}

// TestStartupReconcileKeepsMainRowSoftDeletes covers the current model, where
// RemoveTorrentFile records Deleted on the authoritative main row: a restart
// must not resurrect the file either.
func TestStartupReconcileKeepsMainRowSoftDeletes(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	dbPath := t.TempDir()
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	entry := atomicTestEntry()
	entry.Files["extra.mkv"] = &File{
		Name:     "extra.mkv",
		Size:     5,
		InfoHash: entry.InfoHash,
		AddedOn:  entry.AddedOn,
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	if _, present, err := store.MutateEntryIfPresent(entry.InfoHash, func(current *Entry) (bool, error) {
		current.Files["movie.mkv"].Deleted = true
		return true, nil
	}); err != nil || !present {
		t.Fatalf("soft-delete main row file: present=%v err=%v", present, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewStorage(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	item, err := reopened.GetEntryItem(entry.GetFolder())
	if err != nil {
		t.Fatalf("GetEntryItem: %v", err)
	}
	if _, err := item.GetFile("movie.mkv"); err == nil {
		t.Fatal("main-row soft-delete was resurrected by startup reconcile")
	}
	if _, err := item.GetFile("extra.mkv"); err != nil {
		t.Fatalf("healthy sibling file disappeared: %v", err)
	}
}

// TestStartupReconcileDoesNotAdoptDeletionForReAddedFile ensures the adoption
// is scoped to the exact same durable file instance: a genuinely re-added file
// (different AddedOn) must reappear even when a stale item still calls the old
// instance deleted.
func TestStartupReconcileDoesNotAdoptDeletionForReAddedFile(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	dbPath := t.TempDir()
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	entry := atomicTestEntry()
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	folder := entry.GetFolder()

	// Stale legacy item: the OLD instance of movie.mkv was soft-deleted.
	item, err := store.GetEntryItem(folder)
	if err != nil {
		t.Fatalf("GetEntryItem: %v", err)
	}
	item.Files["movie.mkv"].Deleted = true
	item.Files["movie.mkv"].AddedOn = entry.AddedOn.Add(-time.Hour)
	if err := store.UpdateItem(item); err != nil {
		t.Fatalf("UpdateItem stale soft-delete: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewStorage(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedItem, err := reopened.GetEntryItem(folder)
	if err != nil {
		t.Fatalf("GetEntryItem after reopen: %v", err)
	}
	if _, err := reopenedItem.GetFile("movie.mkv"); err != nil {
		t.Fatalf("re-added file instance was wrongly kept deleted: %v", err)
	}
	main, err := reopened.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("Get main: %v", err)
	}
	if file := main.Files["movie.mkv"]; file == nil || file.Deleted {
		t.Fatalf("main row wrongly adopted a stale deletion: %+v", file)
	}
}

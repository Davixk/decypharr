package manager

import (
	"os"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// 🔴 THE QBIT SHIM PASSES A NIL CLEANUP, AND THAT IS THE WHOLE INVARIANT.
//
// It once passed a placement-releasing cleanup when the caller sent
// deleteFiles=true. An *arr's routine POST-IMPORT cleanup sends exactly that —
// and by then the release is already imported as a symlink pointing INTO the
// mount, so the "downloaded data" the caller means and the bytes the library
// depends on are the same bytes. Releasing the provider copy deleted the file
// the library had just been built from: 2,592 releases in 24h, MissingFromDisk
// reaps going 56/day to 8,302/day, every one re-searched and re-grabbed.
//
// These two tests live here rather than in pkg/server/qbit because that package
// has no configured provider in its fixture, so "the provider was not called" is
// not observable there — a green run proves nothing. Here a fake client records
// every DeleteTorrent, so the assertion has teeth.
func seedPlacedEntry(t *testing.T, m *Manager, hash string) {
	t.Helper()
	entry := &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       hash,
		Name:           hash + ".mkv",
		AddedOn:        time.Unix(1_700_000_000, 0).UTC(),
		ActiveProvider: "prov",
		Providers: map[string]*storage.ProviderEntry{
			"prov": {Provider: "prov", ID: "placement-" + hash},
		},
		Files: map[string]*storage.File{},
	}
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("Add %s: %v", hash, err)
	}
}

func newPlacementTestManager(t *testing.T) (*Manager, *fakeDebridClient) {
	t.Helper()
	client := &fakeDebridClient{
		cfg:      config.Debrid{Name: "prov", Provider: "realdebrid"},
		recorder: &fallbackCallRecorder{},
	}
	return newSyncRefusalManager(t, client), client
}

// The invariant: a nil cleanup must leave the provider copy completely alone,
// however the caller spelled delete_files.
func TestQueueDeleteWithNilCleanupNeverTouchesTheProvider(t *testing.T) {
	m, client := newPlacementTestManager(t)
	seedPlacedEntry(t, m, "keep-the-bytes")

	if err := m.queue.Delete("keep-the-bytes", nil); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := m.queue.GetTorrent("keep-the-bytes"); err == nil {
		t.Fatal("the queue row survived — the caller is still entitled to have its queue item removed")
	}
	if deleted := client.deleted(); len(deleted) != 0 {
		t.Fatalf("a nil-cleanup delete released provider copies %v. The library imports FROM those bytes; "+
			"releasing them here is what produced the MissingFromDisk sawtooth.", deleted)
	}
}

// THE CONTROL, and it is not optional. Without it the test above could pass
// simply because this fixture never reaches a provider at all — the same
// vacuous-coverage trap that let a compaction defect ship. This proves the
// fixture CAN observe a provider delete, so the zero above means something.
func TestPlacementCleanupDoesReachTheProvider(t *testing.T) {
	m, client := newPlacementTestManager(t)
	seedPlacedEntry(t, m, "drop-the-bytes")

	if err := m.queue.Delete("drop-the-bytes", m.RemoveTorrentPlacements); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	deleted := client.deleted()
	if len(deleted) != 1 || deleted[0] != "placement-drop-the-bytes" {
		t.Fatalf("placement cleanup did not release the provider copy, got %v. This test exists to prove the "+
			"fixture can see a provider delete at all — if it cannot, the nil-cleanup test proves nothing.", deleted)
	}
}

// WHAT delete_files ACTUALLY SELECTS: the local download folder.
//
// qBittorrent's contract is "forget the torrent, and optionally delete the
// downloaded data". decypharr's equivalent of that data is the symlink folder in
// downloads — so true removes it and false keeps it. These two tests are a pair;
// each is the other's control, because either one alone would pass against a
// Delete that ignored the flag in the convenient direction.
func seedWithDownloadFolder(t *testing.T, m *Manager, hash string) string {
	t.Helper()
	root := t.TempDir()
	entry := &storage.Entry{
		Protocol: config.ProtocolTorrent,
		InfoHash: hash,
		Name:     hash,
		SavePath: root,
		AddedOn:  time.Unix(1_700_000_000, 0).UTC(),
		Files:    map[string]*storage.File{},
	}
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("Add %s: %v", hash, err)
	}
	dir := entry.DownloadPath()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll %s: %v", dir, err)
	}
	return dir
}

func TestDeleteRemovesTheDownloadFolder(t *testing.T) {
	m, _ := newPlacementTestManager(t)
	dir := seedWithDownloadFolder(t, m, "drop-the-links")

	if err := m.queue.Delete("drop-the-links", nil); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("delete_files=true left the download folder %s behind (stat err=%v)", dir, err)
	}
}

func TestDeleteKeepingLocalFilesLeavesTheDownloadFolder(t *testing.T) {
	m, _ := newPlacementTestManager(t)
	dir := seedWithDownloadFolder(t, m, "keep-the-links")

	if err := m.queue.DeleteKeepingLocalFiles("keep-the-links", nil); err != nil {
		t.Fatalf("DeleteKeepingLocalFiles: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("delete_files=false removed the download folder %s — the caller asked to KEEP its data "+
			"and may still be importing from it (stat err=%v)", dir, err)
	}
	if _, err := m.queue.GetTorrent("keep-the-links"); err == nil {
		t.Fatal("the queue row survived — keeping the files must not keep the row")
	}
}

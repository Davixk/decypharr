package manager

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func addCompletedEntry(t *testing.T, m *Manager, infohash, savePath, name string) *storage.Entry {
	t.Helper()
	now := time.Unix(1_700_000_000, 0).UTC()
	entry := &storage.Entry{
		Protocol:   config.ProtocolTorrent,
		InfoHash:   infohash,
		Name:       name,
		SavePath:   savePath,
		State:      storage.EntryStatePausedUP,
		Status:     debridTypes.TorrentStatusDownloaded,
		IsComplete: true,
		Progress:   1.0,
		Action:     config.DownloadActionNone,
		AddedOn:    now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("queue Add(%s): %v", infohash, err)
	}
	queued, err := m.queue.GetTorrent(infohash)
	if err != nil {
		t.Fatalf("GetTorrent(%s): %v", infohash, err)
	}
	return queued
}

// TestReconcileMissingDownloadsSelectivity pins fix 3's selection and reset:
//   - a completed (pausedUP) entry whose DownloadPath disappeared, with a valid
//     Name, is reset to the claimed shape and its action is resumed;
//   - a completed entry whose folder is still present is left untouched;
//   - a completed entry with an empty (collapsing) Name is skipped — it cannot
//     be re-symlinked and its DownloadPath would be the category dir itself.
//
// A second sweep is a no-op (the recovered row is no longer pausedUP).
func TestReconcileMissingDownloadsSelectivity(t *testing.T) {
	m := newActionLifecycleFixture(t, 2)

	var ranMu sync.Mutex
	ran := map[string]bool{}
	m.claimedActionTestHook = func(entry *storage.Entry) {
		ranMu.Lock()
		ran[entry.InfoHash] = true
		ranMu.Unlock()
	}
	didRun := func(hash string) bool {
		ranMu.Lock()
		defer ranMu.Unlock()
		return ran[hash]
	}

	downloads := t.TempDir()
	radarr := filepath.Join(downloads, "radarr")
	if err := os.MkdirAll(radarr, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	// A: pausedUP, valid name, folder MISSING -> must be recovered.
	missing := addCompletedEntry(t, m, "missing-folder", radarr, "MovieMissing")
	if _, err := os.Stat(missing.DownloadPath()); !os.IsNotExist(err) {
		t.Fatalf("precondition: missing entry's folder should not exist (err=%v)", err)
	}

	// B: pausedUP, valid name, folder PRESENT -> must be untouched.
	present := addCompletedEntry(t, m, "present-folder", radarr, "MoviePresent")
	if err := os.MkdirAll(present.DownloadPath(), 0o755); err != nil {
		t.Fatalf("MkdirAll present: %v", err)
	}

	// C: pausedUP, EMPTY name -> DownloadPath is the category dir; must be skipped.
	empty := addCompletedEntry(t, m, "empty-name", radarr, "")

	recovered := m.reconcileMissingDownloads(context.Background(), missingDownloadSweepLimit, true)
	if recovered != 1 {
		t.Fatalf("reconcileMissingDownloads recovered %d, want 1", recovered)
	}

	// The recovered entry's claimed action must run.
	deadline := time.Now().Add(3 * time.Second)
	for !didRun(missing.InfoHash) {
		if time.Now().After(deadline) {
			t.Fatal("recovered entry's claimed action never ran")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if didRun(present.InfoHash) {
		t.Fatal("present-folder entry was wrongly recovered")
	}
	if didRun(empty.InfoHash) {
		t.Fatal("empty-name entry was wrongly recovered")
	}

	// The recovered row left pausedUP for the claimed shape.
	recoveredRow, err := m.queue.GetTorrent(missing.InfoHash)
	if err != nil {
		t.Fatalf("GetTorrent(missing): %v", err)
	}
	if recoveredRow.State != storage.EntryStateDownloading || !recoveredRow.IsDownloading ||
		recoveredRow.Status != debridTypes.TorrentStatusDownloaded {
		t.Fatalf("recovered row shape = {state:%q downloading:%v status:%q}, want downloading/true/downloaded",
			recoveredRow.State, recoveredRow.IsDownloading, recoveredRow.Status)
	}

	// Untouched rows stay pausedUP.
	for _, hash := range []string{present.InfoHash, empty.InfoHash} {
		row, err := m.queue.GetTorrent(hash)
		if err != nil {
			t.Fatalf("GetTorrent(%s): %v", hash, err)
		}
		if row.State != storage.EntryStatePausedUP {
			t.Fatalf("entry %s state = %q, want pausedUP (untouched)", hash, row.State)
		}
	}

	// No thrash: a second sweep re-selects nothing (recovered row is no longer
	// pausedUP; present has its folder; empty has no usable name).
	if again := m.reconcileMissingDownloads(context.Background(), missingDownloadSweepLimit, true); again != 0 {
		t.Fatalf("second sweep recovered %d, want 0 (idempotent)", again)
	}
}

// TestReconcileMissingDownloadsHonorsLimit pins the per-sweep rate limit.
func TestReconcileMissingDownloadsHonorsLimit(t *testing.T) {
	m := newActionLifecycleFixture(t, 4)
	m.claimedActionTestHook = func(*storage.Entry) {} // swallow resumes

	downloads := t.TempDir()
	radarr := filepath.Join(downloads, "radarr")
	if err := os.MkdirAll(radarr, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	for i := 0; i < 5; i++ {
		addCompletedEntry(t, m, "missing-"+string(rune('a'+i)), radarr, "Movie"+string(rune('A'+i)))
	}

	if got := m.reconcileMissingDownloads(context.Background(), 2, false); got != 2 {
		t.Fatalf("reconcileMissingDownloads with limit 2 recovered %d, want 2", got)
	}
}

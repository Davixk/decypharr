package manager

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// The exact LastError of the 1,891-entry 2026-07-19 cohort.
const doomedArchiveError = "failed to process nzb: failed to process NZB archives: no valid files found in NZB"

// addFailedMeta writes a durably failed NZB meta through the real guarded
// write path. Files=nil reproduces the incident's on-disk shape: the
// quick-parse persist emptied the file table, then markAsFailed flipped
// Status/FailMessage in place.
func addFailedMeta(t *testing.T, m *Manager, id string, files []storage.NZBFile) {
	t.Helper()
	if err := m.usenet.NZBStorage().AddNZB(&storage.NZB{
		ID:          id,
		Name:        id + ".nzb",
		Status:      usenet.NZBStatusFailed,
		FailMessage: "no valid files found in NZB",
		Files:       files,
	}); err != nil {
		t.Fatalf("AddNZB(%s): %v", id, err)
	}
}

func segmentedFiles(id string) []storage.NZBFile {
	return []storage.NZBFile{{
		Name: "movie.mkv",
		Size: 4096,
		Segments: []storage.NZBSegment{{
			Number:    1,
			MessageID: id + "-seg",
			Bytes:     4096,
		}},
	}}
}

// warnLines filters a zerolog JSON capture down to warn-level lines.
func warnLines(logged string) string {
	var out []string
	for _, line := range strings.Split(logged, "\n") {
		if strings.Contains(line, `"level":"warn"`) {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}

func assertStillErrorWithCount(t *testing.T, m *Manager, hash string, wantCount int) {
	t.Helper()
	persisted, err := m.storage.GetQueued(hash)
	if err != nil {
		t.Fatalf("GetQueued(%s): %v", hash, err)
	}
	if persisted.State != storage.EntryStateError {
		t.Fatalf("%s: state = %q, want error (must not be revived)", hash, persisted.State)
	}
	if persisted.ErrorCount != wantCount {
		t.Fatalf("%s: ErrorCount = %d, want %d unchanged", hash, persisted.ErrorCount, wantCount)
	}
	if persisted.LastError != doomedArchiveError {
		t.Fatalf("%s: LastError rewritten to %q", hash, persisted.LastError)
	}
}

// Doomed entries — failed meta with empty or segmentless Files and no
// surviving NZB source — must be skipped by the revival sweep with a single
// WARN, leaving State/ErrorCount/LastError untouched. Recoverable shapes
// (completed meta, populated segment map, or any surviving source) remain
// eligible exactly as before.
func TestRevivalSweepSkipsDoomedRebuilds(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	m, _ := newVerdictTestManager(t, host, port)

	var warnBuf bytes.Buffer
	m.logger = zerolog.New(&warnBuf)

	// Doomed: failed meta, empty Files, no source anywhere.
	addErrorNZBEntry(t, m, "doomed-empty", doomedArchiveError, 1, false)
	addFailedMeta(t, m, "doomed-empty", nil)

	// Doomed: failed meta whose file table survived but whose segments did not.
	addErrorNZBEntry(t, m, "doomed-segmentless", doomedArchiveError, 2, false)
	noSegs := segmentedFiles("doomed-segmentless")
	noSegs[0].Segments = nil
	addFailedMeta(t, m, "doomed-segmentless", noSegs)

	// Recoverable: same empty failed meta, but a staged .source artifact
	// survives on disk, so a rebuild can re-parse it.
	addErrorNZBEntry(t, m, "has-artifact", doomedArchiveError, 1, false)
	addFailedMeta(t, m, "has-artifact", nil)
	nzbDir := filepath.Join(config.GetMainPath(), "usenet", "nzbs")
	if err := os.MkdirAll(nzbDir, 0o755); err != nil {
		t.Fatalf("mkdir nzbs dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nzbDir, "has-artifact.deadbeef.source"), []byte(verdictTestNZB), 0o644); err != nil {
		t.Fatalf("write source artifact: %v", err)
	}

	// Recoverable: empty failed meta but the entry still points at a readable
	// staged Magnet path (rebuild's primary source).
	magnetEntry := addErrorNZBEntry(t, m, "has-magnet", doomedArchiveError, 1, false)
	stagedPath := filepath.Join(t.TempDir(), "has-magnet.nzb.queued")
	if err := os.WriteFile(stagedPath, []byte(verdictTestNZB), 0o644); err != nil {
		t.Fatalf("write staged magnet: %v", err)
	}
	if _, err := m.queue.Mutate(magnetEntry.InfoHash, func(cur *storage.Entry) bool {
		cur.Magnet = stagedPath
		return true
	}); err != nil {
		t.Fatalf("set magnet: %v", err)
	}
	addFailedMeta(t, m, "has-magnet", nil)

	// Recoverable: completed meta resumes without any source.
	addErrorNZBEntry(t, m, "has-completed-meta", doomedArchiveError, 1, false)
	if err := m.usenet.NZBStorage().AddNZB(&storage.NZB{
		ID:     "has-completed-meta",
		Name:   "completed.nzb",
		Status: usenet.NZBStatusCompleted,
		Files:  segmentedFiles("has-completed-meta"),
	}); err != nil {
		t.Fatalf("AddNZB completed: %v", err)
	}

	// Recoverable: failed meta that still carries its full segment map (the
	// offline tool's class A2 un-flip shape) stays eligible — only the
	// empty/segmentless shape is provably unrebuildable.
	addErrorNZBEntry(t, m, "has-segments", doomedArchiveError, 1, false)
	addFailedMeta(t, m, "has-segments", segmentedFiles("has-segments"))

	// Recoverable: mount-timeout failures resume the post-download action, not
	// a rebuild — the doomed check must never apply to them.
	addErrorNZBEntry(t, m, "mount-timeout", "timeout waiting for mount files: 1 files still pending (a.mkv)", 1, false)
	addFailedMeta(t, m, "mount-timeout", nil)

	revived := m.reviveErrorEntries(context.Background(), 0, false)
	if revived != 5 {
		t.Fatalf("sweep revived %d entries, want 5 (all but the two doomed)", revived)
	}

	assertStillErrorWithCount(t, m, "doomed-empty", 1)
	assertStillErrorWithCount(t, m, "doomed-segmentless", 2)

	for _, hash := range []string{"has-artifact", "has-magnet", "has-completed-meta", "has-segments"} {
		persisted, err := m.storage.GetQueued(hash)
		if err != nil {
			t.Fatalf("GetQueued(%s): %v", hash, err)
		}
		if persisted.State != storage.EntryStateDownloading || persisted.Status != debridTypes.TorrentStatusQueued {
			t.Fatalf("%s: state/status = %q/%q, want downloading/queued (must stay eligible)", hash, persisted.State, persisted.Status)
		}
	}
	mount, err := m.storage.GetQueued("mount-timeout")
	if err != nil {
		t.Fatalf("GetQueued(mount-timeout): %v", err)
	}
	if mount.State != storage.EntryStateDownloading || mount.Status != debridTypes.TorrentStatusDownloaded || !mount.IsDownloading {
		t.Fatalf("mount-timeout entry = %q/%q/%v, want the claimed mid-action triple", mount.State, mount.Status, mount.IsDownloading)
	}

	// One WARN per doomed entry, naming the recourse. The captured buffer also
	// holds INFO revival lines, so assertions look at warn-level lines only.
	warns := warnLines(warnBuf.String())
	for _, hash := range []string{"doomed-empty", "doomed-segmentless"} {
		if got := strings.Count(warns, `"infohash":"`+hash+`"`); got != 1 {
			t.Fatalf("WARN lines naming %s = %d, want exactly 1:\n%s", hash, got, warns)
		}
	}
	if !strings.Contains(warns, "unrecoverable offline; requires re-grab") {
		t.Fatalf("WARN must name the recourse:\n%s", warns)
	}
	for _, hash := range []string{"has-artifact", "has-magnet", "has-completed-meta", "has-segments", "mount-timeout"} {
		if strings.Contains(warns, hash) {
			t.Fatalf("recoverable entry %s must not be warned about:\n%s", hash, warns)
		}
	}

	// A second sweep in the same boot re-skips silently (WARN deduped) and
	// still leaves the doomed rows untouched.
	warnBuf.Reset()
	if again := m.reviveErrorEntries(context.Background(), 0, false); again != 0 {
		t.Fatalf("second sweep revived %d entries, want 0", again)
	}
	if again := warnLines(warnBuf.String()); again != "" {
		t.Fatalf("second sweep re-warned:\n%s", again)
	}
	assertStillErrorWithCount(t, m, "doomed-empty", 1)
	assertStillErrorWithCount(t, m, "doomed-segmentless", 2)
}

// The revival reset itself must never touch ErrorCount: the count grows only
// when a subsequent attempt fails (Entry.MarkAsError). This pins the trace
// answer for the sweep: revive → rebuild-fails → MarkAsError is the only
// increment path.
func TestReviveErrorEntryResetDoesNotIncrementErrorCount(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	m, _ := newVerdictTestManager(t, host, port)

	addErrorNZBEntry(t, m, "count-stable", doomedArchiveError, 2, false)
	addFailedMeta(t, m, "count-stable", segmentedFiles("count-stable"))

	updated, _, applied, err := m.reviveErrorEntry("count-stable", false)
	if err != nil || !applied {
		t.Fatalf("reviveErrorEntry: applied=%v err=%v", applied, err)
	}
	if updated.ErrorCount != 2 {
		t.Fatalf("revival reset changed ErrorCount to %d, want 2", updated.ErrorCount)
	}
	persisted, err := m.storage.GetQueued("count-stable")
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	if persisted.ErrorCount != 2 || persisted.LastError != doomedArchiveError {
		t.Fatalf("persisted audit trail changed: count=%d lastError=%q", persisted.ErrorCount, persisted.LastError)
	}
}

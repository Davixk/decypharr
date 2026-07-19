package manager

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func addErrorNZBEntry(t *testing.T, m *Manager, infohash, lastError string, errorCount int, bad bool) *storage.Entry {
	t.Helper()
	now := time.Now()
	entry := &storage.Entry{
		InfoHash:         infohash,
		Name:             "failed-" + infohash,
		OriginalFilename: "failed-" + infohash + ".nzb",
		Protocol:         config.ProtocolNZB,
		Status:           debridTypes.TorrentStatusDownloading,
		State:            storage.EntryStateError,
		LastError:        lastError,
		ErrorCount:       errorCount,
		Bad:              bad,
		CreatedAt:        now,
		UpdatedAt:        now,
		AddedOn:          now,
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("queue.Add(%s): %v", infohash, err)
	}
	return entry
}

func TestIsRevivableErrorEntryEligibility(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	m, _ := newVerdictTestManager(t, host, port)
	if got := m.retryLimit(); got != 3 {
		t.Fatalf("retryLimit = %d, want default 3", got)
	}

	base := func() *storage.Entry {
		return &storage.Entry{
			Protocol:   config.ProtocolNZB,
			State:      storage.EntryStateError,
			LastError:  "articles missing on provider: failed to stat segment",
			ErrorCount: 1,
		}
	}
	cases := []struct {
		name   string
		mutate func(*storage.Entry)
		force  bool
		want   bool
	}{
		{"availability signature eligible", func(*storage.Entry) {}, false, true},
		{"probe infrastructure signature eligible", func(e *storage.Entry) {
			e.LastError = "usenet parse failed: availability probe failed: provider connectivity problem: failed to stat segment"
		}, false, true},
		{"mount timeout signature eligible", func(e *storage.Entry) {
			e.LastError = "timeout waiting for mount files: 2 files still pending"
		}, false, true},
		{"archive processing signature eligible", func(e *storage.Entry) {
			// The exact string stamped on 1,891 entries during the 2026-07-19
			// incident (Process phase, swallowed substrate collapse).
			e.LastError = "failed to process nzb: failed to process NZB archives: no valid files found in NZB"
		}, false, true},
		{"gated archive infra signature eligible", func(e *storage.Entry) {
			e.LastError = "failed to process nzb: failed to process NZB archives: availability probe failed: provider connectivity problem: no valid files found in NZB after 3 file group failure(s)"
		}, false, true},
		{"error count at retries cap", func(e *storage.Entry) { e.ErrorCount = 3 }, false, false},
		{"error count above retries cap", func(e *storage.Entry) { e.ErrorCount = 7 }, false, false},
		{"bad entry", func(e *storage.Entry) { e.Bad = true }, false, false},
		{"bad entry even when forced", func(e *storage.Entry) { e.Bad = true }, true, false},
		{"non-matching last error", func(e *storage.Entry) { e.LastError = "usenet processing timed out after 10m" }, false, false},
		{"non-matching last error forced", func(e *storage.Entry) { e.LastError = "usenet processing timed out after 10m" }, true, true},
		{"exhausted count forced", func(e *storage.Entry) { e.ErrorCount = 9 }, true, true},
		{"torrent protocol", func(e *storage.Entry) { e.Protocol = config.ProtocolTorrent }, false, false},
		{"not in error state", func(e *storage.Entry) { e.State = storage.EntryStateDownloading }, false, false},
		{"not in error state forced", func(e *storage.Entry) { e.State = storage.EntryStatePausedUP }, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entry := base()
			tc.mutate(entry)
			if got := m.isRevivableErrorEntry(entry, tc.force); got != tc.want {
				t.Fatalf("isRevivableErrorEntry(force=%v) = %v, want %v", tc.force, got, tc.want)
			}
		})
	}
}

// Availability-class failures re-enter the queued path; error bookkeeping is
// untouched so config retries keeps capping revival attempts.
func TestReviveErrorEntryAvailabilityFailureBecomesQueued(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	m, _ := newVerdictTestManager(t, host, port)

	addErrorNZBEntry(t, m, "revive-queued", "articles missing on provider: failed to stat segment", 2, false)
	updated, resumeAction, applied, err := m.reviveErrorEntry("revive-queued", false)
	if err != nil || !applied {
		t.Fatalf("reviveErrorEntry: applied=%v err=%v", applied, err)
	}
	if resumeAction {
		t.Fatal("availability failure must not resume a claimed action")
	}
	if updated.State != storage.EntryStateDownloading || updated.Status != debridTypes.TorrentStatusQueued || updated.IsDownloading {
		t.Fatalf("revived entry = state %q status %q downloading=%v, want downloading/queued/false", updated.State, updated.Status, updated.IsDownloading)
	}
	if updated.ErrorCount != 2 || updated.LastError == "" {
		t.Fatalf("revival must not touch error bookkeeping: count=%d lastError=%q", updated.ErrorCount, updated.LastError)
	}
}

// The archive-processing incident cohort (the exact production LastError of
// the 1,891 mass-failed entries) re-enters the queued path, never the
// claimed-action path.
func TestReviveErrorEntryArchiveProcessingFailureBecomesQueued(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	m, _ := newVerdictTestManager(t, host, port)

	addErrorNZBEntry(t, m, "revive-archive", "failed to process nzb: failed to process NZB archives: no valid files found in NZB", 1, false)
	updated, resumeAction, applied, err := m.reviveErrorEntry("revive-archive", false)
	if err != nil || !applied {
		t.Fatalf("reviveErrorEntry: applied=%v err=%v", applied, err)
	}
	if resumeAction {
		t.Fatal("archive-processing failure must not resume a claimed action")
	}
	if updated.State != storage.EntryStateDownloading || updated.Status != debridTypes.TorrentStatusQueued || updated.IsDownloading {
		t.Fatalf("revived entry = state %q status %q downloading=%v, want downloading/queued/false", updated.State, updated.Status, updated.IsDownloading)
	}
	if updated.ErrorCount != 1 || updated.LastError == "" {
		t.Fatalf("revival must not touch error bookkeeping: count=%d lastError=%q", updated.ErrorCount, updated.LastError)
	}
}

// Mount-visibility timeouts happened after a completed download: revival
// restores the durably claimed mid-action triple so the post-download action
// resumes through the gate.
func TestReviveErrorEntryMountTimeoutRestoresClaimedAction(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	m, _ := newVerdictTestManager(t, host, port)

	addErrorNZBEntry(t, m, "revive-action", "timeout waiting for mount files: 2 files still pending (a.mkv, b.mkv)", 1, false)
	updated, resumeAction, applied, err := m.reviveErrorEntry("revive-action", false)
	if err != nil || !applied {
		t.Fatalf("reviveErrorEntry: applied=%v err=%v", applied, err)
	}
	if !resumeAction {
		t.Fatal("mount-timeout revival must resume the claimed action")
	}
	if updated.State != storage.EntryStateDownloading ||
		updated.Status != debridTypes.TorrentStatusDownloaded ||
		!updated.IsDownloading {
		t.Fatalf("revived entry = state %q status %q downloading=%v, want the claimed mid-action triple", updated.State, updated.Status, updated.IsDownloading)
	}
}

// The sweep must skip exhausted, Bad and non-matching entries entirely.
func TestRevivalSweepSkipsIneligibleEntries(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	m, _ := newVerdictTestManager(t, host, port)

	addErrorNZBEntry(t, m, "skip-exhausted", "articles missing on provider: failed to stat segment", 3, false)
	addErrorNZBEntry(t, m, "skip-bad", "articles missing on provider: failed to stat segment", 1, true)
	addErrorNZBEntry(t, m, "skip-signature", "some unrelated processing failure", 1, false)

	if revived := m.reviveErrorEntries(context.Background(), 0, false); revived != 0 {
		t.Fatalf("sweep revived %d ineligible entries", revived)
	}
	for _, hash := range []string{"skip-exhausted", "skip-bad", "skip-signature"} {
		persisted, err := m.storage.GetQueued(hash)
		if err != nil {
			t.Fatalf("GetQueued(%s): %v", hash, err)
		}
		if persisted.State != storage.EntryStateError {
			t.Fatalf("ineligible entry %s left error state: %q", hash, persisted.State)
		}
	}
}

// The periodic sweep is rate-limited so a large backlog drains progressively.
func TestRevivalSweepHonorsRateLimit(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	m, _ := newVerdictTestManager(t, host, port)

	for _, hash := range []string{"limited-1", "limited-2", "limited-3"} {
		addErrorNZBEntry(t, m, hash, "articles missing on provider: failed to stat segment", 1, false)
	}
	if revived := m.reviveErrorEntries(context.Background(), 2, false); revived != 2 {
		t.Fatalf("sweep revived %d entries, want the limit of 2", revived)
	}
	stillFailed := 0
	for _, hash := range []string{"limited-1", "limited-2", "limited-3"} {
		persisted, err := m.storage.GetQueued(hash)
		if err != nil {
			t.Fatalf("GetQueued(%s): %v", hash, err)
		}
		if persisted.State == storage.EntryStateError {
			stillFailed++
		}
	}
	if stillFailed != 1 {
		t.Fatalf("%d entries still failed after a limit-2 sweep over 3, want 1", stillFailed)
	}
}

// Boot restore revives an eligible failed entry and carries it end-to-end
// through pass-2: re-parse succeeds against the healthy provider, completed
// metadata is committed, and a processing job is submitted.
func TestBootRestoreRevivesEligibleErrorEntryEndToEnd(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true) // healthy: STAT answers 223
	host, port := server.hostPort(t)
	m, jobCh := newVerdictTestManager(t, host, port)

	entry := newQueuedNZBEntry(t, m, "boot-revive-entry")
	failed, err := m.queue.Mutate(entry.InfoHash, func(current *storage.Entry) bool {
		current.MarkAsError(context.DeadlineExceeded)
		current.LastError = "usenet parse failed: availability probe failed: provider connectivity problem: failed to stat segment"
		current.ErrorCount = 2
		return true
	})
	if err != nil {
		t.Fatalf("mark entry failed: %v", err)
	}
	if failed.State != storage.EntryStateError {
		t.Fatalf("setup entry state = %q, want error", failed.State)
	}

	m.restoreActiveDownloadJobs()

	persisted, err := m.storage.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	if persisted.State != storage.EntryStateDownloading {
		t.Fatalf("revived entry state = %q (lastError=%q), want downloading", persisted.State, persisted.LastError)
	}
	if persisted.Status != debridTypes.TorrentStatusDownloading {
		t.Fatalf("revived entry status = %q, want downloading after successful rebuild", persisted.Status)
	}
	if persisted.ErrorCount != 2 {
		t.Fatalf("revival changed ErrorCount to %d, want 2 untouched", persisted.ErrorCount)
	}

	select {
	case job := <-jobCh:
		if job.ID != entry.InfoHash {
			t.Fatalf("submitted job ID = %s, want %s", job.ID, entry.InfoHash)
		}
		if job.NZBMeta == nil {
			t.Fatal("submitted job carries no parsed NZB metadata")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no processing job was submitted for the revived entry")
	}
}

// Boot restore routes revived mount-timeout entries through the claimed-action
// resume path (pass-1), not the queued rebuild path.
func TestBootRestoreRevivesMountTimeoutEntryAsResumeAction(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	m, jobCh := newVerdictTestManager(t, host, port)

	addErrorNZBEntry(t, m, "boot-action-entry", "timeout waiting for mount files: 1 files still pending (a.mkv)", 1, false)
	m.restoreActiveDownloadJobs()

	persisted, err := m.storage.GetQueued("boot-action-entry")
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	if persisted.State != storage.EntryStateDownloading ||
		persisted.Status != debridTypes.TorrentStatusDownloaded ||
		!persisted.IsDownloading {
		t.Fatalf("revived entry = state %q status %q downloading=%v, want the claimed mid-action triple", persisted.State, persisted.Status, persisted.IsDownloading)
	}

	select {
	case job := <-jobCh:
		if job.ID != "boot-action-entry" || !job.ResumeAction {
			t.Fatalf("submitted job = %+v, want a ResumeAction job for boot-action-entry", job)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no resume-action job was submitted for the revived entry")
	}
}

// ReviveErrorEntry (SABnzbd mode=retry) bypasses the count/signature gates on
// explicit user intent but refuses entries that are not failed NZBs.
func TestReviveErrorEntryForceSemantics(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	m, _ := newVerdictTestManager(t, host, port)

	addErrorNZBEntry(t, m, "force-exhausted", "some unrelated processing failure", 9, false)
	if err := m.ReviveErrorEntry("force-exhausted"); err != nil {
		t.Fatalf("forced revival failed: %v", err)
	}
	persisted, err := m.storage.GetQueued("force-exhausted")
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	if persisted.State != storage.EntryStateDownloading {
		t.Fatalf("forced revival left state %q, want downloading", persisted.State)
	}

	if err := m.ReviveErrorEntry("no-such-entry"); err == nil {
		t.Fatal("expected an error for an unknown nzo_id")
	} else if !strings.Contains(err.Error(), "no-such-entry") {
		t.Fatalf("unknown-entry error = %v, want it to name the entry", err)
	}

	addErrorNZBEntry(t, m, "force-bad", "articles missing on provider", 1, true)
	if err := m.ReviveErrorEntry("force-bad"); err == nil {
		t.Fatal("expected an error when force-reviving a Bad entry")
	}
}

package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// Class A2 scenarios: metas durably failed by the incident's markAsFailed with
// no surviving XML source. Only the one whose Files still carry the full
// parsed segment map from an earlier completed lifecycle is un-flippable;
// every near-miss stays class C with a precise reason.
const (
	idA2            = "unflip-full-meta"
	idA2EmptyFiles  = "unflip-empty-files"
	idA2NoSegs      = "unflip-no-segments"
	idA2ZeroSize    = "unflip-zero-size"
	idA2Bad         = "unflip-is-bad"
	idA2Deleted     = "unflip-deleted-file"
	idA2GenMismatch = "unflip-gen-mismatch"

	genA2 = "gen-a2-unflip"
)

// failedMetaWithSegments is the durable shape markAsFailed leaves when a false
// verdict hits an NZB completed in an earlier lifecycle: Status flipped to
// failed, FailMessage stamped, Files/segments fully intact.
func failedMetaWithSegments(id, generation string, size int64) *storage.NZB {
	return &storage.NZB{
		ID:          id,
		Name:        id + ".nzb",
		Generation:  generation,
		Status:      usenet.NZBStatusFailed,
		FailMessage: "no valid files found in NZB",
		TotalSize:   size,
		Files: []storage.NZBFile{{
			Name: "movie.mkv",
			Size: size,
			Segments: []storage.NZBSegment{{
				Number:    1,
				MessageID: id + "-segment",
				Bytes:     size,
			}},
		}},
	}
}

// seedUnflipState builds a state dir holding one entry per A2 scenario, all
// carrying the production Process-phase error at the census timestamp and none
// with a surviving XML source. The un-flippable entry also has a main-store
// row parked in State=error.
func seedUnflipState(t *testing.T) string {
	t.Helper()
	stateDir := t.TempDir()
	config.SetConfigPath(stateDir)

	store, err := storage.NewStorage(filepath.Join(stateDir, "db"))
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	nzbs, err := usenet.NewNZBStorage()
	if err != nil {
		t.Fatalf("NewNZBStorage: %v", err)
	}

	addEntry := func(id, generation string) {
		t.Helper()
		e := erroredNZBEntry(id, archiveProductionError, archWindow, 1, false)
		e.NZBGeneration = generation
		if err := store.AddQueue(e); err != nil {
			t.Fatalf("AddQueue %s: %v", id, err)
		}
	}
	addMeta := func(nzb *storage.NZB) {
		t.Helper()
		if err := nzbs.AddNZB(nzb); err != nil {
			t.Fatalf("AddNZB %s: %v", nzb.ID, err)
		}
	}

	// A2: failed meta, matching generation, full segment map, positive size.
	addEntry(idA2, genA2)
	addMeta(failedMetaWithSegments(idA2, genA2, 100))
	mainRow := erroredNZBEntry(idA2, archiveProductionError, archWindow, 1, false)
	mainRow.NZBGeneration = genA2
	if err := store.AddOrUpdate(mainRow); err != nil {
		t.Fatalf("AddOrUpdate a2 main row: %v", err)
	}

	// Near-miss: failed meta that never got past the parse stage (no Files).
	addEntry(idA2EmptyFiles, "")
	addMeta(&storage.NZB{ID: idA2EmptyFiles, Name: "empty.nzb", Status: usenet.NZBStatusFailed, FailMessage: "no valid files found in NZB"})

	// Near-miss: a file without segments.
	addEntry(idA2NoSegs, "")
	noSegs := failedMetaWithSegments(idA2NoSegs, "", 100)
	noSegs.Files[0].Segments = nil
	addMeta(noSegs)

	// Near-miss: segments present but no positive size anywhere.
	addEntry(idA2ZeroSize, "")
	zero := failedMetaWithSegments(idA2ZeroSize, "", 100)
	zero.TotalSize = 0
	zero.Files[0].Size = 0
	addMeta(zero)

	// Refusal: durably bad meta (genuine content verdict).
	addEntry(idA2Bad, "")
	bad := failedMetaWithSegments(idA2Bad, "", 100)
	bad.IsBad = true
	addMeta(bad)

	// Refusal: a permanently failed file.
	addEntry(idA2Deleted, "")
	deleted := failedMetaWithSegments(idA2Deleted, "", 100)
	deleted.Files[0].IsDeleted = true
	addMeta(deleted)

	// Refusal: generation mismatch (stale meta, same rule as class A).
	addEntry(idA2GenMismatch, "gen-entry-owns")
	addMeta(failedMetaWithSegments(idA2GenMismatch, "gen-meta-owns", 100))

	if err := store.Close(); err != nil {
		t.Fatalf("Close seeding store: %v", err)
	}
	return stateDir
}

func openNZBs(t *testing.T, stateDir string) *usenet.NZBStorage {
	t.Helper()
	config.SetConfigPath(stateDir)
	nzbs, err := usenet.NewNZBStorage()
	if err != nil {
		t.Fatalf("reopen NZB storage: %v", err)
	}
	return nzbs
}

func TestUnflipClassificationApplyAndIdempotency(t *testing.T) {
	stateDir := seedUnflipState(t)

	wantDryDecisions := map[string]string{
		idA2:            "\twould-unflip+main",
		idA2EmptyFiles:  "\tskip-meta-failed-no-xml",
		idA2NoSegs:      "\tskip-meta-failed-empty-files",
		idA2ZeroSize:    "\tskip-meta-failed-empty-files",
		idA2Bad:         "\tskip-meta-failed-bad-or-deleted",
		idA2Deleted:     "\tskip-meta-failed-bad-or-deleted",
		idA2GenMismatch: "\tskip-generation-mismatch",
	}

	// --- Dry run: classification only, zero mutation. ---
	var out, errOut bytes.Buffer
	if code := run(testOptions(stateDir, false), &out, &errOut); code != exitOK {
		t.Fatalf("dry-run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitOK, out.String(), errOut.String())
	}
	dry := out.String()
	for id, want := range wantDryDecisions {
		if line := tsvLine(t, dry, id); !strings.HasSuffix(line, want) {
			t.Errorf("dry-run decision for %s = %q, want suffix %q", id, line, want)
		}
	}
	if !strings.Contains(dry, "# census: candidates=7 A-action=0 A-queued=0 A2-unflip=1 B=0 C=6") {
		t.Errorf("dry-run census missing or wrong:\n%s", dry)
	}

	nzbs := openNZBs(t, stateDir)
	if meta, err := nzbs.GetNZB(idA2); err != nil || meta.Status != usenet.NZBStatusFailed || meta.FailMessage == "" {
		t.Errorf("dry run touched the A2 meta: %+v (err=%v)", meta, err)
	}
	store := openStore(t, stateDir)
	for id := range wantDryDecisions {
		assertUntouchedError(t, mustQueued(t, store, id), id)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// --- Apply: the A2 row is un-flipped end to end, near-misses untouched. ---
	out.Reset()
	errOut.Reset()
	applyOpts := testOptions(stateDir, true)
	if code := run(applyOpts, &out, &errOut); code != exitOK {
		t.Fatalf("apply exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitOK, out.String(), errOut.String())
	}
	applied := out.String()
	if line := tsvLine(t, applied, idA2); !strings.HasSuffix(line, "\tunflipped+main") {
		t.Errorf("apply decision for %s = %q, want suffix \"\\tunflipped+main\"", idA2, line)
	}
	for _, id := range []string{idA2EmptyFiles, idA2NoSegs, idA2ZeroSize, idA2Bad, idA2Deleted, idA2GenMismatch} {
		if line := tsvLine(t, applied, id); !strings.HasSuffix(line, wantDryDecisions[id]) {
			t.Errorf("apply decision for %s = %q, want suffix %q", id, line, wantDryDecisions[id])
		}
	}

	// Meta: re-read shows completed, cleared FailMessage, intact segment map,
	// unchanged generation.
	nzbs = openNZBs(t, stateDir)
	meta, err := nzbs.GetNZB(idA2)
	if err != nil {
		t.Fatalf("GetNZB after apply: %v", err)
	}
	if meta.Status != usenet.NZBStatusCompleted || meta.FailMessage != "" {
		t.Errorf("meta not un-flipped: status=%q failMessage=%q", meta.Status, meta.FailMessage)
	}
	if meta.Generation != genA2 {
		t.Errorf("meta generation changed: %q", meta.Generation)
	}
	if len(meta.Files) != 1 || len(meta.Files[0].Segments) != 1 || meta.Files[0].Segments[0].MessageID != idA2+"-segment" {
		t.Errorf("segment map not preserved: %+v", meta.Files)
	}

	// Near-miss metas keep their durable failure verbatim.
	for _, id := range []string{idA2EmptyFiles, idA2NoSegs, idA2ZeroSize, idA2Bad, idA2Deleted, idA2GenMismatch} {
		m, err := nzbs.GetNZB(id)
		if err != nil || m.Status != usenet.NZBStatusFailed || m.FailMessage == "" {
			t.Errorf("%s: near-miss meta touched: %+v (err=%v)", id, m, err)
		}
	}

	// Entry: the exact pass-1 ResumeAction triple, tag appended, audit trail
	// and forbidden fields preserved.
	store = openStore(t, stateDir)
	a2 := mustQueued(t, store, idA2)
	if a2.State != storage.EntryStateDownloading ||
		a2.Status != debridTypes.TorrentStatusDownloaded ||
		!a2.IsDownloading {
		t.Errorf("A2 triple wrong: state=%q status=%q isDownloading=%v", a2.State, a2.Status, a2.IsDownloading)
	}
	if a2.LastError != archiveProductionError || a2.LastErrorTime == nil || a2.ErrorCount != 1 {
		t.Errorf("A2 audit trail touched: lastError=%q time=%v count=%d", a2.LastError, a2.LastErrorTime, a2.ErrorCount)
	}
	if a2.NZBGeneration != genA2 {
		t.Errorf("A2 NZBGeneration touched: %q", a2.NZBGeneration)
	}
	wantTags := []string{seedTag, testTag}
	if strings.Join(a2.Tags, ",") != strings.Join(wantTags, ",") {
		t.Errorf("A2 Tags = %v, want %v", a2.Tags, wantTags)
	}

	// Main-store row got the same cosmetic reset.
	mainAfter, err := store.Get(idA2)
	if err != nil {
		t.Fatalf("Get main row after apply: %v", err)
	}
	if mainAfter.State != storage.EntryStateDownloading ||
		mainAfter.Status != debridTypes.TorrentStatusDownloaded ||
		!mainAfter.IsDownloading {
		t.Errorf("main-store reset wrong: state=%q status=%q isDownloading=%v", mainAfter.State, mainAfter.Status, mainAfter.IsDownloading)
	}

	// Near-miss entries remain parked in error.
	for _, id := range []string{idA2EmptyFiles, idA2NoSegs, idA2ZeroSize, idA2Bad, idA2Deleted, idA2GenMismatch} {
		assertUntouchedError(t, mustQueued(t, store, id), id)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close before idempotency run: %v", err)
	}

	// --- Idempotency: the un-flipped row no longer matches the selector. ---
	out.Reset()
	errOut.Reset()
	if code := run(applyOpts, &out, &errOut); code != exitOK {
		t.Fatalf("second apply exit = %d, want %d\nstdout:\n%s", code, exitOK, out.String())
	}
	second := out.String()
	if strings.Contains(second, idA2+"\t") {
		t.Errorf("un-flipped row matched the selector again:\n%s", second)
	}
	if !strings.Contains(second, "# census: candidates=6 A-action=0 A-queued=0 A2-unflip=0 B=0 C=6") {
		t.Errorf("second-run census should hold only the near-misses:\n%s", second)
	}

	store = openStore(t, stateDir)
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	final := mustQueued(t, store, idA2)
	if final.State != storage.EntryStateDownloading {
		t.Errorf("second run modified the un-flipped row: state=%q", final.State)
	}
	if len(final.Tags) != 2 {
		t.Errorf("second run duplicated tags: %v", final.Tags)
	}
	nzbs = openNZBs(t, stateDir)
	if m, err := nzbs.GetNZB(idA2); err != nil || m.Status != usenet.NZBStatusCompleted {
		t.Errorf("second run disturbed the restored meta: %+v (err=%v)", m, err)
	}
}

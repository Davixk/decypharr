package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// This file reproduces the PRODUCTION meta shape, not a hand-rolled one: the
// meta is written through the exact same durable write path the daemon uses
// (AddNZB -> addNZBWithLifecycleHeld -> writeNZBLocked -> encodeNZBV2, with
// the generation trailer), following the real lifecycle byte-for-byte:
//
//  1. parse-time write: ParseWithID persists the quick-parse NZB with
//     Status=parsing and Files=[] (parser.Parse defers file-group extraction,
//     so the first durable meta has an EMPTY file table);
//  2. completion write: markAsCompleted saves the fully processed NZB
//     (Status=completed, populated Files+segments, Path cleared) over the
//     same generation;
//  3. incident flip: markAsFailed loads the CURRENT durable meta with a full
//     GetNZB, flips only Status and FailMessage, and saves it back through
//     the same guarded path — Files and segments survive on disk.
//
// The 2026-07-19 NAS dry-run returned A2-unflip=0 with every such entry in
// skip-meta-failed-no-xml, so this test is the regression fence: a meta that
// went through the real daemon lifecycle above MUST classify as A2.
const (
	idDaemonA2     = "daemon-shaped-unflip"
	idDaemonNoSegs = "daemon-shaped-no-segments"
)

// daemonWriteFailedMeta drives the daemon's real write sequence for one NZB
// and returns the generation token the queue entry would hold.
func daemonWriteFailedMeta(t *testing.T, nzbs *usenet.NZBStorage, id string, segmentless bool) string {
	t.Helper()
	gen := usenet.NewNZBGeneration()

	// (1) ParseWithID's persist: quick-parse shape. parser.Parse builds
	// `&storage.NZB{Files: []storage.NZBFile{}, ...}` and ParseWithID stamps
	// Status=parsing plus the staged source path before addNZBWithLifecycleHeld.
	parseShape := &storage.NZB{
		ID:         id,
		Generation: gen,
		Name:       id + ".nzb",
		Status:     usenet.NZBStatusParsing,
		Path:       filepath.Join(t.TempDir(), id+"."+gen+".nzb"),
		Files:      []storage.NZBFile{},
	}
	if err := nzbs.AddNZB(parseShape); err != nil {
		t.Fatalf("parse-time AddNZB(%s): %v", id, err)
	}

	// (2) markAsCompleted's persist: the processed NZB with the full segment
	// map, same generation, Path cleared (source deleted on completion).
	completed := &storage.NZB{
		ID:         id,
		Generation: gen,
		Name:       id + ".nzb",
		Status:     usenet.NZBStatusCompleted,
		TotalSize:  3000,
		Files: []storage.NZBFile{
			{
				Name: "movie.mkv",
				Size: 2000,
				Segments: []storage.NZBSegment{
					{Number: 1, MessageID: id + "-seg-1", Bytes: 1000, StartOffset: 0, EndOffset: 999, Group: "alt.binaries.movies"},
					{Number: 2, MessageID: id + "-seg-2", Bytes: 1000, StartOffset: 1000, EndOffset: 1999, Group: "alt.binaries.movies"},
				},
			},
			{
				Name: "movie.par2",
				Size: 1000,
				Segments: []storage.NZBSegment{
					{Number: 1, MessageID: id + "-par-1", Bytes: 1000, Group: "alt.binaries.movies"},
				},
				FileType: storage.NZBFileTypePar2,
			},
		},
	}
	if segmentless {
		// Variant for the near-miss fence: the daemon completed a meta whose
		// file table survived but whose segment map did not (e.g. written by a
		// header-decoded object). Must classify meta-failed-empty-files.
		for i := range completed.Files {
			completed.Files[i].Segments = nil
		}
	}
	if err := nzbs.AddNZB(completed); err != nil {
		t.Fatalf("completion AddNZB(%s): %v", id, err)
	}

	// (3) markAsFailed byte-for-byte (pkg/usenet/usenet.go:1872): load the
	// CURRENT durable meta via a full GetNZB, flip Status+FailMessage only,
	// save through the same guarded add path.
	current, err := nzbs.GetNZB(id)
	if err != nil {
		t.Fatalf("markAsFailed load current(%s): %v", id, err)
	}
	current.Status = usenet.NZBStatusFailed
	current.FailMessage = "no valid files found in NZB"
	if err := nzbs.AddNZB(current); err != nil {
		t.Fatalf("markAsFailed save(%s): %v", id, err)
	}
	return gen
}

// TestIncidentRebuildWipeSequenceIsClassifiedNoXML replays what ACTUALLY
// happened to the 2026-07-19 archive-processing cohort on disk, write for
// write, and pins two facts the A2-unflip=0 investigation established:
//
//  1. the segment map is destroyed by the REBUILD'S QUICK-PARSE PERSIST, not
//     by markAsFailed: when a queued rebuild falls through to
//     ParseWithGeneration (pre-fix it did so even over completed pre-fence
//     metas), ParseWithID persists parser.Parse's NZB — which is built with
//     `Files: []storage.NZBFile{}` — over the completed meta. The completed
//     file table and every segment are gone from disk at that instant;
//  2. markAsFailed then merely flips the ALREADY-EMPTY meta to failed and
//     deletes the re-staged XML source, so the tool's
//     skip-meta-failed-no-xml verdict for these rows is the truth: there is
//     nothing left to un-flip offline (recourse: arr-side re-grab).
//
// This is why the NAS dry-run reported every cohort row as
// skip-meta-failed-no-xml with zero rows in the near-miss buckets, while the
// v2 tests (which seeded failed metas WITH populated Files, modeling
// markAsFailed as the only writer) kept passing. The engine-side fix that
// stops the wipe from ever recurring lives in
// pkg/manager/active_queue.go (rebuildQueuedNZBJob) with its regression test
// in pkg/manager/rebuild_resume_test.go.
func TestIncidentRebuildWipeSequenceIsClassifiedNoXML(t *testing.T) {
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

	const id = "incident-wiped"
	gen := usenet.NewNZBGeneration()

	// Pre-incident: the meta completed with a fully populated segment map and
	// streamed in production.
	if err := nzbs.AddNZB(&storage.NZB{
		ID: id, Generation: gen, Name: id + ".nzb",
		Status: usenet.NZBStatusCompleted, TotalSize: 2000,
		Files: []storage.NZBFile{{
			Name: "movie.mkv", Size: 2000,
			Segments: []storage.NZBSegment{{Number: 1, MessageID: id + "-seg", Bytes: 2000}},
		}},
	}); err != nil {
		t.Fatalf("completion AddNZB: %v", err)
	}

	// Incident, step 1 — the rebuild's quick-parse persist (ParseWithID,
	// pkg/usenet/usenet.go:634-655): parser.Parse's NZB with an EMPTY file
	// table and Status=parsing is saved over the completed meta through the
	// same guarded write path, under the same generation.
	stagedSource := filepath.Join(t.TempDir(), id+".resave.source")
	if err := nzbs.AddNZB(&storage.NZB{
		ID: id, Generation: gen, Name: id + ".nzb",
		Status: usenet.NZBStatusParsing, Path: stagedSource,
		Files: []storage.NZBFile{},
	}); err != nil {
		t.Fatalf("quick-parse persist AddNZB: %v", err)
	}
	wiped, err := nzbs.GetNZB(id)
	if err != nil {
		t.Fatalf("GetNZB after quick-parse persist: %v", err)
	}
	if len(wiped.Files) != 0 {
		t.Fatalf("quick-parse persist did not empty the file table: %+v (this test's premise changed)", wiped.Files)
	}

	// Incident, step 2 — Process fails on the collapsed substrate and
	// markAsFailed flips the (already empty) durable meta and deletes the
	// re-staged source. Byte-for-byte: load current, flip, save.
	current, err := nzbs.GetNZB(id)
	if err != nil {
		t.Fatalf("markAsFailed load current: %v", err)
	}
	current.Status = usenet.NZBStatusFailed
	current.FailMessage = "no valid files found in NZB"
	if err := nzbs.AddNZB(current); err != nil {
		t.Fatalf("markAsFailed save: %v", err)
	}

	e := erroredNZBEntry(id, archiveProductionError, archWindow, 1, false)
	e.NZBGeneration = gen
	if err := store.AddQueue(e); err != nil {
		t.Fatalf("AddQueue: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close seeding store: %v", err)
	}

	var out, errOut bytes.Buffer
	if code := run(testOptions(stateDir, false), &out, &errOut); code != exitOK {
		t.Fatalf("dry-run exit = %d\nstdout:\n%s\nstderr:\n%s", code, out.String(), errOut.String())
	}
	if line := tsvLine(t, out.String(), id); !strings.HasSuffix(line, "\tskip-meta-failed-no-xml") {
		t.Errorf("wiped-meta row = %q, want the truthful skip-meta-failed-no-xml verdict", line)
	}
}

func TestDaemonShapedFailedMetaClassifiesA2(t *testing.T) {
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

	genA2 := daemonWriteFailedMeta(t, nzbs, idDaemonA2, false)
	genNoSegs := daemonWriteFailedMeta(t, nzbs, idDaemonNoSegs, true)

	for id, gen := range map[string]string{idDaemonA2: genA2, idDaemonNoSegs: genNoSegs} {
		e := erroredNZBEntry(id, archiveProductionError, archWindow, 1, false)
		e.NZBGeneration = gen
		if err := store.AddQueue(e); err != nil {
			t.Fatalf("AddQueue %s: %v", id, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close seeding store: %v", err)
	}

	var out, errOut bytes.Buffer
	if code := run(testOptions(stateDir, false), &out, &errOut); code != exitOK {
		t.Fatalf("dry-run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitOK, out.String(), errOut.String())
	}
	dry := out.String()

	if line := tsvLine(t, dry, idDaemonA2); !strings.HasSuffix(line, "\twould-unflip") {
		t.Errorf("daemon-shaped meta with intact segment map = %q, want suffix \"\\twould-unflip\"", line)
	}
	if line := tsvLine(t, dry, idDaemonNoSegs); !strings.HasSuffix(line, "\tskip-meta-failed-empty-files") {
		t.Errorf("daemon-shaped segmentless meta = %q, want suffix \"\\tskip-meta-failed-empty-files\"", line)
	}
	if !strings.Contains(dry, "A2-unflip=1") {
		t.Errorf("census must count exactly one A2 candidate:\n%s", dry)
	}
	if strings.Contains(dry, "skip-meta-failed-no-xml") {
		t.Errorf("no daemon-shaped meta may fall into the no-xml bucket:\n%s", dry)
	}
}

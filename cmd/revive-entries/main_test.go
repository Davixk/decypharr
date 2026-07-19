package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

const (
	idAAction        = "revive-a-action"
	idAQueued        = "revive-a-queued"
	idArchAQueued    = "revive-arch-a-queued"
	idArchFailedMeta = "revive-arch-failed-meta"
	idB              = "revive-b-xml"
	idC              = "revive-c-nothing"
	idOutOfWin       = "revive-out-of-window"
	idBad            = "revive-bad-true"
	idGenuine430     = "revive-430-outside"

	genAAction = "gen-a-action"
	seedTag    = "pre-existing-tag"
	testTag    = "revived-test"

	// The exact LastError stamped on the 1,891 Process-phase incident entries.
	archiveProductionError = "failed to process nzb: failed to process NZB archives: no valid files found in NZB"
)

var (
	pdt      = time.FixedZone("PDT", -7*60*60)
	inWindow = time.Date(2026, 7, 19, 5, 40, 0, 0, pdt)
	// archWindow is the census timestamp of the 1,891 cohort (05:47:00); it
	// must be covered inclusively by the default window.
	archWindow = time.Date(2026, 7, 19, 5, 47, 0, 0, pdt)
	outWindow  = time.Date(2026, 7, 19, 3, 0, 0, 0, pdt)
	addedOn    = time.Date(2026, 7, 18, 12, 0, 0, 0, pdt)
)

func testOptions(stateDir string, apply bool) options {
	from, _ := time.Parse(time.RFC3339, defaultFrom)
	to, _ := time.Parse(time.RFC3339, defaultTo)
	return options{
		stateDir:  stateDir,
		apply:     apply,
		from:      from,
		to:        to,
		maxErrors: defaultMaxErrors,
		tag:       testTag,
	}
}

func erroredNZBEntry(id, lastError string, errTime time.Time, errorCount int, bad bool) *storage.Entry {
	t := errTime
	return &storage.Entry{
		Protocol:      config.ProtocolNZB,
		InfoHash:      id,
		Name:          "Entry " + id,
		State:         storage.EntryStateError,
		Status:        debridTypes.TorrentStatusError,
		Bad:           bad,
		ErrorCount:    errorCount,
		LastError:     lastError,
		LastErrorTime: &t,
		AddedOn:       addedOn,
		SavePath:      "/downloads/" + id,
		CallbackURL:   "http://callback.local/" + id,
		Tags:          []string{seedTag},
		Files: map[string]*storage.File{
			"movie.mkv": {
				Name:     "movie.mkv",
				Size:     10,
				InfoHash: id,
				AddedOn:  addedOn,
			},
		},
	}
}

// seedIncidentState builds a state dir with one entry per scenario:
// A-action, A-queued, B (fake .source artifact), C, out-of-window error,
// Bad=true, and a genuine 430 outside the window. The A-action entry also has
// a main-store row parked in State=error.
func seedIncidentState(t *testing.T) string {
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

	// Class A-action: completed meta with matching generation, mount timeout.
	aAction := erroredNZBEntry(idAAction, "post-download: timeout waiting for mount files", inWindow, 1, false)
	aAction.NZBGeneration = genAAction
	if err := nzbs.AddNZB(&storage.NZB{ID: idAAction, Name: "a-action.nzb", Generation: genAAction, Status: usenet.NZBStatusCompleted}); err != nil {
		t.Fatalf("AddNZB a-action: %v", err)
	}
	if err := store.AddQueue(aAction); err != nil {
		t.Fatalf("AddQueue a-action: %v", err)
	}
	mainRow := erroredNZBEntry(idAAction, aAction.LastError, inWindow, 1, false)
	mainRow.NZBGeneration = genAAction
	if err := store.AddOrUpdate(mainRow); err != nil {
		t.Fatalf("AddOrUpdate a-action main row: %v", err)
	}

	// Class A-queued: completed meta (blank generation), stat-segment verdict.
	aQueued := erroredNZBEntry(idAQueued, "articles missing on provider: failed to stat segment 12 of movie.mkv", inWindow, 2, false)
	if err := nzbs.AddNZB(&storage.NZB{ID: idAQueued, Name: "a-queued.nzb", Status: usenet.NZBStatusCompleted}); err != nil {
		t.Fatalf("AddNZB a-queued: %v", err)
	}
	if err := store.AddQueue(aQueued); err != nil {
		t.Fatalf("AddQueue a-queued: %v", err)
	}

	// Class B: no meta at all, but a staged XML source artifact survives.
	b := erroredNZBEntry(idB, "no valid file groups found in NZB after 3 attempts", inWindow, 3, false)
	nzbDir := filepath.Join(stateDir, "usenet", "nzbs")
	if err := os.MkdirAll(nzbDir, 0o755); err != nil {
		t.Fatalf("mkdir nzbs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nzbDir, idB+".deadbeefdeadbeefdeadbeef.source"), []byte("<nzb/>"), 0o644); err != nil {
		t.Fatalf("write fake source: %v", err)
	}
	if err := store.AddQueue(b); err != nil {
		t.Fatalf("AddQueue b: %v", err)
	}

	// A-queued (archive cohort): the exact Process-phase production string at
	// the 05:47:00 census timestamp with completed metadata — the
	// downloaded-cohort shape inside the 1,891.
	archAQueued := erroredNZBEntry(idArchAQueued, archiveProductionError, archWindow, 1, false)
	if err := nzbs.AddNZB(&storage.NZB{ID: idArchAQueued, Name: "arch-a-queued.nzb", Status: usenet.NZBStatusCompleted}); err != nil {
		t.Fatalf("AddNZB arch-a-queued: %v", err)
	}
	if err := store.AddQueue(archAQueued); err != nil {
		t.Fatalf("AddQueue arch-a-queued: %v", err)
	}

	// Class B (archive cohort): metadata durably marked failed by
	// markAsFailed during the incident, but the XML source artifact survives
	// so boot pass-2 can re-parse it.
	archFailed := erroredNZBEntry(idArchFailedMeta, archiveProductionError, archWindow, 2, false)
	if err := nzbs.AddNZB(&storage.NZB{ID: idArchFailedMeta, Name: "arch-failed.nzb", Status: usenet.NZBStatusFailed, FailMessage: "no valid files found in NZB"}); err != nil {
		t.Fatalf("AddNZB arch-failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nzbDir, idArchFailedMeta+".cafecafecafecafecafecafe.source"), []byte("<nzb/>"), 0o644); err != nil {
		t.Fatalf("write arch-failed source: %v", err)
	}
	if err := store.AddQueue(archFailed); err != nil {
		t.Fatalf("AddQueue arch-failed: %v", err)
	}

	// Class C: matches the selector but has neither meta nor XML.
	c := erroredNZBEntry(idC, "articles missing on provider: failed to stat segment 1 of gone.mkv", inWindow, 1, false)
	if err := store.AddQueue(c); err != nil {
		t.Fatalf("AddQueue c: %v", err)
	}

	// Not selected: same pattern but outside the window.
	outOfWin := erroredNZBEntry(idOutOfWin, "articles missing on provider: failed to stat segment 4 of old.mkv", outWindow, 1, false)
	if err := store.AddQueue(outOfWin); err != nil {
		t.Fatalf("AddQueue out-of-window: %v", err)
	}

	// Not selected: Bad entries are never revived.
	bad := erroredNZBEntry(idBad, "articles missing on provider: failed to stat segment 9 of bad.mkv", inWindow, 1, true)
	if err := store.AddQueue(bad); err != nil {
		t.Fatalf("AddQueue bad: %v", err)
	}

	// Not selected: a genuine 430 verdict outside the incident window.
	genuine := erroredNZBEntry(idGenuine430, "stream failed: NNTP ARTICLE_NOT_FOUND (code 430)", outWindow, 1, false)
	if err := store.AddQueue(genuine); err != nil {
		t.Fatalf("AddQueue 430: %v", err)
	}

	// The tool opens its own storage handle; only one writer may exist.
	if err := store.Close(); err != nil {
		t.Fatalf("Close seeding store: %v", err)
	}
	return stateDir
}

func openStore(t *testing.T, stateDir string) *storage.Storage {
	t.Helper()
	store, err := storage.NewStorage(filepath.Join(stateDir, "db"))
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	return store
}

func mustQueued(t *testing.T, store *storage.Storage, id string) *storage.Entry {
	t.Helper()
	e, err := store.GetQueued(id)
	if err != nil {
		t.Fatalf("GetQueued(%s): %v", id, err)
	}
	return e
}

func tsvLine(t *testing.T, out, id string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, id+"\t") {
			return line
		}
	}
	t.Fatalf("no TSV line for %s in output:\n%s", id, out)
	return ""
}

func assertUntouchedError(t *testing.T, e *storage.Entry, id string) {
	t.Helper()
	if e.State != storage.EntryStateError {
		t.Errorf("%s: State = %q, want error (untouched)", id, e.State)
	}
	if tags := e.Tags; len(tags) != 1 || tags[0] != seedTag {
		t.Errorf("%s: Tags = %v, want only the seeded tag", id, e.Tags)
	}
}

func TestReviveEntriesSelectionClassificationAndApply(t *testing.T) {
	stateDir := seedIncidentState(t)
	opts := testOptions(stateDir, false)

	// --- Dry run: correct selection + classification, zero mutation. ---
	var out, errOut bytes.Buffer
	if code := run(opts, &out, &errOut); code != exitOK {
		t.Fatalf("dry-run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitOK, out.String(), errOut.String())
	}
	dry := out.String()

	for id, want := range map[string]string{
		idAAction:        "would-revive-as-A-action+main",
		idAQueued:        "would-revive-as-A-queued",
		idArchAQueued:    "would-revive-as-A-queued",
		idArchFailedMeta: "would-revive-as-B",
		idB:              "would-revive-as-B",
		idC:              "skip-no-meta-no-xml",
	} {
		line := tsvLine(t, dry, id)
		if !strings.HasSuffix(line, "\t"+want) {
			t.Errorf("dry-run decision for %s = %q, want suffix %q", id, line, want)
		}
	}
	for _, id := range []string{idOutOfWin, idBad, idGenuine430} {
		if strings.Contains(dry, id+"\t") {
			t.Errorf("%s must not be selected, but appears in output:\n%s", id, dry)
		}
	}
	if !strings.Contains(dry, "# census: candidates=6 A-action=1 A-queued=2 B=2 C=1") {
		t.Errorf("dry-run census missing or wrong:\n%s", dry)
	}

	store := openStore(t, stateDir)
	for _, id := range []string{idAAction, idAQueued, idArchAQueued, idArchFailedMeta, idB, idC, idOutOfWin, idBad, idGenuine430} {
		assertUntouchedError(t, mustQueued(t, store, id), id)
	}
	mainBefore, err := store.Get(idAAction)
	if err != nil {
		t.Fatalf("Get main row: %v", err)
	}
	assertUntouchedError(t, mainBefore, idAAction+" (main)")
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// --- Apply: exactly the specified fields are reset. ---
	out.Reset()
	errOut.Reset()
	applyOpts := testOptions(stateDir, true)
	if code := run(applyOpts, &out, &errOut); code != exitOK {
		t.Fatalf("apply exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitOK, out.String(), errOut.String())
	}
	applied := out.String()
	for id, want := range map[string]string{
		idAAction:        "revived-as-A-action+main",
		idAQueued:        "revived-as-A-queued",
		idArchAQueued:    "revived-as-A-queued",
		idArchFailedMeta: "revived-as-B",
		idB:              "revived-as-B",
		idC:              "skip-no-meta-no-xml",
	} {
		line := tsvLine(t, applied, id)
		if !strings.HasSuffix(line, "\t"+want) {
			t.Errorf("apply decision for %s = %q, want suffix %q", id, line, want)
		}
	}

	store = openStore(t, stateDir)
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	aAction := mustQueued(t, store, idAAction)
	if aAction.State != storage.EntryStateDownloading ||
		aAction.Status != debridTypes.TorrentStatusDownloaded ||
		!aAction.IsDownloading {
		t.Errorf("A-action triple wrong: state=%q status=%q isDownloading=%v", aAction.State, aAction.Status, aAction.IsDownloading)
	}
	for _, tc := range []struct {
		id string
		e  *storage.Entry
	}{
		{idAQueued, mustQueued(t, store, idAQueued)},
		{idArchAQueued, mustQueued(t, store, idArchAQueued)},
		{idArchFailedMeta, mustQueued(t, store, idArchFailedMeta)},
		{idB, mustQueued(t, store, idB)},
	} {
		if tc.e.State != storage.EntryStateDownloading ||
			tc.e.Status != debridTypes.TorrentStatusQueued ||
			tc.e.IsDownloading || tc.e.Progress != 0 {
			t.Errorf("%s reset wrong: state=%q status=%q isDownloading=%v progress=%v",
				tc.id, tc.e.State, tc.e.Status, tc.e.IsDownloading, tc.e.Progress)
		}
	}

	// Audit trail and forbidden fields are preserved on every revived row.
	seededErrorTimes := map[string]time.Time{
		idAAction:        inWindow,
		idAQueued:        inWindow,
		idArchAQueued:    archWindow,
		idArchFailedMeta: archWindow,
		idB:              inWindow,
	}
	for _, id := range []string{idAAction, idAQueued, idArchAQueued, idArchFailedMeta, idB} {
		e := mustQueued(t, store, id)
		if e.LastError == "" || e.LastErrorTime == nil || e.ErrorCount == 0 {
			t.Errorf("%s: audit trail was cleared: lastError=%q time=%v count=%d", id, e.LastError, e.LastErrorTime, e.ErrorCount)
		}
		if e.LastErrorTime != nil && e.LastErrorTime.Unix() != seededErrorTimes[id].Unix() {
			t.Errorf("%s: LastErrorTime changed: %v", id, e.LastErrorTime)
		}
		if e.SavePath != "/downloads/"+id || e.CallbackURL != "http://callback.local/"+id {
			t.Errorf("%s: SavePath/CallbackURL touched: %q %q", id, e.SavePath, e.CallbackURL)
		}
		if len(e.Files) != 1 || e.Files["movie.mkv"] == nil {
			t.Errorf("%s: Files touched: %v", id, e.Files)
		}
		wantTags := []string{seedTag, testTag}
		if fmt.Sprint(e.Tags) != fmt.Sprint(wantTags) {
			t.Errorf("%s: Tags = %v, want %v", id, e.Tags, wantTags)
		}
	}
	if e := mustQueued(t, store, idAAction); e.NZBGeneration != genAAction {
		t.Errorf("NZBGeneration touched: %q", e.NZBGeneration)
	}

	// Main-store row for the revived hash got the same cosmetic reset.
	mainAfter, err := store.Get(idAAction)
	if err != nil {
		t.Fatalf("Get main row after apply: %v", err)
	}
	if mainAfter.State != storage.EntryStateDownloading ||
		mainAfter.Status != debridTypes.TorrentStatusDownloaded ||
		!mainAfter.IsDownloading {
		t.Errorf("main-store reset wrong: state=%q status=%q isDownloading=%v", mainAfter.State, mainAfter.Status, mainAfter.IsDownloading)
	}
	if !strings.Contains(strings.Join(mainAfter.Tags, ","), testTag) {
		t.Errorf("main-store row missing tag: %v", mainAfter.Tags)
	}

	// Non-candidates and class C remain parked in error.
	for _, id := range []string{idC, idOutOfWin, idBad, idGenuine430} {
		assertUntouchedError(t, mustQueued(t, store, id), id)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close before idempotency run: %v", err)
	}

	// --- Idempotency: revived rows no longer match the selector. Only the
	// untouched class-C row can still match on a second run. ---
	out.Reset()
	errOut.Reset()
	if code := run(applyOpts, &out, &errOut); code != exitOK {
		t.Fatalf("second apply exit = %d, want %d\nstdout:\n%s", code, exitOK, out.String())
	}
	if !strings.Contains(out.String(), "# census: candidates=1 A-action=0 A-queued=0 B=0 C=1") {
		t.Fatalf("second-run census should contain only the class-C row:\n%s", out.String())
	}
	for _, id := range []string{idAAction, idAQueued, idArchAQueued, idArchFailedMeta, idB} {
		if strings.Contains(out.String(), id+"\t") {
			t.Errorf("revived row %s matched the selector again:\n%s", id, out.String())
		}
	}

	store = openStore(t, stateDir)
	second := mustQueued(t, store, idAAction)
	if second.State != storage.EntryStateDownloading {
		t.Errorf("second run modified a revived row: state=%q", second.State)
	}
	if got := len(second.Tags); got != 2 {
		t.Errorf("second run duplicated tags: %v", second.Tags)
	}
}

// TestReviveEntriesSecondRunZeroCandidates proves the distinct exit code and
// full idempotency when every candidate is revivable: after -apply, a second
// run finds zero candidates and exits 2.
func TestReviveEntriesSecondRunZeroCandidates(t *testing.T) {
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
	const id = "solo-a-queued"
	if err := nzbs.AddNZB(&storage.NZB{ID: id, Name: "solo.nzb", Status: usenet.NZBStatusCompleted}); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	entry := erroredNZBEntry(id, "articles missing on provider: failed to stat segment 2 of solo.mkv", inWindow, 1, false)
	if err := store.AddQueue(entry); err != nil {
		t.Fatalf("AddQueue: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close seeding store: %v", err)
	}

	opts := testOptions(stateDir, true)
	var out, errOut bytes.Buffer
	if code := run(opts, &out, &errOut); code != exitOK {
		t.Fatalf("first apply exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitOK, out.String(), errOut.String())
	}
	if line := tsvLine(t, out.String(), id); !strings.HasSuffix(line, "\trevived-as-A-queued") {
		t.Fatalf("decision = %q, want revived-as-A-queued", line)
	}

	out.Reset()
	errOut.Reset()
	if code := run(opts, &out, &errOut); code != exitZeroCandidate {
		t.Fatalf("second apply exit = %d, want %d\nstdout:\n%s", code, exitZeroCandidate, out.String())
	}
	if !strings.Contains(out.String(), "candidates=0") {
		t.Fatalf("second-run census not empty:\n%s", out.String())
	}
}

func TestReviveEntriesRejectsBogusStateDir(t *testing.T) {
	dir := t.TempDir() // no db/ inside
	var out, errOut bytes.Buffer
	if code := run(testOptions(dir, false), &out, &errOut); code != exitBadState {
		t.Fatalf("exit = %d, want %d", code, exitBadState)
	}
	if !strings.Contains(errOut.String(), "missing db/") {
		t.Errorf("missing db/ diagnostic, got:\n%s", errOut.String())
	}
}

func TestSelectorWindowAndPatterns(t *testing.T) {
	from, _ := time.Parse(time.RFC3339, defaultFrom)
	to, _ := time.Parse(time.RFC3339, defaultTo)

	base := func() *storage.Entry {
		return erroredNZBEntry("x", patternStatSegment, inWindow, 1, false)
	}

	if e := base(); !matchesSelector(e, from, to, 3) {
		t.Error("baseline in-window stat-segment entry must match")
	}
	if e := base(); matchesSelector(e, from, to, 0) {
		t.Error("ErrorCount above max-errors must not match")
	}
	e := base()
	e.LastErrorTime = nil
	if matchesSelector(e, from, to, 3) {
		t.Error("nil LastErrorTime must not match")
	}
	e = base()
	e.Protocol = config.ProtocolTorrent
	if matchesSelector(e, from, to, 3) {
		t.Error("torrent entries must not match")
	}
	e = base()
	e.State = storage.EntryStatePausedUP
	if matchesSelector(e, from, to, 3) {
		t.Error("non-error entries must not match")
	}
	e = base()
	e.LastError = "some unrelated failure"
	if matchesSelector(e, from, to, 3) {
		t.Error("unlisted error patterns must not match")
	}
	boundary := erroredNZBEntry("x", patternNoGroups+" 2 retries", from, 1, false)
	if !matchesSelector(boundary, from, to, 3) {
		t.Error("window boundaries are inclusive")
	}

	// The dominant incident cohort: the exact Process-phase production string
	// at the census timestamp (05:47:00, inside the default window).
	production := erroredNZBEntry("x", archiveProductionError, archWindow, 1, false)
	if !matchesSelector(production, from, to, 3) {
		t.Error("the archive-processing production string at 05:47:00 must match")
	}
	gated := erroredNZBEntry("x", "failed to process nzb: failed to process NZB archives: availability probe failed: provider connectivity problem: no valid files found in NZB after 3 file group failure(s)", archWindow, 1, false)
	if !matchesSelector(gated, from, to, 3) {
		t.Error("the gated archive-processing infrastructure string must match")
	}
}

package manager

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// These tests pin the observability contract of the action components. The
// governing rule is: a component that does not act must SAY SO. A run that
// reports pruned=0 / reacquired=0 with every skip counter also at zero is
// indistinguishable from a component that is simply broken — which is exactly
// how a production operator concluded "PRUNE is broken" while PRUNE was in fact
// correctly refusing to delete partially-broken entries, and how
// /api/repair/fix looked inert while it was silently dropping every nzb
// candidate.

// seedPartiallyBrokenEntry persists a 3-file entry of which exactly ONE file is
// broken. PRUNE must refuse it (deleting the entry would take the two healthy
// files' symlinks with it) — the behaviour under test is that the refusal is
// counted and explained, NOT that it stops happening.
func seedPartiallyBrokenEntry(t *testing.T, m *Manager, hash, name string) {
	t.Helper()
	now := time.Unix(1_700_000_000, 0).UTC()
	entry := &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       hash,
		Name:           name,
		SavePath:       t.TempDir(),
		ActiveProvider: "prov",
		Status:         debridTypes.TorrentStatusDownloaded,
		IsComplete:     true,
		AddedOn:        now,
		CreatedAt:      now,
		UpdatedAt:      now,
		Files: map[string]*storage.File{
			"a.mkv": {Name: "a.mkv", InfoHash: hash, Size: 100, AddedOn: now},
			"b.mkv": {Name: "b.mkv", InfoHash: hash, Size: 100, AddedOn: now},
			"c.mkv": {Name: "c.mkv", InfoHash: hash, Size: 100, AddedOn: now},
		},
	}
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate(%s): %v", hash, err)
	}
	h := &storage.EntryHealth{
		EntryName: name,
		Protocol:  config.ProtocolTorrent,
		Status:    storage.HealthBroken,
		FileCount: 3,
		BrokenFiles: []storage.BrokenFile{{
			EntryName: name,
			FileName:  "b.mkv",
			InfoHash:  hash,
			Protocol:  config.ProtocolTorrent,
			Reason:    "hoster_unavailable",
		}},
	}
	if err := m.storage.SaveEntryHealth(h); err != nil {
		t.Fatalf("SaveEntryHealth(%s): %v", name, err)
	}
}

// seedBrokenNZBEntry persists a fully-broken single-file USENET entry: an entry
// row whose protocol is nzb (so Entry.CanBeFixed() is false) plus a broken
// EntryHealth. This is the shape that made /api/repair/fix look inert.
func seedBrokenNZBEntry(t *testing.T, m *Manager, hash, name string) {
	t.Helper()
	now := time.Unix(1_700_000_000, 0).UTC()
	entry := &storage.Entry{
		Protocol:   config.ProtocolNZB,
		InfoHash:   hash,
		Name:       name,
		SavePath:   t.TempDir(),
		Status:     debridTypes.TorrentStatusDownloaded,
		IsComplete: true,
		AddedOn:    now,
		CreatedAt:  now,
		UpdatedAt:  now,
		Files: map[string]*storage.File{
			"file.mkv": {Name: "file.mkv", InfoHash: hash, Size: 100, AddedOn: now},
		},
	}
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate(%s): %v", hash, err)
	}
	h := &storage.EntryHealth{
		EntryName: name,
		Protocol:  config.ProtocolNZB,
		Status:    storage.HealthBroken,
		FileCount: 1,
		BrokenFiles: []storage.BrokenFile{{
			EntryName: name,
			FileName:  "file.mkv",
			InfoHash:  hash,
			Protocol:  config.ProtocolNZB,
			Reason:    "usenet_segment_missing",
		}},
	}
	if err := m.storage.SaveEntryHealth(h); err != nil {
		t.Fatalf("SaveEntryHealth(%s): %v", name, err)
	}
}

// ---------------------------------------------------------------------------
// FIX 1 — PRUNE declines correctly, but must not decline SILENTLY.
// ---------------------------------------------------------------------------

// TestPruneIneligibleReasons pins the eligibility policy AND the reason it
// reports. The policy itself is unchanged: only a fully-broken entry carrying
// at least one infohash may be deleted decypharr-side.
func TestPruneIneligibleReasons(t *testing.T) {
	cases := []struct {
		name string
		h    *storage.EntryHealth
		want string
	}{
		{"nil_record", nil, reasonPruneNoBrokenFiles},
		{"nothing_broken", &storage.EntryHealth{FileCount: 4}, reasonPruneNoBrokenFiles},
		{
			"partial_entry_is_declined",
			&storage.EntryHealth{FileCount: 13, BrokenCount: 5, BrokenFiles: []storage.BrokenFile{{InfoHash: "h"}}},
			reasonPrunePartialEntry,
		},
		{
			"fully_broken_without_infohash",
			&storage.EntryHealth{FileCount: 1, BrokenCount: 1, BrokenFiles: []storage.BrokenFile{{}}},
			reasonPruneNoInfohash,
		},
		{
			"fully_broken_with_infohash_is_eligible",
			&storage.EntryHealth{FileCount: 2, BrokenCount: 2, BrokenFiles: []storage.BrokenFile{{}, {InfoHash: "h"}}},
			"",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pruneIneligibleReason(tc.h); got != tc.want {
				t.Fatalf("pruneIneligibleReason = %q, want %q", got, tc.want)
			}
			if got := pruneEligible(tc.h); got != (tc.want == "") {
				t.Fatalf("pruneEligible = %v, want %v", got, tc.want == "")
			}
		})
	}
}

// TestPruneDeclineIsCountedAndExplained is the headline FIX 1 case, modelled on
// the production run: PRUNE enabled, entries broken, pruned=0 — and previously
// nothing at all to distinguish "declined correctly" from "broken".
func TestPruneDeclineIsCountedAndExplained(t *testing.T) {
	m, r, arrSrv, _ := newRepairCapFixture(t, 0)

	seedPartiallyBrokenEntry(t, m, "partial-a", "PartialA")
	h, err := m.storage.GetEntryHealth("PartialA")
	if err != nil || h == nil {
		t.Fatalf("GetEntryHealth: %v", err)
	}

	run := newRun(t, m)
	var mu sync.Mutex
	r.actOnDeadEntry(context.Background(), run, &mu, "PartialA", h, repairActions{prune: true}, r.newDeletionBudget(run.ID))

	// The refusal itself is CORRECT and must not change.
	if !entryExists(t, m, "partial-a") {
		t.Fatal("PRUNE deleted a partially-broken entry; it must keep the surviving files' symlinks")
	}
	if got := arrSrv.totalCalls(); got != 0 {
		t.Fatalf("PRUNE made %d arr calls, want 0", got)
	}

	// ...and it is now visible.
	if run.Stats.Pruned != 0 {
		t.Fatalf("Stats.Pruned = %d, want 0", run.Stats.Pruned)
	}
	if run.Stats.PruneSkippedNotEligible != 1 {
		t.Fatalf("Stats.PruneSkippedNotEligible = %d, want 1 (the decline must be counted)", run.Stats.PruneSkippedNotEligible)
	}
	saved, err := m.storage.GetEntryHealth("PartialA")
	if err != nil || saved == nil {
		t.Fatalf("GetEntryHealth after action: %v", err)
	}
	if got := saved.ActionSkips[componentPrune]; got != reasonPrunePartialEntry {
		t.Fatalf("health ActionSkips[prune] = %q, want %q", got, reasonPrunePartialEntry)
	}

	// The counter must reach the persisted run record, not just the in-memory
	// struct, or the UI/API still cannot show it.
	persisted, err := m.storage.GetRepairRun(run.ID)
	if err != nil || persisted == nil {
		t.Fatalf("GetRepairRun: %v", err)
	}
	if persisted.Stats.PruneSkippedNotEligible != 1 {
		t.Fatalf("persisted Stats.PruneSkippedNotEligible = %d, want 1", persisted.Stats.PruneSkippedNotEligible)
	}
}

// TestRegrabDeclineIsCountedAndExplained is the same treatment for the other
// silent decline on the destructive path: RE-GRAB with no resolved arr link.
func TestRegrabDeclineIsCountedAndExplained(t *testing.T) {
	m, r, arrSrv, _ := newRepairCapFixture(t, 0)

	// seedManagedEntry has NO arr linkage at all, so RE-GRAB cannot route it.
	seedManagedEntry(t, m, "noarr-a", "NoArrA")
	h := &storage.EntryHealth{
		EntryName:   "NoArrA",
		Status:      storage.HealthBroken,
		FileCount:   1,
		BrokenFiles: []storage.BrokenFile{{EntryName: "NoArrA", FileName: "file.mkv", InfoHash: "noarr-a"}},
	}
	if err := m.storage.SaveEntryHealth(h); err != nil {
		t.Fatalf("SaveEntryHealth: %v", err)
	}

	run := newRun(t, m)
	var mu sync.Mutex
	r.actOnDeadEntry(context.Background(), run, &mu, "NoArrA", h, repairActions{regrab: true}, r.newDeletionBudget(run.ID))

	if got := arrSrv.totalCalls(); got != 0 {
		t.Fatalf("RE-GRAB made %d arr calls with no arr link, want 0", got)
	}
	if run.Stats.RegrabSkippedNoArrLink != 1 {
		t.Fatalf("Stats.RegrabSkippedNoArrLink = %d, want 1", run.Stats.RegrabSkippedNoArrLink)
	}
	saved, _ := m.storage.GetEntryHealth("NoArrA")
	if saved == nil || saved.ActionSkips[componentRegrab] != reasonRegrabNoArrLink {
		t.Fatalf("health ActionSkips[regrab] = %v, want %q", saved.ActionSkips, reasonRegrabNoArrLink)
	}
}

// ---------------------------------------------------------------------------
// FIX 2 — a FAILED repair must be distinguishable from a never-attempted one.
// ---------------------------------------------------------------------------

// TestFailedRepairIsCountedAndStamped pins the exact production confusion:
// reacquired=0 AND repair_failed=0 AND last_repair_at=never, all at once. The
// fixture has no Fixer wired, so ReinsertEntry deterministically errors.
func TestFailedRepairIsCountedAndStamped(t *testing.T) {
	m, r, arrSrv, _ := newRepairCapFixture(t, 0)
	if m.fixer != nil {
		t.Fatal("fixture unexpectedly has a Fixer; this test needs re-insert to fail")
	}
	seedBrokenEntry(t, m, "failrep-a", "FailRepA")

	run, err := r.FixBroken(context.Background(), nil, &ManualActions{Repair: true})
	if err != nil {
		t.Fatalf("FixBroken(repair): %v", err)
	}
	waitRunComplete(t, m, run.ID)

	final, err := m.storage.GetRepairRun(run.ID)
	if err != nil || final == nil {
		t.Fatalf("GetRepairRun: %v", err)
	}
	if final.Stats.Reacquired != 0 {
		t.Fatalf("Stats.Reacquired = %d, want 0 (the re-insert failed)", final.Stats.Reacquired)
	}
	if final.Stats.RepairFailed != 1 {
		t.Fatalf("Stats.RepairFailed = %d, want 1 (a failed attempt must be counted)", final.Stats.RepairFailed)
	}
	if final.Stats.RepairSkippedUnsupported != 0 {
		t.Fatalf("Stats.RepairSkippedUnsupported = %d, want 0 (a torrent IS supported; it just failed)", final.Stats.RepairSkippedUnsupported)
	}

	h, err := m.storage.GetEntryHealth("FailRepA")
	if err != nil || h == nil {
		t.Fatalf("GetEntryHealth: %v", err)
	}
	if h.LastRepairAt.IsZero() {
		t.Fatal("LastRepairAt is zero after a repair ATTEMPT; a failed repair is indistinguishable from no repair")
	}
	if h.LastRepairError == "" {
		t.Fatal("LastRepairError is empty after a failed repair attempt")
	}
	if h.Status != storage.HealthBroken {
		t.Fatalf("health status = %q, want broken (a failed repair must not clear the verdict)", h.Status)
	}
	if !entryExists(t, m, "failrep-a") {
		t.Fatal("REPAIR-only run deleted an entry")
	}
	if got := arrSrv.totalCalls(); got != 0 {
		t.Fatalf("REPAIR-only run made %d arr calls, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// FIX 2 / FIX 4 — nzb is NOT repairable, and that must be stated, not hidden.
// ---------------------------------------------------------------------------

// TestNZBRepairIsExplicitlyUnsupported pins the decision documented on
// autoHealResults: REPAIR means "re-insert across debrid providers"
// (Entry.CanBeFixed() == IsTorrent()), which usenet has no analogue for. The
// requirement is therefore NOT that nzb becomes repairable — it is that the
// exclusion is counted, named, and reachable from the health record instead of
// being a bare `continue`.
func TestNZBRepairIsExplicitlyUnsupported(t *testing.T) {
	m, r, arrSrv, _ := newRepairCapFixture(t, 0)

	seedBrokenNZBEntry(t, m, "nzbrep-a", "NzbRepA")

	run, err := r.FixBroken(context.Background(), nil, &ManualActions{Repair: true})
	if err != nil {
		t.Fatalf("FixBroken(repair): %v", err)
	}
	waitRunComplete(t, m, run.ID)

	final, err := m.storage.GetRepairRun(run.ID)
	if err != nil || final == nil {
		t.Fatalf("GetRepairRun: %v", err)
	}
	if final.Stats.RepairSkippedUnsupported != 1 {
		t.Fatalf("Stats.RepairSkippedUnsupported = %d, want 1 (nzb exclusion must be counted, never silent)", final.Stats.RepairSkippedUnsupported)
	}
	if final.Stats.RepairFailed != 0 {
		t.Fatalf("Stats.RepairFailed = %d, want 0 (nothing was attempted)", final.Stats.RepairFailed)
	}
	if final.Stats.Reacquired != 0 {
		t.Fatalf("Stats.Reacquired = %d, want 0", final.Stats.Reacquired)
	}

	h, err := m.storage.GetEntryHealth("NzbRepA")
	if err != nil || h == nil {
		t.Fatalf("GetEntryHealth: %v", err)
	}
	skip := h.ActionSkips[componentRepair]
	if !strings.HasPrefix(skip, reasonRepairUnsupportedProtocol) {
		t.Fatalf("health ActionSkips[repair] = %q, want prefix %q", skip, reasonRepairUnsupportedProtocol)
	}
	if !strings.Contains(skip, string(config.ProtocolNZB)) {
		t.Fatalf("health ActionSkips[repair] = %q, want it to name the protocol", skip)
	}
	if !h.LastRepairAt.IsZero() {
		t.Fatal("LastRepairAt was stamped although no repair was ATTEMPTED")
	}
	if !entryExists(t, m, "nzbrep-a") {
		t.Fatal("an unsupported REPAIR deleted the entry")
	}
	if got := arrSrv.totalCalls(); got != 0 {
		t.Fatalf("REPAIR-only run made %d arr calls, want 0", got)
	}
}

// TestFixBrokenActsOnTorrentsAndExplainsNZB is the FIX 4 regression test. The
// old batch selector kept only BrokenFiles whose Protocol was exactly
// `torrent`, then `return false` on an empty set with no counter and no log —
// so a fix-broken run over nzb candidates completed instantly with every
// counter at zero and started_at == completed_at.
//
// The mixed set proves both halves: the torrent is genuinely acted on (so the
// filter is not just dropping everything), and the nzb entries are reported.
func TestFixBrokenActsOnTorrentsAndExplainsNZB(t *testing.T) {
	m, r, arrSrv := healingFixture(t)

	seedBrokenEntry(t, m, "mixed-torrent", "MixedTorrent")
	seedBrokenNZBEntry(t, m, "mixed-nzb-1", "MixedNzb1")
	seedBrokenNZBEntry(t, m, "mixed-nzb-2", "MixedNzb2")

	run, err := r.FixBroken(context.Background(), nil, &ManualActions{Repair: true})
	if err != nil {
		t.Fatalf("FixBroken(repair): %v", err)
	}
	waitRunComplete(t, m, run.ID)

	final, err := m.storage.GetRepairRun(run.ID)
	if err != nil || final == nil {
		t.Fatalf("GetRepairRun: %v", err)
	}
	if final.Stats.Candidates != 3 {
		t.Fatalf("Stats.Candidates = %d, want 3", final.Stats.Candidates)
	}
	if final.Stats.Reacquired != 1 {
		t.Fatalf("Stats.Reacquired = %d, want 1 (the torrent must actually be acted on)", final.Stats.Reacquired)
	}
	if final.Stats.RepairSkippedUnsupported != 2 {
		t.Fatalf("Stats.RepairSkippedUnsupported = %d, want 2 (both nzb entries reported)", final.Stats.RepairSkippedUnsupported)
	}
	// The decisive assertion: a run over broken candidates can no longer come
	// back with every action counter at zero and nothing said about why.
	if final.Stats.Reacquired+final.Stats.RepairFailed+final.Stats.RepairSkippedUnsupported == 0 {
		t.Fatal("fix-broken run reported no action and no reason on 3 broken candidates")
	}
	if got := arrSrv.totalCalls(); got != 0 {
		t.Fatalf("REPAIR-only run made %d arr calls, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// FIX 3 — a verdict that can never change must not be re-probed forever.
// ---------------------------------------------------------------------------

// TestZeroFileEntryIsStructurallyBroken pins the classification of an entry
// that no probe can ever resolve.
//
// LOUD NOTE: this records `broken`, which is the destructive-eligible class.
// The assertions below pin the containment that makes that safe — a zero-file
// entry carries zero BrokenFiles, so PRUNE and RE-GRAB both decline it.
func TestZeroFileEntryIsStructurallyBroken(t *testing.T) {
	cases := []struct {
		name  string
		files map[string]*storage.File
	}{
		{"no_files_listed", map[string]*storage.File{}},
		{"every_file_soft_deleted", map[string]*storage.File{
			"gone.mkv": {Name: "gone.mkv", InfoHash: "zf", Size: 10, Deleted: true},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, r, arrSrv, _ := newRepairCapFixture(t, 0)
			entryName := "Structural" + tc.name
			c := &candidate{name: entryName, item: &storage.EntryItem{Name: entryName, Files: tc.files}}

			h, attempt := r.probeEntry(context.Background(), "run-structural", c, newHealCache(), RepairRunOptions{}, true)
			if h == nil {
				t.Fatal("probeEntry returned nil for a readable zero-file entry; it must record a verdict")
			}
			if attempt.attempted {
				t.Fatal("a zero-file entry triggered a repair attempt")
			}
			if h.Status != storage.HealthBroken {
				t.Fatalf("status = %q, want broken (nothing can be served from it, and no probe can ever say otherwise)", h.Status)
			}
			if !h.Structural {
				t.Fatal("Structural = false; the verdict must be marked terminal or it stays on the every-run treadmill")
			}
			if h.FailureReason != reasonEntryHasNoFiles {
				t.Fatalf("FailureReason = %q, want %q", h.FailureReason, reasonEntryHasNoFiles)
			}
			if h.LastFailedAt.IsZero() {
				t.Fatal("LastFailedAt is zero; the production symptom was last_ok_at AND last_failed_at both never set")
			}

			// It must leave the every-run re-probe treadmill: not due again
			// until the full recheck interval (or a file-set change, which
			// sets Dirty).
			saved, err := m.storage.GetEntryHealth(entryName)
			if err != nil || saved == nil {
				t.Fatalf("GetEntryHealth: %v", err)
			}
			recheck := 7 * 24 * time.Hour
			if saved.IsDue(time.Now(), recheck) {
				t.Fatal("a structural verdict is still due every run (permanent no-op treadmill)")
			}
			if !saved.IsDue(time.Now().Add(recheck+time.Minute), recheck) {
				t.Fatal("a structural verdict is never revisited; the staleness backstop must still apply")
			}
			saved.Dirty = true
			if !saved.IsDue(time.Now(), recheck) {
				t.Fatal("a dirty structural entry must be due immediately; Dirty is the change-detector this relies on")
			}

			// CONTAINMENT: broken, but not deletable.
			if pruneEligible(saved) {
				t.Fatal("a zero-file entry is prune-eligible; it must not be deletable (no infohash, no broken files)")
			}
			run := newRun(t, m)
			var mu sync.Mutex
			r.actOnDeadEntry(context.Background(), run, &mu, entryName, saved, repairActions{prune: true, regrab: true}, r.newDeletionBudget(run.ID))
			if run.Stats.Pruned != 0 || run.Stats.Regrabbed != 0 {
				t.Fatalf("destructive action ran on a zero-file entry: pruned=%d regrabbed=%d", run.Stats.Pruned, run.Stats.Regrabbed)
			}
			if run.Stats.Deletions != 0 {
				t.Fatalf("Stats.Deletions = %d, want 0", run.Stats.Deletions)
			}
			if run.Stats.PruneSkippedNotEligible != 1 {
				t.Fatalf("Stats.PruneSkippedNotEligible = %d, want 1 (the decline must still be visible)", run.Stats.PruneSkippedNotEligible)
			}
			if got := arrSrv.totalCalls(); got != 0 {
				t.Fatalf("zero-file entry caused %d arr calls, want 0", got)
			}
		})
	}
}

// TestBrokenVerdictStaysAlwaysDue is the negative space of the structural
// change: a non-structural BROKEN entry must still be re-picked every run,
// because that is what lets a deletion-cap-skipped entry get its action on the
// next run and what makes a recovered entry flip back to healthy promptly.
func TestBrokenVerdictStaysAlwaysDue(t *testing.T) {
	now := time.Now()
	recheck := 7 * 24 * time.Hour

	broken := &storage.EntryHealth{
		Status:         storage.HealthBroken,
		ProbeVersion:   storage.RepairProbeVersion,
		LastCheckedAt:  now,
		NextCheckDueAt: now.Add(recheck),
	}
	if !broken.IsDue(now, recheck) {
		t.Fatal("a broken entry is not due; the deletion-cap retry depends on it being re-picked every run")
	}

	// A healthy entry keeps its freshness skip. ProbeVersion is set because the
	// freshness skip is only offered to verdicts from the CURRENT probe — see
	// storage.RepairProbeVersion. A record without it is one written by an older
	// algorithm and is due on sight; TestStaleProbeVersionIsDueDespiteFreshness
	// covers that case.
	healthy := &storage.EntryHealth{
		Status:        storage.HealthHealthy,
		ProbeVersion:  storage.RepairProbeVersion,
		LastCheckedAt: now,
	}
	if healthy.IsDue(now, recheck) {
		t.Fatal("a freshly-probed healthy entry must be skipped as fresh")
	}
}

// TestIndeterminateRetryDeadlineIsHonoured pins that the short retry deadline
// the probe writes for an INDETERMINATE verdict actually gates scheduling.
// NextCheckDueAt was written by every probe and read by nothing, so `unknown`
// bypassed the freshness skip on every run — probe, stamp, return the same
// non-verdict, forever, which is exactly the 5-second no-op sweep the operator
// recorded. The deadline is short (6h) by design, so nothing hides for long.
func TestIndeterminateRetryDeadlineIsHonoured(t *testing.T) {
	now := time.Now()
	recheck := 7 * 24 * time.Hour

	// Every record here carries the CURRENT ProbeVersion: the retry deadline is a
	// property of a verdict, and only a verdict from the current probe algorithm
	// is trusted enough for its deadline to gate anything (see
	// storage.RepairProbeVersion). Without the stamp these records would be due
	// on version alone and the assertions below would prove nothing about the
	// deadline.
	pending := &storage.EntryHealth{
		Status:         storage.HealthUnknown,
		ProbeVersion:   storage.RepairProbeVersion,
		LastCheckedAt:  now,
		NextCheckDueAt: now.Add(repairIndeterminateRetry),
	}
	if pending.IsDue(now, recheck) {
		t.Fatal("an unknown entry probed seconds ago is due again already (permanent no-op treadmill)")
	}
	if !pending.IsDue(now.Add(repairIndeterminateRetry+time.Minute), recheck) {
		t.Fatal("an unknown entry is never re-probed after its retry deadline; unknown must not become a resting state")
	}

	// No deadline recorded (a cleared one) ⇒ always due, so the change can never
	// make an entry invisible.
	noDeadline := &storage.EntryHealth{
		Status:        storage.HealthUnknown,
		ProbeVersion:  storage.RepairProbeVersion,
		LastCheckedAt: now,
	}
	if !noDeadline.IsDue(now, recheck) {
		t.Fatal("an unknown entry with no retry deadline must stay always-due")
	}

	// Dirty always wins.
	dirty := &storage.EntryHealth{
		Status:         storage.HealthUnknown,
		ProbeVersion:   storage.RepairProbeVersion,
		Dirty:          true,
		LastCheckedAt:  now,
		NextCheckDueAt: now.Add(repairIndeterminateRetry),
	}
	if !dirty.IsDue(now, recheck) {
		t.Fatal("a dirty entry must be due immediately regardless of its retry deadline")
	}
}

// TestUnknownVerdictRecordsItsReason closes the other half of the `unknown`
// blind spot: the per-file reason that produced a non-verdict was computed by
// every probe path and then discarded, because only BROKEN results are
// flattened onto the health record. An operator with a library of `unknown`
// entries had an empty failure_reason and nothing to diagnose from.
func TestUnknownVerdictRecordsItsReason(t *testing.T) {
	m, r, _, _ := newRepairCapFixture(t, 0)

	// An entry whose active provider has no registered client probes
	// indeterminate ("provider_client_not_found"): never healthy, never broken.
	seedManagedEntry(t, m, "unk-a", "UnknownA")
	m.clients.Delete("prov")

	item, err := m.storage.GetEntryItem("UnknownA")
	if err != nil || item == nil {
		t.Fatalf("GetEntryItem: %v", err)
	}
	c := &candidate{name: "UnknownA", item: item}
	h, _ := r.probeEntry(context.Background(), "run-unknown", c, newHealCache(), RepairRunOptions{}, false)
	if h == nil {
		t.Fatal("probeEntry returned nil")
	}
	if h.Status != storage.HealthUnknown {
		t.Fatalf("status = %q, want unknown (a missing provider client is not a content verdict)", h.Status)
	}
	if h.FailureReason != "provider_client_not_found" {
		t.Fatalf("FailureReason = %q, want %q", h.FailureReason, "provider_client_not_found")
	}
	if h.Structural {
		t.Fatal("an indeterminate verdict must not be marked structural; it can change on the next run")
	}
}

// TestTopIndeterminateReasonIsDeterministic keeps the recorded reason stable
// across runs (Go map iteration order is not).
func TestTopIndeterminateReasonIsDeterministic(t *testing.T) {
	results := []fileResult{
		{reason: "zzz_reason"},
		{reason: "aaa_reason"},
		{healthy: true, reason: "ignored_healthy"},
		{broken: true, reason: "ignored_broken"},
	}
	for i := 0; i < 20; i++ {
		if got := topIndeterminateReason(results); got != "aaa_reason" {
			t.Fatalf("topIndeterminateReason = %q, want the deterministic tie-break %q", got, "aaa_reason")
		}
	}
	if got := topIndeterminateReason([]fileResult{{healthy: true}}); got != "" {
		t.Fatalf("topIndeterminateReason with no indeterminate results = %q, want empty", got)
	}
}

// ---------------------------------------------------------------------------
// FIX 5 — an EXPLICIT all-false selection runs nothing, at the source.
// ---------------------------------------------------------------------------

// TestExplicitAllFalseManualActionsRunsNothing pins the manager-level backstop.
// The HTTP layer rejects this shape with a 400 before calling in, but every
// non-HTTP caller reached resolveManualActions directly, where sel.any() could
// not tell "no components" from "no selection" and fell through to the
// configured — possibly destructive — knobs.
func TestExplicitAllFalseManualActionsRunsNothing(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	r := &Repair{logger: zerolog.Nop()}

	// The most dangerous configuration: every destructive knob on.
	on := true
	config.Get().Repair = config.RepairConfig{Repair: &on, Prune: true, Regrab: true}

	for _, fix := range []bool{false, true} {
		got := r.resolveManualActions(&ManualActions{}, fix)
		if got != (repairActions{}) {
			t.Fatalf("resolveManualActions(all-false, fix=%v) = %+v, want no components", fix, got)
		}
	}
	// A nil selection still falls back to the configured knobs (documented).
	if got := r.resolveManualActions(nil, true); got != (repairActions{repair: true, prune: true, regrab: true}) {
		t.Fatalf("resolveManualActions(nil, true) = %+v, want the configured knobs", got)
	}
	// And a partial selection is still honoured verbatim.
	if got := r.resolveManualActions(&ManualActions{Prune: true}, true); got != (repairActions{prune: true}) {
		t.Fatalf("resolveManualActions(prune-only) = %+v, want prune only", got)
	}
}

// TestFixBrokenRejectsExplicitAllFalse pins the end-to-end consequence: with
// PRUNE and RE-GRAB configured on, an explicit "no components" FixBroken must
// delete nothing and touch no arr, rather than running the configured knobs.
func TestFixBrokenRejectsExplicitAllFalse(t *testing.T) {
	m, r, arrSrv, _ := newRepairCapFixture(t, 0)
	on := true
	cfg := config.Get()
	cfg.Repair.Repair = &on
	cfg.Repair.Prune = true
	cfg.Repair.Regrab = true

	seedBrokenEntry(t, m, "allfalse-a", "AllFalseA")

	if _, err := r.FixBroken(context.Background(), nil, &ManualActions{}); err == nil {
		t.Fatal("FixBroken with an explicit all-false selection must error, not run the configured knobs")
	}
	if !entryExists(t, m, "allfalse-a") {
		t.Fatal("an explicit no-op FixBroken deleted an entry (the all-false footgun)")
	}
	if got := arrSrv.totalCalls(); got != 0 {
		t.Fatalf("an explicit no-op FixBroken made %d arr calls, want 0", got)
	}
}

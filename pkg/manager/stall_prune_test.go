package manager

import (
	"fmt"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Two stages, and they fail in different directions.
//
// Stage 1 (zero bytes for a window) is the trustworthy one: progress is
// monotonic, so the window needs no sampling and the verdict is not an
// estimate. Stage 2 (projected ETA over a ceiling) is a projection and can be
// wrong about a slow torrent that would have finished — which is why it is
// separately configurable, separately disabled by default, and refuses to act
// before the average means anything.
//
// The tests below mostly guard against the two ways this feature could delete
// something it should not: acting on a fluctuating input, and acting before
// there is enough evidence.

func stallSettings(mods func(*stallPruneSettings)) stallPruneSettings {
	s := stallPruneSettings{
		noProgressAfter: time.Hour,
		maxETA:          24 * time.Hour,
		minAge:          30 * time.Minute,
		maxPerSweep:     25,
	}
	if mods != nil {
		mods(&s)
	}
	return s
}

func stallEntry(mods func(*storage.Entry)) *storage.Entry {
	e := &storage.Entry{
		InfoHash: "abc",
		Name:     "Some.Release",
		Protocol: config.ProtocolTorrent,
		Status:   debridTypes.TorrentStatusDownloading,
		State:    storage.EntryStateDownloading,
		Size:     1000,
		Progress: 0,
		AddedOn:  time.Now().Add(-2 * time.Hour),
	}
	if mods != nil {
		mods(e)
	}
	return e
}

// --- stage 1 -------------------------------------------------------------

func TestStage1PrunesZeroBytesOverTheWindow(t *testing.T) {
	if prunableReason(stallEntry(nil), stallSettings(nil), time.Now()) == "" {
		t.Fatal("a torrent the provider calls 'downloading' with zero bytes after 2h must be prunable")
	}
}

// Seeders must not enter the predicate in either direction. A stalled torrent
// reporting seeders is still stalled — if those seeders were useful, bytes
// would have moved — and a moving torrent reporting none is still moving.
func TestSeedersAreNotPartOfThePredicate(t *testing.T) {
	withSeeders := stallEntry(func(e *storage.Entry) { e.Seeders = 12 })
	if prunableReason(withSeeders, stallSettings(nil), time.Now()) == "" {
		t.Fatal("seeders must not rescue an entry that moved zero bytes for the window")
	}

	// Moving fast, no seeders reported, young enough that stage 2 cannot act.
	moving := stallEntry(func(e *storage.Entry) {
		e.Seeders = 0
		e.Progress = 0.9
		e.AddedOn = time.Now().Add(-10 * time.Minute)
	})
	if got := prunableReason(moving, stallSettings(nil), time.Now()); got != "" {
		t.Fatalf("reason = %q: a transferring entry must never be pruned, whatever its seeder count says", got)
	}
}

func TestStage1DoesNotActBeforeItsWindow(t *testing.T) {
	fresh := stallEntry(func(e *storage.Entry) { e.AddedOn = time.Now().Add(-5 * time.Minute) })
	if got := prunableReason(fresh, stallSettings(nil), time.Now()); got != "" {
		t.Fatalf("reason = %q, want empty: zero progress is the NORMAL state of a recent add, and the "+
			"window is the entire diagnostic content", got)
	}
}

// --- stage 2 -------------------------------------------------------------

// A torrent crawling badly enough to project past the ceiling is prunable — but
// the projection must use the LIFETIME AVERAGE, so a momentary spike cannot
// make a dead torrent look healthy.
func TestStage2PrunesOnProjectedETAUsingTheAverage(t *testing.T) {
	// 1 GB, 1% done over 2h => ~1.4 KB/s average => ~200h remaining.
	crawling := stallEntry(func(e *storage.Entry) {
		e.Size = 1_000_000_000
		e.Progress = 0.01
		e.Speed = 50_000_000 // instantaneous burst that would project ~20s
	})

	// Stage 1 must not be what fires here: there IS progress.
	stage2Only := stallSettings(func(s *stallPruneSettings) { s.noProgressAfter = 0 })
	if prunableReason(crawling, stage2Only, time.Now()) == "" {
		t.Fatal("a torrent projecting ~200h at its average rate must exceed a 24h ceiling — using the " +
			"instantaneous 50 MB/s burst instead would have called it 20 seconds from done")
	}
}

// The grace period is what stops every new torrent being deleted on arrival: a
// lifetime average over a few seconds projects to an absurd ETA.
func TestStage2WaitsForTheAverageToMeanSomething(t *testing.T) {
	newborn := stallEntry(func(e *storage.Entry) {
		e.Size = 1_000_000_000
		e.Progress = 0.0001
		e.AddedOn = time.Now().Add(-20 * time.Second)
	})
	stage2Only := stallSettings(func(s *stallPruneSettings) { s.noProgressAfter = 0 })

	if got := prunableReason(newborn, stage2Only, time.Now()); got != "" {
		t.Fatalf("reason = %q: a 20-second-old torrent projects absurdly and must be protected by MinAge", got)
	}
}

// An unknown ETA is not stage 2's verdict to make. Zero rate means "nothing to
// extrapolate from", which is stage 1's question; stage 2 must not invent a
// projection it does not have.
func TestStage2DoesNotActOnAnUnknownETA(t *testing.T) {
	stalled := stallEntry(nil) // zero progress => average 0 => EtaUnknown
	stage2Only := stallSettings(func(s *stallPruneSettings) { s.noProgressAfter = 0 })

	if got := prunableReason(stalled, stage2Only, time.Now()); got != "" {
		t.Fatalf("reason = %q: with stage 1 disabled, an unknown ETA must not be treated as an infinite one", got)
	}
}

// --- shared guards -------------------------------------------------------

func TestNeverPrunesWhatItCannotJudge(t *testing.T) {
	cases := []struct {
		name  string
		entry *storage.Entry
	}{
		{"provider has it queued, not downloading", stallEntry(func(e *storage.Entry) {
			e.Status = debridTypes.TorrentStatusQueued
		})},
		{"already downloaded", stallEntry(func(e *storage.Entry) {
			e.Status = debridTypes.TorrentStatusDownloaded
			e.Progress = 1
		})},
		{"no added timestamp to measure against", stallEntry(func(e *storage.Entry) { e.AddedOn = time.Time{} })},
		{"usenet: no swarm, own add-time gate", stallEntry(func(e *storage.Entry) {
			e.Protocol = config.ProtocolNZB
		})},
		{"nil entry", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := prunableReason(tc.entry, stallSettings(nil), time.Now()); got != "" {
				t.Fatalf("reason = %q, want empty — this entry must not be deleted", got)
			}
		})
	}
}

func TestBothStagesDisabledPrunesNothing(t *testing.T) {
	off := stallSettings(func(s *stallPruneSettings) {
		s.noProgressAfter = 0
		s.maxETA = 0
	})
	if got := prunableReason(stallEntry(nil), off, time.Now()); got != "" {
		t.Fatalf("reason = %q: with every stage disabled nothing may be deleted", got)
	}
}

// --- configuration -------------------------------------------------------

// For a destructive feature an unreadable setting must mean "do nothing", never
// "fall back to a default". This is the opposite of how the rest of the config
// resolves, and it is deliberate.
func TestUnparseableThresholdsDisableTheirStage(t *testing.T) {
	s := resolveStallPruneSettings(config.StallPruneConfig{
		NoProgressAfter: "not-a-duration",
		MaxETA:          "also-nonsense",
	})
	if s.enabled() {
		t.Fatal("unparseable thresholds must disable their stages, not resolve to a guessed default")
	}
	if got := prunableReason(stallEntry(nil), s, time.Now()); got != "" {
		t.Fatalf("reason = %q, want empty", got)
	}
}

func TestEmptyConfigIsFullyDisabled(t *testing.T) {
	if resolveStallPruneSettings(config.StallPruneConfig{}).enabled() {
		t.Fatal("stall pruning must be off unless explicitly configured — it deletes data")
	}
}

func TestOmittedSafetyKnobsGetDefaultsRatherThanZero(t *testing.T) {
	s := resolveStallPruneSettings(config.StallPruneConfig{MaxETA: "24h"})

	if s.minAge <= 0 {
		t.Fatal("MinAge must default rather than resolve to 0: a zero grace period deletes every new " +
			"torrent on arrival, because a lifetime average over seconds projects absurdly")
	}
	if s.maxPerSweep <= 0 {
		t.Fatal("MaxPerSweep must default rather than resolve to 0")
	}
	if s.noProgressAfter != 0 {
		t.Fatal("stage 1 must stay disabled when only MaxETA was configured — stages are independent")
	}
}

// The operator's requirement, in his words: "if you just PRUNE it without
// reporting it as FAILED, the arr doesnt see a failure and can't react to it."
//
// An earlier version of this feature called DeleteEntry, which freed the
// provider slot and told the arr nothing. The arr kept a queue row for a
// download that no longer existed anywhere, believed it was still progressing,
// and would never re-grab — turning a stalled torrent into a permanently
// missing episode. That is worse than leaving the stall in place, because a
// stall is at least visible.
//
// MarkAsError is the path every other failure in decypharr takes to reach the
// arr: it sets EntryStateError, which the qBittorrent shim reports as state
// "error". This asserts the entry ends up in exactly that state rather than
// being deleted out from under the arr.
func TestPrunedEntryIsFailedSoTheArrCanSeeIt(t *testing.T) {
	entry := stallEntry(nil)

	entry.MarkAsError(fmt.Errorf("stall prune: no bytes transferred in 2h"))

	if entry.State != storage.EntryStateError {
		t.Fatalf("State = %q, want %q: the qbit shim reports State verbatim, and it is the only signal "+
			"the arr has that this download failed", entry.State, storage.EntryStateError)
	}
	if entry.Status != debridTypes.TorrentStatusError {
		t.Fatalf("Status = %q, want error", entry.Status)
	}
	if entry.LastError == "" {
		t.Fatal("LastError must record why, or the operator cannot tell a stall prune from any other failure")
	}
	if entry.IsDownloading {
		t.Fatal("a failed entry must not still claim to be downloading")
	}
}

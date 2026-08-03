package manager

import (
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// STAGE 3 — the seeder gate.
//
// Measured on 107 live RealDebrid transfers: 0 seeders stalls 79% of the time,
// 1-2 stalls 24%, 3+ stalls 27%. The cliff is entirely between 0 and 1, so the
// threshold is 1 and "more is safer" is exactly wrong.
//
// The risk this stage carries is that a seeder count is a proxy for an outcome
// while progress IS that outcome. The tests below mostly pin the guards that
// keep the proxy from overruling the measurement.

func seederSettings(mods func(*stallPruneSettings)) stallPruneSettings {
	s := stallSettings(nil)
	s.minSeeders = 1
	s.seederGrace = 10 * time.Minute
	if mods != nil {
		mods(&s)
	}
	return s
}

// TestSeederStagePrunesADeadSwarmEarly is the feature: stage 1's window has not
// elapsed, but a torrent with no seeders and no bytes has nowhere to start from.
func TestSeederStagePrunesADeadSwarmEarly(t *testing.T) {
	e := stallEntry(func(e *storage.Entry) {
		e.AddedOn = time.Now().Add(-15 * time.Minute) // under stage 1's 1h
		e.Progress = 0
		e.Seeders = 0
	})
	if got := prunableReason(e, seederSettings(nil), time.Now()); got == "" {
		t.Fatal("a 15-minute-old torrent with zero seeders and zero bytes must be prunable")
	}
}

// TestSeederStageNeverTouchesAMovingTorrent is the guard that matters most. A
// seeder count fluctuates; bytes transferred do not. A momentary zero reading
// on a torrent that is working must never be fatal.
func TestSeederStageNeverTouchesAMovingTorrent(t *testing.T) {
	e := stallEntry(func(e *storage.Entry) {
		e.AddedOn = time.Now().Add(-15 * time.Minute)
		e.Progress = 0.3
		e.Seeders = 0
		e.Speed = 1 << 20
	})
	if got := prunableReason(e, seederSettings(nil), time.Now()); got != "" {
		t.Fatalf("reason = %q; a torrent that has moved bytes must not be judged on seeders", got)
	}
}

// TestSeederStageRespectsItsSettleWindow: RealDebrid reports 0 seeders at 0%
// on a transfer it has only just created, so sampling immediately would
// condemn every torrent on arrival.
func TestSeederStageRespectsItsSettleWindow(t *testing.T) {
	e := stallEntry(func(e *storage.Entry) {
		e.AddedOn = time.Now().Add(-30 * time.Second)
		e.Progress = 0
		e.Seeders = 0
	})
	if got := prunableReason(e, seederSettings(nil), time.Now()); got != "" {
		t.Fatalf("reason = %q; a just-created transfer always reads 0 seeders", got)
	}
}

// TestSeederStageKeepsATorrentWithASwarm: at or above the threshold is not this
// stage's business, however slow it is. Stage 1 and 2 still apply on their own
// windows.
func TestSeederStageKeepsATorrentWithASwarm(t *testing.T) {
	e := stallEntry(func(e *storage.Entry) {
		e.AddedOn = time.Now().Add(-15 * time.Minute)
		e.Progress = 0
		e.Seeders = 1
	})
	if got := prunableReason(e, seederSettings(nil), time.Now()); got != "" {
		t.Fatalf("reason = %q; 1 seeder meets the default threshold", got)
	}
}

// TestSeederStageCannotEnableTheSweep is the safety property. The threshold
// defaults to a non-zero value, so if it counted towards enabled() every
// operator who never configured stall pruning would suddenly have a
// data-deleting sweep running.
func TestSeederStageCannotEnableTheSweep(t *testing.T) {
	resolved := resolveStallPruneSettings(config.StallPruneConfig{})
	if resolved.minSeeders != config.DefaultStallPruneMinSeeders {
		t.Fatalf("minSeeders = %d, want the %d default", resolved.minSeeders, config.DefaultStallPruneMinSeeders)
	}
	if resolved.enabled() {
		t.Fatal("the seeder threshold turned stall pruning on by itself; it must only refine an enabled sweep")
	}

	e := stallEntry(func(e *storage.Entry) {
		e.AddedOn = time.Now().Add(-24 * time.Hour)
		e.Progress = 0
		e.Seeders = 0
	})
	if got := prunableReason(e, resolved, time.Now()); got != "" {
		t.Fatalf("reason = %q with the sweep disabled, want none", got)
	}
}

// TestMinSeedersIsTriState: absent takes the measured default, an explicit 0
// disables the stage. A plain int could not tell those apart, and the default
// being non-zero is exactly what makes the distinction load-bearing.
func TestMinSeedersIsTriState(t *testing.T) {
	zero := 0
	three := 3

	absent := resolveStallPruneSettings(config.StallPruneConfig{NoProgressAfter: "1h"})
	if absent.minSeeders != 1 {
		t.Fatalf("absent minSeeders = %d, want 1", absent.minSeeders)
	}

	off := resolveStallPruneSettings(config.StallPruneConfig{NoProgressAfter: "1h", MinSeeders: &zero})
	if off.minSeeders != 0 {
		t.Fatalf("explicit 0 resolved to %d; an operator turning the stage off must not be refilled by the default",
			off.minSeeders)
	}
	dead := stallEntry(func(e *storage.Entry) {
		e.AddedOn = time.Now().Add(-15 * time.Minute)
		e.Progress = 0
		e.Seeders = 0
	})
	if got := prunableReason(dead, off, time.Now()); got != "" {
		t.Fatalf("reason = %q with the seeder stage explicitly disabled", got)
	}

	custom := resolveStallPruneSettings(config.StallPruneConfig{NoProgressAfter: "1h", MinSeeders: &three})
	if custom.minSeeders != 3 {
		t.Fatalf("explicit 3 resolved to %d", custom.minSeeders)
	}
}

// TestSeederGraceDefaults: an absent or unreadable window falls back to the
// documented default rather than to zero, which would sample immediately.
func TestSeederGraceDefaults(t *testing.T) {
	for _, raw := range []string{"", "not-a-duration", "-5m"} {
		got := resolveStallPruneSettings(config.StallPruneConfig{NoProgressAfter: "1h", SeederGrace: raw})
		if got.seederGrace != 10*time.Minute {
			t.Fatalf("SeederGrace %q resolved to %s, want 10m", raw, got.seederGrace)
		}
	}
	got := resolveStallPruneSettings(config.StallPruneConfig{NoProgressAfter: "1h", SeederGrace: "45m"})
	if got.seederGrace != 45*time.Minute {
		t.Fatalf("seederGrace = %s, want 45m", got.seederGrace)
	}
}

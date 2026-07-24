package manager

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// These tests pin the component-explicit manual fix endpoints: a request may
// drive a SINGLE component (PRUNE-only, RE-GRAB-only, REPAIR-only), and an
// omitted selection falls back to the CONFIGURED REPAIR/PRUNE/RE-GRAB knobs —
// never the old force-all bundle. They also assert the run record carries the
// per-component outcome counters.

// TestResolveManualActionsPrecedence pins the precedence rules of
// resolveManualActions: explicit selection wins; omitted + fix falls back to the
// configured REPAIR/PRUNE/RE-GRAB knobs (NOT force-all); omitted + no fix is
// CHECK-only.
func TestResolveManualActionsPrecedence(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	r := &Repair{logger: zerolog.Nop()}

	on, off := true, false
	cases := []struct {
		name string
		sel  *ManualActions
		fix  bool
		cfg  config.RepairConfig
		want repairActions
	}{
		// An explicit single-component selection is honored verbatim, ignoring
		// config — this is what makes PRUNE-only / RE-GRAB-only invocation work.
		{"explicit_prune_only_wins", &ManualActions{Prune: true}, false,
			config.RepairConfig{Repair: &on, Prune: true, Regrab: true},
			repairActions{prune: true}},
		{"explicit_regrab_only", &ManualActions{Regrab: true}, false,
			config.RepairConfig{}, repairActions{regrab: true}},
		// Omitted selection + legacy fix:true → configured knobs. Configured
		// prune (repair defaults on, regrab off) → NOT force-all.
		{"omitted_fix_true_uses_configured_not_forceall", nil, true,
			config.RepairConfig{Prune: true},
			repairActions{repair: true, prune: true}},
		// All component knobs off + omitted selection → CHECK-only (the footgun
		// fix: the old code forced all three here).
		{"omitted_fix_true_all_knobs_off_check_only", nil, true,
			config.RepairConfig{Repair: &off, Prune: false, Regrab: false},
			repairActions{}},
		{"omitted_fix_false_check_only", nil, false,
			config.RepairConfig{Prune: true},
			repairActions{}},
		// A present-but-empty selection is treated as "unspecified" → fix path.
		{"present_all_false_falls_to_fix", &ManualActions{}, true,
			config.RepairConfig{Repair: &off, Regrab: true},
			repairActions{regrab: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config.Get().Repair = tc.cfg
			if got := r.resolveManualActions(tc.sel, tc.fix); got != tc.want {
				t.Fatalf("resolveManualActions(%+v, %v) = %+v, want %+v", tc.sel, tc.fix, got, tc.want)
			}
		})
	}
}

// TestFixBrokenPruneOnly pins single-component PRUNE via the manual endpoint:
// deletes decypharr-side, ZERO arr calls, and records Pruned / Deletions.
func TestFixBrokenPruneOnly(t *testing.T) {
	m, r, arrSrv, _ := newRepairCapFixture(t, 0)

	hashes := makeHashes("fbprune-", 3)
	for i, hash := range hashes {
		seedBrokenEntry(t, m, hash, "FbPrune"+string(rune('A'+i)))
	}

	run, err := r.FixBroken(context.Background(), nil, &ManualActions{Prune: true})
	if err != nil {
		t.Fatalf("FixBroken(prune): %v", err)
	}
	waitRunComplete(t, m, run.ID)

	if got := countExisting(t, m, hashes); got != 0 {
		t.Fatalf("%d/3 entries remain, want 0 (all pruned decypharr-side)", got)
	}
	if got := arrSrv.totalCalls(); got != 0 {
		t.Fatalf("PRUNE-only FixBroken made %d arr calls, want 0", got)
	}
	final, err := m.storage.GetRepairRun(run.ID)
	if err != nil || final == nil {
		t.Fatalf("GetRepairRun: %v", err)
	}
	if final.Stats.Pruned != 3 {
		t.Fatalf("Stats.Pruned = %d, want 3", final.Stats.Pruned)
	}
	if final.Stats.Regrabbed != 0 || final.Stats.Reacquired != 0 {
		t.Fatalf("Stats.Regrabbed=%d Reacquired=%d, want 0/0 (prune-only)", final.Stats.Regrabbed, final.Stats.Reacquired)
	}
	if final.Stats.Deletions != 3 {
		t.Fatalf("Stats.Deletions = %d, want 3", final.Stats.Deletions)
	}
}

// TestFixBrokenRegrabOnly pins single-component RE-GRAB via the manual endpoint:
// calls the arr (delete + search), does NOT delete the decypharr entry, and
// records Regrabbed with zero Pruned.
func TestFixBrokenRegrabOnly(t *testing.T) {
	m, r, arrSrv, _ := newRepairCapFixture(t, 0)

	hashes := makeHashes("fbregrab-", 3)
	for i, hash := range hashes {
		seedBrokenEntry(t, m, hash, "FbRegrab"+string(rune('A'+i)))
	}

	run, err := r.FixBroken(context.Background(), nil, &ManualActions{Regrab: true})
	if err != nil {
		t.Fatalf("FixBroken(regrab): %v", err)
	}
	waitRunComplete(t, m, run.ID)

	if got := countExisting(t, m, hashes); got != 3 {
		t.Fatalf("%d/3 entries remain, want 3 (RE-GRAB never prunes decypharr-side)", got)
	}
	if got := arrSrv.deleteCalls(); got != 3 {
		t.Fatalf("arr DeleteFiles calls = %d, want 3", got)
	}
	final, err := m.storage.GetRepairRun(run.ID)
	if err != nil || final == nil {
		t.Fatalf("GetRepairRun: %v", err)
	}
	if final.Stats.Regrabbed != 3 {
		t.Fatalf("Stats.Regrabbed = %d, want 3", final.Stats.Regrabbed)
	}
	if final.Stats.Pruned != 0 {
		t.Fatalf("Stats.Pruned = %d, want 0 (regrab-only)", final.Stats.Pruned)
	}
}

// TestFixBrokenOmittedFallsBackToConfigured pins that omitting the selection
// falls back to the configured REPAIR/PRUNE/RE-GRAB knobs — never force-all.
func TestFixBrokenOmittedFallsBackToConfigured(t *testing.T) {
	t.Run("all_knobs_off_no_selection_errors", func(t *testing.T) {
		m, r, _, _ := newRepairCapFixture(t, 0)
		off := false
		cfg := config.Get()
		cfg.Repair.Repair = &off  // REPAIR off
		cfg.Repair.Prune = false  // PRUNE off
		cfg.Repair.Regrab = false // RE-GRAB off
		seedBrokenEntry(t, m, "fbcfg-a", "FbCfgA")

		if _, err := r.FixBroken(context.Background(), nil, nil); err == nil {
			t.Fatal("FixBroken with all component knobs off + no selection should error (not force-all)")
		}
		if !entryExists(t, m, "fbcfg-a") {
			t.Fatal("nothing should have been deleted when no action resolved")
		}
	})

	t.Run("configured_prune_only_not_forceall", func(t *testing.T) {
		m, r, arrSrv, _ := newRepairCapFixture(t, 0)
		off := false
		cfg := config.Get()
		cfg.Repair.Repair = &off // REPAIR off
		cfg.Repair.Prune = true  // PRUNE on
		cfg.Repair.Regrab = false
		seedBrokenEntry(t, m, "fbcfg-b", "FbCfgB")

		run, err := r.FixBroken(context.Background(), nil, nil)
		if err != nil {
			t.Fatalf("FixBroken: %v", err)
		}
		waitRunComplete(t, m, run.ID)

		if entryExists(t, m, "fbcfg-b") {
			t.Fatal("configured PRUNE should have deleted the entry")
		}
		if got := arrSrv.totalCalls(); got != 0 {
			t.Fatalf("configured prune-only made %d arr calls, want 0 (NOT force-all RE-GRAB)", got)
		}
	})
}

// TestFixBrokenRepairOnlyReacquires pins single-component REPAIR via the manual
// endpoint: it re-acquires the dead item across providers, does NOT delete it,
// makes ZERO arr calls, records Reacquired, and clears the broken health.
func TestFixBrokenRepairOnlyReacquires(t *testing.T) {
	m, r, arrSrv := healingFixture(t)

	seedBrokenEntry(t, m, "reac-a", "ReacA")

	run, err := r.FixBroken(context.Background(), nil, &ManualActions{Repair: true})
	if err != nil {
		t.Fatalf("FixBroken(repair): %v", err)
	}
	waitRunComplete(t, m, run.ID)

	if !entryExists(t, m, "reac-a") {
		t.Fatal("REPAIR-only wrongly deleted the entry (re-acquire must not prune)")
	}
	if got := arrSrv.totalCalls(); got != 0 {
		t.Fatalf("REPAIR-only made %d arr calls, want 0", got)
	}
	final, err := m.storage.GetRepairRun(run.ID)
	if err != nil || final == nil {
		t.Fatalf("GetRepairRun: %v", err)
	}
	if final.Stats.Reacquired != 1 {
		t.Fatalf("Stats.Reacquired = %d, want 1", final.Stats.Reacquired)
	}
	if final.Stats.Pruned != 0 || final.Stats.Regrabbed != 0 {
		t.Fatalf("Stats.Pruned=%d Regrabbed=%d, want 0/0 (repair-only)", final.Stats.Pruned, final.Stats.Regrabbed)
	}
	if h, _ := m.storage.GetEntryHealth("ReacA"); h != nil && h.Status == storage.HealthBroken {
		t.Fatal("re-acquired entry is still marked broken")
	}
}

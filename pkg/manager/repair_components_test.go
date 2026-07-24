package manager

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// These tests pin the 4-component repair model: CHECK (detect via the managed
// path, arr-independent), REPAIR (re-acquire, stops the pipeline on success),
// PRUNE (decypharr-side delete, ZERO arr calls) and RE-GRAB (the only
// arr-coupled action, independent of PRUNE). They complement
// repair_deletion_cap_test.go which pins the shared deletion budget.

// seedManagedEntry persists a single-file main-store entry with NO provider
// placement (so a probe finds it dead: linkOf -> "" -> missing_provider_link)
// and NO pre-seeded health. This is the pure CHECK/managed scenario: the sweep
// discovers the entry on its own with no arr involvement.
func seedManagedEntry(t *testing.T, m *Manager, hash, name string) {
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
			"file.mkv": {Name: "file.mkv", InfoHash: hash, Size: 100, AddedOn: now},
		},
	}
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate(%s): %v", hash, err)
	}
}

// arrLinkedCandidate builds a lazily-probed candidate that already carries the
// arr targeting a managed sweep would merge from the arr enumeration, so a probe
// records BrokenFiles with the arr identifiers RE-GRAB needs.
func arrLinkedCandidate(t *testing.T, m *Manager, name string) *candidate {
	t.Helper()
	item, err := m.storage.GetEntryItem(name)
	if err != nil || item == nil {
		t.Fatalf("GetEntryItem(%s): %v", name, err)
	}
	return &candidate{
		name:       name,
		item:       item,
		arrName:    "radarr",
		arrKind:    storage.ArrKindRadarr,
		contentMap: map[string]arr.ContentFile{"file.mkv": {Id: 555, FileId: 777, Name: "file.mkv"}},
	}
}

func newRun(t *testing.T, m *Manager) *storage.RepairRun {
	t.Helper()
	run := &storage.RepairRun{ID: uuid.NewString(), Status: storage.RepairRunRunning}
	if err := m.storage.SaveRepairRun(run); err != nil {
		t.Fatalf("SaveRepairRun: %v", err)
	}
	return run
}

// TestCheckEnumeratesWholeLibraryViaManaged pins CHECK: enumeration walks the
// WHOLE hosted library via the managed path (no arr gate) and probing records
// health for every entry, all without a single arr API call. This is the core
// detection replacing the old arr-gated enumeration.
func TestCheckEnumeratesWholeLibraryViaManaged(t *testing.T) {
	m, r, arrSrv, _ := newRepairCapFixture(t, 0)

	hashes := makeHashes("check-", 4)
	names := make([]string, 0, len(hashes))
	for i, hash := range hashes {
		name := "CheckMovie" + string(rune('A'+i))
		seedManagedEntry(t, m, hash, name)
		names = append(names, name)
	}

	// CHECK enumeration with no destructive action: whole library via managed.
	cands, err := r.enumerateCandidates(context.Background(), r.cfg(), repairActions{})
	if err != nil {
		t.Fatalf("enumerateCandidates: %v", err)
	}
	if len(cands) != 4 {
		t.Fatalf("enumerated %d candidates, want 4 (whole managed library)", len(cands))
	}
	if got := arrSrv.totalCalls(); got != 0 {
		t.Fatalf("CHECK enumeration made %d arr calls, want 0 (arr-independent)", got)
	}

	run := newRun(t, m)
	// CHECK-only action set (all component knobs off): probe + record only.
	if err := r.probeAndHealCandidates(context.Background(), run, cands, names, newHealCache(), RepairRunOptions{}, repairActions{}, r.newDeletionBudget(run.ID)); err != nil {
		t.Fatalf("probeAndHealCandidates: %v", err)
	}

	for _, name := range names {
		h, herr := m.storage.GetEntryHealth(name)
		if herr != nil || h == nil {
			t.Fatalf("GetEntryHealth(%s): %v", name, herr)
		}
		if h.Status != storage.HealthBroken {
			t.Fatalf("%s health = %q, want broken (CHECK recorded)", name, h.Status)
		}
	}
	if got := countExisting(t, m, hashes); got != 4 {
		t.Fatalf("%d/4 entries remain, want all 4 (CHECK never deletes)", got)
	}
	if got := arrSrv.totalCalls(); got != 0 {
		t.Fatalf("CHECK made %d arr calls total, want 0", got)
	}
}

// TestPruneDeletesDecypharrSideZeroArrCalls pins the PRUNE invariant: on a dead
// item it deletes decypharr-side (db + placements + folder) and makes ZERO arr
// API calls. Uses managed entries with no arr link at all — PRUNE must still act.
func TestPruneDeletesDecypharrSideZeroArrCalls(t *testing.T) {
	m, r, arrSrv, _ := newRepairCapFixture(t, 0)

	hashes := makeHashes("prune-", 3)
	names := make([]string, 0, len(hashes))
	cands := make(map[string]*candidate, len(hashes))
	for i, hash := range hashes {
		name := "PruneMovie" + string(rune('A'+i))
		seedManagedEntry(t, m, hash, name)
		item, err := m.storage.GetEntryItem(name)
		if err != nil || item == nil {
			t.Fatalf("GetEntryItem(%s): %v", name, err)
		}
		cands[name] = &candidate{name: name, item: item} // managed: no arr link
		names = append(names, name)
	}

	run := newRun(t, m)
	actions := repairActions{prune: true} // PRUNE only, RE-GRAB off
	if err := r.probeAndHealCandidates(context.Background(), run, cands, names, newHealCache(), RepairRunOptions{}, actions, r.newDeletionBudget(run.ID)); err != nil {
		t.Fatalf("probeAndHealCandidates: %v", err)
	}

	if got := countExisting(t, m, hashes); got != 0 {
		t.Fatalf("%d/3 entries remain, want 0 (all pruned decypharr-side)", got)
	}
	if got := arrSrv.totalCalls(); got != 0 {
		t.Fatalf("PRUNE made %d arr calls, want 0 (arr keeps monitoring)", got)
	}
}

// TestRegrabIndependentOfPrune pins that RE-GRAB is NOT coupled to PRUNE:
//   - prune=false regrab=true  => arr called, entry NOT decypharr-deleted.
//   - prune=true  regrab=false => entry decypharr-deleted, ZERO arr calls.
func TestRegrabIndependentOfPrune(t *testing.T) {
	t.Run("regrab_only_arr_called_entry_kept", func(t *testing.T) {
		m, r, arrSrv, _ := newRepairCapFixture(t, 0)
		seedManagedEntry(t, m, "regrab-a", "RegrabOnlyA")
		cands := map[string]*candidate{"RegrabOnlyA": arrLinkedCandidate(t, m, "RegrabOnlyA")}

		run := newRun(t, m)
		actions := repairActions{regrab: true} // RE-GRAB only, PRUNE off
		if err := r.probeAndHealCandidates(context.Background(), run, cands, []string{"RegrabOnlyA"}, newHealCache(), RepairRunOptions{}, actions, r.newDeletionBudget(run.ID)); err != nil {
			t.Fatalf("probeAndHealCandidates: %v", err)
		}
		if got := arrSrv.deleteCalls(); got != 1 {
			t.Fatalf("RE-GRAB arr DeleteFiles calls = %d, want 1", got)
		}
		if !entryExists(t, m, "regrab-a") {
			t.Fatal("RE-GRAB-only wrongly deleted the decypharr entry (must NOT prune)")
		}
	})

	t.Run("prune_only_entry_deleted_zero_arr", func(t *testing.T) {
		m, r, arrSrv, _ := newRepairCapFixture(t, 0)
		seedManagedEntry(t, m, "prune-b", "PruneOnlyB")
		cands := map[string]*candidate{"PruneOnlyB": arrLinkedCandidate(t, m, "PruneOnlyB")}

		run := newRun(t, m)
		actions := repairActions{prune: true} // PRUNE only, RE-GRAB off
		if err := r.probeAndHealCandidates(context.Background(), run, cands, []string{"PruneOnlyB"}, newHealCache(), RepairRunOptions{}, actions, r.newDeletionBudget(run.ID)); err != nil {
			t.Fatalf("probeAndHealCandidates: %v", err)
		}
		if entryExists(t, m, "prune-b") {
			t.Fatal("PRUNE-only did not delete the decypharr entry")
		}
		if got := arrSrv.totalCalls(); got != 0 {
			t.Fatalf("PRUNE-only made %d arr calls, want 0", got)
		}
	})
}

// healingFixture wires a Manager with a real Fixer and a fake provider whose
// re-insert succeeds (returns a downloaded torrent with a linked file), so the
// REPAIR component can genuinely re-acquire a dead item end to end.
func healingFixture(t *testing.T) (*Manager, *Repair, *fakeArrServer) {
	t.Helper()
	m := newActionLifecycleFixture(t, 2)

	cfg := config.Get()
	cfg.Debrids = []config.Debrid{{Name: "prov"}, {Name: "prov2"}}

	healing := &fakeDebridClient{
		cfg: config.Debrid{Name: "prov"},
		checkFn: func(tr *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			tr.Status = debridTypes.TorrentStatusDownloaded
			tr.Files = map[string]debridTypes.File{
				"file.mkv": {Name: "file.mkv", Id: "f1", Link: "http://dl/file.mkv", Size: 100},
			}
			return tr, nil
		},
	}
	m.clients = xsync.NewMap[string, debrid.Client]()
	m.clients.Store("prov", healing)
	m.clients.Store("prov2", &fakeDebridClient{cfg: config.Debrid{Name: "prov2"}})
	m.fixer = NewFixer(m)

	arrSrv := newFakeArrServer(t)
	m.arr.AddOrUpdate(arr.NewWithOptions("radarr", arrSrv.server.URL, "test-token", arr.Options{}))

	r := &Repair{manager: m, logger: zerolog.Nop(), parentCtx: context.Background()}
	return m, r, arrSrv
}

// TestRepairReacquiresAndStopsPipeline pins REPAIR: a dead item is re-acquired
// across providers (reusing ReinsertEntry/Fixer.FixTorrent), which makes it
// servable, so the destructive pipeline (PRUNE/RE-GRAB) never runs for it even
// though both knobs are on — the entry survives and the arr is never called.
func TestRepairReacquiresAndStopsPipeline(t *testing.T) {
	m, r, arrSrv := healingFixture(t)

	seedManagedEntry(t, m, "heal-a", "HealMovieA")
	// Carry arr linkage so that, HAD it stayed dead, RE-GRAB would have fired —
	// making the "arr never called" assertion meaningful.
	cands := map[string]*candidate{"HealMovieA": arrLinkedCandidate(t, m, "HealMovieA")}

	run := newRun(t, m)
	actions := repairActions{repair: true, prune: true, regrab: true}
	if err := r.probeAndHealCandidates(context.Background(), run, cands, []string{"HealMovieA"}, newHealCache(), RepairRunOptions{}, actions, r.newDeletionBudget(run.ID)); err != nil {
		t.Fatalf("probeAndHealCandidates: %v", err)
	}

	h, _ := m.storage.GetEntryHealth("HealMovieA")
	if h == nil || h.Status != storage.HealthHealthy {
		t.Fatalf("health = %v, want healthy (REPAIR re-acquired it)", h)
	}
	if !entryExists(t, m, "heal-a") {
		t.Fatal("REPAIR succeeded but the entry was still pruned (pipeline did not stop)")
	}
	if got := arrSrv.totalCalls(); got != 0 {
		t.Fatalf("REPAIR healed the item but RE-GRAB still called the arr %d times (pipeline did not stop)", got)
	}
}

// TestResolveActionsComponents pins that the configured REPAIR/PRUNE/RE-GRAB
// knobs drive the action set directly (there is no master gate): REPAIR
// defaults on, PRUNE/RE-GRAB default off, and all three off ⇒ CHECK-only.
func TestResolveActionsComponents(t *testing.T) {
	on := true
	off := false
	cases := []struct {
		name string
		cfg  config.RepairConfig
		want repairActions
	}{
		{"all_knobs_off_is_check_only", config.RepairConfig{Repair: &off, Prune: false, Regrab: false}, repairActions{}},
		{"defaults", config.RepairConfig{}, repairActions{repair: true}},
		{"repair_explicit_off", config.RepairConfig{Repair: &off}, repairActions{}},
		{"all", config.RepairConfig{Repair: &on, Prune: true, Regrab: true}, repairActions{repair: true, prune: true, regrab: true}},
		{"prune_only", config.RepairConfig{Repair: &off, Prune: true}, repairActions{prune: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveActions(tc.cfg); got != tc.want {
				t.Fatalf("resolveActions = %+v, want %+v", got, tc.want)
			}
		})
	}

	// RepairEnabled defaults to true when the knob is unset (nil).
	if !(config.RepairConfig{}).RepairEnabled() {
		t.Fatal("RepairEnabled() with nil Repair = false, want true (safe default)")
	}
	if (config.RepairConfig{Repair: &off}).RepairEnabled() {
		t.Fatal("RepairEnabled() with explicit false = true, want false")
	}
}

// TestPruneOnlyEnforcesDeletionCap pins that PRUNE alone consumes the shared
// per-run deletion budget: 5 dead entries, cap 2 => 2 pruned, 3 remain, WARN.
func TestPruneOnlyEnforcesDeletionCap(t *testing.T) {
	m, r, arrSrv, buf := newRepairCapFixture(t, 2)

	hashes := makeHashes("prunecap-", 5)
	names := make([]string, 0, len(hashes))
	cands := make(map[string]*candidate, len(hashes))
	for i, hash := range hashes {
		name := "PruneCapMovie" + string(rune('A'+i))
		seedManagedEntry(t, m, hash, name)
		item, err := m.storage.GetEntryItem(name)
		if err != nil || item == nil {
			t.Fatalf("GetEntryItem(%s): %v", name, err)
		}
		cands[name] = &candidate{name: name, item: item}
		names = append(names, name)
	}

	run := newRun(t, m)
	budget := r.newDeletionBudget(run.ID)
	if err := r.probeAndHealCandidates(context.Background(), run, cands, names, newHealCache(), RepairRunOptions{}, repairActions{prune: true}, budget); err != nil {
		t.Fatalf("probeAndHealCandidates: %v", err)
	}

	if budget.deletions() != 2 {
		t.Fatalf("budget deletions = %d, want 2", budget.deletions())
	}
	if got := countExisting(t, m, hashes); got != 3 {
		t.Fatalf("%d/5 entries remain, want 3 (2 pruned under cap)", got)
	}
	if got := arrSrv.totalCalls(); got != 0 {
		t.Fatalf("PRUNE made %d arr calls, want 0", got)
	}
	if !strings.Contains(buf.String(), "deletion cap reached") {
		t.Fatalf("expected deletion-cap WARN, got: %s", buf.String())
	}
}

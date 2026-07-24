package manager

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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

// syncBuffer is a concurrency-safe io.Writer so the sweep's parallel workers can
// share one log sink without racing the assertion read.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// fakeArrServer stands in for a Sonarr/Radarr instance. It answers every call
// 200 OK with an empty history (so FindGrabHistoryID falls through to
// SearchMissing) and counts the destructive moviefile/bulk DELETEs so a test
// can assert exactly how many Arr file-record deletions the cap allowed.
type fakeArrServer struct {
	server   *httptest.Server
	total    atomic.Int64
	deletes  atomic.Int64
	searches atomic.Int64
}

func newFakeArrServer(t *testing.T) *fakeArrServer {
	t.Helper()
	f := &fakeArrServer{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Count every request so a test can assert a component made ZERO arr
		// calls (the PRUNE invariant).
		f.total.Add(1)
		switch {
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/bulk"):
			f.deletes.Add(1)
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "command"):
			f.searches.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"records":[]}`))
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeArrServer) deleteCalls() int { return int(f.deletes.Load()) }

// totalCalls reports every HTTP request the fake arr received. Used to assert
// that PRUNE / CHECK make ZERO arr API calls.
func (f *fakeArrServer) totalCalls() int { return int(f.total.Load()) }

// newRepairCapFixture builds a Manager + Repair wired for repair-cap tests: real
// on-disk storage, a registered fake Radarr, a fake provider client, and a
// buffered logger so the cap WARN can be asserted. maxDeletions writes the
// config cap (0 => leave unset so the default applies).
func newRepairCapFixture(t *testing.T, maxDeletions int) (*Manager, *Repair, *fakeArrServer, *syncBuffer) {
	t.Helper()
	m := newActionLifecycleFixture(t, 2)

	// Provider client so torrent probes resolve; entries carry no placement, so
	// probeTorrentFile reports "missing_provider_link" (broken) without a real
	// CheckFile, and DeleteEntry has no placement to tear down.
	m.clients = xsync.NewMap[string, debrid.Client]()
	m.clients.Store("prov", &fakeDebridClient{cfg: config.Debrid{Name: "prov"}})

	arrSrv := newFakeArrServer(t)
	m.arr.AddOrUpdate(arr.NewWithOptions("radarr", arrSrv.server.URL, "test-token", arr.Options{}))

	cfg := config.Get()
	cfg.Repair.MaxDeletionsPerRun = maxDeletions

	buf := &syncBuffer{}
	r := &Repair{
		manager:   m,
		logger:    zerolog.New(buf).Level(zerolog.InfoLevel),
		parentCtx: context.Background(),
	}
	return m, r, arrSrv, buf
}

// seedBrokenEntry persists a fully-broken single-file entry: a main-store row
// (so DeleteEntry can remove it) plus a broken EntryHealth carrying the Arr
// identifiers the heal path needs to delete + re-search.
func seedBrokenEntry(t *testing.T, m *Manager, hash, name string) {
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
	h := &storage.EntryHealth{
		EntryName: name,
		Protocol:  config.ProtocolTorrent,
		Status:    storage.HealthBroken,
		FileCount: 1,
		BrokenFiles: []storage.BrokenFile{{
			EntryName: name,
			FileName:  "file.mkv",
			InfoHash:  hash,
			Protocol:  config.ProtocolTorrent,
			Reason:    "hoster_unavailable",
			ArrName:   "radarr",
			ArrKind:   storage.ArrKindRadarr,
			MediaID:   555,
			ArrFileID: 777,
		}},
	}
	if err := m.storage.SaveEntryHealth(h); err != nil {
		t.Fatalf("SaveEntryHealth(%s): %v", name, err)
	}
}

func entryExists(t *testing.T, m *Manager, hash string) bool {
	t.Helper()
	ok, err := m.storage.Exists(hash)
	if err != nil {
		t.Fatalf("Exists(%s): %v", hash, err)
	}
	return ok
}

func countExisting(t *testing.T, m *Manager, hashes []string) int {
	t.Helper()
	n := 0
	for _, h := range hashes {
		if entryExists(t, m, h) {
			n++
		}
	}
	return n
}

func makeHashes(prefix string, n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = prefix + string(rune('a'+i))
	}
	return out
}

// TestRepairDeletionBudgetReserve pins the budget primitive: a finite cap grants
// exactly N slots then denies, nil/unlimited always grants, and the first denial
// logs a single WARN.
func TestRepairDeletionBudgetReserve(t *testing.T) {
	// nil budget is unlimited.
	var nilBudget *repairDeletionBudget
	for i := 0; i < 5; i++ {
		if !nilBudget.reserve() {
			t.Fatalf("nil budget denied reservation %d", i)
		}
	}

	// limit <= 0 is unlimited.
	unlimited := &repairDeletionBudget{limit: 0}
	for i := 0; i < 5; i++ {
		if !unlimited.reserve() {
			t.Fatalf("unlimited budget denied reservation %d", i)
		}
	}

	// Finite cap grants exactly `limit`, denies the rest, WARNs once.
	buf := &syncBuffer{}
	b := &repairDeletionBudget{limit: 3, logger: zerolog.New(buf), runID: "run-x"}
	granted := 0
	for i := 0; i < 10; i++ {
		if b.reserve() {
			granted++
		}
	}
	if granted != 3 {
		t.Fatalf("granted = %d, want 3", granted)
	}
	if b.deletions() != 3 {
		t.Fatalf("deletions = %d, want 3", b.deletions())
	}
	if b.skippedCount() != 7 {
		t.Fatalf("skippedCount = %d, want 7", b.skippedCount())
	}
	if got := strings.Count(buf.String(), "deletion cap reached"); got != 1 {
		t.Fatalf("WARN logged %d times, want exactly 1: %s", got, buf.String())
	}
}

// TestRepairDeletionBudgetConcurrent pins concurrency safety: with many parallel
// callers, a cap of N grants exactly N slots (no overshoot from races).
func TestRepairDeletionBudgetConcurrent(t *testing.T) {
	b := &repairDeletionBudget{limit: 100, logger: zerolog.Nop()}
	var granted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.reserve() {
				granted.Add(1)
			}
		}()
	}
	wg.Wait()
	if granted.Load() != 100 {
		t.Fatalf("granted = %d, want exactly 100", granted.Load())
	}
}

// TestMaxDeletionsPerRunResolution pins the config accessor semantics: unset =>
// default 100, positive => itself, negative => unlimited (0 sentinel).
func TestMaxDeletionsPerRunResolution(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()
	r := &Repair{logger: zerolog.Nop()}

	cfg.Repair.MaxDeletionsPerRun = 0
	if got := r.maxDeletionsPerRun(); got != repairDefaultMaxDeletionsPerRun {
		t.Fatalf("unset cap => %d, want default %d", got, repairDefaultMaxDeletionsPerRun)
	}
	cfg.Repair.MaxDeletionsPerRun = 250
	if got := r.maxDeletionsPerRun(); got != 250 {
		t.Fatalf("cap 250 => %d, want 250", got)
	}
	cfg.Repair.MaxDeletionsPerRun = -1
	if got := r.maxDeletionsPerRun(); got != 0 {
		t.Fatalf("cap -1 => %d, want 0 (unlimited)", got)
	}
	// -1 config must produce an unlimited budget.
	cfg.Repair.MaxDeletionsPerRun = -1
	b := r.newDeletionBudget("run")
	for i := 0; i < 500; i++ {
		if !b.reserve() {
			t.Fatalf("unlimited (cap -1) budget denied reservation %d", i)
		}
	}
}

// TestSweepEnforcesDeletionCap is the headline case: 5 fully-broken entries, a
// per-run cap of 2, PRUNE + RE-GRAB on. The scheduled/RunNow sweep path
// (probeAndHealCandidates -> actOnDeadEntry -> regrab/prune) reserves one budget
// slot per destructively-acted entry, so it must act on at most 2 entries: 2
// arr file-record deletes (RE-GRAB) + 2 decypharr entry deletes (PRUNE), leave
// the other 3 dead in storage, and WARN.
//
// Change vs pre-split: this used to pass a single `autoRepair=true` bool; the
// action set is now explicit. prune+regrab reproduces the old coupled heal
// (arr delete + decypharr delete) so the deletion counts are unchanged.
func TestSweepEnforcesDeletionCap(t *testing.T) {
	m, r, arrSrv, buf := newRepairCapFixture(t, 2)

	hashes := makeHashes("sweep-", 5)
	candidates := make(map[string]*candidate, len(hashes))
	names := make([]string, 0, len(hashes))
	for i, hash := range hashes {
		name := "SweepMovie" + string(rune('A'+i))
		seedBrokenEntry(t, m, hash, name)
		item, err := m.storage.GetEntryItem(name)
		if err != nil || item == nil {
			t.Fatalf("GetEntryItem(%s): %v", name, err)
		}
		candidates[name] = &candidate{
			name:    name,
			item:    item,
			arrName: "radarr",
			arrKind: storage.ArrKindRadarr,
			contentMap: map[string]arr.ContentFile{
				"file.mkv": {Id: 555, FileId: 777, Name: "file.mkv"},
			},
		}
		names = append(names, name)
	}

	run := &storage.RepairRun{ID: uuid.NewString(), Status: storage.RepairRunRunning}
	if err := m.storage.SaveRepairRun(run); err != nil {
		t.Fatalf("SaveRepairRun: %v", err)
	}

	budget := r.newDeletionBudget(run.ID)
	heal := newHealCache()
	actions := repairActions{repair: true, prune: true, regrab: true}
	if err := r.probeAndHealCandidates(context.Background(), run, candidates, names, heal, RepairRunOptions{}, actions, budget); err != nil {
		t.Fatalf("probeAndHealCandidates: %v", err)
	}

	if budget.deletions() != 2 {
		t.Fatalf("budget deletions = %d, want 2", budget.deletions())
	}
	if got := countExisting(t, m, hashes); got != 3 {
		t.Fatalf("%d/5 entries remain, want 3 (2 deleted under cap)", got)
	}
	if got := arrSrv.deleteCalls(); got != 2 {
		t.Fatalf("Arr DeleteFiles calls = %d, want 2 (cap bounds Arr deletes too)", got)
	}
	if !strings.Contains(buf.String(), "deletion cap reached") {
		t.Fatalf("expected deletion-cap WARN, got: %s", buf.String())
	}
}

// TestFixBrokenEnforcesDeletionCap pins the same cap on the bulk "Fix broken"
// path (FixBroken -> repairBroken -> actOnDeadEntry): 5 broken, cap 2 => 2
// deleted, 3 remain, WARN.
//
// Change vs pre-split: FixBroken now takes an explicit component selection
// instead of forcing all three. This passes {Prune,Regrab} (the destructive
// pair) so the deletion-cap behavior under test is unchanged; REPAIR is omitted
// because these entries carry no re-acquirable provider placement.
func TestFixBrokenEnforcesDeletionCap(t *testing.T) {
	m, r, arrSrv, buf := newRepairCapFixture(t, 2)

	hashes := makeHashes("fix-", 5)
	for i, hash := range hashes {
		seedBrokenEntry(t, m, hash, "FixMovie"+string(rune('A'+i)))
	}

	run, err := r.FixBroken(context.Background(), nil, &ManualActions{Prune: true, Regrab: true})
	if err != nil {
		t.Fatalf("FixBroken: %v", err)
	}
	waitRunComplete(t, m, run.ID)

	if got := countExisting(t, m, hashes); got != 3 {
		t.Fatalf("%d/5 entries remain, want 3 (2 deleted under cap)", got)
	}
	if got := arrSrv.deleteCalls(); got != 2 {
		t.Fatalf("Arr DeleteFiles calls = %d, want 2", got)
	}
	if !strings.Contains(buf.String(), "deletion cap reached") {
		t.Fatalf("expected deletion-cap WARN, got: %s", buf.String())
	}
}

// TestSingleItemHealNotBlockedByCap pins that a single legitimate destructive
// heal always proceeds: the single-item paths (RecheckEntry/RecheckMedia) hand
// healBrokenEntry a nil (unlimited) budget, and even a cap-of-1 run deletes its
// one item.
func TestSingleItemHealNotBlockedByCap(t *testing.T) {
	m, r, arrSrv, _ := newRepairCapFixture(t, 1)

	// Single-item path uses a nil budget: never blocked. actOnDeadEntry applies
	// only the destructive components, so pass {Prune,Regrab} directly (was
	// manualFixActions(); repair is a no-op in actOnDeadEntry).
	destructive := repairActions{prune: true, regrab: true}
	seedBrokenEntry(t, m, "single-nil", "SingleNil")
	hNil, _ := m.storage.GetEntryHealth("SingleNil")
	run := &storage.RepairRun{ID: uuid.NewString()}
	var mu sync.Mutex
	r.actOnDeadEntry(context.Background(), run, &mu, "SingleNil", hNil, destructive, nil)
	if entryExists(t, m, "single-nil") {
		t.Fatal("single-item heal (nil budget) did not delete its entry")
	}

	// A cap-of-1 run still lets its one legitimate deletion through.
	seedBrokenEntry(t, m, "single-cap", "SingleCap")
	hCap, _ := m.storage.GetEntryHealth("SingleCap")
	budget := r.newDeletionBudget(run.ID)
	r.actOnDeadEntry(context.Background(), run, &mu, "SingleCap", hCap, destructive, budget)
	if entryExists(t, m, "single-cap") {
		t.Fatal("cap=1 run wrongly blocked its single legitimate deletion")
	}
	if got := arrSrv.deleteCalls(); got != 2 {
		t.Fatalf("Arr DeleteFiles calls = %d, want 2 (both single heals ran)", got)
	}
}

// TestNonDestructiveRepairUnaffectedByCap pins that when no destructive
// component is enabled (CHECK-only, e.g. all knobs off) the sweep never
// reaches PRUNE/RE-GRAB, so nothing is deleted and no cap slot is consumed
// regardless of how many entries are broken.
func TestNonDestructiveRepairUnaffectedByCap(t *testing.T) {
	m, r, arrSrv, _ := newRepairCapFixture(t, 2)

	hashes := makeHashes("noheal-", 5)
	candidates := make(map[string]*candidate, len(hashes))
	names := make([]string, 0, len(hashes))
	for i, hash := range hashes {
		name := "NoHealMovie" + string(rune('A'+i))
		seedBrokenEntry(t, m, hash, name)
		item, err := m.storage.GetEntryItem(name)
		if err != nil || item == nil {
			t.Fatalf("GetEntryItem(%s): %v", name, err)
		}
		candidates[name] = &candidate{
			name:       name,
			item:       item,
			arrName:    "radarr",
			arrKind:    storage.ArrKindRadarr,
			contentMap: map[string]arr.ContentFile{"file.mkv": {Id: 555, FileId: 777, Name: "file.mkv"}},
		}
		names = append(names, name)
	}

	run := &storage.RepairRun{ID: uuid.NewString(), Status: storage.RepairRunRunning}
	if err := m.storage.SaveRepairRun(run); err != nil {
		t.Fatalf("SaveRepairRun: %v", err)
	}

	budget := r.newDeletionBudget(run.ID)
	// CHECK-only (no action components): pure health check, no destructive heal.
	if err := r.probeAndHealCandidates(context.Background(), run, candidates, names, newHealCache(), RepairRunOptions{}, repairActions{}, budget); err != nil {
		t.Fatalf("probeAndHealCandidates: %v", err)
	}

	if budget.deletions() != 0 {
		t.Fatalf("budget deletions = %d, want 0 (CHECK-only)", budget.deletions())
	}
	if got := countExisting(t, m, hashes); got != 5 {
		t.Fatalf("%d/5 entries remain, want all 5 (nothing deleted)", got)
	}
	if got := arrSrv.deleteCalls(); got != 0 {
		t.Fatalf("Arr DeleteFiles calls = %d, want 0", got)
	}
}

func waitRunComplete(t *testing.T, m *Manager, runID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		run, err := m.storage.GetRepairRun(runID)
		if err == nil && run != nil && run.Status != storage.RepairRunRunning {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("repair run %s did not complete in time", runID)
		}
		time.Sleep(15 * time.Millisecond)
	}
}

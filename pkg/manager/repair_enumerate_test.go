package manager

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// enumClient is a provider whose whole-account enumeration is scripted.
type enumClient struct {
	fakeDebridClient
	torrents []*debridTypes.Torrent
	err      error
	calls    int
	mu       sync.Mutex
}

func (e *enumClient) GetAllTorrents() ([]*debridTypes.Torrent, error) {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	if e.err != nil {
		return nil, e.err
	}
	return e.torrents, nil
}

func newEnumerateFixture(t *testing.T, clients map[string]debrid.Client) (*Manager, *Repair) {
	t.Helper()
	m := newActionLifecycleFixture(t, 2)
	m.clients = xsync.NewMap[string, debrid.Client]()
	for name, c := range clients {
		m.clients.Store(name, c)
	}
	r := &Repair{manager: m, logger: zerolog.Nop(), parentCtx: context.Background()}
	return m, r
}

// seedEnumerateEntry persists an entry (and its derived entry-item) whose
// active placement is on `provider` with debrid id `placementID`.
func seedEnumerateEntry(t *testing.T, m *Manager, hash, name, provider, placementID string, fileNames ...string) *storage.Entry {
	t.Helper()
	if len(fileNames) == 0 {
		fileNames = []string{"file.mkv"}
	}
	entry := probeTorrentEntry(hash, name)
	entry.ActiveProvider = provider
	entry.Providers = map[string]*storage.ProviderEntry{
		provider: {
			Provider: provider,
			ID:       placementID,
			Status:   debridTypes.TorrentStatusDownloaded,
			Files:    map[string]*storage.ProviderFile{},
		},
	}
	entry.Files = map[string]*storage.File{}
	for _, f := range fileNames {
		entry.Files[f] = &storage.File{Name: f, InfoHash: hash, Size: 4096, AddedOn: entry.AddedOn}
		entry.Providers[provider].Files[f] = &storage.ProviderFile{Id: "id-" + f, Link: "https://p.invalid/" + f, Path: "/" + f}
	}
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate(%s): %v", hash, err)
	}
	if err := m.storage.UpdateEntryItem(entry); err != nil {
		t.Fatalf("UpdateEntryItem(%s): %v", hash, err)
	}
	return entry
}

func deadTorrent(hash, id, status string) *debridTypes.Torrent {
	return &debridTypes.Torrent{
		Id:             id,
		InfoHash:       hash,
		Status:         debridTypes.TorrentStatusError,
		ProviderStatus: status,
		ProviderDead:   true,
	}
}

func liveTorrent(hash, id string) *debridTypes.Torrent {
	return &debridTypes.Torrent{
		Id:             id,
		InfoHash:       hash,
		Status:         debridTypes.TorrentStatusDownloaded,
		ProviderStatus: "Ready",
		ProviderDead:   false,
	}
}

func runEnumerate(t *testing.T, r *Repair, actions repairActions) *storage.RepairRun {
	t.Helper()
	run := &storage.RepairRun{ID: "run-enum", Stats: storage.RepairRunStats{}}
	var mu sync.Mutex
	r.runProviderEnumeration(context.Background(), run, &mu, config.RepairConfig{}, actions, nil)
	return run
}

// TestEnumerateMarksProviderDeadEntry is the happy path and the acceptance
// criterion: a placement the provider itself calls dead becomes a broken health
// record immediately, with the provider's own wording preserved, without any
// payload probe running.
func TestEnumerateMarksProviderDeadEntry(t *testing.T) {
	client := &enumClient{torrents: []*debridTypes.Torrent{
		deadTorrent("deadhash", "placement-deadhash", "Expired - Files removed"),
	}}
	m, r := newEnumerateFixture(t, map[string]debrid.Client{"prov": client})
	seedEnumerateEntry(t, m, "deadhash", "Dead.Entry", "prov", "placement-deadhash")

	run := runEnumerate(t, r, repairActions{})

	entry, err := m.GetEntry("deadhash")
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	h, err := m.storage.GetEntryHealth(entry.GetFolder())
	if err != nil || h == nil {
		t.Fatalf("GetEntryHealth: h=%v err=%v", h, err)
	}
	if h.Status != storage.HealthBroken {
		t.Fatalf("status = %q, want broken", h.Status)
	}
	if !strings.Contains(h.FailureReason, "Expired - Files removed") {
		t.Fatalf("failure reason %q does not carry the provider's wording", h.FailureReason)
	}
	if !strings.HasPrefix(h.FailureReason, reasonProviderReportsDead) {
		t.Fatalf("failure reason %q lacks the %s prefix", h.FailureReason, reasonProviderReportsDead)
	}
	if h.BrokenCount != h.FileCount || h.FileCount != 1 {
		t.Fatalf("broken=%d file=%d, want every file condemned", h.BrokenCount, h.FileCount)
	}
	// The verdict must be actionable by PRUNE, or the operation produces
	// records no component can use.
	if !pruneEligible(h) {
		t.Fatalf("verdict is not prune-eligible: %s", pruneIneligibleReason(h))
	}
	// It must NOT claim to have been byte-verified by the current prober.
	if h.ProbeVersion != 0 {
		t.Fatalf("ProbeVersion = %d, want 0 (this verdict ran no payload probe)", h.ProbeVersion)
	}
	if run.Stats.EnumMarkedBroken != 1 || run.Stats.EnumReportedDead != 1 || run.Stats.EnumScanned != 1 {
		t.Fatalf("stats = %+v, want scanned/reported/marked = 1/1/1", run.Stats)
	}
}

// TestEnumerateAbsenceIsNotEvidence is the contract rule. An entry the provider
// simply does not mention must receive NO verdict — enumeration may be partial,
// and a missing hash carries no information whatsoever.
func TestEnumerateAbsenceIsNotEvidence(t *testing.T) {
	// The provider answers successfully and says nothing about our entry.
	client := &enumClient{torrents: []*debridTypes.Torrent{liveTorrent("otherhash", "other-id")}}
	m, r := newEnumerateFixture(t, map[string]debrid.Client{"prov": client})
	seedEnumerateEntry(t, m, "absenthash", "Absent.Entry", "prov", "placement-absenthash")

	run := runEnumerate(t, r, repairActions{})

	entry, _ := m.GetEntry("absenthash")
	h, _ := m.storage.GetEntryHealth(entry.GetFolder())
	if h != nil && h.Status == storage.HealthBroken {
		t.Fatalf("entry absent from enumeration was condemned: %+v", h)
	}
	if run.Stats.EnumMarkedBroken != 0 {
		t.Fatalf("marked %d broken from an absence, want 0", run.Stats.EnumMarkedBroken)
	}
}

// TestEnumerateProviderFailureIsIsolated pins the other half of the same rule: a
// provider that ERRORS contributes nothing and is counted, so its silence can
// never be read as "everything on it is fine" — and a healthy provider's
// findings still land.
func TestEnumerateProviderFailureIsIsolated(t *testing.T) {
	broken := &enumClient{err: errors.New("provider exploded")}
	working := &enumClient{torrents: []*debridTypes.Torrent{
		deadTorrent("livehash", "placement-livehash", "dead"),
	}}
	m, r := newEnumerateFixture(t, map[string]debrid.Client{"broken": broken, "working": working})
	seedEnumerateEntry(t, m, "onbroken", "On.Broken", "broken", "placement-onbroken")
	seedEnumerateEntry(t, m, "livehash", "On.Working", "working", "placement-livehash")

	run := runEnumerate(t, r, repairActions{})

	if run.Stats.EnumProvidersFailed != 1 {
		t.Fatalf("EnumProvidersFailed = %d, want 1", run.Stats.EnumProvidersFailed)
	}
	// Nothing on the failed provider may be condemned.
	onBroken, _ := m.GetEntry("onbroken")
	if h, _ := m.storage.GetEntryHealth(onBroken.GetFolder()); h != nil && h.Status == storage.HealthBroken {
		t.Fatalf("entry on a FAILED provider enumeration was condemned: %+v", h)
	}
	// The working provider's finding still lands.
	onWorking, _ := m.GetEntry("livehash")
	h, _ := m.storage.GetEntryHealth(onWorking.GetFolder())
	if h == nil || h.Status != storage.HealthBroken {
		t.Fatalf("healthy provider's finding did not land: %+v", h)
	}
}

// TestEnumerateIgnoresNonActiveProvider: AllDebrid calling a magnet dead says
// nothing about an entry we have already moved to RealDebrid. Acting on it would
// condemn a working entry on the word of a provider it no longer uses.
func TestEnumerateIgnoresNonActiveProvider(t *testing.T) {
	stale := &enumClient{torrents: []*debridTypes.Torrent{
		deadTorrent("movedhash", "old-placement", "Expired - Files removed"),
	}}
	current := &enumClient{}
	m, r := newEnumerateFixture(t, map[string]debrid.Client{"old": stale, "new": current})
	// The entry lives on "new"; "old" is the one reporting it dead.
	seedEnumerateEntry(t, m, "movedhash", "Moved.Entry", "new", "new-placement")

	run := runEnumerate(t, r, repairActions{})

	entry, _ := m.GetEntry("movedhash")
	h, _ := m.storage.GetEntryHealth(entry.GetFolder())
	if h != nil && h.Status == storage.HealthBroken {
		t.Fatalf("entry was condemned by a provider that is not serving it: %+v", h)
	}
	if run.Stats.EnumMarkedBroken != 0 {
		t.Fatalf("EnumMarkedBroken = %d, want 0", run.Stats.EnumMarkedBroken)
	}
}

// TestEnumerateIgnoresStalePlacementID: same provider, different submission. The
// provider's dead record describes a torrent id we do not hold, so it is not
// about our placement and must not condemn it.
func TestEnumerateIgnoresStalePlacementID(t *testing.T) {
	client := &enumClient{torrents: []*debridTypes.Torrent{
		deadTorrent("samehash", "an-older-submission", "Expired - Files removed"),
	}}
	m, r := newEnumerateFixture(t, map[string]debrid.Client{"prov": client})
	seedEnumerateEntry(t, m, "samehash", "Same.Hash", "prov", "our-current-submission")

	runEnumerate(t, r, repairActions{})

	entry, _ := m.GetEntry("samehash")
	h, _ := m.storage.GetEntryHealth(entry.GetFolder())
	if h != nil && h.Status == storage.HealthBroken {
		t.Fatalf("entry condemned by a dead record for a DIFFERENT submission: %+v", h)
	}
}

// TestEnumerateIgnoresUnmanagedHashes: a shared provider account holds torrents
// that are not ours. Finding them dead must be a no-op, not an error.
func TestEnumerateIgnoresUnmanagedHashes(t *testing.T) {
	client := &enumClient{torrents: []*debridTypes.Torrent{
		deadTorrent("notours1", "x1", "dead"),
		deadTorrent("notours2", "x2", "virus"),
	}}
	_, r := newEnumerateFixture(t, map[string]debrid.Client{"prov": client})

	run := runEnumerate(t, r, repairActions{})

	if run.Stats.EnumReportedDead != 2 {
		t.Fatalf("EnumReportedDead = %d, want 2", run.Stats.EnumReportedDead)
	}
	if run.Stats.EnumMatched != 0 || run.Stats.EnumMarkedBroken != 0 {
		t.Fatalf("matched=%d marked=%d, want 0/0 for unmanaged hashes", run.Stats.EnumMatched, run.Stats.EnumMarkedBroken)
	}
}

// TestEnumerateLiveTorrentsAreNotCondemned: only ProviderDead findings count.
// A torrent the provider reports as fine must never produce a verdict.
func TestEnumerateLiveTorrentsAreNotCondemned(t *testing.T) {
	client := &enumClient{torrents: []*debridTypes.Torrent{liveTorrent("okhash", "placement-okhash")}}
	m, r := newEnumerateFixture(t, map[string]debrid.Client{"prov": client})
	seedEnumerateEntry(t, m, "okhash", "Ok.Entry", "prov", "placement-okhash")

	run := runEnumerate(t, r, repairActions{})

	entry, _ := m.GetEntry("okhash")
	if h, _ := m.storage.GetEntryHealth(entry.GetFolder()); h != nil && h.Status == storage.HealthBroken {
		t.Fatalf("a live torrent was condemned: %+v", h)
	}
	if run.Stats.EnumScanned != 1 || run.Stats.EnumReportedDead != 0 {
		t.Fatalf("stats = %+v, want scanned=1 reported_dead=0", run.Stats)
	}
}

// TestEnumerateAsksEachProviderExactlyOnce: the operation's whole economic
// argument is that it is a handful of bulk calls, not per-entry work. One call
// per provider per run, whatever the library size.
func TestEnumerateAsksEachProviderExactlyOnce(t *testing.T) {
	a := &enumClient{torrents: []*debridTypes.Torrent{
		deadTorrent("h1", "p1", "dead"), liveTorrent("h2", "p2"), liveTorrent("h3", "p3"),
	}}
	b := &enumClient{torrents: []*debridTypes.Torrent{liveTorrent("h4", "p4")}}
	m, r := newEnumerateFixture(t, map[string]debrid.Client{"a": a, "b": b})
	seedEnumerateEntry(t, m, "h1", "One", "a", "p1")
	seedEnumerateEntry(t, m, "h2", "Two", "a", "p2")
	seedEnumerateEntry(t, m, "h3", "Three", "a", "p3")
	seedEnumerateEntry(t, m, "h4", "Four", "b", "p4")

	run := runEnumerate(t, r, repairActions{})

	for name, c := range map[string]*enumClient{"a": a, "b": b} {
		c.mu.Lock()
		calls := c.calls
		c.mu.Unlock()
		if calls != 1 {
			t.Errorf("provider %q enumerated %d times, want exactly 1", name, calls)
		}
	}
	if run.Stats.EnumScanned != 4 {
		t.Fatalf("EnumScanned = %d, want 4", run.Stats.EnumScanned)
	}
}

// TestEnumerateSkipGuardPremise: executeSweep only enumerates when some
// component could act on the result. Pin the guard's premise so a change to
// any() cannot silently start enumerating providers on CHECK-only runs.
func TestEnumerateSkipGuardPremise(t *testing.T) {
	if (repairActions{}).any() {
		t.Fatalf("empty repairActions reported any() = true; executeSweep would enumerate on a CHECK-only run")
	}
	for _, a := range []repairActions{{repair: true}, {prune: true}, {arrDelete: true}} {
		if !a.any() {
			t.Errorf("repairActions %+v reported any() = false; ENUMERATE would be skipped where it should run", a)
		}
	}
}

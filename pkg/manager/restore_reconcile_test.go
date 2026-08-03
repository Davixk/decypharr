package manager

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"

	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// reconcileClient enumerates a scripted account and serves per-torrent details.
type reconcileClient struct {
	fakeDebridClient
	all          []*debridTypes.Torrent
	allErr       error
	details      map[string]*debridTypes.Torrent
	detailErr    error
	allCalls     int
	detailsCalls int
	mu           sync.Mutex
}

func (c *reconcileClient) GetAllTorrents() ([]*debridTypes.Torrent, error) {
	c.mu.Lock()
	c.allCalls++
	c.mu.Unlock()
	if c.allErr != nil {
		return nil, c.allErr
	}
	return c.all, nil
}

func (c *reconcileClient) GetTorrent(id string) (*debridTypes.Torrent, error) {
	c.mu.Lock()
	c.detailsCalls++
	c.mu.Unlock()
	if c.detailErr != nil {
		return nil, c.detailErr
	}
	t, ok := c.details[id]
	if !ok {
		return nil, errors.New("not found")
	}
	return t, nil
}

func newReconcileFixture(t *testing.T, clients map[string]debrid.Client) *Manager {
	t.Helper()
	m := newActionLifecycleFixture(t, 2)
	m.clients = xsync.NewMap[string, debrid.Client]()
	for name, c := range clients {
		m.clients.Store(name, c)
	}
	return m
}

// queuedTorrentNoPlacement is the shape the bug lives in: queued, and we hold no
// provider placement.
func queuedTorrentNoPlacement(hash, name string) *storage.Entry {
	entry := probeTorrentEntry(hash, name)
	entry.ActiveProvider = ""
	entry.Providers = map[string]*storage.ProviderEntry{}
	entry.Status = debridTypes.TorrentStatusQueued
	entry.Magnet = "magnet:?xt=urn:btih:" + hash
	entry.Category = "sonarr"
	entry.CallbackURL = "https://callback.invalid/hook"
	return entry
}

// seedQueuedTorrent puts the entry in the ACTIVE QUEUE, which is where restore
// reads from and where adoption persists back to. Registering it only in the
// main store makes queue.Update fail, and adoption then falls back to
// re-submit — the correct behaviour on a persist failure, but it would mask
// whether the adopt path works at all.
func seedQueuedTorrent(t *testing.T, m *Manager, hash, name string) *storage.Entry {
	t.Helper()
	entry := queuedTorrentNoPlacement(hash, name)
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("queue.Add(%s): %v", hash, err)
	}
	return entry
}

func remoteTorrent(hash, id, provider string) *debridTypes.Torrent {
	return &debridTypes.Torrent{
		Id:       id,
		InfoHash: hash,
		Name:     "Remote.Name",
		Debrid:   provider,
		Status:   debridTypes.TorrentStatusDownloaded,
		Files: map[string]debridTypes.File{
			"file.mkv": {Name: "file.mkv", Size: 4096},
		},
	}
}

// TestReconcileAdoptsProviderHeldTorrent is the fix. The provider holds a copy
// we have no placement for — the exact crash-window state — and restore must
// adopt it instead of adding the magnet a second time.
func TestReconcileAdoptsProviderHeldTorrent(t *testing.T) {
	client := &reconcileClient{
		all:     []*debridTypes.Torrent{{Id: "rd-1", InfoHash: "hash1", Status: debridTypes.TorrentStatusDownloaded}},
		details: map[string]*debridTypes.Torrent{"rd-1": remoteTorrent("hash1", "rd-1", "prov")},
	}
	m := newReconcileFixture(t, map[string]debrid.Client{"prov": client})
	entry := seedQueuedTorrent(t, m, "hash1", "Queued.Entry")

	rec := m.buildRestoreReconciliation(context.Background())
	if rec == nil {
		t.Fatal("buildRestoreReconciliation returned nil with a working provider")
	}
	job, err := m.rebuildQueuedTorrentJob(entry, rec)
	if err != nil {
		t.Fatalf("rebuildQueuedTorrentJob: %v", err)
	}
	if !job.ResumeExisting {
		t.Fatal("job re-submits the magnet; the provider already holds this torrent")
	}
	if job.Request != nil {
		t.Fatalf("job carries a submission request %+v, want adoption only", job.Request)
	}
	if entry.ActiveProvider != "prov" {
		t.Fatalf("ActiveProvider = %q, want prov", entry.ActiveProvider)
	}
	if p := entry.GetActiveProvider(); p == nil || p.ID != "rd-1" {
		t.Fatalf("placement = %+v, want adopted provider id rd-1", p)
	}
	// MERGE DIRECTION: provider status adopted ONTO our entry; the arr
	// association our record alone holds must survive intact.
	if entry.Category != "sonarr" || entry.CallbackURL != "https://callback.invalid/hook" {
		t.Fatalf("arr association was clobbered by the merge: category=%q callback=%q", entry.Category, entry.CallbackURL)
	}
}

// TestReconcileAbsenceIsNotEvidence: a hash no provider mentions must fall
// straight through to the unchanged re-submit path. Enumeration may be partial
// and absence proves nothing.
func TestReconcileAbsenceIsNotEvidence(t *testing.T) {
	client := &reconcileClient{
		all: []*debridTypes.Torrent{{Id: "rd-9", InfoHash: "someoneelse", Status: debridTypes.TorrentStatusDownloaded}},
	}
	m := newReconcileFixture(t, map[string]debrid.Client{"prov": client})
	entry := seedQueuedTorrent(t, m, "missinghash", "Missing.Entry")

	rec := m.buildRestoreReconciliation(context.Background())
	job, err := m.rebuildQueuedTorrentJob(entry, rec)
	if err != nil {
		t.Fatalf("rebuildQueuedTorrentJob: %v", err)
	}
	if job.ResumeExisting {
		t.Fatal("adopted a placement for a hash no provider reported")
	}
	if job.Request == nil {
		t.Fatal("absence must fall through to the ordinary re-submit path")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.detailsCalls != 0 {
		t.Fatalf("fetched details %d times for an unsighted hash, want 0", client.detailsCalls)
	}
}

// TestReconcileEnumerationFailureFallsBack: when the provider cannot be
// enumerated at all, behaviour must be exactly what it was before this check
// existed — re-submit — never a new conclusion drawn from silence.
func TestReconcileEnumerationFailureFallsBack(t *testing.T) {
	client := &reconcileClient{allErr: errors.New("provider unreachable")}
	m := newReconcileFixture(t, map[string]debrid.Client{"prov": client})
	entry := seedQueuedTorrent(t, m, "hash1", "Queued.Entry")

	rec := m.buildRestoreReconciliation(context.Background())
	job, err := m.rebuildQueuedTorrentJob(entry, rec)
	if err != nil {
		t.Fatalf("rebuildQueuedTorrentJob: %v", err)
	}
	if job.ResumeExisting || job.Request == nil {
		t.Fatalf("a failed enumeration changed behaviour: resume=%v request=%v", job.ResumeExisting, job.Request != nil)
	}
}

// TestReconcileDetailFetchFailureFallsBack: a positive sighting whose details
// cannot be fetched must not produce a fabricated placement.
func TestReconcileDetailFetchFailureFallsBack(t *testing.T) {
	client := &reconcileClient{
		all:       []*debridTypes.Torrent{{Id: "rd-1", InfoHash: "hash1", Status: debridTypes.TorrentStatusDownloaded}},
		detailErr: errors.New("boom"),
	}
	m := newReconcileFixture(t, map[string]debrid.Client{"prov": client})
	entry := seedQueuedTorrent(t, m, "hash1", "Queued.Entry")

	rec := m.buildRestoreReconciliation(context.Background())
	job, err := m.rebuildQueuedTorrentJob(entry, rec)
	if err != nil {
		t.Fatalf("rebuildQueuedTorrentJob: %v", err)
	}
	if job.ResumeExisting || job.Request == nil {
		t.Fatal("a failed detail fetch must fall back to re-submit, not adopt a half-known placement")
	}
	if entry.ActiveProvider != "" {
		t.Fatalf("ActiveProvider = %q, want empty; nothing was successfully adopted", entry.ActiveProvider)
	}
}

// TestReconcileSkipsDeadProviderCopies: adopting a copy the provider already
// calls dead would resume a torrent that can never serve. Falling through to
// re-submit at least attempts recovery.
func TestReconcileSkipsDeadProviderCopies(t *testing.T) {
	client := &reconcileClient{
		all: []*debridTypes.Torrent{{
			Id: "rd-1", InfoHash: "hash1",
			Status: debridTypes.TorrentStatusError, ProviderDead: true, ProviderStatus: "dead",
		}},
		details: map[string]*debridTypes.Torrent{"rd-1": remoteTorrent("hash1", "rd-1", "prov")},
	}
	m := newReconcileFixture(t, map[string]debrid.Client{"prov": client})
	entry := seedQueuedTorrent(t, m, "hash1", "Queued.Entry")

	rec := m.buildRestoreReconciliation(context.Background())
	job, err := m.rebuildQueuedTorrentJob(entry, rec)
	if err != nil {
		t.Fatalf("rebuildQueuedTorrentJob: %v", err)
	}
	if job.ResumeExisting {
		t.Fatal("adopted a placement the provider reports terminally dead")
	}
}

// TestReconcileLeavesExistingPlacementsAlone: an entry we already hold a
// placement for takes the pre-existing adopt branch and must never consult
// reconciliation or cost a provider call.
func TestReconcileLeavesExistingPlacementsAlone(t *testing.T) {
	client := &reconcileClient{}
	m := newReconcileFixture(t, map[string]debrid.Client{"prov": client})
	entry := probeTorrentEntry("placedhash", "Placed.Entry") // has ActiveProvider + placement
	entry.Status = debridTypes.TorrentStatusQueued

	job, err := m.rebuildQueuedTorrentJob(entry, nil)
	if err != nil {
		t.Fatalf("rebuildQueuedTorrentJob: %v", err)
	}
	if !job.ResumeExisting {
		t.Fatal("an entry with a placement must resume, not re-submit")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.allCalls != 0 || client.detailsCalls != 0 {
		t.Fatalf("already-placed entry cost provider calls: all=%d details=%d", client.allCalls, client.detailsCalls)
	}
}

// TestReconciliationSkippedWhenNothingCouldReconcile: the enumeration is only
// worth its cost when some entry would actually reach the re-submit branch.
func TestReconciliationSkippedWhenNothingCouldReconcile(t *testing.T) {
	m := newReconcileFixture(t, map[string]debrid.Client{"prov": &reconcileClient{}})

	placed := probeTorrentEntry("placedhash", "Placed")
	placed.Status = debridTypes.TorrentStatusQueued
	if m.queuedTorrentNeedsReconciliation([]*storage.Entry{placed}) {
		t.Error("an already-placed torrent should not trigger enumeration")
	}

	notQueued := queuedTorrentNoPlacement("h", "NotQueued")
	notQueued.Status = debridTypes.TorrentStatusDownloading
	if m.queuedTorrentNeedsReconciliation([]*storage.Entry{notQueued}) {
		t.Error("a non-queued torrent should not trigger enumeration")
	}

	needs := queuedTorrentNoPlacement("h2", "Needs")
	if !m.queuedTorrentNeedsReconciliation([]*storage.Entry{placed, notQueued, needs}) {
		t.Error("a queued torrent with no placement MUST trigger enumeration")
	}
}

// TestReconcileIsolatesProviderFailures: one provider erroring must not suppress
// another's sightings.
func TestReconcileIsolatesProviderFailures(t *testing.T) {
	bad := &reconcileClient{allErr: errors.New("down")}
	good := &reconcileClient{
		all:     []*debridTypes.Torrent{{Id: "g-1", InfoHash: "hash1", Status: debridTypes.TorrentStatusDownloaded}},
		details: map[string]*debridTypes.Torrent{"g-1": remoteTorrent("hash1", "g-1", "good")},
	}
	m := newReconcileFixture(t, map[string]debrid.Client{"bad": bad, "good": good})
	entry := seedQueuedTorrent(t, m, "hash1", "Queued.Entry")

	rec := m.buildRestoreReconciliation(context.Background())
	if rec == nil {
		t.Fatal("reconciliation nil despite one healthy provider")
	}
	if len(rec.failed) != 1 || len(rec.answered) != 1 {
		t.Fatalf("answered=%v failed=%v, want one of each", rec.answered, rec.failed)
	}
	job, err := m.rebuildQueuedTorrentJob(entry, rec)
	if err != nil {
		t.Fatalf("rebuildQueuedTorrentJob: %v", err)
	}
	if !job.ResumeExisting {
		t.Fatal("a healthy provider's sighting was lost because another provider failed")
	}
}

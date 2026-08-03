package manager

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/sirrobot01/decypharr/internal/config"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// THE BUG THESE TESTS EXIST FOR.
//
// The stall sweep listed candidates from the QUEUE store and then re-read each
// one through Manager.GetEntry, which resolves the MAIN store. An in-flight
// download is written to the main store only on completion, so the re-read
// missed on every candidate the sweep could ever act on and dropped it with a
// bare `continue`. The sweep ran, judged correctly, and pruned nothing —
// permanently, and without a single log line.
//
// The tests below are written against the sweep's real entry point rather than
// against prunableReason, because prunableReason was never the broken part. A
// test of the predicate alone passes on the defective code.

// stallPruneClient accepts every placement release and records which provider
// IDs it was asked to delete.
type stallPruneClient struct {
	fakeDebridClient
	mu      sync.Mutex
	deleted []string
}

func (c *stallPruneClient) DeleteTorrent(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleted = append(c.deleted, id)
	return nil
}

func (c *stallPruneClient) released() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.deleted...)
}

func newStallPruneFixture(t *testing.T) (*Manager, *stallPruneClient) {
	t.Helper()
	m := newActionLifecycleFixture(t, 2)
	client := &stallPruneClient{}
	m.clients = xsync.NewMap[string, debrid.Client]()
	m.clients.Store("prov", client)
	return m, client
}

// seedStalledQueueEntry writes a torrent that is unambiguously stalled: on the
// provider, downloading, zero bytes moved, and old enough to clear any sane
// threshold.
func seedStalledQueueEntry(t *testing.T, m *Manager, hash string) *storage.Entry {
	t.Helper()
	entry := &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       hash,
		Name:           "Stalled." + hash,
		SavePath:       t.TempDir(),
		State:          storage.EntryStateDownloading,
		Status:         debridTypes.TorrentStatusDownloading,
		ActiveProvider: "prov",
		Size:           1 << 30,
		Progress:       0,
		Speed:          0,
		Seeders:        0,
		AddedOn:        time.Now().Add(-7 * time.Hour),
		Providers: map[string]*storage.ProviderEntry{
			"prov": {Provider: "prov", ID: "placement-" + hash, Status: debridTypes.TorrentStatusDownloading},
		},
		Files: map[string]*storage.File{},
	}
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("queue.Add(%s): %v", hash, err)
	}
	return entry
}

// TestStallPrunePrunesAQueuedStalledTorrent is the regression test. The entry
// exists only in the queue — which is what "still downloading" means — and the
// sweep must act on it. On the defective re-read this returns 0.
func TestStallPrunePrunesAQueuedStalledTorrent(t *testing.T) {
	m, client := newStallPruneFixture(t)
	entry := seedStalledQueueEntry(t, m, "stalledhash")

	pruned := m.pruneStalledDownloads(context.Background(), stallSettings(nil))
	if pruned != 1 {
		t.Fatalf("pruned = %d, want 1: a 7h-old torrent at zero bytes must be pruned", pruned)
	}

	// The slot is the resource being reclaimed, so the provider delete is the
	// half that must actually have happened.
	if got := client.released(); len(got) != 1 || got[0] != "placement-stalledhash" {
		t.Fatalf("provider deletes = %v, want [placement-stalledhash]", got)
	}

	current, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetTorrent after prune: %v", err)
	}
	// Failed, not deleted: the arr has to observe the failure to re-search.
	if current.State != storage.EntryStateError {
		t.Fatalf("state = %q, want %q — the arr never learns from a silently removed row",
			current.State, storage.EntryStateError)
	}
	if current.LastError == "" {
		t.Fatal("prune recorded no reason; the arr-visible failure must say why")
	}
}

// TestPrunableEntryReadsTheQueue pins the store choice directly, so a future
// refactor that "simplifies" this back to GetEntry fails here with a message
// that explains why rather than in a distant behavioural test.
func TestPrunableEntryReadsTheQueue(t *testing.T) {
	m, _ := newStallPruneFixture(t)
	seedStalledQueueEntry(t, m, "queueonly")

	if _, err := m.GetEntry("queueonly"); err == nil {
		t.Fatal("precondition failed: an in-flight entry must NOT be in the main store, " +
			"otherwise this test proves nothing")
	}
	got, err := m.PrunableEntry("queueonly")
	if err != nil {
		t.Fatalf("PrunableEntry on a live queue row: %v", err)
	}
	if got.InfoHash != "queueonly" {
		t.Fatalf("infohash = %q, want queueonly", got.InfoHash)
	}
}

// TestPrunableEntryRefusesNonQueueHashes: a completed library entry has no arr
// queue row to fail, so pruning it cannot do the thing a prune is for. Refuse
// with a typed error rather than half-performing a different action.
func TestPrunableEntryRefusesNonQueueHashes(t *testing.T) {
	m, _ := newStallPruneFixture(t)
	libraryOnly := probeTorrentEntry("libraryhash", "Library.Entry")
	if err := m.storage.AddOrUpdate(libraryOnly); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}

	if _, err := m.PrunableEntry("libraryhash"); !errors.Is(err, errNotAnActiveEntry) {
		t.Fatalf("err = %v, want errNotAnActiveEntry", err)
	}
	if _, err := m.PrunableEntry("nothing-anywhere"); !errors.Is(err, errNotAnActiveEntry) {
		t.Fatalf("err = %v, want errNotAnActiveEntry", err)
	}
}

// TestStallPruneLeavesHealthyTorrentsAlone is the mirror: now that the sweep can
// actually reach entries, a test that only proves "it prunes" would pass on an
// implementation that prunes everything.
func TestStallPruneLeavesHealthyTorrentsAlone(t *testing.T) {
	m, _ := newStallPruneFixture(t)

	moving := seedStalledQueueEntry(t, m, "movinghash")
	moving.AddedOn = time.Now().Add(-40 * time.Minute)
	moving.Progress = 0.5
	moving.Speed = 5 << 20
	if err := m.queue.Update(moving); err != nil {
		t.Fatalf("queue.Update: %v", err)
	}

	fresh := seedStalledQueueEntry(t, m, "freshhash")
	fresh.AddedOn = time.Now().Add(-2 * time.Minute)
	if err := m.queue.Update(fresh); err != nil {
		t.Fatalf("queue.Update: %v", err)
	}

	if pruned := m.pruneStalledDownloads(context.Background(), stallSettings(nil)); pruned != 0 {
		t.Fatalf("pruned = %d, want 0: a moving torrent and a 2-minute-old one are both healthy", pruned)
	}
}

// TestStallPruneSkipsQueuedEntriesHoldingNoSlot: an entry parked by the capacity
// hold reports Status queued and occupies nothing on the provider. Pruning it
// would fail a download the provider was never asked to start.
func TestStallPruneSkipsQueuedEntriesHoldingNoSlot(t *testing.T) {
	m, _ := newStallPruneFixture(t)
	held := seedStalledQueueEntry(t, m, "heldhash")
	held.Status = debridTypes.TorrentStatusQueued
	if err := m.queue.Update(held); err != nil {
		t.Fatalf("queue.Update: %v", err)
	}

	if pruned := m.pruneStalledDownloads(context.Background(), stallSettings(nil)); pruned != 0 {
		t.Fatalf("pruned = %d, want 0: a capacity-held entry holds no provider slot", pruned)
	}
}

// TestStallPruneDisabledDoesNothing keeps the destructive stage off by default.
func TestStallPruneDisabledDoesNothing(t *testing.T) {
	m, _ := newStallPruneFixture(t)
	seedStalledQueueEntry(t, m, "disabledhash")

	empty := resolveStallPruneSettings(config.StallPruneConfig{})
	if empty.enabled() {
		t.Fatal("an empty stall-prune config must resolve to disabled")
	}
	if pruned := m.pruneStalledDownloads(context.Background(), empty); pruned != 0 {
		t.Fatalf("pruned = %d with the feature disabled, want 0", pruned)
	}
}

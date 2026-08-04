package manager

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"

	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// THE ARCHITECTURAL INVERSION.
//
// The local sweep asks "which of MY entries look stalled" and is therefore
// blind to anything that fell out of local state — measured, decypharr could
// see 4 downloading entries while RealDebrid ran 108 transfers, 68 of them dead
// or crawling. This pass asks the PROVIDER instead.
//
// It deletes provider-side transfers, so the tests below are mostly about what
// it must REFUSE to touch.

// providerPruneClient serves a scripted account listing and records deletes.
type providerPruneClient struct {
	stallPruneClient
	torrents []*debridTypes.Torrent
}

func (c *providerPruneClient) GetAllTorrents() ([]*debridTypes.Torrent, error) {
	return c.torrents, nil
}

func remoteActive(id, hash string, progress float64, size int64, age time.Duration) *debridTypes.Torrent {
	return &debridTypes.Torrent{
		Id:       id,
		InfoHash: hash,
		Name:     "Remote." + id,
		Debrid:   "prov",
		Status:   debridTypes.TorrentStatusDownloading,
		Progress: progress,
		Size:     size,
		Added:    time.Now().Add(-age),
	}
}

func newProviderPruneFixture(t *testing.T, torrents ...*debridTypes.Torrent) (*Manager, *providerPruneClient) {
	t.Helper()
	m := newActionLifecycleFixture(t, 2)
	client := &providerPruneClient{torrents: torrents}
	m.clients = xsync.NewMap[string, debrid.Client]()
	m.clients.Store("prov", client)
	return m, client
}

// TestProviderPruneReleasesAnUntrackedStalledTransfer is the whole point: no
// local record exists, and the slot is freed anyway.
func TestProviderPruneReleasesAnUntrackedStalledTransfer(t *testing.T) {
	dead := remoteActive("rd-dead", strings.Repeat("a", 40), 0, 1<<30, 7*time.Hour)
	m, client := newProviderPruneFixture(t, dead)

	if _, err := m.GetEntry(dead.InfoHash); err == nil {
		t.Fatal("precondition: this transfer must have no local record")
	}

	released := m.pruneProviderStalled(context.Background(), stallSettings(nil))
	if released != 1 {
		t.Fatalf("released = %d, want 1: a 7h-old zero-progress transfer must be pruned even with no local row", released)
	}
	if got := client.released(); len(got) != 1 || got[0] != "rd-dead" {
		t.Fatalf("provider deletes = %v, want [rd-dead]", got)
	}
}

// TestProviderPruneKeepsProgressingTransfers is the guard that matters most on
// this population: 67 of 94 measured orphans were 50-99% complete, and killing
// those throws away days of transfer on nearly-usable content.
func TestProviderPruneKeepsProgressingTransfers(t *testing.T) {
	// 1 GB, 70% done in 2h => ~102 KB/s average => ~50min remaining. Healthy
	// against a 24h ceiling.
	healthy := remoteActive("rd-live", strings.Repeat("b", 40), 0.70, 1<<30, 2*time.Hour)
	m, client := newProviderPruneFixture(t, healthy)

	if released := m.pruneProviderStalled(context.Background(), stallSettings(nil)); released != 0 {
		t.Fatalf("released = %d, want 0: a transfer at 70%% and moving must not be killed", released)
	}
	if got := client.released(); len(got) != 0 {
		t.Fatalf("deleted %v; nearly-complete transfers are exactly what must be preserved", got)
	}
}

// TestProviderPruneIgnoresCompletedAndDead: a downloaded transfer occupies
// storage, not a download slot; a terminally dead one is ENUMERATE's to reap
// through the health path that records why.
func TestProviderPruneIgnoresCompletedAndDead(t *testing.T) {
	done := remoteActive("rd-done", strings.Repeat("c", 40), 1, 1<<30, 9*time.Hour)
	done.Status = debridTypes.TorrentStatusDownloaded

	dead := remoteActive("rd-dead", strings.Repeat("d", 40), 0, 1<<30, 9*time.Hour)
	dead.ProviderDead = true
	dead.ProviderStatus = "magnet_error"

	m, client := newProviderPruneFixture(t, done, dead)

	if released := m.pruneProviderStalled(context.Background(), stallSettings(nil)); released != 0 {
		t.Fatalf("released = %d, want 0", released)
	}
	if got := client.released(); len(got) != 0 {
		t.Fatalf("deleted %v; neither a completed nor a terminally dead copy belongs to this pass", got)
	}
}

// TestProviderPruneRespectsTheWindow: zero progress is the NORMAL state of a
// transfer the provider only just started.
func TestProviderPruneRespectsTheWindow(t *testing.T) {
	fresh := remoteActive("rd-new", strings.Repeat("e", 40), 0, 1<<30, 3*time.Minute)
	m, client := newProviderPruneFixture(t, fresh)

	if released := m.pruneProviderStalled(context.Background(), stallSettings(nil)); released != 0 {
		t.Fatalf("released = %d, want 0 for a 3-minute-old transfer", released)
	}
	if got := client.released(); len(got) != 0 {
		t.Fatalf("deleted %v", got)
	}
}

// TestProviderPruneFailsTheArrWhenWeDoHaveARow: where a local row exists it
// goes through the shared PruneEntry, so the *arr learns and re-searches, and
// the two prune paths cannot drift.
func TestProviderPruneFailsTheArrWhenWeDoHaveARow(t *testing.T) {
	hash := "trackedhash"
	m, client := newProviderPruneFixture(t)
	entry := seedStalledQueueEntry(t, m, hash)
	client.torrents = []*debridTypes.Torrent{
		remoteActive(placementIDOf(entry), hash, 0, 1<<30, 7*time.Hour),
	}

	if released := m.pruneProviderStalled(context.Background(), stallSettings(nil)); released != 1 {
		t.Fatalf("released = %d, want 1", released)
	}

	current, err := m.queue.GetTorrent(hash)
	if err != nil {
		t.Fatalf("GetTorrent: %v", err)
	}
	if current.State != storage.EntryStateError {
		t.Fatalf("state = %q, want %q — a tracked entry must be failed so the arr re-searches",
			current.State, storage.EntryStateError)
	}
}

// TestProviderPruneIsOffWhenTheFeatureIsDisabled. It deletes provider data, so
// an unconfigured install must not run it.
func TestProviderPruneIsOffWhenTheFeatureIsDisabled(t *testing.T) {
	dead := remoteActive("rd-dead", strings.Repeat("f", 40), 0, 1<<30, 30*time.Hour)
	m, client := newProviderPruneFixture(t, dead)

	off := stallSettings(func(s *stallPruneSettings) {
		s.noProgressAfter = 0
		s.maxETA = 0
	})
	if released := m.pruneProviderStalled(context.Background(), off); released != 0 {
		t.Fatalf("released = %d with the feature disabled", released)
	}
	if got := client.released(); len(got) != 0 {
		t.Fatalf("a disabled sweep deleted %v", got)
	}
}

// TestProviderPruneHonoursTheSweepCap keeps a bad threshold from emptying an
// account in one tick.
func TestProviderPruneHonoursTheSweepCap(t *testing.T) {
	torrents := make([]*debridTypes.Torrent, 0, 20)
	for i := 0; i < 20; i++ {
		torrents = append(torrents,
			remoteActive("rd-"+string(rune('a'+i)), strings.Repeat("9", 39)+string(rune('a'+i)), 0, 1<<30, 8*time.Hour))
	}
	m, client := newProviderPruneFixture(t, torrents...)

	capped := stallSettings(func(s *stallPruneSettings) { s.maxPerSweep = 5 })
	if released := m.pruneProviderStalled(context.Background(), capped); released != 5 {
		t.Fatalf("released = %d, want 5", released)
	}
	if got := len(client.released()); got != 5 {
		t.Fatalf("provider deletes = %d, want 5", got)
	}
}

// TestProgressScaleIsNormalisedOneWay. Providers disagree about whether
// progress is 0-1 or 0-100; reading a 0-100 value as a fraction would make a
// 70%-complete transfer look like 7000% and mask a real stall, while the
// reverse would read 0.5 as half a percent and kill a healthy one.
func TestProgressScaleIsNormalisedOneWay(t *testing.T) {
	percent := remoteActive("rd-pct", strings.Repeat("1", 40), 70, 1<<30, 2*time.Hour)
	fraction := remoteActive("rd-frac", strings.Repeat("2", 40), 0.70, 1<<30, 2*time.Hour)
	m, client := newProviderPruneFixture(t, percent, fraction)

	if released := m.pruneProviderStalled(context.Background(), stallSettings(nil)); released != 0 {
		t.Fatalf("released = %d, want 0: both spellings of 70%% describe a healthy transfer", released)
	}
	if got := client.released(); len(got) != 0 {
		t.Fatalf("deleted %v", got)
	}
}

// TestEnumerationFailureIsNotAnEmptyAccount: a provider that cannot be listed
// must not read as "nothing to prune", nor cause anything to be deleted.
func TestEnumerationFailureIsNotAnEmptyAccount(t *testing.T) {
	m, _ := newProviderPruneFixture(t)
	failing := &failingListClient{}
	m.clients = xsync.NewMap[string, debrid.Client]()
	m.clients.Store("prov", failing)

	if released := m.pruneProviderStalled(context.Background(), stallSettings(nil)); released != 0 {
		t.Fatalf("released = %d on a provider that could not be enumerated", released)
	}
	if failing.deletes != 0 {
		t.Fatalf("deleted %d items despite having no usable listing", failing.deletes)
	}
}

type failingListClient struct {
	stallPruneClient
	deletes int
}

func (c *failingListClient) GetAllTorrents() ([]*debridTypes.Torrent, error) {
	return nil, context.DeadlineExceeded
}

func (c *failingListClient) DeleteTorrent(string) error {
	c.deletes++
	return nil
}

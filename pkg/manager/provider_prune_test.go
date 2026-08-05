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

// THE PROVIDER-SOURCED SWEEP, ON THE OPERATOR'S MODEL.
//
// The candidate set is the PROVIDER'S active list, because the slots are the
// provider's. The judgement is ONE test — failsafe, then ETA — measured over
// the sampling window.
//
// The window is the whole safety property. Torrent speeds float, so a delta
// between two consecutive sweeps is noise: a swarm that goes quiet for five
// minutes would read as stopped, and under a pure-ETA test that means deleted.
// Several tests below exist only to pin that.

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
	m.progress = newProgressTracker()
	return m, client
}

// seedSamples installs a reading history spanning the window, so a test can
// reach the ETA branch without waiting out real time. progressAt is called with
// each sample's age (oldest first) and returns the progress at that moment.
func seedSamples(m *Manager, key string, window time.Duration, count int, progressAt func(age time.Duration) float64) {
	now := time.Now()
	series := make(sampleSeries, 0, count)
	for i := count - 1; i >= 0; i-- {
		age := time.Duration(i) * (window / time.Duration(count-1))
		series = append(series, progressObservation{progress: progressAt(age), at: now.Add(-age)})
	}
	m.progress.mu.Lock()
	m.progress.seen[key] = series
	m.progress.mu.Unlock()
}

// TestFailsafePrunesWithNoSamplesAtAll. The backstop needs no measurement — the
// provider's own `added` timestamp is enough — which is exactly why it still
// works after a restart, when the ETA test cannot answer.
func TestFailsafePrunesWithNoSamplesAtAll(t *testing.T) {
	ancient := remoteActive("rd-old", strings.Repeat("a", 40), 0.99, 1<<30, 72*time.Hour)
	m, client := newProviderPruneFixture(t, ancient)

	if released := m.pruneProviderStalled(context.Background(), stallSettings(nil)); released != 1 {
		t.Fatalf("released = %d, want 1: 72h is over the 48h hard limit regardless of ETA", released)
	}
	if got := client.released(); len(got) != 1 || got[0] != "rd-old" {
		t.Fatalf("provider deletes = %v", got)
	}
}

// TestFirstSweepReachesNoETAVerdict.
//
// A real operational consequence, pinned so it cannot be mistaken for the
// feature being broken: with no reading history, the ETA branch cannot answer,
// so a restart delays ETA-based pruning by one sampling window. Only the
// failsafe fires before then. That is the correct trade — the alternative is
// judging on a lifetime average, which flatters a transfer that died hours ago.
func TestFirstSweepReachesNoETAVerdict(t *testing.T) {
	dead := remoteActive("rd-dead", strings.Repeat("b", 40), 0, 1<<30, 7*time.Hour)
	m, client := newProviderPruneFixture(t, dead)

	if released := m.pruneProviderStalled(context.Background(), stallSettings(nil)); released != 0 {
		t.Fatalf("released = %d; with no samples there is no ETA verdict to reach", released)
	}
	if got := client.released(); len(got) != 0 {
		t.Fatalf("deleted %v on a first sighting", got)
	}
}

// TestZeroMovementAcrossTheWindowIsPruned — the case the whole feature exists
// for, once there is enough data to say so.
func TestZeroMovementAcrossTheWindowIsPruned(t *testing.T) {
	stuck := remoteActive("rd-stuck", strings.Repeat("c", 40), 0.76, 1<<30, 30*time.Hour)
	m, client := newProviderPruneFixture(t, stuck)
	s := stallSettings(nil)

	// Frozen at 76% for the whole window.
	seedSamples(m, "prov\x00rd-stuck", s.sampleWindow, 7, func(time.Duration) float64 { return 0.76 })

	if released := m.pruneProviderStalled(context.Background(), s); released != 1 {
		t.Fatalf("released = %d, want 1: no bytes moved across the window is an infinite ETA", released)
	}
	if got := client.released(); len(got) != 1 {
		t.Fatalf("provider deletes = %v", got)
	}
}

// TestHealthyMovementSurvives is the mirror.
func TestHealthyMovementSurvives(t *testing.T) {
	// 1 GB, currently 50%, moving ~1%/5min across the window => finishes in
	// well under the 24h ceiling.
	live := remoteActive("rd-live", strings.Repeat("d", 40), 0.50, 1<<30, 4*time.Hour)
	m, client := newProviderPruneFixture(t, live)
	s := stallSettings(nil)

	seedSamples(m, "prov\x00rd-live", s.sampleWindow, 7, func(age time.Duration) float64 {
		return 0.50 - age.Minutes()*0.002 // ~0.2%/min
	})

	if released := m.pruneProviderStalled(context.Background(), s); released != 0 {
		t.Fatalf("released = %d, want 0: this transfer is moving and finishes inside the ceiling", released)
	}
	if got := client.released(); len(got) != 0 {
		t.Fatalf("deleted %v", got)
	}
}

// 🛑 TestAMomentaryLullDoesNotDelete.
//
// THE REASON THE WINDOW EXISTS. A transfer that moved steadily and then paused
// for the most recent sweep must NOT be deleted — swarms do that routinely. A
// single-interval delta would read the last five minutes as zero, project an
// infinite ETA, and destroy a healthy download.
func TestAMomentaryLullDoesNotDelete(t *testing.T) {
	live := remoteActive("rd-lull", strings.Repeat("e", 40), 0.60, 1<<30, 4*time.Hour)
	m, client := newProviderPruneFixture(t, live)
	s := stallSettings(nil)

	// Moved for most of the window, flat for the newest sample only.
	seedSamples(m, "prov\x00rd-lull", s.sampleWindow, 7, func(age time.Duration) float64 {
		if age < s.sampleWindow/6 {
			return 0.60 // the lull
		}
		return 0.60 - (age - s.sampleWindow/6).Minutes()*0.002
	})

	if released := m.pruneProviderStalled(context.Background(), s); released != 0 {
		t.Fatalf("released = %d; a single quiet sweep inside an otherwise moving window must not delete", released)
	}
	if got := client.released(); len(got) != 0 {
		t.Fatalf("deleted %v on a momentary lull — this is the failure the window prevents", got)
	}
}

// TestPartialSeriesReachesNoVerdict: old enough to judge, but our own history
// does not span the window yet. Waiting one window costs nothing.
func TestPartialSeriesReachesNoVerdict(t *testing.T) {
	stuck := remoteActive("rd-partial", strings.Repeat("f", 40), 0.10, 1<<30, 30*time.Hour)
	m, client := newProviderPruneFixture(t, stuck)
	s := stallSettings(nil)

	// Only 5 minutes of history against a 30-minute window.
	now := time.Now()
	m.progress.mu.Lock()
	m.progress.seen["prov\x00rd-partial"] = sampleSeries{
		{progress: 0.10, at: now.Add(-5 * time.Minute)},
		{progress: 0.10, at: now},
	}
	m.progress.mu.Unlock()

	if released := m.pruneProviderStalled(context.Background(), s); released != 0 {
		t.Fatalf("released = %d on a partial series; that reading is not trustworthy yet", released)
	}
	if got := client.released(); len(got) != 0 {
		t.Fatalf("deleted %v on an untrustworthy reading", got)
	}
}

func TestProviderPruneIgnoresCompletedAndDead(t *testing.T) {
	done := remoteActive("rd-done", strings.Repeat("1", 40), 1, 1<<30, 90*time.Hour)
	done.Status = debridTypes.TorrentStatusDownloaded

	dead := remoteActive("rd-dead", strings.Repeat("2", 40), 0, 1<<30, 90*time.Hour)
	dead.ProviderDead = true
	dead.ProviderStatus = "magnet_error"

	m, client := newProviderPruneFixture(t, done, dead)

	if released := m.pruneProviderStalled(context.Background(), stallSettings(nil)); released != 0 {
		t.Fatalf("released = %d, want 0 — and note both are past the failsafe age, so this "+
			"pins that the status guards come first", released)
	}
	if got := client.released(); len(got) != 0 {
		t.Fatalf("deleted %v", got)
	}
}

func TestProviderPruneFailsTheArrWhenWeDoHaveARow(t *testing.T) {
	hash := "trackedhash"
	m, client := newProviderPruneFixture(t)
	entry := seedStalledQueueEntry(t, m, hash)
	client.torrents = []*debridTypes.Torrent{
		remoteActive(placementIDOf(entry), hash, 0, 1<<30, 72*time.Hour),
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

func TestProviderPruneIsOffWhenTheFeatureIsDisabled(t *testing.T) {
	dead := remoteActive("rd-dead", strings.Repeat("3", 40), 0, 1<<30, 90*time.Hour)
	m, client := newProviderPruneFixture(t, dead)

	off := stallSettings(func(s *stallPruneSettings) {
		s.sampleWindow = 0
		s.maxETA = 0
	})
	if released := m.pruneProviderStalled(context.Background(), off); released != 0 {
		t.Fatalf("released = %d with the feature disabled", released)
	}
	if got := client.released(); len(got) != 0 {
		t.Fatalf("a disabled sweep deleted %v", got)
	}
}

// TestMisconfiguredFailsafeDoesNotArm: refusing beats clamping to a number we
// invented, because the invented number would delete things.
func TestMisconfiguredFailsafeDoesNotArm(t *testing.T) {
	dead := remoteActive("rd-dead", strings.Repeat("4", 40), 0, 1<<30, 90*time.Hour)
	m, client := newProviderPruneFixture(t, dead)

	bad := stallSettings(func(s *stallPruneSettings) {
		s.misconfigured = "max_downloading_time is below eta_sample_window + max_eta"
	})
	if released := m.pruneProviderStalled(context.Background(), bad); released != 0 {
		t.Fatalf("released = %d from a refused configuration", released)
	}
	if got := client.released(); len(got) != 0 {
		t.Fatalf("a refused configuration deleted %v", got)
	}
}

func TestProviderPruneHonoursTheSweepCap(t *testing.T) {
	torrents := make([]*debridTypes.Torrent, 0, 20)
	for i := 0; i < 20; i++ {
		torrents = append(torrents,
			remoteActive("rd-"+string(rune('a'+i)), strings.Repeat("9", 39)+string(rune('a'+i)), 0, 1<<30, 90*time.Hour))
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

// TestProgressScaleIsNormalisedOneWay. Reading a 0-100 value as a fraction
// would make 70% look like 7000% and mask a real problem; the reverse would
// read 0.5 as half a percent and kill a healthy transfer.
func TestProgressScaleIsNormalisedOneWay(t *testing.T) {
	percent := remoteActive("rd-pct", strings.Repeat("5", 40), 70, 1<<30, 4*time.Hour)
	fraction := remoteActive("rd-frac", strings.Repeat("6", 40), 0.70, 1<<30, 4*time.Hour)
	m, deletes := newProviderPruneFixture(t, percent, fraction)
	s := stallSettings(nil)

	for _, key := range []string{"prov\x00rd-pct", "prov\x00rd-frac"} {
		seedSamples(m, key, s.sampleWindow, 7, func(age time.Duration) float64 {
			return 0.70 - age.Minutes()*0.002
		})
	}

	if released := m.pruneProviderStalled(context.Background(), s); released != 0 {
		t.Fatalf("released = %d; both spellings of 70%% describe a healthy transfer", released)
	}
	if got := deletes.released(); len(got) != 0 {
		t.Fatalf("deleted %v", got)
	}
}

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

// TestSpeedOverWindowRequiresFullCoverage pins the trust rule directly.
func TestSpeedOverWindowRequiresFullCoverage(t *testing.T) {
	now := time.Now()
	window := 30 * time.Minute

	short := sampleSeries{
		{progress: 0.1, at: now.Add(-5 * time.Minute)},
		{progress: 0.2, at: now},
	}
	if _, ok := short.speedOver(window, 1<<30, now); ok {
		t.Fatal("a series covering 5 minutes must not answer for a 30-minute window")
	}

	full := sampleSeries{
		{progress: 0.10, at: now.Add(-35 * time.Minute)},
		{progress: 0.20, at: now},
	}
	speed, ok := full.speedOver(window, 1<<30, now)
	if !ok || speed <= 0 {
		t.Fatalf("speed = %d ok = %v; a full-width series must answer", speed, ok)
	}

	flat := sampleSeries{
		{progress: 0.5, at: now.Add(-35 * time.Minute)},
		{progress: 0.5, at: now},
	}
	speed, ok = flat.speedOver(window, 1<<30, now)
	if !ok || speed != 0 {
		t.Fatalf("speed = %d ok = %v; a measured zero is a real reading, not an absent one", speed, ok)
	}
}

func TestTrackerForgetsWhatTheProviderNoLongerLists(t *testing.T) {
	p := newProgressTracker()
	now := time.Now()
	p.observe("gone", 0.5, time.Hour, now)
	p.observe("kept", 0.5, time.Hour, now)

	p.retain(map[string]struct{}{"kept": {}})

	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.seen["gone"]; ok {
		t.Fatal("an observation survived for a transfer the provider no longer lists")
	}
	if _, ok := p.seen["kept"]; !ok {
		t.Fatal("a live transfer's observation was dropped")
	}
}

package manager

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// newInfraTestManager builds a Manager with real storage/queue and a recording
// job queue but NO usenet client or fake NNTP server — enough to exercise the
// pure infra-cap/park/sweep logic without adding network listeners to the suite.
func newInfraTestManager(t *testing.T) (*Manager, chan *Job) {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()

	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	jobCh := make(chan *Job, 16)
	jobQueue := NewJobQueue(context.Background(), 1, func(_ context.Context, job *Job) {
		jobCh <- job
	})
	t.Cleanup(jobQueue.Close)

	m := &Manager{
		storage:  store,
		queue:    newQueue(store, ""),
		logger:   zerolog.Nop(),
		config:   cfg,
		arr:      arr.NewStorage(),
		jobQueue: jobQueue,
	}
	return m, jobCh
}

// ============================================================================
// Fix 2 — the infra-retry fast loop is capped and entries park for the slow
// sweep instead of re-parsing forever.
// ============================================================================

// readRetryJob returns the next retry job scheduled on the recording jobQueue,
// or nil if none arrives within the window (the entry parked).
func readRetryJob(t *testing.T, jobCh chan *Job, within time.Duration) *Job {
	t.Helper()
	select {
	case job := <-jobCh:
		return job
	case <-time.After(within):
		return nil
	}
}

// End-to-end livelock reproduction: an entry whose rebuild/Process always fails
// on the substrate (infrastructure class) is driven through the real processJob
// path. WITHOUT a cap it re-parses forever (a retry every cycle); WITH the cap
// the fast chain stops after nzbInfraFastRetryCap attempts and the entry parks —
// queued, not a terminal error, so it never reaches the Failed history. A dead
// substrate (connection refused) drives the infrastructure failure without
// leaving blocked server goroutines behind.
func TestInfraRetryChainIsCappedAndParks(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	m, jobCh := newVerdictTestManager(t, host, port)
	server.Close() // dead substrate: every re-parse probe fails infrastructure-class

	prevBase, prevCap := nzbInfraRetryBaseDelay, nzbInfraFastRetryCap
	nzbInfraRetryBaseDelay = time.Millisecond
	nzbInfraFastRetryCap = 3
	t.Cleanup(func() { nzbInfraRetryBaseDelay, nzbInfraFastRetryCap = prevBase, prevCap })

	entry := newQueuedNZBEntry(t, m, "livelock-entry")
	job := &Job{ID: entry.InfoHash, Type: JobTypeNZB, Entry: entry, RebuildQueued: true, CreatedAt: time.Now()}

	reparses := 0
	const hardStop = 20
	for reparses < hardStop {
		m.processJob(context.Background(), job)
		reparses++
		next := readRetryJob(t, jobCh, 500*time.Millisecond)
		if next == nil {
			break // no further fast retry scheduled: the entry parked
		}
		if next.ID != entry.InfoHash || !next.RebuildQueued {
			t.Fatalf("unexpected retry job %+v", next)
		}
		job = next
	}
	if reparses >= hardStop {
		t.Fatalf("infra retry chain never parked after %d re-parses: fast loop is unbounded", reparses)
	}
	if reparses > nzbInfraFastRetryCap+1 {
		t.Fatalf("fast loop ran %d re-parses, want <= cap+1 (%d)", reparses, nzbInfraFastRetryCap+1)
	}

	persisted, err := m.storage.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	if persisted.State == storage.EntryStateError {
		t.Fatalf("parked entry became a terminal error (LastError=%q); infra failures must not blocklist", persisted.LastError)
	}
	if persisted.Status != debridTypes.TorrentStatusQueued {
		t.Fatalf("parked entry status = %q, want %q", persisted.Status, debridTypes.TorrentStatusQueued)
	}
	if persisted.ErrorCount <= nzbInfraFastRetryCap {
		t.Fatalf("durable infra count = %d, want it to have grown past the cap %d", persisted.ErrorCount, nzbInfraFastRetryCap)
	}
	// No-false-blocklist invariant: a parked entry must NOT appear in the SAB
	// Failed history projection (which lists EntryStateError NZBs).
	for _, e := range m.queue.ListFilter("", config.ProtocolNZB, storage.EntryStateError, nil, "added_on", false) {
		if e.InfoHash == entry.InfoHash {
			t.Fatalf("parked infra entry leaked into the Failed history projection")
		}
	}
}

// deferInfraRetry schedules fast retries only up to the cap, then parks; and the
// durable count survives a re-feed so a parked entry cannot restart a fresh fast
// burst (requirement (d)). The entry never enters the terminal-error state.
func TestDeferInfraRetryCapIsDurableAcrossReFeed(t *testing.T) {
	m, jobCh := newInfraTestManager(t)

	prevBase, prevCap := nzbInfraRetryBaseDelay, nzbInfraFastRetryCap
	nzbInfraRetryBaseDelay = time.Millisecond
	nzbInfraFastRetryCap = 3
	t.Cleanup(func() { nzbInfraRetryBaseDelay, nzbInfraFastRetryCap = prevBase, prevCap })

	entry := newQueuedNZBEntry(t, m, "durable-entry")

	// The first `cap` deferrals each schedule exactly one fast retry.
	fastRetries := 0
	for i := 0; i < nzbInfraFastRetryCap; i++ {
		if err := m.deferInfraRetry(entry); err != nil {
			t.Fatalf("deferInfraRetry #%d: %v", i, err)
		}
		if readRetryJob(t, jobCh, 500*time.Millisecond) != nil {
			fastRetries++
		}
	}
	if fastRetries != nzbInfraFastRetryCap {
		t.Fatalf("scheduled %d fast retries within the cap, want %d", fastRetries, nzbInfraFastRetryCap)
	}

	// The next deferral crosses the cap: park, no fast retry.
	if err := m.deferInfraRetry(entry); err != nil {
		t.Fatalf("deferInfraRetry over cap: %v", err)
	}
	if job := readRetryJob(t, jobCh, 300*time.Millisecond); job != nil {
		t.Fatalf("a fast retry was scheduled past the cap: %+v", job)
	}

	// Simulate the slow sweep re-feeding the parked entry and it failing infra
	// again: exactly one more deferral, still no fresh fast burst.
	if err := m.deferInfraRetry(entry); err != nil {
		t.Fatalf("deferInfraRetry re-fed: %v", err)
	}
	if job := readRetryJob(t, jobCh, 300*time.Millisecond); job != nil {
		t.Fatalf("re-feed restarted a fresh fast burst: %+v", job)
	}

	persisted, err := m.storage.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	if persisted.State == storage.EntryStateError {
		t.Fatalf("parked entry became a terminal error; must not blocklist")
	}
	if persisted.Status != debridTypes.TorrentStatusQueued {
		t.Fatalf("parked entry status = %q, want queued", persisted.Status)
	}
	if persisted.ErrorCount != nzbInfraFastRetryCap+2 {
		t.Fatalf("durable infra count = %d, want %d (cap + 2 over-cap attempts)", persisted.ErrorCount, nzbInfraFastRetryCap+2)
	}
}

// seedParkedInfraNZB adds an NZB entry already parked by the infra cap: queued,
// downloading state, durable ErrorCount past the cap.
func seedParkedInfraNZB(t *testing.T, m *Manager, infohash string) *storage.Entry {
	t.Helper()
	now := time.Now()
	entry := &storage.Entry{
		InfoHash:         infohash,
		Name:             "parked-" + infohash,
		OriginalFilename: "parked-" + infohash + ".nzb",
		Protocol:         config.ProtocolNZB,
		Status:           debridTypes.TorrentStatusQueued,
		State:            storage.EntryStateDownloading,
		ErrorCount:       nzbInfraFastRetryCap + 2,
		CreatedAt:        now,
		UpdatedAt:        now,
		AddedOn:          now,
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("seed parked entry %s: %v", infohash, err)
	}
	return entry
}

// The parked-entry slow sweep re-feeds parked NZBs at a bounded rate, claims
// each out of the parked set before submitting (so a later sweep cannot
// double-feed it), and honors its limit.
func TestResweepParkedInfraNZBsFeedsBoundedly(t *testing.T) {
	m, jobCh := newInfraTestManager(t)

	prevCap := nzbInfraFastRetryCap
	nzbInfraFastRetryCap = 3
	t.Cleanup(func() { nzbInfraFastRetryCap = prevCap })

	seedParkedInfraNZB(t, m, "parked-a")
	seedParkedInfraNZB(t, m, "parked-b")
	seedParkedInfraNZB(t, m, "parked-c")

	// Limit is respected: only 2 of the 3 parked entries are fed.
	if fed := m.resweepParkedInfraNZBs(context.Background(), 2); fed != 2 {
		t.Fatalf("resweep fed %d entries, want the limit of 2", fed)
	}
	fedIDs := map[string]bool{}
	for i := 0; i < 2; i++ {
		job := readRetryJob(t, jobCh, time.Second)
		if job == nil || !job.RebuildQueued {
			t.Fatalf("expected a RebuildQueued job for a parked entry, got %+v", job)
		}
		fedIDs[job.ID] = true
	}
	if len(fedIDs) != 2 {
		t.Fatalf("expected 2 distinct parked entries fed, got %v", fedIDs)
	}

	// Fed entries were claimed out of the parked set (queued -> downloading), so
	// they are no longer eligible; only the 1 remaining parked entry is fed next.
	if fed := m.resweepParkedInfraNZBs(context.Background(), 10); fed != 1 {
		t.Fatalf("second resweep fed %d, want just the 1 remaining parked entry (no double-feed)", fed)
	}
	if job := readRetryJob(t, jobCh, time.Second); job == nil || fedIDs[job.ID] {
		t.Fatalf("second resweep must feed the previously-unfed entry, got %+v", job)
	}

	// Everything is now claimed out of the parked set: a third sweep feeds none.
	if fed := m.resweepParkedInfraNZBs(context.Background(), 10); fed != 0 {
		t.Fatalf("third resweep fed %d, want 0 (all entries already in flight)", fed)
	}
}

// A parked entry (durable ErrorCount past the cap, queued) is exactly what
// isParkedInfraNZB recognizes; genuine-error and in-flight entries are not.
func TestIsParkedInfraNZB(t *testing.T) {
	m, _ := newInfraTestManager(t)

	prevCap := nzbInfraFastRetryCap
	nzbInfraFastRetryCap = 3
	t.Cleanup(func() { nzbInfraFastRetryCap = prevCap })

	base := func() *storage.Entry {
		return &storage.Entry{
			Protocol:   config.ProtocolNZB,
			State:      storage.EntryStateDownloading,
			Status:     debridTypes.TorrentStatusQueued,
			ErrorCount: nzbInfraFastRetryCap + 1,
		}
	}
	cases := []struct {
		name   string
		mutate func(*storage.Entry)
		want   bool
	}{
		{"parked", func(*storage.Entry) {}, true},
		{"still within fast budget", func(e *storage.Entry) { e.ErrorCount = nzbInfraFastRetryCap }, false},
		{"in-flight (downloading)", func(e *storage.Entry) { e.Status = debridTypes.TorrentStatusDownloading }, false},
		{"terminal error", func(e *storage.Entry) { e.State = storage.EntryStateError }, false},
		{"torrent protocol", func(e *storage.Entry) { e.Protocol = config.ProtocolTorrent }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entry := base()
			tc.mutate(entry)
			if got := m.isParkedInfraNZB(entry); got != tc.want {
				t.Fatalf("isParkedInfraNZB = %v, want %v", got, tc.want)
			}
		})
	}
}

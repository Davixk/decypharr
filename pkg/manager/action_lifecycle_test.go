package manager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// newActionLifecycleFixture builds a manager wired closely enough to the real
// construction path that post-download actions (claim, gate, downloader) run
// end to end against on-disk storage.
func newActionLifecycleFixture(t *testing.T, actionGate int) *Manager {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()

	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close storage: %v", err)
		}
	})

	m := &Manager{
		storage:           store,
		queue:             newQueue(store, ""),
		config:            cfg,
		logger:            zerolog.Nop(),
		ctx:               context.Background(),
		arr:               arr.NewStorage(),
		processingEntries: xsync.NewMap[string, struct{}](),
		actionInflight:    xsync.NewMap[string, struct{}](),
	}
	if actionGate > 0 {
		m.actionSem = make(chan struct{}, actionGate)
	}
	m.Notifications = notifications.New(&cfg.Notifications, m.logger)
	m.downloader = NewDownloadManager(m)
	return m
}

func addActionLifecycleEntry(t *testing.T, m *Manager, infohash string) *storage.Entry {
	t.Helper()
	added := time.Unix(1_700_000_000, 0).UTC()
	entry := &storage.Entry{
		Protocol:        config.ProtocolTorrent,
		InfoHash:        infohash,
		Name:            "Lifecycle " + infohash,
		SavePath:        t.TempDir(),
		State:           storage.EntryStateDownloading,
		Status:          debridTypes.TorrentStatusDownloading,
		Action:          config.DownloadActionNone,
		SkipMultiSeason: true,
		AddedOn:         added,
		CreatedAt:       added,
		UpdatedAt:       added,
		Size:            10,
		Bytes:           10,
		Files: map[string]*storage.File{
			"lifecycle.mkv": {
				Name:     "lifecycle.mkv",
				InfoHash: infohash,
				Size:     10,
				AddedOn:  added,
			},
		},
	}
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("queue Add(%s): %v", infohash, err)
	}
	queued, err := m.queue.GetTorrent(infohash)
	if err != nil {
		t.Fatalf("GetTorrent(%s): %v", infohash, err)
	}
	return queued
}

// markClaimed flips the queue row into the durable post-download claim shape
// (Status downloaded + IsDownloading true) with the given UpdatedAt.
func markClaimed(t *testing.T, m *Manager, infohash string, updatedAt time.Time) {
	t.Helper()
	if _, err := m.queue.Mutate(infohash, func(current *storage.Entry) bool {
		current.State = storage.EntryStateDownloading
		current.Status = debridTypes.TorrentStatusDownloaded
		current.IsDownloading = true
		current.UpdatedAt = updatedAt
		return true
	}); err != nil {
		t.Fatalf("mark claimed(%s): %v", infohash, err)
	}
}

// TestWorkerSlotFreesOnceActionClaimed pins fix 1: a single-worker job queue
// parked in waitForDownloadCompletion must release its slot as soon as the
// post-download action is durably claimed, even though the entry is not yet
// terminal (the action is still "running").
func TestWorkerSlotFreesOnceActionClaimed(t *testing.T) {
	m := newActionLifecycleFixture(t, 0)
	entry := addActionLifecycleEntry(t, m, "slot-decouple-entry")

	secondStarted := make(chan struct{})
	jq := NewJobQueue(context.Background(), 1, func(ctx context.Context, job *Job) {
		if job.ID == "second" {
			close(secondStarted)
			return
		}
		m.processJob(ctx, job)
	})
	t.Cleanup(jq.Close)

	if err := jq.Submit(&Job{ID: entry.InfoHash, Type: JobTypeTorrent, Entry: entry}); err != nil {
		t.Fatalf("submit wait job: %v", err)
	}
	if err := jq.Submit(&Job{ID: "second", Type: JobTypeTorrent}); err != nil {
		t.Fatalf("submit second job: %v", err)
	}

	// The sole worker is parked on the still-downloading entry: the second job
	// must not start (poll interval is 1s, so 1.5s covers a full refresh tick).
	select {
	case <-secondStarted:
		t.Fatal("second job started while the worker should be parked on an unclaimed downloading entry")
	case <-time.After(1500 * time.Millisecond):
	}

	// Durably claim the post-download action. The parked worker must observe
	// the claim on its next refresh and free the slot while the entry is still
	// non-terminal (State stays "downloading" for the whole action).
	markClaimed(t, m, entry.InfoHash, time.Now())

	select {
	case <-secondStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("worker slot was not released after the post-download action was claimed")
	}

	current, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetTorrent after claim: %v", err)
	}
	if current.State != storage.EntryStateDownloading {
		t.Fatalf("entry state = %q, want still-downloading (action not finished)", current.State)
	}
}

// TestActionGateBoundsConcurrentClaimedActions pins fix 2: with a gate of
// size G, N claimed post-download actions run at most G at a time while the
// rest wait on the gate (already claimed, so worker slots stay free).
func TestActionGateBoundsConcurrentClaimedActions(t *testing.T) {
	const gateSize = 2
	const totalActions = 6

	m := newActionLifecycleFixture(t, gateSize)

	var current, peak atomic.Int32
	release := make(chan struct{})
	m.claimedActionTestHook = func(*storage.Entry) {
		c := current.Add(1)
		for {
			p := peak.Load()
			if c <= p || peak.CompareAndSwap(p, c) {
				break
			}
		}
		<-release
		current.Add(-1)
	}

	var wg sync.WaitGroup
	for i := 0; i < totalActions; i++ {
		entry := addActionLifecycleEntry(t, m, fmt.Sprintf("gate-entry-%d", i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.processAction(entry)
		}()
	}

	// Wait until the gate is saturated, then confirm nothing sneaks past it.
	deadline := time.Now().Add(5 * time.Second)
	for current.Load() != gateSize {
		if time.Now().After(deadline) {
			t.Fatalf("gate never saturated: running=%d", current.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if got := current.Load(); got != gateSize {
		t.Fatalf("running actions = %d, want exactly %d while gate is held", got, gateSize)
	}

	close(release)
	wg.Wait()
	if got := peak.Load(); got != gateSize {
		t.Fatalf("peak concurrent actions = %d, want %d", got, gateSize)
	}

	// Every action must have claimed its entry and cleaned up its in-process
	// registration once finished.
	for i := 0; i < totalActions; i++ {
		hash := fmt.Sprintf("gate-entry-%d", i)
		queued, err := m.queue.GetTorrent(hash)
		if err != nil {
			t.Fatalf("GetTorrent(%s): %v", hash, err)
		}
		if queued.Status != debridTypes.TorrentStatusDownloaded || !queued.IsDownloading {
			t.Fatalf("entry %s was not claimed: status=%s downloading=%t", hash, queued.Status, queued.IsDownloading)
		}
		if m.isActionInflight(hash) {
			t.Fatalf("entry %s still registered in-flight after its action returned", hash)
		}
	}
}

// TestResumeActionDetachesFromWorkerAndWaitsOnGate pins the restore path
// interaction between fix 1 and fix 2: a ResumeAction job must free its
// worker slot immediately (next job starts) while the resumed action itself
// waits for the action gate.
func TestResumeActionDetachesFromWorkerAndWaitsOnGate(t *testing.T) {
	m := newActionLifecycleFixture(t, 1)
	entry := addActionLifecycleEntry(t, m, "resume-detach-entry")
	markClaimed(t, m, entry.InfoHash, time.Now())
	snapshot, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetTorrent claimed snapshot: %v", err)
	}

	actionRan := make(chan struct{})
	m.claimedActionTestHook = func(*storage.Entry) {
		close(actionRan)
	}

	// Occupy the only gate slot so the resumed action must wait.
	m.actionSem <- struct{}{}

	secondStarted := make(chan struct{})
	jq := NewJobQueue(context.Background(), 1, func(ctx context.Context, job *Job) {
		if job.ID == "second" {
			close(secondStarted)
			return
		}
		m.processJob(ctx, job)
	})
	t.Cleanup(jq.Close)

	if err := jq.Submit(&Job{ID: entry.InfoHash, Type: JobTypeTorrent, Entry: snapshot, ResumeAction: true}); err != nil {
		t.Fatalf("submit resume job: %v", err)
	}
	if err := jq.Submit(&Job{ID: "second", Type: JobTypeTorrent}); err != nil {
		t.Fatalf("submit second job: %v", err)
	}

	// The worker must not be held hostage by the gated resume.
	select {
	case <-secondStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("worker slot was held by a ResumeAction job waiting on the action gate")
	}
	select {
	case <-actionRan:
		t.Fatal("resumed action ran while the gate slot was occupied")
	case <-time.After(300 * time.Millisecond):
	}
	if !m.isActionInflight(entry.InfoHash) {
		t.Fatal("resume was not registered in-flight while waiting on the gate")
	}

	// Free the gate: the resumed action must now run.
	<-m.actionSem
	select {
	case <-actionRan:
	case <-time.After(3 * time.Second):
		t.Fatal("resumed action did not run after the gate slot freed")
	}
}

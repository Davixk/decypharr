package manager

import (
	"context"
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
	}
	_ = actionGate
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

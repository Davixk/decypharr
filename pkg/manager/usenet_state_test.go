package manager

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"google.golang.org/protobuf/proto"
)

func TestEnsureNZBGenerationCoversLegacyCrashStates(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	cfg := config.Get()
	previousUsenet := cfg.Usenet
	cfg.Usenet.Providers = []config.UsenetProvider{{Host: "127.0.0.1", Port: 119, MaxConnections: 1}}
	t.Cleanup(func() { cfg.Usenet = previousUsenet })

	const legacyID = "legacy-both-blank"
	metaDir := filepath.Join(config.GetMainPath(), "usenet", "meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatalf("create metadata dir: %v", err)
	}
	legacyBlob, err := proto.Marshal(&usenet.NZBProto{Id: legacyID, Name: "legacy.nzb", Status: usenet.NZBStatusCompleted})
	if err != nil {
		t.Fatalf("marshal legacy metadata: %v", err)
	}
	if err := os.WriteFile(filepath.Join(metaDir, legacyID+".meta"), legacyBlob, 0o644); err != nil {
		t.Fatalf("write legacy metadata: %v", err)
	}

	u, err := usenet.New()
	if err != nil {
		t.Fatalf("usenet.New: %v", err)
	}
	t.Cleanup(func() { _ = u.Close() })
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	queue := newQueue(store, "")
	m := &Manager{storage: store, queue: queue, usenet: u}

	persistBoth := func(t *testing.T, entry *storage.Entry) {
		t.Helper()
		if err := store.AddOrUpdate(entry); err != nil {
			t.Fatalf("AddOrUpdate: %v", err)
		}
		if err := queue.Add(entry); err != nil {
			t.Fatalf("queue Add: %v", err)
		}
	}
	assertBoth := func(t *testing.T, id, generation string) {
		t.Helper()
		mainEntry, err := store.Get(id)
		if err != nil {
			t.Fatalf("Get main: %v", err)
		}
		queuedEntry, err := store.GetQueued(id)
		if err != nil {
			t.Fatalf("Get queue: %v", err)
		}
		if generation == "" || mainEntry.NZBGeneration != generation || queuedEntry.NZBGeneration != generation {
			t.Fatalf("generation not persisted to both rows: want=%q main=%q queue=%q", generation, mainEntry.NZBGeneration, queuedEntry.NZBGeneration)
		}
	}

	t.Run("entry and metadata blank", func(t *testing.T) {
		entry := &storage.Entry{Protocol: config.ProtocolNZB, InfoHash: legacyID, Name: "legacy.nzb", Files: make(map[string]*storage.File)}
		persistBoth(t, entry)
		generation, err := m.ensureNZBGeneration(entry)
		if err != nil {
			t.Fatalf("ensureNZBGeneration: %v", err)
		}
		assertBoth(t, legacyID, generation)
		header, err := u.GetNZBHeader(legacyID)
		if err != nil || header.Generation != generation {
			t.Fatalf("metadata generation = %q err=%v, want %q", header.Generation, err, generation)
		}
	})

	t.Run("entry blank metadata populated", func(t *testing.T) {
		const id = "legacy-entry-blank"
		const generation = "metadata-generation"
		if err := u.NZBStorage().AddNZB(&storage.NZB{ID: id, Name: "metadata.nzb", Generation: generation, Status: usenet.NZBStatusCompleted}); err != nil {
			t.Fatalf("AddNZB: %v", err)
		}
		entry := &storage.Entry{Protocol: config.ProtocolNZB, InfoHash: id, Name: "metadata.nzb", Files: make(map[string]*storage.File)}
		persistBoth(t, entry)
		got, err := m.ensureNZBGeneration(entry)
		if err != nil {
			t.Fatalf("ensureNZBGeneration: %v", err)
		}
		if got != generation {
			t.Fatalf("adopted generation = %q, want %q", got, generation)
		}
		assertBoth(t, id, generation)
	})

	t.Run("queued entry blank metadata absent", func(t *testing.T) {
		const id = "legacy-queue-no-metadata"
		entry := &storage.Entry{Protocol: config.ProtocolNZB, InfoHash: id, Name: "missing.nzb", Files: make(map[string]*storage.File)}
		if err := queue.Add(entry); err != nil {
			t.Fatalf("queue Add: %v", err)
		}
		_, err := m.rebuildQueuedNZBJob(entry)
		if err == nil || !strings.Contains(err.Error(), "source is unavailable") {
			t.Fatalf("rebuildQueuedNZBJob error = %v, want unavailable source after reservation", err)
		}
		persisted, err := store.GetQueued(id)
		if err != nil {
			t.Fatalf("GetQueued: %v", err)
		}
		if persisted.NZBGeneration == "" || entry.NZBGeneration != persisted.NZBGeneration {
			t.Fatalf("generation was not reserved before parse: entry=%q persisted=%q", entry.NZBGeneration, persisted.NZBGeneration)
		}
	})
}

func TestProcessNZBRejectsStaleMetadataBeforeMutation(t *testing.T) {
	entry := &storage.Entry{
		Protocol:      config.ProtocolNZB,
		InfoHash:      "stale-completion",
		NZBGeneration: "current-generation",
		Size:          10,
		Bytes:         10,
		Files: map[string]*storage.File{
			"old.mkv": {Name: "old.mkv", InfoHash: "stale-completion", Size: 10},
		},
	}
	metadata := &storage.NZB{
		ID:         entry.InfoHash,
		Generation: "old-generation",
		TotalSize:  99,
		Files:      []storage.NZBFile{{Name: "replacement.mkv", Size: 99}},
	}

	err := (&Manager{}).processNZB(context.Background(), entry, metadata)
	if !errors.Is(err, usenet.ErrStaleNZBGeneration) {
		t.Fatalf("processNZB error = %v, want ErrStaleNZBGeneration", err)
	}
	if entry.Size != 10 || entry.Bytes != 10 || entry.Progress != 0 || len(entry.Files) != 1 || entry.Files["old.mkv"] == nil {
		t.Fatalf("stale completion mutated entry: %+v", entry)
	}
}

func TestProcessNZBRejectsWrongMetadataIDBeforeMutation(t *testing.T) {
	entry := &storage.Entry{
		Protocol:      config.ProtocolNZB,
		InfoHash:      "queued-id",
		NZBGeneration: "current-generation",
		Size:          10,
		Bytes:         10,
		Files: map[string]*storage.File{
			"old.mkv": {Name: "old.mkv", InfoHash: "queued-id", Size: 10},
		},
	}
	metadata := &storage.NZB{
		ID:         "different-id",
		Generation: entry.NZBGeneration,
		TotalSize:  99,
		Files:      []storage.NZBFile{{Name: "replacement.mkv", Size: 99}},
	}

	err := (&Manager{}).processNZB(context.Background(), entry, metadata)
	if err == nil || !strings.Contains(err.Error(), "does not match queued entry") {
		t.Fatalf("processNZB error = %v, want metadata ID mismatch", err)
	}
	if entry.Size != 10 || entry.Bytes != 10 || entry.Progress != 0 || len(entry.Files) != 1 || entry.Files["old.mkv"] == nil {
		t.Fatalf("wrong-ID completion mutated entry: %+v", entry)
	}
}

func TestRebuildNZBCompletionFilesDropsAbsentNamesAndPreservesDurableState(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	queue := newQueue(store, "")
	entryAdded := time.Unix(1_700_000_000, 0)
	durableAdded := entryAdded.Add(-time.Hour)
	entry := &storage.Entry{
		Protocol:      config.ProtocolNZB,
		InfoHash:      "repeated-completion",
		NZBGeneration: "repeated-generation",
		AddedOn:       entryAdded,
	}
	rebuildNZBCompletionFiles(entry, &storage.NZB{Files: []storage.NZBFile{
		{Name: "kept.mkv", Size: 10},
		{Name: "removed.mkv", Size: 20},
	}})
	if err := queue.Add(entry); err != nil {
		t.Fatalf("queue Add initial completion: %v", err)
	}

	durableRange := &[2]int64{100, 199}
	entry.Files["kept.mkv"].Path = "durable/path/kept.mkv"
	entry.Files["kept.mkv"].Deleted = true
	entry.Files["kept.mkv"].AddedOn = durableAdded
	entry.Files["kept.mkv"].ByteRange = durableRange
	if err := queue.Update(entry); err != nil {
		t.Fatalf("persist durable file state: %v", err)
	}

	rebuildNZBCompletionFiles(entry, &storage.NZB{Files: []storage.NZBFile{
		{Name: "kept.mkv", Size: 99},
		{Name: "new.mkv", Size: 30},
	}})
	if err := queue.UpdateNZBCompletion(entry); err != nil {
		t.Fatalf("persist repeated completion: %v", err)
	}
	entry, err = queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetTorrent repeated completion: %v", err)
	}

	if len(entry.Files) != 2 || entry.Files["removed.mkv"] != nil {
		t.Fatalf("authoritative completion retained absent files: %+v", entry.Files)
	}
	kept := entry.Files["kept.mkv"]
	if kept == nil || kept.Size != 99 || kept.InfoHash != entry.InfoHash || kept.Path != "durable/path/kept.mkv" || !kept.Deleted || !kept.AddedOn.Equal(durableAdded) {
		t.Fatalf("kept file did not merge authoritative/durable state: %+v", kept)
	}
	if kept.ByteRange == nil || *kept.ByteRange != *durableRange || kept.ByteRange == durableRange {
		t.Fatalf("durable byte range was not preserved by value: got=%v want=%v", kept.ByteRange, durableRange)
	}
	fresh := entry.Files["new.mkv"]
	if fresh == nil || fresh.Size != 30 || fresh.InfoHash != entry.InfoHash || !fresh.AddedOn.Equal(entryAdded) || fresh.Path != "" || fresh.Deleted || fresh.ByteRange != nil {
		t.Fatalf("new authoritative file inherited stale durable state: %+v", fresh)
	}
}

func TestRebuildQueuedNZBCompletedMetadataPersistsBeforeRemovingStage(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()
	previousUsenet := cfg.Usenet
	cfg.Usenet.Providers = []config.UsenetProvider{{Host: "127.0.0.1", Port: 119, MaxConnections: 1}}
	t.Cleanup(func() { cfg.Usenet = previousUsenet })

	u, err := usenet.New()
	if err != nil {
		t.Fatalf("usenet.New: %v", err)
	}
	t.Cleanup(func() { _ = u.Close() })
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	queue := newQueue(store, "")

	const (
		id         = "completed-restore-no-source"
		generation = "completed-generation"
	)
	meta := &storage.NZB{
		ID:         id,
		Name:       "completed.nzb",
		Generation: generation,
		Status:     usenet.NZBStatusCompleted,
		TotalSize:  42,
		Files:      []storage.NZBFile{{Name: "movie.mkv", Size: 42}},
	}
	if err := u.NZBStorage().AddNZB(meta); err != nil {
		t.Fatalf("AddNZB completed metadata: %v", err)
	}
	stagedPath, err := u.StageNZBForGeneration(id, generation, []byte("staged source"))
	if err != nil {
		t.Fatalf("StageNZBForGeneration: %v", err)
	}
	entry := &storage.Entry{
		Protocol:         config.ProtocolNZB,
		InfoHash:         id,
		Name:             "queued.nzb",
		OriginalFilename: "queued.nzb",
		Magnet:           stagedPath,
		NZBGeneration:    generation,
		Status:           debridTypes.TorrentStatusQueued,
		State:            storage.EntryStateDownloading,
		AddedOn:          time.Now(),
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
	}
	if err := queue.Add(entry); err != nil {
		t.Fatalf("queue Add: %v", err)
	}

	persisted := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	queue.queueUpdateTestHook = func(stage string) {
		if stage == "persisted" {
			close(persisted)
			<-release
		}
	}
	m := &Manager{storage: store, queue: queue, usenet: u, config: cfg}
	type rebuildResult struct {
		job *Job
		err error
	}
	result := make(chan rebuildResult, 1)
	go func() {
		job, err := m.rebuildQueuedNZBJob(entry)
		result <- rebuildResult{job: job, err: err}
	}()

	select {
	case <-persisted:
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not persist the source-free queue state")
	}
	durable, err := store.GetQueued(id)
	if err != nil {
		t.Fatalf("GetQueued after restore persist: %v", err)
	}
	if durable.Magnet != "" || durable.Status != debridTypes.TorrentStatusDownloading {
		t.Fatalf("durable restore state = magnet %q status %q; want empty/downloading", durable.Magnet, durable.Status)
	}
	if _, err := os.Stat(stagedPath); err != nil {
		t.Fatalf("staged source was unlinked before durable update returned: %v", err)
	}
	close(release)
	released = true

	select {
	case rebuilt := <-result:
		if rebuilt.err != nil {
			t.Fatalf("rebuildQueuedNZBJob: %v", rebuilt.err)
		}
		if rebuilt.job == nil || !rebuilt.job.ResumeExisting || rebuilt.job.NZBMeta == nil || rebuilt.job.NZBMeta.Generation != generation {
			t.Fatalf("completed metadata was not restored directly: %+v", rebuilt.job)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not finish after durable update")
	}
	if _, err := os.Stat(stagedPath); !os.IsNotExist(err) {
		t.Fatalf("staged source remained after durable restore; stat error = %v", err)
	}
}

func TestProcessNZBPersistsSizeAndBytesTogether(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	queue := newQueue(store, "")
	entry := &storage.Entry{
		Protocol:      config.ProtocolNZB,
		InfoHash:      "completion-size",
		NZBGeneration: "generation",
		Files:         make(map[string]*storage.File),
	}
	if err := queue.Add(entry); err != nil {
		t.Fatalf("queue Add: %v", err)
	}
	m := &Manager{queue: queue}
	err = m.processNZB(context.Background(), entry, &storage.NZB{
		ID:         entry.InfoHash,
		Generation: entry.NZBGeneration,
		TotalSize:  42,
	})
	if err == nil || !strings.Contains(err.Error(), "nzb has no files") {
		t.Fatalf("processNZB error = %v, want no-files guard", err)
	}
	persisted, err := queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetTorrent: %v", err)
	}
	if persisted.Size != 42 || persisted.Bytes != 42 {
		t.Fatalf("persisted totals = size %d bytes %d, want 42/42", persisted.Size, persisted.Bytes)
	}
}

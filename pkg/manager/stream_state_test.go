package manager

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestPersistUsenetFileSizeUpdatesAuthoritativeEntryIndexAndQueue(t *testing.T) {
	m, entry := newUsenetStateTestManager(t)

	updated, err := m.persistUsenetFileSize(entry.InfoHash, "one.mkv", 90)
	if err != nil {
		t.Fatalf("persistUsenetFileSize: %v", err)
	}
	if got := updated.Files["one.mkv"].Size; got != 90 {
		t.Fatalf("returned file size = %d, want 90", got)
	}
	assertUsenetSizes(t, m.storage, entry, 90, 200)
}

func TestApplyUsenetFileSizesRepairsSizeAndBytesWithoutFileChange(t *testing.T) {
	entry := &storage.Entry{
		InfoHash: "aggregate-repair",
		Size:     999,
		Bytes:    998,
		Files: map[string]*storage.File{
			"one.mkv": {Name: "one.mkv", Size: 90},
			"two.mkv": {Name: "two.mkv", Size: 200},
		},
	}
	changed, err := applyUsenetFileSizes(entry, map[string]int64{"one.mkv": 90})
	if err != nil {
		t.Fatalf("applyUsenetFileSizes: %v", err)
	}
	if !changed {
		t.Fatal("aggregate repair reported no change")
	}
	if entry.Size != 290 || entry.Bytes != 290 {
		t.Fatalf("aggregate fields = Size:%d Bytes:%d, want 290/290", entry.Size, entry.Bytes)
	}
}

func TestConcurrentUsenetSizeCorrectionsDoNotOverwriteEachOther(t *testing.T) {
	m, entry := newUsenetStateTestManager(t)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for filename, size := range map[string]int64{"one.mkv": 90, "two.mkv": 180} {
		wg.Add(1)
		go func(filename string, size int64) {
			defer wg.Done()
			_, err := m.persistUsenetFileSize(entry.InfoHash, filename, size)
			errs <- err
		}(filename, size)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("persistUsenetFileSize: %v", err)
		}
	}

	assertUsenetSizes(t, m.storage, entry, 90, 180)
}

func TestPersistUsenetFileSizeRepairsStaleQueueMirror(t *testing.T) {
	m, entry := newUsenetStateTestManager(t)
	if _, err := m.persistUsenetFileSize(entry.InfoHash, "one.mkv", 90); err != nil {
		t.Fatalf("initial persistUsenetFileSize: %v", err)
	}

	staleQueue, err := m.storage.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	staleQueue.Files["one.mkv"].Size = 100
	staleQueue.Size = 300
	staleQueue.Bytes = 300
	if err := m.storage.UpdateQueue(staleQueue); err != nil {
		t.Fatalf("restore stale queue: %v", err)
	}

	// The authoritative entry already has size 90. The helper must still
	// revisit the optional queue mirror so a previous queue write failure is
	// recoverable on the next request.
	if _, err := m.persistUsenetFileSize(entry.InfoHash, "one.mkv", 90); err != nil {
		t.Fatalf("retry persistUsenetFileSize: %v", err)
	}
	assertUsenetSizes(t, m.storage, entry, 90, 200)
}

func TestMarkUsenetStreamFailureUpdatesMainAndQueueIdempotently(t *testing.T) {
	m, entry := newUsenetStateTestManager(t)
	cause := errors.New("430 no such article")

	if err := m.markUsenetStreamFailure(entry.InfoHash, "one.mkv", cause, true); err != nil {
		t.Fatalf("markUsenetStreamFailure: %v", err)
	}
	assertUsenetFailureState(t, m.storage, entry.InfoHash, 1)

	if err := m.markUsenetStreamFailure(entry.InfoHash, "one.mkv", cause, true); err != nil {
		t.Fatalf("second markUsenetStreamFailure: %v", err)
	}
	assertUsenetFailureState(t, m.storage, entry.InfoHash, 1)
}

func TestPermanentUsenetFailureIdentityDoesNotDependOnRequestSubset(t *testing.T) {
	m, entry := newUsenetStateTestManager(t)
	batch := map[string]usenetFileFailure{
		"two.mkv": {cause: errors.New("430 missing two"), articlesMissing: true},
		"one.mkv": {cause: errors.New("430 missing one"), articlesMissing: true},
	}
	if err := m.markUsenetStreamFailures(entry.InfoHash, batch); err != nil {
		t.Fatalf("mark batch failures: %v", err)
	}
	main, err := m.storage.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("Get after batch: %v", err)
	}
	firstMessage := main.LastError
	if firstMessage != "multiple usenet files failed: articles missing on provider for \"one.mkv\"; articles missing on provider for \"two.mkv\"" {
		t.Fatalf("aggregate message = %q", firstMessage)
	}

	// A direct HEAD/GET observes only one failed child. It must not turn the
	// same terminal entry into a new error occurrence.
	if err := m.markUsenetStreamFailure(entry.InfoHash, "one.mkv", errors.New("430 missing one"), true); err != nil {
		t.Fatalf("mark single failure after batch: %v", err)
	}
	if err := m.markUsenetStreamFailures(entry.InfoHash, batch); err != nil {
		t.Fatalf("mark batch after single: %v", err)
	}

	for name, load := range map[string]func() (*storage.Entry, error){
		"main":  func() (*storage.Entry, error) { return m.storage.Get(entry.InfoHash) },
		"queue": func() (*storage.Entry, error) { return m.storage.GetQueued(entry.InfoHash) },
	} {
		persisted, err := load()
		if err != nil {
			t.Fatalf("load %s: %v", name, err)
		}
		if persisted.ErrorCount != 1 {
			t.Errorf("%s ErrorCount = %d, want 1", name, persisted.ErrorCount)
		}
		if persisted.LastError != firstMessage {
			t.Errorf("%s LastError churned with request subset: %q", name, persisted.LastError)
		}
	}
}

func newUsenetStateTestManager(t *testing.T) (*Manager, *storage.Entry) {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	})

	added := time.Unix(1_700_000_000, 0).UTC()
	entry := &storage.Entry{
		Protocol:         config.ProtocolNZB,
		InfoHash:         "manager-state-nzb",
		Name:             "Manager State Release",
		OriginalFilename: "Manager State Release",
		Size:             300,
		Bytes:            300,
		AddedOn:          added,
		Files: map[string]*storage.File{
			"one.mkv": {
				Name:     "one.mkv",
				Size:     100,
				InfoHash: "manager-state-nzb",
				AddedOn:  added,
			},
			"two.mkv": {
				Name:     "two.mkv",
				Size:     200,
				InfoHash: "manager-state-nzb",
				AddedOn:  added,
			},
		},
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	if err := store.AddQueue(entry); err != nil {
		t.Fatalf("AddQueue: %v", err)
	}

	m := &Manager{
		storage: store,
		config:  &config.Config{},
		logger:  zerolog.Nop(),
	}
	m.entry = NewEntryCache(m)
	return m, entry
}

func assertUsenetSizes(t *testing.T, store *storage.Storage, original *storage.Entry, one, two int64) {
	t.Helper()
	wantTotal := one + two
	main, err := store.Get(original.InfoHash)
	if err != nil {
		t.Fatalf("Get main entry: %v", err)
	}
	if main.Files["one.mkv"].Size != one || main.Files["two.mkv"].Size != two || main.Size != wantTotal || main.Bytes != wantTotal {
		t.Fatalf("main sizes = one:%d two:%d total:%d bytes:%d, want one:%d two:%d total:%d",
			main.Files["one.mkv"].Size, main.Files["two.mkv"].Size, main.Size, main.Bytes, one, two, wantTotal)
	}

	queued, err := store.GetQueued(original.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	if queued.Files["one.mkv"].Size != one || queued.Files["two.mkv"].Size != two || queued.Size != wantTotal || queued.Bytes != wantTotal {
		t.Fatalf("queue sizes = one:%d two:%d total:%d bytes:%d, want one:%d two:%d total:%d",
			queued.Files["one.mkv"].Size, queued.Files["two.mkv"].Size, queued.Size, queued.Bytes, one, two, wantTotal)
	}

	item, err := store.GetEntryItem(original.GetFolder())
	if err != nil {
		t.Fatalf("GetEntryItem: %v", err)
	}
	if item.Files["one.mkv"].Size != one || item.Files["two.mkv"].Size != two || item.Size != wantTotal {
		t.Fatalf("entry item sizes = one:%d two:%d total:%d, want one:%d two:%d total:%d",
			item.Files["one.mkv"].Size, item.Files["two.mkv"].Size, item.Size, one, two, wantTotal)
	}
}

func assertUsenetFailureState(t *testing.T, store *storage.Storage, infohash string, wantCount int) {
	t.Helper()
	for name, load := range map[string]func() (*storage.Entry, error){
		"main":  func() (*storage.Entry, error) { return store.Get(infohash) },
		"queue": func() (*storage.Entry, error) { return store.GetQueued(infohash) },
	} {
		entry, err := load()
		if err != nil {
			t.Fatalf("load %s entry: %v", name, err)
		}
		if entry.State != storage.EntryStateError || !entry.Bad {
			t.Errorf("%s state = %q bad=%v, want error/true", name, entry.State, entry.Bad)
		}
		if entry.LastError != "articles missing on provider for \"one.mkv\"" {
			t.Errorf("%s LastError = %q", name, entry.LastError)
		}
		if entry.ErrorCount != wantCount {
			t.Errorf("%s ErrorCount = %d, want %d", name, entry.ErrorCount, wantCount)
		}
	}
}

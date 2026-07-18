package storage

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage/hybrid"
	"google.golang.org/protobuf/proto"
)

func TestNewStorageRejectsCorruptVersionRows(t *testing.T) {
	for _, storeName := range []string{"main", "queue"} {
		t.Run(storeName, func(t *testing.T) {
			config.SetConfigPath(t.TempDir())
			dbPath := t.TempDir()
			store, err := NewStorage(dbPath)
			if err != nil {
				t.Fatalf("NewStorage: %v", err)
			}
			key := "corrupt-" + storeName
			target := store.entries
			if storeName == "queue" {
				target = store.queue
			}
			if err := target.Put(key, []byte{0xff, 0x80, 0xff}, nil); err != nil {
				t.Fatalf("insert corrupt row: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			reopened, err := NewStorage(dbPath)
			if reopened != nil {
				_ = reopened.Close()
				t.Fatal("NewStorage returned a store containing a corrupt row")
			}
			if err == nil || !strings.Contains(err.Error(), storeName) || !strings.Contains(err.Error(), key) {
				t.Fatalf("NewStorage error = %v, want store and key context", err)
			}
		})
	}
}

func TestDerivativeDecodeFailuresAreRebuiltFromPrimaryTruth(t *testing.T) {
	for _, operation := range []string{"update", "delete"} {
		t.Run(operation, func(t *testing.T) {
			store, entry := newAtomicMutationTestStorage(t)
			folder := entry.GetFolder()
			if err := store.entryItems.Put(folder, []byte{0xff, 0x80, 0xff}, nil); err != nil {
				t.Fatalf("corrupt entry item: %v", err)
			}

			if operation == "update" {
				current, err := store.Get(entry.InfoHash)
				if err != nil {
					t.Fatalf("Get current: %v", err)
				}
				current.Size = 99
				if err := store.AddOrUpdate(current); err != nil {
					t.Fatalf("AddOrUpdate primary: %v", err)
				}
				persisted, err := store.Get(entry.InfoHash)
				if err != nil || persisted.Size != 99 {
					t.Fatalf("authoritative update not committed: entry=%+v err=%v", persisted, err)
				}
			} else {
				if err := store.Delete(entry.InfoHash); err != nil {
					t.Fatalf("Delete primary: %v", err)
				}
				if _, err := store.Get(entry.InfoHash); err == nil {
					t.Fatal("authoritative delete was not committed")
				}
			}

			if operation == "delete" {
				if _, err := store.GetEntryItem(folder); err == nil {
					t.Fatal("deleted folder retained its corrupt derivative")
				}
				if _, err := store.GetEntryHealth(folder); err == nil {
					t.Fatal("deleted folder retained obsolete health state")
				}
				return
			}

			item, err := store.GetEntryItem(folder)
			if err != nil {
				t.Fatalf("GetEntryItem after rebuild: %v", err)
			}
			if file := item.Files["movie.mkv"]; file == nil || file.InfoHash != entry.InfoHash {
				t.Fatalf("rebuilt derivative = %+v, want authoritative file", item)
			}
			health, err := store.GetEntryHealth(folder)
			if err != nil {
				t.Fatalf("GetEntryHealth: %v", err)
			}
			if !health.Dirty || health.DirtyReason != "entry_item_rebuilt" {
				t.Fatalf("repair marker = %+v, want successful rebuild", health)
			}
		})
	}
}

func TestDeletingNewerDuplicateRestoresShadowedAuthoritativeFile(t *testing.T) {
	store, older := newAtomicMutationTestStorage(t)
	older.Name = "Shared Duplicate"
	older.OriginalFilename = older.Name
	older.Files = map[string]*File{
		"movie.mkv": {
			Name:     "movie.mkv",
			Size:     10,
			InfoHash: older.InfoHash,
			AddedOn:  older.AddedOn,
		},
	}
	if err := store.AddOrUpdate(older); err != nil {
		t.Fatalf("move older entry into shared folder: %v", err)
	}

	newer := &Entry{
		Protocol: config.ProtocolNZB,
		InfoHash: "newer-duplicate",
		Name:     older.Name,
		AddedOn:  older.AddedOn.Add(time.Second),
		Size:     20,
		Bytes:    20,
		Files: map[string]*File{
			"movie.mkv": {
				Name:     "movie.mkv",
				Size:     20,
				InfoHash: "newer-duplicate",
				AddedOn:  older.AddedOn.Add(time.Second),
			},
		},
	}
	if err := store.AddOrUpdate(newer); err != nil {
		t.Fatalf("AddOrUpdate newer duplicate: %v", err)
	}
	item, err := store.GetEntryItem(older.GetFolder())
	if err != nil {
		t.Fatalf("GetEntryItem with newer duplicate: %v", err)
	}
	if file := item.Files["movie.mkv"]; file == nil || file.InfoHash != newer.InfoHash {
		t.Fatalf("newer duplicate did not win: %+v", item.Files)
	}

	if err := store.Delete(newer.InfoHash); err != nil {
		t.Fatalf("Delete newer duplicate: %v", err)
	}
	item, err = store.GetEntryItem(older.GetFolder())
	if err != nil {
		t.Fatalf("GetEntryItem after newer delete: %v", err)
	}
	if file := item.Files["movie.mkv"]; file == nil || file.InfoHash != older.InfoHash || file.Size != 10 {
		t.Fatalf("shadowed authoritative file was not restored: %+v", item.Files)
	}
}

func TestRemovingNewerDuplicateFileRestoresShadowedAuthoritativeFile(t *testing.T) {
	store, older := newAtomicMutationTestStorage(t)
	older.Name = "Shared Removal"
	older.OriginalFilename = older.Name
	older.Files = map[string]*File{
		"movie.mkv": {
			Name:     "movie.mkv",
			Size:     10,
			InfoHash: older.InfoHash,
			AddedOn:  older.AddedOn,
		},
	}
	if err := store.AddOrUpdate(older); err != nil {
		t.Fatalf("move older entry into shared folder: %v", err)
	}

	newer := &Entry{
		Protocol: config.ProtocolNZB,
		InfoHash: "newer-removal",
		Name:     older.Name,
		AddedOn:  older.AddedOn.Add(time.Second),
		Size:     20,
		Bytes:    20,
		Files: map[string]*File{
			"movie.mkv": {
				Name:     "movie.mkv",
				Size:     20,
				InfoHash: "newer-removal",
				AddedOn:  older.AddedOn.Add(time.Second),
			},
		},
	}
	if err := store.AddOrUpdate(newer); err != nil {
		t.Fatalf("AddOrUpdate newer duplicate: %v", err)
	}

	current, err := store.Get(newer.InfoHash)
	if err != nil {
		t.Fatalf("Get newer duplicate: %v", err)
	}
	current.Files = map[string]*File{}
	current.Size = 0
	current.Bytes = 0
	if err := store.AddOrUpdate(current); err != nil {
		t.Fatalf("remove newer duplicate file: %v", err)
	}

	item, err := store.GetEntryItem(older.GetFolder())
	if err != nil {
		t.Fatalf("GetEntryItem after file removal: %v", err)
	}
	if file := item.Files["movie.mkv"]; file == nil || file.InfoHash != older.InfoHash || file.Size != 10 {
		t.Fatalf("shadowed authoritative file was not restored: %+v", item.Files)
	}
}

func TestStartupReconcilesMissingAndCorruptModernEntryItems(t *testing.T) {
	for _, damage := range []string{"missing", "corrupt"} {
		t.Run(damage, func(t *testing.T) {
			config.SetConfigPath(t.TempDir())
			dbPath := t.TempDir()
			store, err := NewStorage(dbPath)
			if err != nil {
				t.Fatalf("NewStorage: %v", err)
			}
			entry := atomicTestEntry()
			if err := store.AddOrUpdate(entry); err != nil {
				t.Fatalf("AddOrUpdate: %v", err)
			}
			folder := entry.GetFolder()
			if damage == "missing" {
				if err := store.entryItems.Delete(folder); err != nil {
					t.Fatalf("delete entry item: %v", err)
				}
			} else if err := store.entryItems.Put(folder, []byte{0xff, 0x80, 0xff}, nil); err != nil {
				t.Fatalf("corrupt entry item: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			reopened, err := NewStorage(dbPath)
			if err != nil {
				t.Fatalf("reopen storage: %v", err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			item, err := reopened.GetEntryItem(folder)
			if err != nil {
				t.Fatalf("GetEntryItem after startup reconciliation: %v", err)
			}
			if file := item.Files["movie.mkv"]; file == nil || file.InfoHash != entry.InfoHash {
				t.Fatalf("startup projection = %+v, want authoritative file", item)
			}
			health, err := reopened.GetEntryHealth(folder)
			if err != nil {
				t.Fatalf("GetEntryHealth after startup reconciliation: %v", err)
			}
			if !health.Dirty || health.DirtyReason != "entry_item_rebuilt" {
				t.Fatalf("startup repair marker = %+v", health)
			}
		})
	}
}

func TestAuthoritativeIterationReportsRuntimeCorruption(t *testing.T) {
	store, _ := newAtomicMutationTestStorage(t)
	const key = "runtime-corrupt-main"
	if err := store.entries.Put(key, []byte{0xff, 0x80, 0xff}, nil); err != nil {
		t.Fatalf("insert corrupt row: %v", err)
	}
	err := store.ForEach(func(*Entry) error { return nil })
	if err == nil || !strings.Contains(err.Error(), key) {
		t.Fatalf("ForEach error = %v, want corrupt key context", err)
	}
}

func TestIndependentStoreVersionsAllowImmediateSamePointerUpdates(t *testing.T) {
	store, entry := newAtomicMutationTestStorage(t)
	mainGeneration, mainRevision := entry.mainStoreGeneration, entry.mainStoreRevision
	queueGeneration, queueRevision := entry.queueStoreGeneration, entry.queueStoreRevision
	if mainGeneration == "" || mainRevision == 0 || queueGeneration == "" || queueRevision == 0 {
		t.Fatalf("missing initial versions: main=(%q,%d) queue=(%q,%d)", mainGeneration, mainRevision, queueGeneration, queueRevision)
	}

	entry.Size = 11
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("same-pointer main update: %v", err)
	}
	if entry.mainStoreGeneration != mainGeneration || entry.mainStoreRevision != mainRevision+1 {
		t.Fatalf("main version not advanced exactly: (%q,%d)", entry.mainStoreGeneration, entry.mainStoreRevision)
	}
	if entry.queueStoreGeneration != queueGeneration || entry.queueStoreRevision != queueRevision {
		t.Fatal("main update changed independent queue version")
	}

	entry.Size = 12
	if err := store.UpdateQueue(entry); err != nil {
		t.Fatalf("same-pointer queue update: %v", err)
	}
	if entry.queueStoreGeneration != queueGeneration || entry.queueStoreRevision != queueRevision+1 {
		t.Fatalf("queue version not advanced exactly: (%q,%d)", entry.queueStoreGeneration, entry.queueStoreRevision)
	}
	if entry.mainStoreGeneration != mainGeneration || entry.mainStoreRevision != mainRevision+1 {
		t.Fatal("queue update changed independent main version")
	}
}

func TestExactRevisionsRejectSameGenerationStaleSnapshots(t *testing.T) {
	store, entry := newAtomicMutationTestStorage(t)

	mainFirst, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("Get first main snapshot: %v", err)
	}
	mainStale, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("Get stale main snapshot: %v", err)
	}
	mainFirst.Size = 21
	if err := store.AddOrUpdate(mainFirst); err != nil {
		t.Fatalf("update first main snapshot: %v", err)
	}
	mainStale.Size = 22
	if err := store.AddOrUpdate(mainStale); !errors.Is(err, ErrStaleEntryGeneration) {
		t.Fatalf("stale main update error = %v, want ErrStaleEntryGeneration", err)
	}

	queueFirst, err := store.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued first snapshot: %v", err)
	}
	queueStale, err := store.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued stale snapshot: %v", err)
	}
	queueFirst.Size = 31
	if err := store.UpdateQueue(queueFirst); err != nil {
		t.Fatalf("update first queue snapshot: %v", err)
	}
	queueStale.Size = 32
	if err := store.UpdateQueue(queueStale); !errors.Is(err, ErrStaleEntryGeneration) {
		t.Fatalf("stale queue update error = %v, want ErrStaleEntryGeneration", err)
	}
}

func TestBatchAddOrUpdateContinuesAfterStaleMember(t *testing.T) {
	store, first := newAtomicMutationTestStorage(t)
	second := atomicTestEntry()
	second.InfoHash = "independent-batch-member"
	second.Name = "Independent Batch Member"
	second.Files = map[string]*File{
		"second.mkv": {Name: "second.mkv", Size: 10, InfoHash: second.InfoHash, AddedOn: second.AddedOn},
	}
	if err := store.AddOrUpdate(second); err != nil {
		t.Fatalf("Add second: %v", err)
	}

	staleFirst, err := store.Get(first.InfoHash)
	if err != nil {
		t.Fatalf("Get stale first: %v", err)
	}
	currentFirst, _ := store.Get(first.InfoHash)
	currentFirst.Category = "advance-first"
	if err := store.AddOrUpdate(currentFirst); err != nil {
		t.Fatalf("advance first: %v", err)
	}
	currentSecond, _ := store.Get(second.InfoHash)
	currentSecond.Category = "must-still-commit"

	err = store.BatchAddOrUpdate([]*Entry{staleFirst, currentSecond})
	if !errors.Is(err, ErrStaleEntryGeneration) {
		t.Fatalf("BatchAddOrUpdate error=%v, want stale member error", err)
	}
	secondAfter, err := store.Get(second.InfoHash)
	if err != nil {
		t.Fatalf("Get second after batch: %v", err)
	}
	if secondAfter.Category != "must-still-commit" {
		t.Fatalf("batch stopped before independent member: category=%q", secondAfter.Category)
	}
}

func TestDeleteQueuedSnapshotWhereRechecksLivePredicateAfterRevisionDrift(t *testing.T) {
	store, entry := newAtomicMutationTestStorage(t)
	snapshot, err := store.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued snapshot: %v", err)
	}
	updated, present, err := store.MutateQueuedIfPresent(entry.InfoHash, func(current *Entry) (bool, error) {
		current.Category = "recovered"
		current.Bad = false
		return true, nil
	})
	if err != nil || !present {
		t.Fatalf("MutateQueuedIfPresent: present=%v err=%v", present, err)
	}
	if !SameQueueGeneration(snapshot, updated) {
		t.Fatal("ordinary queue edit unexpectedly changed lifecycle generation")
	}

	deleted, err := store.DeleteQueuedSnapshotWhere(snapshot, func(current *Entry) bool {
		return current.Category == "stalled" || current.Bad
	}, nil)
	if err != nil {
		t.Fatalf("DeleteQueuedSnapshotWhere: %v", err)
	}
	if deleted {
		t.Fatal("stale bulk predicate deleted a recovered row")
	}
	current, err := store.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("recovered row missing: %v", err)
	}
	if current.Category != "recovered" || current.Bad {
		t.Fatalf("recovered row changed: %+v", current)
	}
}

func TestStoreVersionsRoundTripAndSurviveReopen(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	dbPath := t.TempDir()
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	entry := atomicTestEntry()
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	if err := store.AddQueue(entry); err != nil {
		t.Fatalf("AddQueue: %v", err)
	}
	wantMainGeneration, wantMainRevision := entry.mainStoreGeneration, entry.mainStoreRevision
	wantQueueGeneration, wantQueueRevision := entry.queueStoreGeneration, entry.queueStoreRevision
	if err := store.Close(); err != nil {
		t.Fatalf("Close first store: %v", err)
	}

	reopened, err := NewStorage(dbPath)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	mainEntry, err := reopened.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("Get reopened main: %v", err)
	}
	queueEntry, err := reopened.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("Get reopened queue: %v", err)
	}
	if mainEntry.mainStoreGeneration != wantMainGeneration || mainEntry.mainStoreRevision != wantMainRevision {
		t.Fatalf("main version changed across reopen: got=(%q,%d) want=(%q,%d)", mainEntry.mainStoreGeneration, mainEntry.mainStoreRevision, wantMainGeneration, wantMainRevision)
	}
	if queueEntry.queueStoreGeneration != wantQueueGeneration || queueEntry.queueStoreRevision != wantQueueRevision {
		t.Fatalf("queue version changed across reopen: got=(%q,%d) want=(%q,%d)", queueEntry.queueStoreGeneration, queueEntry.queueStoreRevision, wantQueueGeneration, wantQueueRevision)
	}
}

func TestLegacyRowsAreVersionedBeforeSnapshotsEscape(t *testing.T) {
	store, _ := newAtomicMutationTestStorage(t)
	legacy := atomicTestEntry()
	legacy.InfoHash = "legacy-version"
	legacy.Name = "Legacy Version"
	legacy.Files = map[string]*File{
		"legacy.mkv": {
			Name:     "legacy.mkv",
			Size:     10,
			InfoHash: legacy.InfoHash,
			AddedOn:  legacy.AddedOn,
		},
	}
	legacy.CreatedAt = time.Unix(1_700_000_100, 0).UTC()
	legacy.UpdatedAt = time.Unix(1_700_000_200, 0).UTC()
	data, err := proto.Marshal(EntryToProto(legacy))
	if err != nil {
		t.Fatalf("marshal legacy row: %v", err)
	}
	if err := store.entries.Put(legacy.InfoHash, data, nil); err != nil {
		t.Fatalf("insert legacy main: %v", err)
	}
	if err := store.queue.Put(legacy.InfoHash, data, nil); err != nil {
		t.Fatalf("insert legacy queue: %v", err)
	}

	if _, err := store.MigrateStoreVersions(); err != nil {
		t.Fatalf("MigrateStoreVersions: %v", err)
	}
	if _, err := store.MigrateMetadata(); err != nil {
		t.Fatalf("MigrateMetadata: %v", err)
	}
	mainEntry, err := store.Get(legacy.InfoHash)
	if err != nil {
		t.Fatalf("Get migrated main: %v", err)
	}
	queueEntry, err := store.GetQueued(legacy.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued migrated queue: %v", err)
	}
	if mainEntry.mainStoreGeneration == "" || mainEntry.mainStoreRevision == 0 {
		t.Fatal("legacy main row escaped without an exact version")
	}
	if queueEntry.queueStoreGeneration == "" || queueEntry.queueStoreRevision == 0 {
		t.Fatal("legacy queue row escaped without an exact version")
	}
	for name, migrated := range map[string]*Entry{"main": mainEntry, "queue": queueEntry} {
		if !migrated.CreatedAt.Equal(legacy.CreatedAt) || !migrated.UpdatedAt.Equal(legacy.UpdatedAt) {
			t.Errorf("%s migration changed business timestamps: created=%v updated=%v", name, migrated.CreatedAt, migrated.UpdatedAt)
		}
	}
	item, err := store.GetEntryItem(legacy.GetFolder())
	if err != nil {
		t.Fatalf("GetEntryItem after legacy migration: %v", err)
	}
	if file := item.Files["legacy.mkv"]; file == nil || file.InfoHash != legacy.InfoHash {
		t.Fatalf("legacy metadata migration did not rebuild folder index: %+v", item.Files)
	}
	for storeName, hybridStore := range map[string]*hybrid.Store{"main": store.entries, "queue": store.queue} {
		meta, err := hybridStore.GetMeta(legacy.InfoHash)
		if err != nil {
			t.Fatalf("GetMeta %s: %v", storeName, err)
		}
		if meta.Protocol != string(legacy.Protocol) || meta.Name != legacy.GetFolder() {
			t.Fatalf("%s legacy metadata not rebuilt: %+v", storeName, meta)
		}
	}
}

func TestHandleExistingEntryMergeCarriesOnlyMainVersion(t *testing.T) {
	store, entry := newAtomicMutationTestStorage(t)
	existing, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("Get existing: %v", err)
	}
	incoming, err := store.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued incoming: %v", err)
	}
	incoming.mainStoreGeneration = ""
	incoming.mainStoreRevision = 0
	wantQueueGeneration, wantQueueRevision := incoming.queueStoreGeneration, incoming.queueStoreRevision

	merged := HandleExistingEntryMerge(existing, incoming)
	if merged.mainStoreGeneration != existing.mainStoreGeneration || merged.mainStoreRevision != existing.mainStoreRevision {
		t.Fatal("merge did not carry current main-store version")
	}
	if merged.queueStoreGeneration != wantQueueGeneration || merged.queueStoreRevision != wantQueueRevision {
		t.Fatal("merge overwrote independent queue-store version")
	}
	if err := store.AddOrUpdate(merged); err != nil {
		t.Fatalf("persist merged existing NZB: %v", err)
	}
}

func TestConditionalMainDeleteRejectsStaleScanCandidate(t *testing.T) {
	store, entry := newAtomicMutationTestStorage(t)
	candidate, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("Get candidate: %v", err)
	}
	current, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("Get current: %v", err)
	}
	current.Size = 77
	if err := store.AddOrUpdate(current); err != nil {
		t.Fatalf("advance current revision: %v", err)
	}
	deleted, err := store.DeleteIfCurrent(candidate)
	if err != nil {
		t.Fatalf("DeleteIfCurrent stale candidate: %v", err)
	}
	if deleted {
		t.Fatal("stale scan candidate deleted a newer main revision")
	}
	fresh, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("Get fresh candidate: %v", err)
	}
	deleted, err = store.DeleteIfCurrent(fresh)
	if err != nil || !deleted {
		t.Fatalf("DeleteIfCurrent fresh candidate: deleted=%v err=%v", deleted, err)
	}
}

func TestKeyedLockPoolReleasesInactiveKeys(t *testing.T) {
	var pool keyedLockPool
	for i := 0; i < 1000; i++ {
		unlock := pool.lock(string(rune(i)))
		unlock()
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.locks) != 0 {
		t.Fatalf("inactive lock keys retained: %d", len(pool.locks))
	}
}

func TestQueueDeleteReportsCleanupFailureAfterCommittedDelete(t *testing.T) {
	store, entry := newAtomicMutationTestStorage(t)
	cleanupErr := errors.New("provider cleanup failed")
	err := store.DeleteQueued(entry.InfoHash, func(*Entry) error { return cleanupErr })
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("DeleteQueued error = %v, want cleanup failure", err)
	}
	if store.QueueExists(entry.InfoHash) {
		t.Fatal("queue row still exists after committed delete with cleanup error")
	}
}

func TestDeleteQueuedSnapshotAcceptsRevisionDriftInSameGeneration(t *testing.T) {
	store, entry := newAtomicMutationTestStorage(t)
	snapshot, err := store.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued snapshot: %v", err)
	}
	if _, present, err := store.MutateQueuedIfPresent(entry.InfoHash, func(current *Entry) (bool, error) {
		current.Category = "user-edited"
		return true, nil
	}); err != nil || !present {
		t.Fatalf("advance queue revision: present=%v err=%v", present, err)
	}

	var cleanedCategory string
	deleted, err := store.DeleteQueuedSnapshot(snapshot, func(current *Entry) error {
		cleanedCategory = current.Category
		return nil
	})
	if err != nil || !deleted {
		t.Fatalf("DeleteQueuedSnapshot: deleted=%v err=%v", deleted, err)
	}
	if cleanedCategory != "user-edited" {
		t.Fatalf("cleanup received stale row: category=%q", cleanedCategory)
	}
}

func TestDeleteQueuedSnapshotRejectsDeleteReAddGeneration(t *testing.T) {
	store, entry := newAtomicMutationTestStorage(t)
	stale, err := store.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued stale snapshot: %v", err)
	}
	if err := store.DeleteQueued(entry.InfoHash, nil); err != nil {
		t.Fatalf("DeleteQueued old generation: %v", err)
	}
	replacement := cloneAtomicTestEntry(stale)
	replacement.Category = "replacement"
	if err := store.AddQueue(replacement); err != nil {
		t.Fatalf("AddQueue replacement: %v", err)
	}

	cleanupCalled := false
	deleted, err := store.DeleteQueuedSnapshot(stale, func(*Entry) error {
		cleanupCalled = true
		return nil
	})
	if err != nil {
		t.Fatalf("DeleteQueuedSnapshot stale generation: %v", err)
	}
	if deleted || cleanupCalled {
		t.Fatalf("stale generation affected replacement: deleted=%v cleanup=%v", deleted, cleanupCalled)
	}
	current, err := store.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued replacement: %v", err)
	}
	if current.Category != "replacement" {
		t.Fatalf("replacement changed: category=%q", current.Category)
	}
}

func TestBulkQueueDeleteJoinsCleanupFailures(t *testing.T) {
	store, first := newAtomicMutationTestStorage(t)
	second := cloneAtomicTestEntry(first)
	second.InfoHash = "atomic-mutation-second"
	second.Files["movie.mkv"].InfoHash = second.InfoHash
	if err := store.AddQueue(second); err != nil {
		t.Fatalf("AddQueue second: %v", err)
	}
	firstErr := errors.New("cleanup first")
	secondErr := errors.New("cleanup second")
	err := store.DeleteWhereQueued(nil, func(entry *Entry) error {
		if entry.InfoHash == first.InfoHash {
			return firstErr
		}
		return secondErr
	})
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("DeleteWhereQueued error = %v, want both cleanup failures", err)
	}
	if store.QueueExists(first.InfoHash) || store.QueueExists(second.InfoHash) {
		t.Fatal("bulk queue rows still exist after committed deletes")
	}
}

func TestMutateEntryIfPresentCannotResurrectConcurrentDelete(t *testing.T) {
	store, entry := newAtomicMutationTestStorage(t)

	mutationStarted := make(chan struct{})
	releaseMutation := make(chan struct{})
	mutationDone := make(chan error, 1)
	go func() {
		_, _, err := store.MutateEntryIfPresent(entry.InfoHash, func(current *Entry) (bool, error) {
			close(mutationStarted)
			<-releaseMutation
			current.Size = 99
			return true, nil
		})
		mutationDone <- err
	}()
	<-mutationStarted

	deleteAttempted := make(chan struct{})
	deleteFinished := make(chan struct{})
	var deleteErr error
	go func() {
		close(deleteAttempted)
		deleteErr = store.Delete(entry.InfoHash)
		close(deleteFinished)
	}()
	<-deleteAttempted
	select {
	case <-deleteFinished:
		// Without shared mutation locking, deletion completes inside the
		// callback window and the subsequent write resurrects the entry.
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseMutation)

	if err := <-mutationDone; err != nil {
		t.Fatalf("MutateEntryIfPresent: %v", err)
	}
	<-deleteFinished
	if deleteErr != nil {
		t.Fatalf("Delete: %v", deleteErr)
	}
	if exists, err := store.Exists(entry.InfoHash); err != nil || exists {
		t.Fatalf("entry exists after concurrent delete: exists=%v err=%v", exists, err)
	}

	if _, present, err := store.MutateEntryIfPresent(entry.InfoHash, func(*Entry) (bool, error) {
		return true, nil
	}); err != nil || present {
		t.Fatalf("mutation after delete: present=%v err=%v", present, err)
	}
}

func TestMutateQueuedIfPresentCannotResurrectConcurrentDelete(t *testing.T) {
	store, entry := newAtomicMutationTestStorage(t)

	mutationStarted := make(chan struct{})
	releaseMutation := make(chan struct{})
	mutationDone := make(chan error, 1)
	go func() {
		_, _, err := store.MutateQueuedIfPresent(entry.InfoHash, func(current *Entry) (bool, error) {
			close(mutationStarted)
			<-releaseMutation
			current.Size = 99
			return true, nil
		})
		mutationDone <- err
	}()
	<-mutationStarted

	deleteAttempted := make(chan struct{})
	deleteFinished := make(chan struct{})
	var deleteErr error
	go func() {
		close(deleteAttempted)
		deleteErr = store.DeleteQueued(entry.InfoHash, nil)
		close(deleteFinished)
	}()
	<-deleteAttempted
	select {
	case <-deleteFinished:
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseMutation)

	if err := <-mutationDone; err != nil {
		t.Fatalf("MutateQueuedIfPresent: %v", err)
	}
	<-deleteFinished
	if deleteErr != nil {
		t.Fatalf("DeleteQueued: %v", deleteErr)
	}
	if store.QueueExists(entry.InfoHash) {
		t.Fatal("queue row exists after concurrent delete")
	}

	if _, present, err := store.MutateQueuedIfPresent(entry.InfoHash, func(*Entry) (bool, error) {
		return true, nil
	}); err != nil || present {
		t.Fatalf("queue mutation after delete: present=%v err=%v", present, err)
	}
}

func TestStaleMainWriterCannotResurrectOrOverwriteNewGeneration(t *testing.T) {
	store, entry := newAtomicMutationTestStorage(t)
	stale, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if err := store.Delete(entry.InfoHash); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	replacement := cloneAtomicTestEntry(stale)
	replacement.mainStoreGeneration = ""
	replacement.mainStoreRevision = 0
	replacement.CreatedAt = time.Now().Add(time.Second)
	replacement.Size = 55
	replacement.Bytes = 55
	replacement.Files["movie.mkv"].Size = 55
	if err := store.AddOrUpdate(replacement); err != nil {
		t.Fatalf("AddOrUpdate replacement: %v", err)
	}

	stale.Size = 99
	stale.Bytes = 99
	stale.Files["movie.mkv"].Size = 99
	if err := store.AddOrUpdate(stale); !errors.Is(err, ErrStaleEntryGeneration) {
		t.Fatalf("stale AddOrUpdate error = %v, want ErrStaleEntryGeneration", err)
	}
	current, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("Get replacement: %v", err)
	}
	if current.Size != 55 || current.Files["movie.mkv"].Size != 55 {
		t.Fatalf("replacement overwritten by stale writer: total=%d file=%d", current.Size, current.Files["movie.mkv"].Size)
	}
}

func TestQueueCleanupBlocksReAddAndRejectsOldGeneration(t *testing.T) {
	store, entry := newAtomicMutationTestStorage(t)
	stale, err := store.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}

	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- store.DeleteQueued(entry.InfoHash, func(*Entry) error {
			close(cleanupStarted)
			<-releaseCleanup
			return nil
		})
	}()
	<-cleanupStarted

	replacement := cloneAtomicTestEntry(stale)
	replacement.Size = 55
	replacement.Bytes = 55
	replacement.Files["movie.mkv"].Size = 55
	addDone := make(chan error, 1)
	go func() { addDone <- store.AddQueue(replacement) }()
	select {
	case err := <-addDone:
		t.Fatalf("same-key AddQueue completed during old cleanup: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseCleanup)
	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteQueued: %v", err)
	}
	if err := <-addDone; err != nil {
		t.Fatalf("AddQueue replacement: %v", err)
	}

	stale.Size = 99
	stale.Bytes = 99
	stale.Files["movie.mkv"].Size = 99
	if err := store.UpdateQueue(stale); !errors.Is(err, ErrStaleEntryGeneration) {
		t.Fatalf("stale UpdateQueue error = %v, want ErrStaleEntryGeneration", err)
	}
	current, err := store.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued replacement: %v", err)
	}
	if current.Size != 55 || current.Files["movie.mkv"].Size != 55 {
		t.Fatalf("queue replacement overwritten by stale writer: total=%d file=%d", current.Size, current.Files["movie.mkv"].Size)
	}
}

func TestSharedFolderIndexKeepsSurvivorDuringConcurrentDelete(t *testing.T) {
	store, first := newAtomicMutationTestStorage(t)
	first.Name = "Shared Folder"
	first.OriginalFilename = first.Name
	first.Files = map[string]*File{
		"first.mkv": {
			Name:     "first.mkv",
			Size:     10,
			InfoHash: first.InfoHash,
			AddedOn:  first.AddedOn,
		},
	}
	first.Size, first.Bytes = 10, 10
	if err := store.AddOrUpdate(first); err != nil {
		t.Fatalf("AddOrUpdate first shared entry: %v", err)
	}

	for i := 0; i < 100; i++ {
		second := &Entry{
			Protocol:  config.ProtocolNZB,
			InfoHash:  "shared-second",
			Name:      "Shared Folder",
			AddedOn:   first.AddedOn.Add(time.Duration(i+1) * time.Nanosecond),
			CreatedAt: time.Now().Add(time.Second),
			Size:      20,
			Bytes:     20,
			Files: map[string]*File{
				"second.mkv": {
					Name:     "second.mkv",
					Size:     20,
					InfoHash: "shared-second",
					AddedOn:  first.AddedOn.Add(time.Duration(i+1) * time.Nanosecond),
				},
			},
		}
		if err := store.AddOrUpdate(second); err != nil {
			t.Fatalf("AddOrUpdate second iteration %d: %v", i, err)
		}

		currentFirst, err := store.Get(first.InfoHash)
		if err != nil {
			t.Fatalf("Get first iteration %d: %v", i, err)
		}
		currentFirst.Files["first.mkv"].Size++
		currentFirst.Size++
		currentFirst.Bytes++

		start := make(chan struct{})
		errs := make(chan error, 2)
		go func() {
			<-start
			errs <- store.AddOrUpdate(currentFirst)
		}()
		go func() {
			<-start
			errs <- store.Delete(second.InfoHash)
		}()
		close(start)
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatalf("shared-folder mutation iteration %d: %v", i, err)
			}
		}

		item, err := store.GetEntryItem(first.GetFolder())
		if err != nil {
			t.Fatalf("GetEntryItem iteration %d: %v", i, err)
		}
		if _, ok := item.Files["first.mkv"]; !ok {
			t.Fatalf("survivor missing from shared folder iteration %d", i)
		}
		if _, ok := item.Files["second.mkv"]; ok {
			t.Fatalf("deleted file resurrected in shared folder iteration %d", i)
		}
	}
}

func TestCopyFromPreservesMetadataForAlreadyVersionedRows(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	source, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage source: %v", err)
	}
	t.Cleanup(func() { _ = source.Close() })
	destination, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage destination: %v", err)
	}
	t.Cleanup(func() { _ = destination.Close() })

	entry := atomicTestEntry()
	entry.Category = "movies"
	entry.ActiveProvider = "provider-a"
	entry.Bad = true
	if err := source.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate source: %v", err)
	}
	if err := source.AddQueue(entry); err != nil {
		t.Fatalf("AddQueue source: %v", err)
	}
	if entry.mainStoreGeneration == "" || entry.queueStoreGeneration == "" {
		t.Fatal("source rows were not versioned before copy")
	}

	if err := destination.copyFrom(source); err != nil {
		t.Fatalf("copyFrom: %v", err)
	}

	assertMetadata := func(storeName string, each func(func(string, *hybrid.IndexEntry) error) error) {
		t.Helper()
		found := false
		if err := each(func(key string, meta *hybrid.IndexEntry) error {
			if key != entry.InfoHash {
				return nil
			}
			found = true
			if meta.Name != entry.GetFolder() || meta.Protocol != string(entry.Protocol) || meta.Provider != entry.ActiveProvider || meta.TotalSize != entry.Size || !meta.Bad {
				t.Errorf("%s metadata not preserved: %+v", storeName, meta)
			}
			return nil
		}); err != nil {
			t.Fatalf("scan %s metadata: %v", storeName, err)
		}
		if !found {
			t.Fatalf("%s metadata missing copied row", storeName)
		}
	}
	assertMetadata("main", destination.entries.ForEachMeta)
	assertMetadata("queue", destination.queue.ForEachMeta)
}

func newAtomicMutationTestStorage(t *testing.T) (*Storage, *Entry) {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	store, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	entry := atomicTestEntry()
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	if err := store.AddQueue(entry); err != nil {
		t.Fatalf("AddQueue: %v", err)
	}
	return store, entry
}

func atomicTestEntry() *Entry {
	added := time.Unix(1_700_000_000, 0).UTC()
	return &Entry{
		Protocol: config.ProtocolNZB,
		InfoHash: "atomic-mutation",
		Name:     "Atomic Mutation",
		AddedOn:  added,
		Size:     10,
		Bytes:    10,
		Files: map[string]*File{
			"movie.mkv": {
				Name:     "movie.mkv",
				Size:     10,
				InfoHash: "atomic-mutation",
				AddedOn:  added,
			},
		},
	}
}

func cloneAtomicTestEntry(entry *Entry) *Entry {
	cloned := *entry
	cloned.Files = make(map[string]*File, len(entry.Files))
	for name, file := range entry.Files {
		clonedFile := *file
		cloned.Files[name] = &clonedFile
	}
	return &cloned
}

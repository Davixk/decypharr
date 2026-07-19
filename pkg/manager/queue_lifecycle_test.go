package manager

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestDeleteEntryCancelsActionAndFencesReplacementAdmission(t *testing.T) {
	store, queue, manager, entry := newQueueLifecycleFixture(t)
	actionWaitStarted := make(chan struct{})
	admissionObservedDeletion := make(chan struct{})
	queue.lifecycleTestHook = func(stage string) {
		switch stage {
		case "action-wait-started":
			close(actionWaitStarted)
		case "deletion-observed":
			close(admissionObservedDeletion)
		}
	}
	queued, err := queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetTorrent: %v", err)
	}
	actionCtx, release, err := queue.BeginAction(context.Background(), queued)
	if err != nil {
		t.Fatalf("BeginAction: %v", err)
	}
	cancelled := make(chan struct{})
	allowWorkerExit := make(chan struct{})
	go func() {
		<-actionCtx.Done()
		close(cancelled)
		<-allowWorkerExit
		release()
	}()

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- manager.DeleteEntry(entry.InfoHash, false) }()
	awaitSignal(t, cancelled, "active action cancellation")
	awaitSignal(t, actionWaitStarted, "deletion waiting for the active action")

	replacement := queueLifecycleEntry(t, entry.InfoHash)
	addDone := make(chan error, 1)
	go func() { addDone <- queue.Add(replacement) }()
	awaitSignal(t, admissionObservedDeletion, "replacement admission observing the deletion tombstone")

	close(allowWorkerExit)
	if err := awaitResult(t, deleteDone, "DeleteEntry"); err != nil {
		t.Fatalf("DeleteEntry: %v", err)
	}
	if err := awaitResult(t, addDone, "replacement Add"); err != nil {
		t.Fatalf("replacement Add: %v", err)
	}
	if _, err := store.Get(entry.InfoHash); err == nil {
		t.Fatal("main entry survived coordinated deletion")
	}
	current, err := queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatalf("replacement queue row missing: %v", err)
	}
	if storage.SameQueueGeneration(queued, current) {
		t.Fatal("replacement reused the deleted queue generation")
	}
	queued.Progress = 99
	if err := queue.Update(queued); !errors.Is(err, storage.ErrStaleEntryGeneration) {
		t.Fatalf("stale worker update error = %v, want ErrStaleEntryGeneration", err)
	}
}

func TestQueueDeletionBarrierCoversCleanupAndRejectsDuplicates(t *testing.T) {
	_, queue, _, entry := newQueueLifecycleFixture(t)
	duplicate := queueLifecycleEntry(t, entry.InfoHash)
	if err := queue.Add(duplicate); err == nil {
		t.Fatal("duplicate queue admission succeeded")
	}

	cleanupStarted := make(chan struct{})
	allowCleanup := make(chan struct{})
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- queue.withDeletionBarrier(entry.InfoHash, nil, func(*storage.Entry) error {
			close(cleanupStarted)
			<-allowCleanup
			return nil
		})
	}()
	awaitSignal(t, cleanupStarted, "deletion cleanup")

	admissionObservedDeletion := make(chan struct{})
	queue.lifecycleTestHook = func(stage string) {
		if stage == "deletion-observed" {
			close(admissionObservedDeletion)
		}
	}
	replacement := queueLifecycleEntry(t, entry.InfoHash)
	addDone := make(chan error, 1)
	go func() { addDone <- queue.Add(replacement) }()
	awaitSignal(t, admissionObservedDeletion, "replacement admission observing cleanup tombstone")
	close(allowCleanup)
	if err := awaitResult(t, deleteDone, "deletion barrier"); err != nil {
		t.Fatalf("withDeletionBarrier: %v", err)
	}
	if err := awaitResult(t, addDone, "replacement Add"); err != nil {
		t.Fatalf("replacement Add: %v", err)
	}
}

func TestConcurrentQueueDeleteWaiterDoesNotRetargetReplacement(t *testing.T) {
	store, queue, _, original := newQueueLifecycleFixture(t)
	queuedOriginal, err := queue.GetTorrent(original.InfoHash)
	if err != nil {
		t.Fatalf("GetTorrent original: %v", err)
	}

	d1SnapshotLoaded := make(chan struct{})
	d2SnapshotLoaded := make(chan struct{})
	allowD1 := make(chan struct{})
	allowD2 := make(chan struct{})
	var snapshotCount atomic.Int32
	queue.queueDeleteTestHook = func(stage string) {
		if stage != "snapshot-loaded" {
			return
		}
		switch snapshotCount.Add(1) {
		case 1:
			close(d1SnapshotLoaded)
			<-allowD1
		case 2:
			close(d2SnapshotLoaded)
			<-allowD2
		}
	}

	cleanupStarted := make(chan struct{})
	allowCleanup := make(chan struct{})
	d1Done := make(chan error, 1)
	go func() {
		d1Done <- queue.Delete(original.InfoHash, func(deleted *storage.Entry) error {
			if !storage.SameQueueGeneration(queuedOriginal, deleted) {
				return errors.New("D1 cleanup received the wrong queue generation")
			}
			close(cleanupStarted)
			<-allowCleanup
			return nil
		})
	}()
	awaitSignal(t, d1SnapshotLoaded, "D1 queue snapshot")

	var d2CleanupCalled atomic.Bool
	d2Done := make(chan error, 1)
	go func() {
		d2Done <- queue.Delete(original.InfoHash, func(*storage.Entry) error {
			d2CleanupCalled.Store(true)
			return nil
		})
	}()
	awaitSignal(t, d2SnapshotLoaded, "D2 queue snapshot")
	close(allowD1)
	awaitSignal(t, cleanupStarted, "D1 cleanup")

	replacement := queueLifecycleEntry(t, original.InfoHash)
	replacement.Category = "replacement"
	admissionObservedDeletion := make(chan struct{})
	queue.lifecycleTestHook = func(stage string) {
		if stage == "deletion-observed" {
			close(admissionObservedDeletion)
		}
	}
	addDone := make(chan error, 1)
	go func() { addDone <- queue.Add(replacement) }()
	awaitSignal(t, admissionObservedDeletion, "replacement admission observing D1 tombstone")

	close(allowCleanup)
	if err := awaitResult(t, d1Done, "D1 queue deletion"); err != nil {
		t.Fatalf("D1 Delete: %v", err)
	}
	if err := awaitResult(t, addDone, "replacement admission"); err != nil {
		t.Fatalf("replacement Add: %v", err)
	}
	close(allowD2)

	if err := awaitResult(t, d2Done, "D2 queue deletion"); !errors.Is(err, storage.ErrEntryNotFound) {
		t.Fatalf("D2 Delete error = %v, want ErrEntryNotFound for its old generation", err)
	}
	if d2CleanupCalled.Load() {
		t.Fatal("D2 cleanup ran against the replacement generation")
	}
	current, err := store.GetQueued(original.InfoHash)
	if err != nil {
		t.Fatalf("replacement queue row missing: %v", err)
	}
	if !storage.SameQueueGeneration(replacement, current) {
		t.Fatal("D2 consumed or replaced the new queue generation")
	}
	if storage.SameQueueGeneration(queuedOriginal, current) {
		t.Fatal("replacement reused the original queue generation")
	}
	if current.Category != "replacement" {
		t.Fatalf("replacement category = %q", current.Category)
	}
}

func TestStaleDeleteWaiterDoesNotTakeReplacementQueueGeneration(t *testing.T) {
	store, queue, manager, original := newQueueLifecycleFixture(t)
	queuedOriginal, err := store.GetQueued(original.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued original: %v", err)
	}

	unlock, finishDeletion := queue.beginDeletion(original.InfoHash)
	released := false
	releaseBarrier := func() {
		if released {
			return
		}
		released = true
		unlock()
		finishDeletion()
	}
	defer releaseBarrier()

	snapshotLoaded := make(chan struct{})
	manager.deleteEntryTestHook = func(stage string) {
		if stage == "snapshot-loaded" {
			close(snapshotLoaded)
		}
	}
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- manager.DeleteEntry(original.InfoHash, false) }()
	awaitSignal(t, snapshotLoaded, "stale DeleteEntry snapshot")

	if _, present, err := store.TakeQueued(original.InfoHash); err != nil || !present {
		t.Fatalf("TakeQueued original: present=%v err=%v", present, err)
	}
	if err := store.Delete(original.InfoHash); err != nil {
		t.Fatalf("Delete original main row: %v", err)
	}
	replacement := queueLifecycleEntry(t, original.InfoHash)
	replacement.Category = "replacement"
	if err := store.AddOrUpdate(replacement); err != nil {
		t.Fatalf("AddOrUpdate replacement: %v", err)
	}
	if err := store.AddQueue(replacement); err != nil {
		t.Fatalf("AddQueue replacement: %v", err)
	}
	releaseBarrier()

	if err := awaitResult(t, deleteDone, "stale DeleteEntry"); err == nil || !strings.Contains(err.Error(), "was replaced before deletion") {
		t.Fatalf("stale DeleteEntry error = %v, want replacement fence", err)
	}
	currentMain, err := store.Get(original.InfoHash)
	if err != nil {
		t.Fatalf("replacement main row missing: %v", err)
	}
	if storage.SameMainGeneration(original, currentMain) {
		t.Fatal("replacement main row reused the original generation")
	}
	currentQueue, err := store.GetQueued(original.InfoHash)
	if err != nil {
		t.Fatalf("replacement queue row missing: %v", err)
	}
	if !storage.SameQueueGeneration(replacement, currentQueue) {
		t.Fatal("stale DeleteEntry consumed or replaced the new queue generation")
	}
	if storage.SameQueueGeneration(queuedOriginal, currentQueue) {
		t.Fatal("replacement queue row reused the original generation")
	}
}

// TestDeleteEntryDoesNotBlockOnWorkerIgnoringCancellation pins the bounded
// deletion join: a worker that never observes its cancelled context must not
// hold a synchronous arr/WebUI DELETE hostage. The delete proceeds after the
// bound; generation fencing keeps the straggler from resurrecting state.
func TestDeleteEntryDoesNotBlockOnWorkerIgnoringCancellation(t *testing.T) {
	store, queue, manager, entry := newQueueLifecycleFixture(t)
	previous := queueActionJoinTimeout
	queueActionJoinTimeout = 100 * time.Millisecond
	t.Cleanup(func() { queueActionJoinTimeout = previous })

	queued, err := queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetTorrent: %v", err)
	}
	actionCtx, release, err := queue.BeginAction(context.Background(), queued)
	if err != nil {
		t.Fatalf("BeginAction: %v", err)
	}
	// The worker deliberately ignores cancellation until the very end.
	released := false
	t.Cleanup(func() {
		if !released {
			release()
		}
	})

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- manager.DeleteEntry(entry.InfoHash, false) }()
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatalf("DeleteEntry: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteEntry blocked past the bounded join on a worker that ignored cancellation")
	}
	if actionCtx.Err() == nil {
		t.Fatal("worker context was not cancelled by the delete")
	}
	if _, err := store.Get(entry.InfoHash); err == nil {
		t.Fatal("main entry survived the bounded delete")
	}
	if store.QueueExists(entry.InfoHash) {
		t.Fatal("queue row survived the bounded delete")
	}

	// The straggler cannot resurrect any state: its row is gone.
	queued.Progress = 50
	if err := queue.Update(queued); !errors.Is(err, storage.ErrEntryNotFound) {
		t.Fatalf("straggler update error = %v, want ErrEntryNotFound", err)
	}

	// A same-hash replacement can be admitted and can begin its own action
	// even while the straggler still holds its stale lease open.
	replacement := queueLifecycleEntry(t, entry.InfoHash)
	if err := queue.Add(replacement); err != nil {
		t.Fatalf("replacement Add: %v", err)
	}
	replacementRow, err := queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetTorrent replacement: %v", err)
	}
	_, releaseNext, err := queue.BeginAction(context.Background(), replacementRow)
	if err != nil {
		t.Fatalf("BeginAction after timed-out join: %v", err)
	}
	releaseNext()

	// The straggler's writes stay fenced off the replacement generation.
	queued.Progress = 99
	if err := queue.Update(queued); !errors.Is(err, storage.ErrStaleEntryGeneration) {
		t.Fatalf("straggler update against replacement = %v, want ErrStaleEntryGeneration", err)
	}

	// Late release of the abandoned lease is harmless.
	release()
	released = true
}

func TestBeginActionRejectsDuplicateAndReleaseAllowsNext(t *testing.T) {
	_, queue, _, entry := newQueueLifecycleFixture(t)
	snapshot, err := queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetTorrent: %v", err)
	}
	_, release, err := queue.BeginAction(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("first BeginAction: %v", err)
	}
	if _, duplicateRelease, err := queue.BeginAction(context.Background(), snapshot); err == nil {
		if duplicateRelease != nil {
			duplicateRelease()
		}
		t.Fatal("duplicate active action was accepted")
	}
	release()
	_, nextRelease, err := queue.BeginAction(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("BeginAction after release: %v", err)
	}
	nextRelease()
}

func TestCompleteActionReleasesLifecycleBeforeCallback(t *testing.T) {
	store, queue, _, entry := newQueueLifecycleFixture(t)
	snapshot, err := queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetTorrent: %v", err)
	}
	snapshot.State = storage.EntryStateDownloading
	snapshot.IsDownloading = true
	if err := queue.Update(snapshot); err != nil {
		t.Fatalf("mark queue action active: %v", err)
	}
	actionCtx, release, err := queue.BeginAction(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("BeginAction: %v", err)
	}
	t.Cleanup(release)

	callbackStarted := make(chan struct{})
	allowCallback := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-allowCallback:
		default:
			close(allowCallback)
		}
	})
	completeDone := make(chan error, 1)
	go func() {
		completeDone <- queue.CompleteAction(snapshot, snapshot.DownloadPath(), func(*storage.Entry) {
			close(callbackStarted)
			<-allowCallback
		})
	}()
	awaitSignal(t, callbackStarted, "completion callback")
	awaitSignal(t, actionCtx.Done(), "completed action lease finalization")

	deleteSnapshotLoaded := make(chan struct{})
	queue.queueDeleteTestHook = func(stage string) {
		if stage == "snapshot-loaded" {
			close(deleteSnapshotLoaded)
		}
	}
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- queue.Delete(entry.InfoHash, nil) }()
	awaitSignal(t, deleteSnapshotLoaded, "delete snapshot while callback is blocked")
	if err := awaitResult(t, deleteDone, "delete while completion callback is blocked"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.GetQueued(entry.InfoHash); err == nil {
		t.Fatal("queue row survived delete while completion callback was blocked")
	}

	close(allowCallback)
	if err := awaitResult(t, completeDone, "CompleteAction callback return"); err != nil {
		t.Fatalf("CompleteAction: %v", err)
	}
}

func newQueueLifecycleFixture(t *testing.T) (*storage.Storage, *Queue, *Manager, *storage.Entry) {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close storage: %v", err)
		}
	})
	queue := newQueue(store, "")
	manager := &Manager{storage: store, queue: queue}
	entry := queueLifecycleEntry(t, "delete-barrier")
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	if err := queue.Add(entry); err != nil {
		t.Fatalf("queue Add: %v", err)
	}
	return store, queue, manager, entry
}

func queueLifecycleEntry(t *testing.T, infohash string) *storage.Entry {
	t.Helper()
	added := time.Unix(1_700_000_000, 0).UTC()
	return &storage.Entry{
		Protocol: config.ProtocolTorrent,
		InfoHash: infohash,
		Name:     "Delete Barrier.mkv",
		SavePath: t.TempDir(),
		AddedOn:  added,
		Size:     10,
		Bytes:    10,
		Files: map[string]*storage.File{
			"Delete Barrier.mkv": {
				Name:     "Delete Barrier.mkv",
				InfoHash: infohash,
				Size:     10,
				AddedOn:  added,
			},
		},
	}
}

func awaitSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func awaitResult(t *testing.T, result <-chan error, description string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return nil
	}
}

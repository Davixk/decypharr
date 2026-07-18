package usenet

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"google.golang.org/protobuf/proto"
)

type staticPrefetchReader []byte

func (r staticPrefetchReader) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r)) {
		return 0, io.EOF
	}
	n := copy(p, r[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r staticPrefetchReader) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return r.ReadAt(p, off)
}

func (staticPrefetchReader) Prefetch(context.Context, int64, int64) {}

func TestPreparePublicationCannotSurviveDeleteAndReAdd(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id       = "prepare-aba"
		filename = "movie.mkv"
	)
	if err := store.AddNZB(lifecycleTestNZB(id, filename, 4)); err != nil {
		t.Fatalf("AddNZB old generation: %v", err)
	}
	u := newTestUsenet(store)

	reachedPublish := make(chan struct{})
	releasePublish := make(chan struct{})
	var hookOnce sync.Once
	u.lifecycleTestHook = func(operation, gotID string) {
		if operation != "prepare-publish" || gotID != id {
			return
		}
		hookOnce.Do(func() { close(reachedPublish) })
		<-releasePublish
	}

	prepareDone := make(chan struct {
		size int64
		err  error
	}, 1)
	go func() {
		size, err := u.PrepareStream(id, filename)
		prepareDone <- struct {
			size int64
			err  error
		}{size: size, err: err}
	}()
	waitLifecycleSignal(t, reachedPublish, "old prepare to reach cache publication")

	replaceDone := make(chan error, 1)
	go func() {
		if err := u.Delete(id); err != nil {
			replaceDone <- err
			return
		}
		replaceDone <- store.AddNZB(lifecycleTestNZB(id, filename, 8))
	}()
	waitLifecycleRefs(t, store, id, 2)
	select {
	case err := <-replaceDone:
		t.Fatalf("delete/re-add passed an in-flight prepare publication: %v", err)
	default:
	}

	close(releasePublish)
	prepared := waitLifecycleResult(t, prepareDone, "old prepare completion")
	if prepared.err != nil || prepared.size != 4 {
		t.Fatalf("old prepare = size %d, err %v; want size 4", prepared.size, prepared.err)
	}
	if err := waitLifecycleError(t, replaceDone, "delete/re-add completion"); err != nil {
		t.Fatalf("delete/re-add: %v", err)
	}

	key := fsKey(id, filename)
	if _, ok := u.preparedSizes.Load(key); ok {
		t.Fatal("old prepared size survived delete/re-add")
	}
	if _, ok := u.failedFiles.Load(key); ok {
		t.Fatal("old failure cache survived delete/re-add")
	}
	newSize, err := u.PrepareStream(id, filename)
	if err != nil || newSize != 8 {
		t.Fatalf("new generation prepare = size %d, err %v; want size 8", newSize, err)
	}
}

func TestLifecycleLockSetKeepsQueuedSuccessorsOnOneMutex(t *testing.T) {
	var locks nzbLifecycleLockSet
	const id = "three-party-lock"
	unlockFirst := locks.lock(id)

	secondAcquired := make(chan struct{})
	releaseSecond := make(chan struct{})
	go func() {
		unlock := locks.lock(id)
		close(secondAcquired)
		<-releaseSecond
		unlock()
	}()
	waitLockSetRefs(t, &locks, id, 2)
	unlockFirst()
	waitLifecycleSignal(t, secondAcquired, "second lifecycle owner")

	thirdAcquired := make(chan struct{})
	releaseThird := make(chan struct{})
	thirdDone := make(chan struct{})
	go func() {
		unlock := locks.lock(id)
		close(thirdAcquired)
		<-releaseThird
		unlock()
		close(thirdDone)
	}()
	waitLockSetRefs(t, &locks, id, 2)
	select {
	case <-thirdAcquired:
		t.Fatal("third caller acquired while queued successor still owned the lock")
	default:
	}

	close(releaseSecond)
	waitLifecycleSignal(t, thirdAcquired, "third lifecycle owner")
	close(releaseThird)
	waitLifecycleSignal(t, thirdDone, "third lifecycle release")
}

func TestPermanentFailurePublicationCannotSurviveDeleteAndReAdd(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id       = "failure-aba"
		filename = "movie.mkv"
	)
	if err := store.AddNZB(lifecycleTestNZB(id, filename, 4)); err != nil {
		t.Fatalf("AddNZB old generation: %v", err)
	}
	u := newTestUsenet(store)

	reachedPublish := make(chan struct{})
	releasePublish := make(chan struct{})
	u.lifecycleTestHook = func(operation, gotID string) {
		if operation != "failure-publish" || gotID != id {
			return
		}
		close(reachedPublish)
		<-releasePublish
	}

	failureDone := make(chan error, 1)
	go func() {
		failureDone <- u.recordPermanentArticleFailure(id, filename, errors.New("430 no such article"))
	}()
	waitLifecycleSignal(t, reachedPublish, "permanent failure to reach cache publication")

	replaceDone := make(chan error, 1)
	go func() {
		if err := u.Delete(id); err != nil {
			replaceDone <- err
			return
		}
		replaceDone <- store.AddNZB(lifecycleTestNZB(id, filename, 8))
	}()
	waitLifecycleRefs(t, store, id, 2)
	select {
	case err := <-replaceDone:
		t.Fatalf("delete/re-add passed an in-flight failure publication: %v", err)
	default:
	}

	close(releasePublish)
	if err := waitLifecycleError(t, failureDone, "permanent failure completion"); err == nil {
		t.Fatal("recordPermanentArticleFailure returned nil")
	}
	if err := waitLifecycleError(t, replaceDone, "delete/re-add completion"); err != nil {
		t.Fatalf("delete/re-add: %v", err)
	}

	key := fsKey(id, filename)
	if _, ok := u.failedFiles.Load(key); ok {
		t.Fatal("old permanent failure survived delete/re-add")
	}
	if _, ok := u.preparedSizes.Load(key); ok {
		t.Fatal("old prepared size survived delete/re-add")
	}
	newSize, err := u.PrepareStream(id, filename)
	if err != nil || newSize != 8 {
		t.Fatalf("new generation prepare = size %d, err %v; want size 8", newSize, err)
	}
}

func TestCompletionCannotOverwriteDurablePermanentFailure(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id       = "failure-before-completion"
		filename = "movie.mkv"
	)
	if err := store.AddNZB(lifecycleTestNZB(id, filename, 4)); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	staleParserSnapshot, err := store.GetNZB(id)
	if err != nil {
		t.Fatalf("GetNZB stale snapshot: %v", err)
	}
	u := newTestUsenet(store)
	if err := u.recordPermanentArticleFailure(id, filename, errors.New("430 no such article")); err == nil {
		t.Fatal("recordPermanentArticleFailure returned nil")
	}
	if err := u.markAsCompleted(staleParserSnapshot); err == nil {
		t.Fatal("markAsCompleted accepted a snapshot older than the durable failure")
	}

	stored, err := store.GetNZB(id)
	if err != nil {
		t.Fatalf("GetNZB final: %v", err)
	}
	if !stored.Files[0].IsDeleted || !stored.IsBad || stored.Status != NZBStatusFailed || !strings.Contains(stored.FailMessage, "430 no such article") {
		t.Fatalf("completion overwrote durable failure: deleted=%v bad=%v status=%q reason=%q", stored.Files[0].IsDeleted, stored.IsBad, stored.Status, stored.FailMessage)
	}
}

func TestMarkAsFailedUsesFreshDurableMetadata(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id       = "normalize-before-failure"
		filename = "movie.mkv"
	)
	nzb := lifecycleTestNZB(id, filename, 10)
	nzb.Files[0].Segments[0].Bytes = 4
	nzb.Files[0].Segments[0].EndOffset = 3
	if err := store.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	staleParserSnapshot, err := store.GetNZB(id)
	if err != nil {
		t.Fatalf("GetNZB stale snapshot: %v", err)
	}
	u := newTestUsenet(store)
	if _, changed, err := u.NormalizeNZBFileSizes(id); err != nil || !changed {
		t.Fatalf("NormalizeNZBFileSizes = changed %v, err %v", changed, err)
	}
	if err := u.markAsFailed(staleParserSnapshot, errors.New("parser failed")); err != nil {
		t.Fatalf("markAsFailed: %v", err)
	}

	stored, err := store.GetNZB(id)
	if err != nil {
		t.Fatalf("GetNZB final: %v", err)
	}
	if stored.Files[0].Size != 4 || stored.TotalSize != 4 || stored.Status != NZBStatusFailed {
		t.Fatalf("failed metadata = size %d, total %d, status %q; want 4, 4, failed", stored.Files[0].Size, stored.TotalSize, stored.Status)
	}
}

func TestMarkAsCompletedPreservesFreshNormalizedSizes(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id       = "normalize-before-completion"
		filename = "movie.mkv"
	)
	nzb := lifecycleTestNZB(id, filename, 10)
	nzb.Files[0].Segments[0].Bytes = 4
	nzb.Files[0].Segments[0].EndOffset = 3
	if err := store.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	staleParserSnapshot, err := store.GetNZB(id)
	if err != nil {
		t.Fatalf("GetNZB stale snapshot: %v", err)
	}
	u := newTestUsenet(store)
	if _, changed, err := u.NormalizeNZBFileSizes(id); err != nil || !changed {
		t.Fatalf("NormalizeNZBFileSizes = changed %v, err %v", changed, err)
	}
	if err := u.markAsCompleted(staleParserSnapshot); err != nil {
		t.Fatalf("markAsCompleted: %v", err)
	}

	stored, err := store.GetNZB(id)
	if err != nil {
		t.Fatalf("GetNZB final: %v", err)
	}
	if stored.Files[0].Size != 4 || stored.TotalSize != 4 || stored.Status != NZBStatusCompleted {
		t.Fatalf("completed metadata = size %d, total %d, status %q; want 4, 4, completed", stored.Files[0].Size, stored.TotalSize, stored.Status)
	}
}

func TestPrepareStreamFilesReadsUnrelatedIDsInParallel(t *testing.T) {
	store := newTestNZBStorage(t)
	for _, id := range []string{"parallel-a", "parallel-b"} {
		if err := store.AddNZB(lifecycleTestNZB(id, "movie.mkv", 4)); err != nil {
			t.Fatalf("AddNZB %s: %v", id, err)
		}
	}

	reachedRead := make(chan string, 2)
	releaseReads := make(chan struct{})
	store.prepareAfterReadHook = func(id string) {
		reachedRead <- id
		<-releaseReads
	}

	errCh := make(chan error, 2)
	for _, id := range []string{"parallel-a", "parallel-b"} {
		go func() {
			_, err := store.prepareStreamFiles(id, []string{"movie.mkv"})
			errCh <- err
		}()
	}

	seen := make(map[string]bool, 2)
	for len(seen) < 2 {
		select {
		case id := <-reachedRead:
			seen[id] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("unrelated metadata reads serialized; reached=%v", seen)
		}
	}
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- store.AddNZB(lifecycleTestNZB("parallel-writer", "other.mkv", 8))
	}()
	select {
	case err := <-writerDone:
		if err != nil {
			t.Fatalf("unrelated metadata write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unrelated metadata write blocked while validation was paused")
	}
	close(releaseReads)
	for range 2 {
		if err := waitLifecycleError(t, errCh, "parallel prepare completion"); err != nil {
			t.Fatalf("parallel prepare: %v", err)
		}
	}
}

func TestLegacyMigrationCannotResurrectDeletedNZB(t *testing.T) {
	store := newTestNZBStorage(t)
	const id = "migration-delete"
	legacy, err := proto.Marshal(nzbToProto(lifecycleTestNZB(id, "movie.mkv", 4)))
	if err != nil {
		t.Fatalf("marshal legacy metadata: %v", err)
	}
	path := store.metaFilePath(id)
	if err := os.WriteFile(path, legacy, 0o644); err != nil {
		t.Fatalf("write legacy metadata: %v", err)
	}

	reachedCommit := make(chan struct{})
	releaseCommit := make(chan struct{})
	store.migrationBeforeCommitHook = func(gotPath string) {
		if gotPath != path {
			return
		}
		close(reachedCommit)
		<-releaseCommit
	}

	type migrationResult struct {
		migrated bool
		err      error
	}
	migrationDone := make(chan migrationResult, 1)
	go func() {
		migrated, err := store.migrateFile(path)
		migrationDone <- migrationResult{migrated: migrated, err: err}
	}()
	waitLifecycleSignal(t, reachedCommit, "legacy migration commit")
	if err := store.DeleteNZB(id); err != nil {
		t.Fatalf("DeleteNZB: %v", err)
	}
	close(releaseCommit)

	select {
	case result := <-migrationDone:
		if result.err != nil || result.migrated {
			t.Fatalf("migrateFile after delete = migrated %v, err %v; want false, nil", result.migrated, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for migration")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("deleted NZB was resurrected: stat err = %v", err)
	}
}

func TestFSGenerationRetiredAcrossDeleteAndReAdd(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id       = "fs-generation-aba"
		filename = "movie.mkv"
	)
	oldNZB := lifecycleTestNZB(id, filename, 4)
	oldNZB.Files[0].Segments[0].MessageID = "old-segment"
	if err := store.AddNZB(oldNZB); err != nil {
		t.Fatalf("AddNZB old generation: %v", err)
	}
	u := newTestUsenet(store)
	u.fs = xsync.NewMap[string, *fsEntry]()

	beforePublish := make(chan struct{})
	releasePublish := make(chan struct{})
	publishedRefCount := make(chan int32, 1)
	var beforeOnce sync.Once
	var publishedOnce sync.Once
	key := fsKey(id, filename)
	u.lifecycleTestHook = func(operation, gotID string) {
		if gotID != id {
			return
		}
		switch operation {
		case "fs-before-publish":
			beforeOnce.Do(func() {
				close(beforePublish)
				<-releasePublish
			})
		case "fs-publish":
			publishedOnce.Do(func() {
				entry, ok := u.fs.Load(key)
				if !ok {
					publishedRefCount <- -1
					return
				}
				publishedRefCount <- entry.refCount.Load()
			})
		}
	}

	type entryResult struct {
		entry *fsEntry
		err   error
	}
	oldEntryDone := make(chan entryResult, 1)
	go func() {
		entry, err := u.getOrCreateEntry(context.Background(), id, filename)
		oldEntryDone <- entryResult{entry: entry, err: err}
	}()
	waitLifecycleSignal(t, beforePublish, "old fs entry to reach publication")

	replaceDone := make(chan error, 1)
	go func() {
		if err := u.Delete(id); err != nil {
			replaceDone <- err
			return
		}
		newNZB := lifecycleTestNZB(id, filename, 4)
		newNZB.Files[0].Segments[0].MessageID = "new-segment"
		replaceDone <- store.AddNZB(newNZB)
	}()
	waitLifecycleRefs(t, store, id, 2)
	select {
	case err := <-replaceDone:
		t.Fatalf("delete/re-add passed an in-flight fs publication: %v", err)
	default:
	}

	close(releasePublish)
	var oldResult entryResult
	select {
	case oldResult = <-oldEntryDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for old fs entry")
	}
	if oldResult.err != nil || oldResult.entry == nil {
		t.Fatalf("old getOrCreateEntry = entry %v, err %v", oldResult.entry, oldResult.err)
	}
	if got := <-publishedRefCount; got != 1 {
		t.Fatalf("new fs entry published with refCount %d; want creator reference 1", got)
	}

	cleanupCalled := make(chan struct{})
	oldResult.entry.readerCleanup = func() { close(cleanupCalled) }
	if err := waitLifecycleError(t, replaceDone, "delete/re-add completion"); err != nil {
		t.Fatalf("delete/re-add: %v", err)
	}
	if !oldResult.entry.retired.Load() || !oldResult.entry.unmapped.Load() {
		t.Fatal("old fs entry was not retired and unmapped")
	}
	select {
	case <-cleanupCalled:
		t.Fatal("active old fs entry was cleaned before its final release")
	default:
	}

	newEntry, err := u.getOrCreateEntry(context.Background(), id, filename)
	if err != nil {
		t.Fatalf("new getOrCreateEntry: %v", err)
	}
	if newEntry == oldResult.entry {
		t.Fatal("replacement generation reused the old fs entry")
	}
	if got := newEntry.volumes[0].Segments[0].MessageID; got != "new-segment" {
		t.Fatalf("replacement fs entry segment = %q; want new-segment", got)
	}

	// Releasing the retired generation must not look up the key and decrement
	// the replacement entry that is now mapped there.
	u.releaseFS(oldResult.entry)
	waitLifecycleSignal(t, cleanupCalled, "old fs entry cleanup after final release")
	if got := newEntry.refCount.Load(); got != 1 {
		t.Fatalf("old generation release changed replacement refCount to %d; want 1", got)
	}
	u.releaseFS(newEntry)
}

func TestNormalizeNZBFileSizesSerializesWithPermanentFailure(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id       = "normalize-failure"
		filename = "movie.mkv"
		other    = "subtitles.srt"
	)
	nzb := lifecycleTestNZB(id, filename, 10)
	nzb.Files[0].Segments[0].Bytes = 4
	nzb.Files[0].Segments[0].EndOffset = 3
	nzb.Files = append(nzb.Files, storage.NZBFile{
		Name: other,
		Size: 2,
		Segments: []storage.NZBSegment{{
			Number:    1,
			MessageID: id + "-subtitle",
			Bytes:     2,
			EndOffset: 1,
		}},
	})
	nzb.TotalSize = 12
	if err := store.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	u := newTestUsenet(store)

	// A normalization write must invalidate every per-file result, including a
	// filename unrelated to the concurrent failure below.
	otherKey := fsKey(id, other)
	u.preparedSizes.Store(otherKey, 2)
	u.failedFiles.Store(otherKey, errors.New("stale cached failure"))

	reachedWrite := make(chan struct{})
	releaseWrite := make(chan struct{})
	u.lifecycleTestHook = func(operation, gotID string) {
		if operation != "normalize-write" || gotID != id {
			return
		}
		close(reachedWrite)
		<-releaseWrite
	}

	type normalizeResult struct {
		nzb     *storage.NZB
		changed bool
		err     error
	}
	normalizeDone := make(chan normalizeResult, 1)
	go func() {
		normalized, changed, err := u.NormalizeNZBFileSizes(id)
		normalizeDone <- normalizeResult{nzb: normalized, changed: changed, err: err}
	}()
	waitLifecycleSignal(t, reachedWrite, "normalization to reach its write")

	failureDone := make(chan error, 1)
	go func() {
		failureDone <- u.recordPermanentArticleFailure(id, filename, errors.New("430 no such article"))
	}()
	waitLifecycleRefs(t, store, id, 2)
	select {
	case err := <-failureDone:
		t.Fatalf("permanent failure passed an in-flight normalization: %v", err)
	default:
	}

	close(releaseWrite)
	var normalized normalizeResult
	select {
	case normalized = <-normalizeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for normalization")
	}
	if normalized.err != nil || !normalized.changed {
		t.Fatalf("NormalizeNZBFileSizes = changed %v, err %v", normalized.changed, normalized.err)
	}
	if normalized.nzb == nil || normalized.nzb.TotalSize != 6 || normalized.nzb.Files[0].Size != 4 {
		t.Fatalf("normalized metadata = %#v; want total 6 and first file size 4", normalized.nzb)
	}
	if err := waitLifecycleError(t, failureDone, "permanent failure completion"); err == nil {
		t.Fatal("recordPermanentArticleFailure returned nil")
	}

	stored, err := store.GetNZB(id)
	if err != nil {
		t.Fatalf("GetNZB: %v", err)
	}
	if stored.TotalSize != 6 || stored.Files[0].Size != 4 {
		t.Fatalf("persisted normalized sizes = total %d, file %d; want 6, 4", stored.TotalSize, stored.Files[0].Size)
	}
	if !stored.Files[0].IsDeleted || !stored.IsBad || stored.Status != NZBStatusFailed || !strings.Contains(stored.FailMessage, "430 no such article") {
		t.Fatalf("concurrent permanent failure was lost: deleted=%v bad=%v status=%q reason=%q", stored.Files[0].IsDeleted, stored.IsBad, stored.Status, stored.FailMessage)
	}
	if _, ok := u.preparedSizes.Load(otherKey); ok {
		t.Fatal("normalization left an unrelated prepared-size cache entry")
	}
	if _, ok := u.failedFiles.Load(otherKey); ok {
		t.Fatal("normalization left an unrelated failure cache entry")
	}
}

func TestNormalizeNZBFileSizesUsesOffsetlessSegmentBytes(t *testing.T) {
	store := newTestNZBStorage(t)
	const id = "normalize-offsetless"
	nzb := &storage.NZB{
		ID:        id,
		TotalSize: 99,
		Files: []storage.NZBFile{{
			Name: "movie.mkv",
			Size: 99,
			Segments: []storage.NZBSegment{
				{Number: 1, MessageID: "one", Bytes: 3},
				{Number: 2, MessageID: "two", Bytes: 4},
			},
		}},
	}
	if err := store.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}

	normalized, changed, err := newTestUsenet(store).NormalizeNZBFileSizes(id)
	if err != nil {
		t.Fatalf("NormalizeNZBFileSizes: %v", err)
	}
	if !changed || normalized.Files[0].Size != 7 || normalized.TotalSize != 7 {
		t.Fatalf("normalized = changed %v, file %d, total %d; want true, 7, 7", changed, normalized.Files[0].Size, normalized.TotalSize)
	}
}

func TestNZBGenerationRoundTripsV2AndLegacyProto(t *testing.T) {
	nzb := lifecycleTestNZB("generation-codec", "movie.mkv", 7)
	nzb.Generation = "queue-generation-1"

	v2, err := encodeNZBV2(nzb)
	if err != nil {
		t.Fatalf("encodeNZBV2: %v", err)
	}
	decoded, err := decodeNZBV2(v2)
	if err != nil {
		t.Fatalf("decodeNZBV2: %v", err)
	}
	if decoded.Generation != nzb.Generation {
		t.Fatalf("v2 generation = %q; want %q", decoded.Generation, nzb.Generation)
	}
	header, err := decodeNZBV2Header(v2)
	if err != nil {
		t.Fatalf("decodeNZBV2Header: %v", err)
	}
	if header.Generation != nzb.Generation {
		t.Fatalf("v2 header generation = %q; want %q", header.Generation, nzb.Generation)
	}

	legacyBytes, err := proto.Marshal(nzbToProto(nzb))
	if err != nil {
		t.Fatalf("marshal legacy proto: %v", err)
	}
	legacy, err := decodeNZB(legacyBytes)
	if err != nil {
		t.Fatalf("decode legacy proto: %v", err)
	}
	if legacy.Generation != nzb.Generation {
		t.Fatalf("legacy generation = %q; want %q", legacy.Generation, nzb.Generation)
	}

	// The generation trailer is optional so metadata emitted by the pre-fence
	// v2 encoder remains readable.
	oldHeader := encodeHeader(&storage.NZB{ID: "old-v2"})
	oldHeader = oldHeader[:len(oldHeader)-1] // remove empty generation span
	oldDecoded, _, err := decodeHeader(oldHeader)
	if err != nil {
		t.Fatalf("decode pre-generation v2 header: %v", err)
	}
	if oldDecoded.Generation != "" {
		t.Fatalf("pre-generation header unexpectedly has generation %q", oldDecoded.Generation)
	}
}

func TestLegacyGenerationAdoptionIsAtomicAndExact(t *testing.T) {
	store := newTestNZBStorage(t)
	legacy := lifecycleTestNZB("legacy-generation-adoption", "movie.mkv", 4)
	data, err := encodeNZBV2(legacy)
	if err != nil {
		t.Fatalf("encode legacy metadata: %v", err)
	}
	// Strip the optional empty-generation trailer before writing the fixture.
	hc, sc, mc, err := splitRegions(data)
	if err != nil {
		t.Fatalf("splitRegions: %v", err)
	}
	header, err := zstdDec.DecodeAll(hc, nil)
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	header = header[:len(header)-1]
	hc = zstdEnc.EncodeAll(header, nil)
	oldBlob := []byte{codecMagicV2}
	oldBlob = binary.AppendUvarint(oldBlob, uint64(len(hc)))
	oldBlob = append(oldBlob, hc...)
	oldBlob = binary.AppendUvarint(oldBlob, uint64(len(sc)))
	oldBlob = append(oldBlob, sc...)
	oldBlob = append(oldBlob, mc...)
	if err := os.WriteFile(store.metaFilePath(legacy.ID), oldBlob, 0644); err != nil {
		t.Fatalf("write legacy fixture: %v", err)
	}

	if err := store.AssertGeneration(legacy.ID, "owner-a"); err != nil {
		t.Fatalf("first adoption: %v", err)
	}
	current, err := store.GetNZBHeader(legacy.ID)
	if err != nil {
		t.Fatalf("GetNZBHeader: %v", err)
	}
	if current.Generation != "owner-a" {
		t.Fatalf("adopted generation = %q; want owner-a", current.Generation)
	}
	if err := store.AssertGeneration(legacy.ID, "owner-b"); !errors.Is(err, ErrStaleNZBGeneration) {
		t.Fatalf("competing adoption error = %v; want ErrStaleNZBGeneration", err)
	}
}

func TestMissingNZBErrorsWrapSentinel(t *testing.T) {
	store := newTestNZBStorage(t)
	for operation, call := range map[string]func() error{
		"GetNZB": func() error {
			_, err := store.GetNZB("missing")
			return err
		},
		"GetNZBHeader": func() error {
			_, err := store.GetNZBHeader("missing")
			return err
		},
		"DeleteNZB": func() error { return store.DeleteNZB("missing") },
	} {
		if err := call(); !errors.Is(err, ErrNZBNotFound) {
			t.Fatalf("%s error = %v; want ErrNZBNotFound", operation, err)
		}
	}
}

func TestStaleNZBGenerationCannotPoisonReplacement(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id       = "persisted-generation-aba"
		filename = "movie.mkv"
	)
	old := lifecycleTestNZB(id, filename, 4)
	if err := store.AddNZB(old); err != nil {
		t.Fatalf("AddNZB old: %v", err)
	}
	oldGeneration := old.Generation
	if oldGeneration == "" {
		t.Fatal("old NZB was not assigned a generation")
	}
	staleParserSnapshot := *old

	if err := store.DeleteNZB(id); err != nil {
		t.Fatalf("DeleteNZB old: %v", err)
	}
	replacement := lifecycleTestNZB(id, filename, 8)
	replacement.Status = NZBStatusPending
	if err := store.AddNZB(replacement); err != nil {
		t.Fatalf("AddNZB replacement: %v", err)
	}
	if replacement.Generation == "" || replacement.Generation == oldGeneration {
		t.Fatalf("replacement generation = %q; old = %q", replacement.Generation, oldGeneration)
	}

	u := newTestUsenet(store)
	u.metadataDir = t.TempDir()
	if err := u.markAsCompleted(&staleParserSnapshot); !errors.Is(err, ErrStaleNZBGeneration) {
		t.Fatalf("stale completion error = %v; want ErrStaleNZBGeneration", err)
	}
	if err := u.markAsFailed(&staleParserSnapshot, errors.New("old parser failed")); !errors.Is(err, ErrStaleNZBGeneration) {
		t.Fatalf("stale failure error = %v; want ErrStaleNZBGeneration", err)
	}
	if err := u.recordPermanentArticleFailureForGeneration(id, oldGeneration, filename, errors.New("430 old stream")); !errors.Is(err, ErrStaleNZBGeneration) {
		t.Fatalf("stale stream failure error = %v; want ErrStaleNZBGeneration", err)
	}
	if _, err := u.PrepareStreamForGeneration(id, oldGeneration, filename); !errors.Is(err, ErrStaleNZBGeneration) {
		t.Fatalf("stale prepare error = %v; want ErrStaleNZBGeneration", err)
	}
	if _, _, err := u.ParseWithGeneration(context.Background(), id, oldGeneration, "old.nzb", []byte("stale source"), "tv"); !errors.Is(err, ErrStaleNZBGeneration) {
		t.Fatalf("stale ParseWithGeneration error = %v; want ErrStaleNZBGeneration", err)
	}
	if err := u.DeleteForGeneration(id, oldGeneration); !errors.Is(err, ErrStaleNZBGeneration) {
		t.Fatalf("stale delete error = %v; want ErrStaleNZBGeneration", err)
	}

	current, err := store.GetNZB(id)
	if err != nil {
		t.Fatalf("GetNZB replacement: %v", err)
	}
	if current.Generation != replacement.Generation || current.TotalSize != 8 || current.IsBad || current.Status != NZBStatusPending || current.FailMessage != "" {
		t.Fatalf("replacement was changed by stale work: generation=%q size=%d bad=%v status=%q failure=%q", current.Generation, current.TotalSize, current.IsBad, current.Status, current.FailMessage)
	}
	key := fsKey(id, filename)
	if _, ok := u.failedFiles.Load(key); ok {
		t.Fatal("stale stream failure poisoned replacement hot cache")
	}
}

func TestDeleteForGenerationIsIdempotentWhenAlreadyAbsent(t *testing.T) {
	store := newTestNZBStorage(t)
	u := newTestUsenet(store)
	if err := u.DeleteForGeneration("already-absent", "owned-generation"); err != nil {
		t.Fatalf("DeleteForGeneration absent: %v", err)
	}
}

func TestDeleteForGenerationRemovesOnlyExactTokenArtifacts(t *testing.T) {
	for _, metadataPresent := range []bool{false, true} {
		name := "metadata absent"
		if metadataPresent {
			name = "metadata present"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestNZBStorage(t)
			u := newTestUsenet(store)
			u.metadataDir = t.TempDir()
			const (
				id                    = "exact-token-delete"
				ownedGeneration       = "owned-generation"
				replacementGeneration = "replacement-generation"
			)

			ownedQueued, err := u.StageNZBForGeneration(id, ownedGeneration, []byte("owned queued"))
			if err != nil {
				t.Fatalf("stage owned generation: %v", err)
			}
			ownedSource, err := u.saveNZBFile(id, ownedGeneration, []byte("owned source"))
			if err != nil {
				t.Fatalf("save owned source: %v", err)
			}
			ownedArtifacts := []string{ownedQueued, ownedSource, ownedQueued + ".processing", ownedSource + ".failed"}
			for _, path := range ownedArtifacts[2:] {
				if err := os.WriteFile(path, []byte("marker"), 0o644); err != nil {
					t.Fatalf("write owned marker %s: %v", path, err)
				}
			}

			replacementQueued, err := u.StageNZBForGeneration(id, replacementGeneration, []byte("replacement queued"))
			if err != nil {
				t.Fatalf("stage replacement generation: %v", err)
			}
			replacementSource, err := u.saveNZBFile(id, replacementGeneration, []byte("replacement source"))
			if err != nil {
				t.Fatalf("save replacement source: %v", err)
			}

			if metadataPresent {
				if err := store.AddNZB(&storage.NZB{ID: id, Name: "owned.nzb", Path: ownedSource, Generation: ownedGeneration}); err != nil {
					t.Fatalf("AddNZB owned generation: %v", err)
				}
			}
			if err := u.DeleteForGeneration(id, ownedGeneration); err != nil {
				t.Fatalf("DeleteForGeneration: %v", err)
			}

			for _, path := range ownedArtifacts {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("owned artifact %s still exists; stat error = %v", path, err)
				}
			}
			for path, want := range map[string]string{
				replacementQueued: "replacement queued",
				replacementSource: "replacement source",
			} {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("replacement artifact %s was removed: %v", path, err)
				}
				if string(content) != want {
					t.Fatalf("replacement artifact %s = %q; want %q", path, content, want)
				}
			}
		})
	}
}

func TestGenerationQualifiedStagingCleanupCannotRemoveReplacement(t *testing.T) {
	u := newTestUsenet(newTestNZBStorage(t))
	u.metadataDir = t.TempDir()
	oldPath, err := u.StageNZBForGeneration("same-id", "old-generation", []byte("old"))
	if err != nil {
		t.Fatalf("stage old: %v", err)
	}
	newPath, err := u.StageNZBForGeneration("same-id", "new-generation", []byte("new"))
	if err != nil {
		t.Fatalf("stage replacement: %v", err)
	}
	if oldPath == newPath {
		t.Fatalf("different generations shared staged path %q", oldPath)
	}
	u.RemoveStagedNZB(oldPath)
	content, err := os.ReadFile(newPath)
	if err != nil {
		t.Fatalf("replacement staged source removed by stale cleanup: %v", err)
	}
	if string(content) != "new" {
		t.Fatalf("replacement staged content = %q; want new", content)
	}
	managedPath, err := u.saveNZBFile("same-id", "managed-generation", []byte("managed"))
	if err != nil {
		t.Fatalf("save managed source: %v", err)
	}
	pending, err := u.ClaimNewNZBs()
	if err != nil {
		t.Fatalf("ClaimNewNZBs: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("watcher claimed %d managed sources; want 0", len(pending))
	}
	if _, err := os.Stat(managedPath); err != nil {
		t.Fatalf("watcher moved managed source: %v", err)
	}
}

func TestCleanupOrphanedStagedNZBsPreservesLiveAndUnmanagedFiles(t *testing.T) {
	u := newTestUsenet(newTestNZBStorage(t))
	u.metadataDir = t.TempDir()
	orphanPath, err := u.StageNZBForGeneration("orphan", "orphan-generation", []byte("orphan"))
	if err != nil {
		t.Fatalf("stage orphan: %v", err)
	}
	livePath, err := u.StageNZBForGeneration("live", "live-generation", []byte("live"))
	if err != nil {
		t.Fatalf("stage live: %v", err)
	}
	sourcePath, err := u.saveNZBFile("source", "source-generation", []byte("source"))
	if err != nil {
		t.Fatalf("save managed source: %v", err)
	}
	unmanagedPath := filepath.Join(u.metadataDir, "manual.queued")
	if err := os.WriteFile(unmanagedPath, []byte("manual"), 0o644); err != nil {
		t.Fatalf("write unmanaged queued file: %v", err)
	}

	removed, err := u.CleanupOrphanedStagedNZBs([]string{filepath.Clean(livePath)})
	if err != nil {
		t.Fatalf("CleanupOrphanedStagedNZBs: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed staged files = %d; want 1", removed)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("orphan staged file still exists; stat error = %v", err)
	}
	for _, path := range []string{livePath, sourcePath, unmanagedPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("live/unmanaged artifact %s was removed: %v", path, err)
		}
	}
}

func TestConditionalParseHoldsLifecycleFenceUntilItReturns(t *testing.T) {
	store := newTestNZBStorage(t)
	const id = "parse-delete-fence"
	nzb := lifecycleTestNZB(id, "movie.mkv", 4)
	if err := store.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	u := newTestUsenet(store)

	checked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	u.lifecycleTestHook = func(operation, gotID string) {
		if operation == "parse-generation-checked" && gotID == id {
			once.Do(func() { close(checked) })
			<-release
		}
	}
	parseDone := make(chan error, 1)
	go func() {
		_, _, err := u.ParseWithGeneration(context.Background(), id, nzb.Generation, "movie.nzb", []byte("invalid after fence"), "tv")
		parseDone <- err
	}()
	waitLifecycleSignal(t, checked, "conditional parse generation check")

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- u.DeleteForGeneration(id, nzb.Generation) }()
	waitLifecycleRefs(t, store, id, 2)
	select {
	case err := <-deleteDone:
		t.Fatalf("delete passed in-flight conditional parse: %v", err)
	default:
	}
	close(release)
	if err := waitLifecycleError(t, parseDone, "conditional parse return"); err == nil {
		t.Fatal("invalid parse unexpectedly succeeded")
	}
	if err := waitLifecycleError(t, deleteDone, "delete after conditional parse"); err != nil {
		t.Fatalf("DeleteForGeneration: %v", err)
	}
	if _, err := store.GetNZB(id); err == nil {
		t.Fatal("conditional parse resurrected metadata after delete")
	}
}

func TestGenerationReadyStreamRejectsReplacementAfterReaderAcquisition(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id       = "ready-stream-generation"
		filename = "movie.mkv"
	)
	old := lifecycleTestNZB(id, filename, 4)
	if err := store.AddNZB(old); err != nil {
		t.Fatalf("AddNZB old: %v", err)
	}
	u := newTestUsenet(store)
	u.fs = xsync.NewMap[string, *fsEntry]()
	entry := &fsEntry{generation: old.Generation}
	entry.readerOnce.Do(func() {
		entry.reader = staticPrefetchReader("old!")
		entry.readerSize = 4
	})
	u.fs.Store(fsKey(id, filename), entry)

	acquired := make(chan struct{})
	release := make(chan struct{})
	var hookOnce sync.Once
	u.lifecycleTestHook = func(operation, gotID string) {
		if operation == "stream-reader-acquired" && gotID == id {
			hookOnce.Do(func() { close(acquired) })
			<-release
		}
	}
	var output bytes.Buffer
	callbackCalled := make(chan struct{}, 1)
	streamDone := make(chan error, 1)
	go func() {
		streamDone <- u.StreamForGenerationReady(context.Background(), id, old.Generation, filename, 0, 3, &output, func(StreamReadyInfo) error {
			callbackCalled <- struct{}{}
			return nil
		})
	}()
	waitLifecycleSignal(t, acquired, "stream reader acquisition")

	if err := u.DeleteForGeneration(id, old.Generation); err != nil {
		t.Fatalf("DeleteForGeneration old: %v", err)
	}
	replacement := lifecycleTestNZB(id, filename, 8)
	if err := store.AddNZB(replacement); err != nil {
		t.Fatalf("AddNZB replacement: %v", err)
	}
	close(release)
	err := waitLifecycleError(t, streamDone, "stale ready stream return")
	if !errors.Is(err, ErrStaleNZBGeneration) {
		t.Fatalf("ready stream error = %v; want ErrStaleNZBGeneration", err)
	}
	select {
	case <-callbackCalled:
		t.Fatal("readiness callback ran after acquired generation was replaced")
	default:
	}
	if output.Len() != 0 {
		t.Fatalf("stale ready stream wrote %d bytes before rejection", output.Len())
	}
}

func TestGenerationReadyStreamReportsExactRetainedReader(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id       = "ready-stream-size"
		filename = "movie.mkv"
	)
	nzb := lifecycleTestNZB(id, filename, 9) // advertised metadata differs from reader
	if err := store.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	u := newTestUsenet(store)
	u.fs = xsync.NewMap[string, *fsEntry]()
	entry := &fsEntry{generation: nzb.Generation}
	entry.readerOnce.Do(func() {
		entry.reader = staticPrefetchReader("exact")
		entry.readerSize = 5
	})
	u.fs.Store(fsKey(id, filename), entry)

	var got StreamReadyInfo
	var output bytes.Buffer
	err := u.StreamForGenerationReady(context.Background(), id, nzb.Generation, filename, 0, 8, &output, func(info StreamReadyInfo) error {
		got = info
		return nil
	})
	if err != nil {
		t.Fatalf("StreamForGenerationReady: %v", err)
	}
	if got != (StreamReadyInfo{Size: 5, Start: 0, End: 4}) {
		t.Fatalf("ready info = %+v; want size=5 range=0-4", got)
	}
	if output.String() != "exact" {
		t.Fatalf("streamed bytes = %q; want exact", output.String())
	}
}

func TestAvailabilityGatePropagatesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	nzb := lifecycleTestNZB("cancelled-availability", "movie.mkv", 4)
	err := newTestUsenet(newTestNZBStorage(t)).checkNZBAvailability(ctx, nzb)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("availability cancellation error = %v; want context.Canceled", err)
	}
}

func lifecycleTestNZB(id, filename string, size int64) *storage.NZB {
	return &storage.NZB{
		ID:        id,
		TotalSize: size,
		Files: []storage.NZBFile{{
			Name: filename,
			Size: size,
			Segments: []storage.NZBSegment{{
				Number:    1,
				MessageID: id + "-segment",
				Bytes:     size,
			}},
		}},
	}
}

func waitLifecycleRefs(t *testing.T, store *NZBStorage, id string, want int) {
	t.Helper()
	waitLockSetRefs(t, &store.lifecycle, id, want)
}

func waitLockSetRefs(t *testing.T, locks *nzbLifecycleLockSet, id string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		locks.mu.Lock()
		entry := locks.entries[id]
		refs := 0
		if entry != nil {
			refs = entry.refs
		}
		locks.mu.Unlock()
		if refs >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for lifecycle refs for %s to reach %d", id, want)
}

func waitLifecycleSignal(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitLifecycleError(t *testing.T, ch <-chan error, description string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return nil
	}
}

func waitLifecycleResult(t *testing.T, ch <-chan struct {
	size int64
	err  error
}, description string) struct {
	size int64
	err  error
} {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return struct {
			size int64
			err  error
		}{}
	}
}

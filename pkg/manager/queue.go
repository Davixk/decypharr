package manager

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type ImportType string

const (
	ImportTypeQBit    ImportType = "qbit"
	ImportTypeAPI     ImportType = "api"
	ImportTypeSABnzbd ImportType = "sabnzbd"
	ImportTypeWatch   ImportType = "watch"
	ImportSwitcher    ImportType = "switcher"
)

type ImportRequest struct {
	Name              string                `json:"name"`
	NZBContent        []byte                `json:"-"`
	Id                string                `json:"id"`
	DownloadFolder    string                `json:"downloadFolder"`
	SelectedDebrid    string                `json:"debrid"`
	Magnet            *utils.Magnet         `json:"magnet"`
	Arr               *arr.Arr              `json:"arr"`
	Action            config.DownloadAction `json:"action"`
	DownloadUncached  *bool                 `json:"downloadUncached"`
	FallbackOnFailure bool                  `json:"fallbackOnFailure"`
	CallBackUrl       string                `json:"callBackUrl"`
	SkipMultiSeason   bool                  `json:"skip_multi_season"`

	Status      string    `json:"status"`
	CompletedAt time.Time `json:"completedAt"`
	Error       string    `json:"error,omitempty"`

	Type  ImportType `json:"type"`
	Async bool       `json:"async"`
}

func NewTorrentRequest(debrid string, downloadFolder string, magnet *utils.Magnet, arr *arr.Arr, action config.DownloadAction, downloadUncached *bool, callBackUrl string, importType ImportType, skipMultiSeason bool) *ImportRequest {

	return &ImportRequest{
		Id:                uuid.New().String(),
		Status:            "started",
		DownloadFolder:    downloadFolder,
		SelectedDebrid:    cmp.Or(arr.SelectedDebrid, debrid), // Use debrid from arr if available
		Magnet:            magnet,
		Arr:               arr,
		Action:            action,
		DownloadUncached:  downloadUncached,
		FallbackOnFailure: arr.FallbackOnFailure,
		CallBackUrl:       callBackUrl,
		Type:              importType,
		SkipMultiSeason:   skipMultiSeason,
	}
}

func NewNZBRequest(name, downloadFolder string, nzbContent []byte, arr *arr.Arr, action config.DownloadAction, callBackUrl string, importType ImportType, skipMultiSeason bool) *ImportRequest {
	return &ImportRequest{
		Name:            name,
		Id:              uuid.New().String(),
		Status:          "started",
		DownloadFolder:  downloadFolder,
		SelectedDebrid:  "usenet", // NZB imports always use usenet
		NZBContent:      nzbContent,
		Arr:             arr,
		Action:          action,
		CallBackUrl:     callBackUrl,
		Type:            importType,
		SkipMultiSeason: skipMultiSeason,
	}
}

type Queue struct {
	storage             *storage.Storage
	logger              zerolog.Logger
	removeStalledAfter  time.Duration
	lifecycleLocks      [64]sync.Mutex
	actionMu            sync.Mutex
	actionLeases        map[string]*queueActionLease
	deletionMu          sync.Mutex
	deletions           map[string]chan struct{}
	queueDeleteTestHook func(stage string)
	lifecycleTestHook   func(stage string)
	queueUpdateTestHook func(stage string)
}

type queueActionLease struct {
	snapshot *storage.Entry
	cancel   context.CancelFunc
	done     chan struct{}
	once     sync.Once
}

func newQueue(storage *storage.Storage, removeStalledAfterStr string) *Queue {
	q := &Queue{
		storage: storage,
		logger:  logger.New("queue"),
	}

	if removeStalledAfterStr != "" {
		removeStalledAfter, err := utils.ParseDuration(removeStalledAfterStr)
		if err == nil {
			q.removeStalledAfter = removeStalledAfter
		}
	}

	return q
}

func (q *Queue) Add(torrent *storage.Entry) error {
	unlock := q.lockLifecycleAfterDeletion(torrent.InfoHash)
	defer unlock()
	if q.storage.QueueExists(torrent.InfoHash) {
		q.reportInvisibleDuplicate(torrent.InfoHash)
		return fmt.Errorf("queue entry %s already exists", strings.ToLower(torrent.InfoHash))
	}
	return q.storage.AddQueue(torrent)
}

// reportInvisibleDuplicate checks whether the entry this Add was just refused
// for is actually visible to a listing.
//
// This is the moment the defect does its damage: the index says the infohash is
// present so the add is refused, while every listing an arr polls comes from a
// scan. If the scan cannot yield it, the arr can neither see the entry nor
// re-add it, and re-grabs it forever with nothing to show for it.
//
// Checking here rather than only on a timer removes the need to be lucky. A
// periodic sample against a possibly seconds-long event may simply never
// intersect one; a duplicate rejection is by definition the event happening, so
// this catches it deterministically at the point of harm. The cost is one scan
// of the queue store per refused duplicate — a rare path, and refusals are
// exactly the case worth paying for.
//
// Detection only: the refusal still stands. Whether an entry proven invisible
// should instead be replaced is a behavioural question, and not one to decide
// silently inside a diagnostic.
func (q *Queue) reportInvisibleDuplicate(infohash string) {
	diagnosis, err := q.storage.QueueKeyState(infohash)
	if err != nil || diagnosis == nil || !diagnosis.Poisoned {
		return
	}
	q.logger.Error().
		Str("infohash", diagnosis.InfoHash).
		Str("name", diagnosis.Name).
		Str("category", diagnosis.Category).
		Str("protocol", diagnosis.Protocol).
		Str("status", diagnosis.Status).
		Bool("direct_read_ok", diagnosis.DirectReadOK).
		Msg("Refused a duplicate for an entry no listing can show: the index resolves this infohash but a full scan does not yield it, so the caller can neither see nor re-add it")
}

func (q *Queue) GetTorrent(infohash string) (*storage.Entry, error) {
	return q.storage.GetQueued(infohash)
}

func (q *Queue) deleteEntryFiles(entry *storage.Entry) error {
	var errs []error
	if entry.IsNZB() && entry.Magnet != "" {
		if err := os.Remove(entry.Magnet); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("remove staged NZB %s: %w", entry.Magnet, err))
		}
	}
	downloadedPath := entry.DownloadPath()
	cleanDL := filepath.Clean(downloadedPath)
	cleanSave := filepath.Clean(entry.SavePath)
	if downloadedPath == "" || cleanDL == cleanSave ||
		cleanDL == "." || cleanDL == string(os.PathSeparator) ||
		!strings.HasPrefix(cleanDL+string(os.PathSeparator), cleanSave+string(os.PathSeparator)) {
		// An empty or collapsed Name makes DownloadPath() resolve to the category
		// SavePath itself (or a parent of it): filepath.Join(SavePath, "") and
		// Join(SavePath, ".") both clean back to SavePath. RemoveAll on that would
		// destroy every sibling entry's symlinks in the same category directory —
		// the confirmed downloads/radarr + downloads/sonarr data-loss incident.
		// Refuse and log loudly instead of deleting a shared directory.
		q.logger.Error().
			Str("path", downloadedPath).
			Str("save_path", entry.SavePath).
			Str("infohash", entry.InfoHash).
			Str("name", entry.Name).
			Msg("Refusing to delete download path at or above SavePath; removing it would destroy sibling entries")
		return errors.Join(errs...)
	}
	if err := os.RemoveAll(downloadedPath); err != nil {
		errs = append(errs, fmt.Errorf("remove downloaded path %s: %w", downloadedPath, err))
	}
	return errors.Join(errs...)
}

func (q *Queue) wrapCleanupWithFileDelete(cleanup func(t *storage.Entry) error) func(*storage.Entry) error {
	return func(entry *storage.Entry) error {
		fileErr := q.deleteEntryFiles(entry)
		if cleanup != nil {
			return errors.Join(fileErr, cleanup(entry))
		}
		return fileErr
	}
}

func (q *Queue) Delete(infohash string, cleanup func(t *storage.Entry) error) error {
	snapshot, err := q.storage.GetQueued(infohash)
	if err != nil {
		return err
	}
	q.runQueueDeleteTestHook("snapshot-loaded")
	unlock, finishDeletion := q.beginDeletion(infohash)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
		finishDeletion()
	}()
	entry, present, err := q.storage.TakeQueuedSnapshotWhere(snapshot, nil)
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("%w for queue entry %s", storage.ErrEntryNotFound, strings.ToLower(infohash))
	}
	q.cancelAction(infohash, entry)
	unlock()
	locked = false
	q.cancelAndWaitAction(infohash, entry)
	return q.wrapCleanupWithFileDelete(cleanup)(entry)
}

func (q *Queue) runQueueDeleteTestHook(stage string) {
	if q.queueDeleteTestHook != nil {
		q.queueDeleteTestHook(stage)
	}
}

// withDeletionBarrier serializes a direct main-entry deletion with queue
// admission. If a queued workflow exists, it is removed, cancelled, and fully
// joined before fn runs. The same-hash admission tombstone remains active
// through file/provider cleanup and the authoritative main-row deletion.
// validate runs after the admission tombstone is established but before any
// queue row is taken. fn receives nil when no queue row existed.
func (q *Queue) withDeletionBarrier(infohash string, validate func() error, fn func(*storage.Entry) error) error {
	unlock, finishDeletion := q.beginDeletion(infohash)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
		finishDeletion()
	}()

	if validate != nil {
		if err := validate(); err != nil {
			return err
		}
	}
	entry, present, err := q.storage.TakeQueued(infohash)
	if err != nil {
		return err
	}
	if present {
		q.cancelAction(infohash, entry)
	}
	unlock()
	locked = false
	if present {
		q.cancelAndWaitAction(infohash, entry)
	}

	var fileErr error
	if present {
		fileErr = q.deleteEntryFiles(entry)
	}
	if fn == nil {
		return fileErr
	}
	return errors.Join(fileErr, fn(entry))
}

// DeleteCurrent is the workflow-owned deletion path. It cannot remove a row
// that was deleted and re-added after the retained snapshot was created.
func (q *Queue) DeleteCurrent(snapshot *storage.Entry, cleanup func(t *storage.Entry) error) (bool, error) {
	return q.deleteCurrentWhere(snapshot, nil, cleanup)
}

func (q *Queue) deleteCurrentWhere(snapshot *storage.Entry, predicate func(*storage.Entry) bool, cleanup func(t *storage.Entry) error) (bool, error) {
	if snapshot == nil {
		return false, fmt.Errorf("queue snapshot is nil")
	}
	unlock, finishDeletion := q.beginDeletion(snapshot.InfoHash)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
		finishDeletion()
	}()
	entry, taken, err := q.storage.TakeQueuedSnapshotWhere(snapshot, predicate)
	if err != nil || !taken {
		return false, err
	}
	q.cancelAction(snapshot.InfoHash, entry)
	unlock()
	locked = false
	q.cancelAndWaitAction(snapshot.InfoHash, entry)
	if err := q.wrapCleanupWithFileDelete(cleanup)(entry); err != nil {
		return true, err
	}
	return true, nil
}

func (q *Queue) DeleteWhere(category string, protocol config.Protocol, state storage.TorrentState, hashes []string, cleanup func(t *storage.Entry) error) error {
	predicate := q.ListFilterFunc(category, protocol, state, hashes)
	entries, err := q.storage.FilterQueued(predicate)
	if err != nil {
		return err
	}
	var errs []error
	for _, entry := range entries {
		if _, err := q.deleteCurrentWhere(entry, predicate, cleanup); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (q *Queue) DeleteStalled() error {
	cutoff := time.Now().Add(-q.removeStalledAfter)
	predicate := func(t *storage.Entry) bool {
		if t.IsDownloading {
			return false
		}
		if !t.AddedOn.Before(cutoff) {
			return false
		}
		if t.Status == debridTypes.TorrentStatusQueued {
			return false
		}
		// Torrent entries: not downloading, no seeders, no progress
		if t.Status != debridTypes.TorrentStatusDownloading && t.Seeders == 0 && t.Progress == 0 {
			return true
		}
		// NZB entries stuck in error state with no progress
		if t.State == storage.EntryStateError && t.Progress == 0 {
			return true
		}
		return false
	}
	entries, err := q.storage.FilterQueued(predicate)
	if err != nil {
		return err
	}
	var errs []error
	for _, entry := range entries {
		if _, err := q.deleteCurrentWhere(entry, predicate, nil); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (q *Queue) BeginAction(parent context.Context, snapshot *storage.Entry) (context.Context, func(), error) {
	if snapshot == nil {
		return nil, nil, fmt.Errorf("queue snapshot is nil")
	}
	unlock := q.lockLifecycle(snapshot.InfoHash)
	defer unlock()
	current, err := q.RefreshSnapshot(snapshot)
	if err != nil {
		return nil, nil, err
	}
	if !current {
		return nil, nil, fmt.Errorf("%w for queued action %s", storage.ErrStaleEntryGeneration, snapshot.InfoHash)
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	key := strings.ToLower(snapshot.InfoHash)
	leaseSnapshot := *snapshot
	lease := &queueActionLease{snapshot: &leaseSnapshot, cancel: cancel, done: make(chan struct{})}
	q.actionMu.Lock()
	if q.actionLeases == nil {
		q.actionLeases = make(map[string]*queueActionLease)
	}
	if _, exists := q.actionLeases[key]; exists {
		q.actionMu.Unlock()
		cancel()
		return nil, nil, fmt.Errorf("queued action already active for %s", snapshot.InfoHash)
	}
	q.actionLeases[key] = lease
	q.actionMu.Unlock()

	release := func() {
		q.finalizeActionLease(key, lease)
	}
	return ctx, release, nil
}

// CompleteAction atomically commits successful completion before running any
// external side effects. The lifecycle guard protects only the authoritative
// queue transition; notifications and Arr refreshes may block and must not
// prevent another lifecycle operation from observing the committed state.
func (q *Queue) CompleteAction(snapshot *storage.Entry, contentPath string, onCompleted func(*storage.Entry)) error {
	if snapshot == nil {
		return fmt.Errorf("queue snapshot is nil")
	}
	err := func() error {
		unlock := q.lockLifecycle(snapshot.InfoHash)
		defer unlock()

		updated, present, err := q.storage.MutateQueuedSnapshot(snapshot, func(current *storage.Entry) (bool, error) {
			if current.State != storage.EntryStateDownloading || current.Bad {
				return false, fmt.Errorf("queued action is no longer active for %s", current.InfoHash)
			}
			mergeWorkflowSnapshot(current, snapshot)
			if current.State != storage.EntryStateDownloading || current.Bad {
				return false, fmt.Errorf("queued action became terminal for %s", current.InfoHash)
			}
			current.MarkAsCompleted(contentPath)
			return true, nil
		})
		if err != nil {
			return err
		}
		if !present || updated == nil {
			return fmt.Errorf("%w for queue entry %s", storage.ErrEntryNotFound, strings.ToLower(snapshot.InfoHash))
		}
		*snapshot = *updated
		return nil
	}()
	if err != nil {
		return err
	}
	q.finalizeMatchingAction(snapshot.InfoHash, snapshot)
	if onCompleted != nil {
		onCompleted(snapshot)
	}
	return nil
}

func (q *Queue) withLifecycle(infohash string, fn func() error) error {
	unlock := q.lockLifecycle(infohash)
	defer unlock()
	if fn == nil {
		return nil
	}
	return fn()
}

// mutateTerminalLocked must be called while the queue lifecycle guard for the
// hash is held. It commits the terminal row before cancelling the action so a
// worker that wakes up can only observe the terminal state.
func (q *Queue) mutateTerminalLocked(infohash string, update func(*storage.Entry) bool) (*storage.Entry, bool, error) {
	updated, present, err := q.storage.MutateQueuedIfPresent(infohash, func(current *storage.Entry) (bool, error) {
		if update == nil {
			return false, nil
		}
		return update(current), nil
	})
	if err != nil || !present || updated == nil {
		return updated, present, err
	}
	if updated.State == storage.EntryStateError || updated.Bad {
		q.cancelAction(infohash, updated)
	}
	return updated, true, nil
}

// queueActionJoinTimeout bounds how long a deletion waits for an in-flight
// worker to observe cancellation before proceeding without it. Deletion paths
// run synchronously inside HTTP handlers (arr/WebUI DELETE), so an unbounded
// join lets any worker that ignores its context hang the request forever.
// Overridable in tests.
var queueActionJoinTimeout = 30 * time.Second

func (q *Queue) cancelAndWaitAction(infohash string, expected *storage.Entry) {
	key := strings.ToLower(infohash)
	q.actionMu.Lock()
	lease := q.actionLeases[key]
	if lease != nil && expected != nil && !storage.SameQueueGeneration(lease.snapshot, expected) {
		lease = nil
	}
	if lease != nil {
		lease.cancel()
	}
	q.actionMu.Unlock()
	if lease == nil {
		return
	}
	q.runLifecycleTestHook("action-wait-started")
	timer := time.NewTimer(queueActionJoinTimeout)
	defer timer.Stop()
	select {
	case <-lease.done:
	case <-timer.C:
		// The worker did not exit after cancellation. Proceed with the delete:
		// its queue row is already gone (or replaced), so every straggler write
		// fails the generation fence (ErrStaleEntryGeneration/ErrEntryNotFound)
		// and cannot resurrect state. Drop the stale lease from the table so a
		// same-hash replacement can begin its own action; the straggler's own
		// release still closes the lease exactly once.
		q.logger.Error().
			Str("infohash", key).
			Dur("timeout", queueActionJoinTimeout).
			Msg("Queued action ignored cancellation; proceeding with deletion without joining the worker")
		q.actionMu.Lock()
		if q.actionLeases[key] == lease {
			delete(q.actionLeases, key)
		}
		q.actionMu.Unlock()
	}
}

// HasActionLease reports whether a live action lease exists for the hash. The
// orphaned-claim reconciler uses it to distinguish a claimed entry whose
// action worker is alive (holding a BeginAction lease) from one whose
// goroutine died without committing a terminal state.
func (q *Queue) HasActionLease(infohash string) bool {
	key := strings.ToLower(infohash)
	q.actionMu.Lock()
	defer q.actionMu.Unlock()
	_, ok := q.actionLeases[key]
	return ok
}

func (q *Queue) cancelAction(infohash string, expected *storage.Entry) {
	key := strings.ToLower(infohash)
	q.actionMu.Lock()
	lease := q.actionLeases[key]
	if lease != nil && expected != nil && !storage.SameQueueGeneration(lease.snapshot, expected) {
		lease = nil
	}
	if lease != nil {
		lease.cancel()
	}
	q.actionMu.Unlock()
}

// finalizeMatchingAction ends the action lease for the committed generation.
// Successful completion owns the durable terminal transition, so post-complete
// side effects no longer need to keep deletion waiting on the worker lease.
func (q *Queue) finalizeMatchingAction(infohash string, expected *storage.Entry) {
	key := strings.ToLower(infohash)
	q.actionMu.Lock()
	lease := q.actionLeases[key]
	if lease != nil && expected != nil && !storage.SameQueueGeneration(lease.snapshot, expected) {
		lease = nil
	}
	q.actionMu.Unlock()
	q.finalizeActionLease(key, lease)
}

func (q *Queue) finalizeActionLease(key string, lease *queueActionLease) {
	if lease == nil {
		return
	}
	lease.once.Do(func() {
		lease.cancel()
		q.actionMu.Lock()
		if q.actionLeases[key] == lease {
			delete(q.actionLeases, key)
		}
		close(lease.done)
		q.actionMu.Unlock()
	})
}

func (q *Queue) lockLifecycle(infohash string) func() {
	key := strings.ToLower(infohash)
	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	lock := &q.lifecycleLocks[hash%uint32(len(q.lifecycleLocks))]
	lock.Lock()
	return lock.Unlock
}

func (q *Queue) lockLifecycleAfterDeletion(infohash string) func() {
	key := strings.ToLower(infohash)
	for {
		unlock := q.lockLifecycle(key)
		q.deletionMu.Lock()
		pending := q.deletions[key]
		q.deletionMu.Unlock()
		if pending == nil {
			return unlock
		}
		unlock()
		q.runLifecycleTestHook("deletion-observed")
		<-pending
	}
}

func (q *Queue) runLifecycleTestHook(stage string) {
	if q.lifecycleTestHook != nil {
		q.lifecycleTestHook(stage)
	}
}

func (q *Queue) beginDeletion(infohash string) (func(), func()) {
	key := strings.ToLower(infohash)
	for {
		unlock := q.lockLifecycle(key)
		q.deletionMu.Lock()
		pending := q.deletions[key]
		if pending == nil {
			if q.deletions == nil {
				q.deletions = make(map[string]chan struct{})
			}
			done := make(chan struct{})
			q.deletions[key] = done
			q.deletionMu.Unlock()
			finish := func() {
				finishUnlock := q.lockLifecycle(key)
				q.deletionMu.Lock()
				if q.deletions[key] == done {
					delete(q.deletions, key)
					close(done)
				}
				q.deletionMu.Unlock()
				finishUnlock()
			}
			return unlock, finish
		}
		q.deletionMu.Unlock()
		unlock()
		<-pending
	}
}

func (q *Queue) Update(torrent *storage.Entry) error {
	return q.updateSnapshot(torrent, mergeWorkflowSnapshot)
}

// UpdateNZBCompletion commits an authoritative NZB file list while preserving
// only same-file durable state from a concurrent queue mutation. Unlike the
// general workflow merge, names absent from the completion metadata must not
// be resurrected.
func (q *Queue) UpdateNZBCompletion(torrent *storage.Entry) error {
	if torrent == nil || !torrent.IsNZB() {
		return fmt.Errorf("NZB queue entry is required")
	}
	return q.updateSnapshot(torrent, mergeNZBCompletionSnapshot)
}

func (q *Queue) updateSnapshot(torrent *storage.Entry, merge func(current, incoming *storage.Entry)) error {
	updated, present, err := q.storage.MutateQueuedSnapshot(torrent, func(current *storage.Entry) (bool, error) {
		merge(current, torrent)
		return true, nil
	})
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("%w for queue entry %s", storage.ErrEntryNotFound, strings.ToLower(torrent.InfoHash))
	}
	// Refresh the long-lived job pointer to the merged authoritative revision.
	// A later workflow update can then observe user/mirror changes as well.
	*torrent = *updated
	if q.queueUpdateTestHook != nil {
		q.queueUpdateTestHook("persisted")
	}
	return nil
}

func preserveDurableNZBFileState(authoritative, durable *storage.File) {
	if authoritative == nil || durable == nil {
		return
	}
	authoritative.Path = durable.Path
	authoritative.Deleted = durable.Deleted
	if !durable.AddedOn.IsZero() {
		authoritative.AddedOn = durable.AddedOn
	}
	if durable.ByteRange == nil {
		authoritative.ByteRange = nil
	} else {
		byteRange := *durable.ByteRange
		authoritative.ByteRange = &byteRange
	}
}

func mergeNZBCompletionSnapshot(current, incoming *storage.Entry) {
	durableFiles := current.Files
	// mergeWorkflowSnapshot intentionally merges durable-only names into its
	// destination map. Snapshot the authoritative membership first because the
	// initial struct assignment aliases incoming.Files and would otherwise add
	// those obsolete names back into the caller's map as well.
	authoritativeFiles := make(map[string]*storage.File, len(incoming.Files))
	for name, incomingFile := range incoming.Files {
		if incomingFile == nil {
			continue
		}
		cloned := *incomingFile
		if incomingFile.ByteRange != nil {
			byteRange := *incomingFile.ByteRange
			cloned.ByteRange = &byteRange
		}
		authoritativeFiles[name] = &cloned
	}
	authoritativeSize := incoming.Size
	authoritativeBytes := incoming.Bytes
	mergeWorkflowSnapshot(current, incoming)

	files := make(map[string]*storage.File, len(authoritativeFiles))
	for name, incomingFile := range authoritativeFiles {
		authoritative := *incomingFile
		preserveDurableNZBFileState(&authoritative, durableFiles[name])
		files[name] = &authoritative
	}
	current.Files = files
	current.Size = authoritativeSize
	current.Bytes = authoritativeBytes
}

// Mutate atomically applies an external/user patch to the live queue row.
func (q *Queue) Mutate(infohash string, update func(*storage.Entry) bool) (*storage.Entry, error) {
	updated, present, err := q.storage.MutateQueuedIfPresent(infohash, func(current *storage.Entry) (bool, error) {
		if update == nil {
			return false, nil
		}
		return update(current), nil
	})
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, fmt.Errorf("%w for queue entry %s", storage.ErrEntryNotFound, strings.ToLower(infohash))
	}
	return updated, nil
}

func (q *Queue) IsCurrent(snapshot *storage.Entry) (bool, error) {
	return q.storage.IsCurrentQueuedSnapshot(snapshot)
}

// RefreshSnapshot replaces a retained job value with the authoritative live
// revision when it is still the same generation. A false result means the row
// was deleted/re-added (or removed), so the old job must stop before side
// effects.
func (q *Queue) RefreshSnapshot(snapshot *storage.Entry) (bool, error) {
	updated, present, err := q.storage.MutateQueuedSnapshot(snapshot, nil)
	if err != nil {
		if errors.Is(err, storage.ErrStaleEntryGeneration) {
			return false, nil
		}
		return false, err
	}
	if !present || updated == nil {
		return false, nil
	}
	*snapshot = *updated
	return true, nil
}

// ClaimPostDownload atomically grants one worker ownership of the post-download
// action for this queue generation. The durable IsDownloading flag prevents a
// scheduler tick or a second completion callback from running the same action.
func (q *Queue) ClaimPostDownload(snapshot *storage.Entry) (bool, error) {
	claimed := false
	updated, present, err := q.storage.MutateQueuedSnapshot(snapshot, func(current *storage.Entry) (bool, error) {
		if current.State != storage.EntryStateDownloading || current.IsDownloading {
			return false, nil
		}
		mergeWorkflowSnapshot(current, snapshot)
		current.Status = debridTypes.TorrentStatusDownloaded
		current.IsDownloading = true
		current.UpdatedAt = time.Now()
		claimed = true
		return true, nil
	})
	if err != nil {
		return false, err
	}
	if !present || updated == nil {
		return false, nil
	}
	*snapshot = *updated
	return claimed, nil
}

func mergeWorkflowSnapshot(current, incoming *storage.Entry) {
	// Category and tags can be edited through qBittorrent while a provider job
	// is running. Workflow snapshots must not roll those user changes back.
	category := current.Category
	tags := append([]string(nil), current.Tags...)

	// WebDAV may have durably corrected NZB sizes after the workflow retained
	// its snapshot. Preserve the authoritative per-file sizes and any files
	// already learned by another queue mutation.
	durableFiles := current.Files

	terminalFailure := current.Protocol == config.ProtocolNZB && current.State == storage.EntryStateError && current.Bad
	terminalState := current.State
	terminalStatus := current.Status
	terminalBad := current.Bad
	terminalDownloading := current.IsDownloading
	terminalLastError := current.LastError
	terminalErrorCount := current.ErrorCount
	terminalLastErrorTime := current.LastErrorTime

	*current = *incoming
	current.Category = category
	current.Tags = tags

	if current.Protocol == config.ProtocolNZB && len(durableFiles) > 0 {
		if current.Files == nil {
			current.Files = make(map[string]*storage.File, len(durableFiles))
		}
		for name, durable := range durableFiles {
			if durable == nil {
				continue
			}
			if workflowFile, ok := current.Files[name]; ok && workflowFile != nil {
				merged := *workflowFile
				merged.Size = durable.Size
				merged.Deleted = merged.Deleted || durable.Deleted
				current.Files[name] = &merged
			} else {
				preserved := *durable
				current.Files[name] = &preserved
			}
		}
		var total int64
		for _, file := range current.Files {
			if file != nil {
				total += file.Size
			}
		}
		current.Size = total
		current.Bytes = total
	}

	if terminalFailure {
		current.State = terminalState
		current.Status = terminalStatus
		current.Bad = terminalBad
		current.IsDownloading = terminalDownloading
		current.LastError = terminalLastError
		current.ErrorCount = terminalErrorCount
		current.LastErrorTime = terminalLastErrorTime
	}
}

func (q *Queue) ListFilterFunc(category string, protocol config.Protocol, state storage.TorrentState, hashes []string) func(*storage.Entry) bool {
	hashSet := make(map[string]struct{}, len(hashes))
	allHashes := len(hashes) == 1 && strings.EqualFold(strings.TrimSpace(hashes[0]), "all")
	if len(hashes) > 0 && !allHashes {
		for _, h := range hashes {
			hashSet[strings.ToLower(h)] = struct{}{}
		}
	}

	var filterFunc func(*storage.Entry) bool
	if category != "" || (len(hashes) != 0 && !allHashes) || state != "" || protocol != config.ProtocolAll {
		filterFunc = func(t *storage.Entry) bool {
			if category != "" && t.Category != category {
				return false
			}
			if state != "" && t.State != state {
				return false
			}
			if len(hashSet) > 0 {
				if _, ok := hashSet[strings.ToLower(t.InfoHash)]; !ok {
					return false
				}
			}
			if protocol != config.ProtocolAll && t.Protocol != protocol {
				return false
			}
			return true
		}
	}
	return filterFunc
}

func (q *Queue) ListFilter(category string, protocol config.Protocol, state storage.TorrentState, hashes []string, sortBy string, reverse bool) []*storage.Entry {
	filterFunc := q.ListFilterFunc(category, protocol, state, hashes)
	torrents, err := q.storage.FilterQueued(filterFunc)
	if err != nil {
		// return empty list on error
		return []*storage.Entry{}
	}

	if sortBy != "" {
		sort.Slice(torrents, func(i, j int) bool {
			// If ascending is false, swap i and j to get descending order
			if !reverse {
				i, j = j, i
			}

			switch sortBy {
			case "name":
				return torrents[i].Name < torrents[j].Name
			case "size":
				return torrents[i].Size < torrents[j].Size
			case "added_on":
				return torrents[i].AddedOn.Before(torrents[j].AddedOn)
			case "completed", "downloaded":
				return torrents[i].CompletedAt.Before(*torrents[j].CompletedAt)
			case "progress":
				return torrents[i].Progress < torrents[j].Progress
			case "category":
				return torrents[i].Category < torrents[j].Category
			case "seeders":
				return torrents[i].Seeders < torrents[j].Seeders
			default:
				// Default sort by added_on
				return torrents[i].AddedOn.Before(torrents[j].AddedOn)
			}
		})
	}
	return torrents
}

func (q *Queue) UpdateWhere(predicate func(*storage.Entry) bool, updateFunc func(*storage.Entry) bool) error {
	return q.storage.UpdateWhereQueued(predicate, updateFunc)
}

package manager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Fixer handles torrent repair with cascading re-insertion across debrids
type Fixer struct {
	manager *Manager

	// reinsertMu serializes MoveTorrent end to end.
	//
	// It exists to PRESERVE, not to add: re-insertions were already serialized,
	// because MoveTorrent used to hold the manager's global copyEntryMu for its
	// whole body. Narrowing that lock off the provider calls would otherwise
	// have silently changed how many magnets can be in flight against a provider
	// at once — an unrelated behaviour change smuggled into a deadlock fix.
	//
	// It is deliberately NOT the file-operation mutex. A provider poll stuck
	// here delays other repairs (and is itself bounded by
	// config.DebridStatusTimeout); it can no longer touch WebDAV
	// DELETE/COPY/MOVE.
	reinsertMu sync.Mutex

	// Track re-insertion attempts and failures
	failedToReinsert   *xsync.Map[string, struct{}]      // infohash:debrid -> failed completely
	inFlightRepairs    *xsync.Map[string, *FixerRequest] // infohash -> repair request
	providerOrder      []string                          // Order of providers to try (from config)
	maxReinsertRetries int
}

// FixerRequest tracks an ongoing repair operation
type FixerRequest struct {
	InfoHash         string
	CurrentDebrid    string
	AttemptedDebrids []string
	StartedAt        time.Time
	LastAttempt      time.Time
	result           chan *FixResult
}

// FixResult is the result of a fix operation
type FixResult struct {
	Success       bool
	NewDebrid     string
	Error         error
	AttemptsCount int
}

// NewFixer creates a new Fixer instance
func NewFixer(manager *Manager) *Fixer {
	// GetReader debrid order from config
	cfg := config.Get()
	debridOrder := make([]string, 0, len(cfg.Debrids))
	for _, d := range cfg.Debrids {
		debridOrder = append(debridOrder, d.Name)
	}

	return &Fixer{
		manager:            manager,
		failedToReinsert:   xsync.NewMap[string, struct{}](),
		inFlightRepairs:    xsync.NewMap[string, *FixerRequest](),
		providerOrder:      debridOrder,
		maxReinsertRetries: 2, // retry each debrid up to 2 times
	}
}

// FixTorrent attempts to fix a broken torrent by re-inserting across debrids
// Strategy:
// 1. Try to re-insert on current active debrid, except if skipCurrent is true
// 2. If fails, cascade through other debrids in config order
// 3. Skip debrids where torrent already exists (unless they're also broken)
// 4. Mark as completely failed if all debrids fail
func (f *Fixer) FixTorrent(ctx context.Context, entry *storage.Entry, skipCurrent bool) (*FixResult, error) {
	if entry == nil {
		return nil, fmt.Errorf("entry is nil")
	}
	if !entry.CanBeFixed() {
		return &FixResult{
			Success:       false,
			Error:         fmt.Errorf("entry %s cannot be fixed", entry.Name),
			AttemptsCount: 0,
		}, nil
	}
	// Check if repair is already in flight
	if req, exists := f.inFlightRepairs.Load(entry.InfoHash); exists {
		// Wait for existing repair to complete
		select {
		case result := <-req.result:
			return result, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Minute):
			return nil, fmt.Errorf("repair timeout for %s", entry.Name)
		}
	}

	// Create new repair request
	req := &FixerRequest{
		InfoHash:         entry.InfoHash,
		AttemptedDebrids: make([]string, 0),
		StartedAt:        time.Now(),
		LastAttempt:      time.Now(),
		result:           make(chan *FixResult, 1),
	}
	f.inFlightRepairs.Store(entry.InfoHash, req)
	defer f.inFlightRepairs.Delete(entry.InfoHash)
	req.CurrentDebrid = entry.ActiveProvider

	// Build debrid attempt order: current debrid first, then others in config order
	attemptOrder := f.buildAttemptOrder(entry, skipCurrent)

	var lastErr error
	totalAttempts := 0
	// inconclusive records that at least one provider was abandoned on
	// decypharr's OWN ceiling instead of answering. It is the difference between
	// "every debrid says this is broken" and "we stopped waiting", and only the
	// first of those may become a durable verdict below.
	inconclusive := false

	for _, debridName := range attemptOrder {
		// Check if entry has been marked as failed to re-insert
		if f.IsFailedToReinsert(entry.InfoHash, debridName) {
			continue
		}

		select {
		case <-ctx.Done():
			result := &FixResult{Success: false, Error: ctx.Err(), AttemptsCount: totalAttempts}
			req.result <- result
			return result, ctx.Err()
		default:
		}

		req.AttemptedDebrids = append(req.AttemptedDebrids, debridName)
		req.LastAttempt = time.Now()

		f.manager.logger.Trace().
			Str("debrid", debridName).
			Str("infohash", entry.InfoHash).
			Str("name", entry.Name).
			Int("attempt", totalAttempts+1).
			Msg("Attempting re-insertion")

		// Force a fresh submit only for the broken active provider; for any other
		// debrid, let MoveTorrent reuse an existing valid placement if present.
		reinsert := debridName == entry.ActiveProvider
		success, err := f.MoveTorrent(entry, debridName, reinsert)
		totalAttempts++

		if success {
			if err != nil {
				f.manager.logger.Warn().Err(err).Str("debrid", debridName).Str("infohash", entry.InfoHash).Msg("Provider ownership committed with cleanup warning")
			}
			f.manager.logger.Info().
				Str("debrid", debridName).
				Str("name", entry.Name).
				Str("infohash", entry.InfoHash).
				Msg("Successfully re-inserted entry")

			// Mark as successful
			f.ResetFailureState(entry.InfoHash)

			result := &FixResult{
				Success:       true,
				NewDebrid:     debridName,
				Error:         nil,
				AttemptsCount: totalAttempts,
			}
			req.result <- result
			return result, nil
		}

		lastErr = err
		if customerror.IsBackendTimeout(err) {
			// OUR ceiling fired; this provider never reached a verdict. The marker
			// below is effectively permanent — it is keyed "infohash:debrid" and
			// ResetFailureState only deletes the bare "infohash" key — so storing
			// it here would exclude a perfectly healthy debrid from ever repairing
			// this entry again for the lifetime of the process, on the strength of
			// one slow minute. Record the attempt as inconclusive and move on.
			inconclusive = true
			f.manager.logger.Warn().Err(err).
				Str("debrid", debridName).
				Str("infohash", entry.InfoHash).
				Msg("Re-insertion abandoned on its own ceiling; provider gave no verdict")
			continue
		}
		// Add failed state for this debrid
		f.failedToReinsert.Store(fmt.Sprintf("%s:%s", entry.InfoHash, debridName), struct{}{})
	}

	if inconclusive {
		// NOT A VERDICT, SO NOT A VERDICT ANYWHERE.
		//
		// Entry.Bad short-circuits every subsequent read of this entry before any
		// provider call (link.badEntryError), and nothing clears it except a
		// re-insertion that succeeds. Setting it because a provider was too slow
		// to answer would convert a backend flap into a library that serves
		// errors until somebody notices — the precise failure mode this codebase
		// has already been burned by, and the reason NewBackendTimeoutError is
		// retryable and explicitly not permanent.
		//
		// So: fail this attempt, say why, and leave the entry's health untouched.
		// The next sweep or the next read retries from the same state.
		f.manager.logger.Warn().
			Err(lastErr).
			Str("infohash", entry.InfoHash).
			Int("attempts", totalAttempts).
			Msg("Re-insertion inconclusive: a provider timed out rather than answering; entry left unmarked")

		result := &FixResult{
			Success:       false,
			Error:         customerror.NewBackendTimeoutError(fmt.Errorf("re-insertion of %s was inconclusive: %w", entry.InfoHash, lastErr)),
			AttemptsCount: totalAttempts,
		}
		req.result <- result
		return result, result.Error
	}

	// All debrids failed - mark as completely failed
	f.manager.logger.Error().
		Err(lastErr).
		Str("infohash", entry.InfoHash).
		Int("attempts", totalAttempts).
		Msg("All re-insertion attempts failed")

	f.failedToReinsert.Store(entry.InfoHash, struct{}{})

	// Mark only the Bad field on the matching lifecycle. A repair snapshot may
	// have drifted while remote calls were in flight; writing the whole value
	// here would erase terminal queue/user state.
	entry.Bad = true
	entry.UpdatedAt = time.Now()
	if persistErr := f.manager.persistLinkEntryBad(entry); persistErr != nil {
		f.manager.logger.Warn().Err(persistErr).Str("infohash", entry.InfoHash).Msg("Failed to persist repair failure state")
	}

	result := &FixResult{
		Success:       false,
		Error:         fmt.Errorf("all re-insertion attempts failed: %w", lastErr),
		AttemptsCount: totalAttempts,
	}
	req.result <- result
	return result, result.Error
}

// MoveTorrent attempts to re-insert a torrent on a specific debrid.
//
// NO PROVIDER CALL RUNS UNDER copyEntryMu.
//
// copyEntryMu is the manager's GLOBAL file-operation mutex: WebDAV DELETE, COPY
// and MOVE all take it, as does every provider-placement cleanup. This function
// used to hold it across SubmitMagnet AND CheckStatus — and CheckStatus is a
// re-poll loop against the provider. One account stuck in
// "waiting_files_selection" therefore froze every file operation in the process
// for as long as the provider stayed stuck, which was forever.
//
// That is the same rule the refresh path already states at torrent.go's
// adoption site ("Remote calls remain outside this mutex; the exact snapshot
// fence rejects a lifecycle that changed while the provider was queried"). This
// function was the one place that broke it. Bounding the poll alone would have
// left a global lock held across a network call — a shorter wedge, not an
// absent one — so the lock is narrowed here and the poll is bounded separately
// (config.DebridStatusTimeout), which are two different defects.
//
// What copyEntryMu is actually for is unchanged and still enforced below:
// folder COPY aliases can share a provider ID, so ownership COMMITS and
// last-reference CLEANUP must not interleave with alias creation/deletion.
// Those are local operations plus a bounded DELETE; they stay inside it.
//
// Correctness while the lock is released is carried by the generation fence
// that was already there: the commit uses MutateEntrySnapshot against the exact
// snapshot read in phase 1, so a delete/re-add or a competing move that lands
// mid-flight is rejected and the placement we created is compensated away
// instead of being adopted onto somebody else's row.
func (f *Fixer) MoveTorrent(entry *storage.Entry, debridName string, reinsert bool) (bool, error) {
	if entry == nil {
		return false, fmt.Errorf("entry is nil")
	}
	if !entry.CanBeMoved() {
		return false, fmt.Errorf("entry %s cannot be moved", entry.Name)
	}

	client := f.manager.ProviderClient(debridName)
	if client == nil {
		return false, fmt.Errorf("debrid client %s not found", debridName)
	}

	// reinsertMu keeps re-insertions serialized exactly as they were when they
	// ran under copyEntryMu. That serialization was incidental, not designed,
	// but it is the live behaviour of the shipped release and changing the
	// number of magnets in flight at once is a separate decision from fixing the
	// wedge. Preserving it here means this change has exactly one observable
	// effect: file operations no longer queue behind a provider poll.
	//
	// LOCK ORDER is always reinsertMu -> copyEntryMu, and nothing anywhere takes
	// them the other way round (reinsertMu is taken only here).
	f.reinsertMu.Lock()
	defer f.reinsertMu.Unlock()

	expected, oldTargetID, activated, err := f.prepareMove(entry, debridName, reinsert)
	if err != nil || activated {
		return activated, err
	}

	// Construct magnet
	magnet, err := utils.GetMagnetInfo(expected.Magnet, f.manager.config.AlwaysRmTrackerUrls)
	if err != nil {
		magnet = utils.ConstructMagnet(expected.InfoHash, expected.Name)
	}

	if magnet == nil {
		return false, fmt.Errorf("failed to construct magnet for entry %s", expected.Name)
	}
	if magnet.Link == "" {
		return false, fmt.Errorf("failed to construct magnet for entry %s", expected.Name)
	}

	// ---- PROVIDER PHASE. copyEntryMu is NOT held for any of this. ----

	newDebridTorrent := &types.Torrent{
		Name:             expected.Name,
		Magnet:           magnet,
		InfoHash:         expected.InfoHash,
		Size:             expected.Size,
		Files:            make(map[string]types.File),
		DownloadUncached: false,
	}

	newDebridTorrent, err = client.SubmitMagnet(newDebridTorrent)
	if err != nil {
		return false, fmt.Errorf("failed to submit magnet: %w", err)
	}

	if newDebridTorrent == nil || newDebridTorrent.Id == "" {
		return false, fmt.Errorf("failed to submit magnet: empty entry")
	}
	submittedID := newDebridTorrent.Id

	// Check status
	newDebridTorrent.DownloadUncached = false
	newDebridTorrent, statusErr := client.CheckStatus(newDebridTorrent)

	// ---- COMMIT PHASE. Local state plus bounded cleanup, under copyEntryMu. ----
	return f.commitMove(entry, expected, client, debridName, oldTargetID, submittedID, newDebridTorrent, statusErr)
}

// prepareMove performs the local half of a move under copyEntryMu: fence the
// caller's snapshot against the store, and take the cheap exit when the target
// provider already holds a completed placement. It returns activated=true when
// that exit was taken and the caller is done.
//
// Everything here is local. The lock is released before the caller makes any
// provider call — that is the entire point of the split.
func (f *Fixer) prepareMove(entry *storage.Entry, debridName string, reinsert bool) (expected *storage.Entry, oldTargetID string, activated bool, err error) {
	f.manager.copyEntryMu.Lock()
	defer f.manager.copyEntryMu.Unlock()

	expected, err = f.manager.storage.Get(entry.InfoHash)
	if err != nil {
		return nil, "", false, fmt.Errorf("load current entry %s: %w", entry.InfoHash, err)
	}
	if !storage.SameMainGeneration(entry, expected) {
		return nil, "", false, fmt.Errorf("%w for main entry %s", storage.ErrStaleEntryGeneration, entry.InfoHash)
	}

	// Prefer activating an existing, completed placement on the target debrid
	// before re-submitting the magnet. Skipped when reinsert=true — e.g. the
	// current active provider just failed and its placement is presumed stale.
	if !reinsert {
		didActivate := false
		updated, present, activateErr := f.manager.storage.MutateEntrySnapshot(expected, func(current *storage.Entry) (bool, error) {
			target := current.Providers[debridName]
			if target == nil || target.ID == "" || target.Status != types.TorrentStatusDownloaded {
				return false, nil
			}
			if err := current.ActivatePlacement(debridName); err != nil {
				return false, err
			}
			current.Bad = false
			didActivate = true
			return true, nil
		})
		if activateErr != nil {
			return nil, "", false, fmt.Errorf("activate existing %s placement: %w", debridName, activateErr)
		}
		if !present {
			return nil, "", false, fmt.Errorf("%w for deleted main entry %s", storage.ErrStaleEntryGeneration, entry.InfoHash)
		}
		if didActivate {
			copyProviderView(entry, updated)
			return nil, "", true, nil
		}
		expected = updated
	}

	// Fixer owns replacement of an old placement on the target provider. It
	// never removes a different source provider; Switcher applies KeepOld after
	// the target ownership commit succeeds.
	if target := expected.Providers[debridName]; target != nil {
		oldTargetID = target.ID
	}
	return expected, oldTargetID, false, nil
}

// commitMove adopts a freshly created provider placement, or compensates it
// away when it cannot be adopted. It runs under copyEntryMu because that is
// what the mutex is for: an ownership commit and a last-reference cleanup must
// not interleave with folder-alias creation or deletion.
//
// statusErr is passed in rather than handled by the caller so that every exit
// from the provider phase — including a status ceiling firing — compensates the
// placement it created under the same lock, in one place.
func (f *Fixer) commitMove(
	entry, expected *storage.Entry,
	client debrid.Client,
	debridName, oldTargetID, submittedID string,
	newDebridTorrent *types.Torrent,
	statusErr error,
) (bool, error) {
	f.manager.copyEntryMu.Lock()
	defer f.manager.copyEntryMu.Unlock()

	if statusErr != nil {
		return false, errors.Join(
			fmt.Errorf("failed to check status: %w", statusErr),
			f.cleanupUnownedPlacement(expected.InfoHash, debridName, submittedID, client.DeleteTorrent),
		)
	}
	if newDebridTorrent == nil {
		return false, errors.Join(
			fmt.Errorf("failed to check status: empty entry"),
			f.cleanupUnownedPlacement(expected.InfoHash, debridName, submittedID, client.DeleteTorrent),
		)
	}
	if newDebridTorrent.Id == "" {
		newDebridTorrent.Id = submittedID
	}
	if newDebridTorrent.InfoHash == "" {
		newDebridTorrent.InfoHash = expected.InfoHash
	}
	if newDebridTorrent.Debrid == "" {
		newDebridTorrent.Debrid = debridName
	}

	// Verify files have links
	if len(newDebridTorrent.Files) == 0 {
		return false, errors.Join(
			fmt.Errorf("no files in entry after re-insertion"),
			f.cleanupUnownedPlacement(expected.InfoHash, debridName, newDebridTorrent.Id, client.DeleteTorrent),
		)
	}
	if newDebridTorrent.Status != types.TorrentStatusDownloaded {
		return false, errors.Join(
			fmt.Errorf("entry on %s is not complete after re-insertion", debridName),
			f.cleanupUnownedPlacement(expected.InfoHash, debridName, newDebridTorrent.Id, client.DeleteTorrent),
		)
	}

	for _, remoteFile := range newDebridTorrent.GetFiles() {
		if remoteFile.Link == "" && remoteFile.Id == "" {
			return false, errors.Join(
				fmt.Errorf("empty link/id for file %s", remoteFile.Name),
				f.cleanupUnownedPlacement(expected.InfoHash, debridName, newDebridTorrent.Id, client.DeleteTorrent),
			)
		}
	}

	updated, present, commitErr := f.manager.storage.MutateEntrySnapshot(expected, func(current *storage.Entry) (bool, error) {
		applyProviderTorrent(current, newDebridTorrent)
		if err := current.ActivatePlacement(debridName); err != nil {
			return false, err
		}
		current.Bad = false
		return true, nil
	})
	if commitErr != nil || !present {
		if commitErr == nil {
			commitErr = fmt.Errorf("%w for deleted main entry %s", storage.ErrStaleEntryGeneration, expected.InfoHash)
		}
		return false, errors.Join(
			fmt.Errorf("commit %s placement: %w", debridName, commitErr),
			f.cleanupUnownedPlacement(expected.InfoHash, debridName, newDebridTorrent.Id, client.DeleteTorrent),
		)
	}

	copyProviderView(entry, updated)
	if oldTargetID != "" && oldTargetID != newDebridTorrent.Id {
		if cleanupErr := f.cleanupUnownedPlacement(expected.InfoHash, debridName, oldTargetID, client.DeleteTorrent); cleanupErr != nil {
			return true, cleanupErr
		}
	}

	return true, nil
}

func (f *Fixer) cleanupUnownedPlacement(infohash, provider, id string, deleteTorrent func(string) error) error {
	_, err := f.manager.storage.CleanupUnownedProviderPlacement(infohash, provider, id, func() error {
		return deleteTorrent(id)
	})
	return wrapProviderCleanupError(provider, id, "clean up unowned", err)
}

func wrapProviderCleanupError(provider, id, action string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s %s placement %s: %w", action, provider, id, err)
}

// buildAttemptOrder creates the order of debrids to attempt re-insertion
// Priority: current active debrid first, then others in config order
// If skipCurrent is true, current active debrid is skipped
func (f *Fixer) buildAttemptOrder(torrent *storage.Entry, skipCurrent bool) []string {
	order := make([]string, 0, len(f.providerOrder))

	// AddOrUpdate other debrids in config order
	for _, debridName := range f.providerOrder {
		if debridName == torrent.ActiveProvider && skipCurrent {
			continue
		}
		order = append(order, debridName)
	}

	return order
}

// IsFailedToReinsert checks if a torrent has been marked as failed to re-insert
func (f *Fixer) IsFailedToReinsert(infohash, debrid string) bool {
	_, failed := f.failedToReinsert.Load(fmt.Sprintf("%s:%s", infohash, debrid))
	return failed
}

// ResetFailureState manually resets the failure state for a torrent
func (f *Fixer) ResetFailureState(infohash string) {
	f.failedToReinsert.Delete(infohash)
}

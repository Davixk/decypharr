package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	// inconclusive records that at least one provider failed WITHOUT stating
	// anything about the content — either it never answered (our own ceiling
	// fired) or it answered with a failure of its machinery. It is the
	// difference between "every debrid says this content is gone" and "the
	// attempts did not work", and only the first of those may become a durable
	// verdict below.
	//
	// ceilingFired distinguishes the two causes for the error this reports back.
	// Calling an answered 503 a backend timeout would misname the one thing an
	// operator reads to tell a slow provider apart from a refusing one.
	inconclusive := false
	ceilingFired := false

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
			// OUR ceiling fired; this provider never reached a verdict. Storing
			// the marker below would exclude a perfectly healthy debrid from
			// repairing this entry again — until something clears it, and the
			// only thing that clears it is a re-insertion that succeeds, which
			// the marker itself prevents from being attempted. Record the attempt
			// as inconclusive and move on.
			inconclusive = true
			ceilingFired = true
			f.manager.logger.Warn().Err(err).
				Str("debrid", debridName).
				Str("infohash", entry.InfoHash).
				Msg("Re-insertion abandoned on its own ceiling; provider gave no verdict")
			continue
		}
		if !customerror.IsContentPermanentlyGone(err) {
			// THE PROVIDER ANSWERED, BUT NOT WITH A VERDICT ABOUT THE CONTENT.
			//
			// This is the ANSWERED-TRANSIENT case, and it is the one that used to
			// fall through. A submit refused on a quota, a status poll that came
			// back "not complete", a 503 out of a provider mid-outage — all of
			// them landed here and were recorded as this debrid's permanent
			// failure for this entry, and once every debrid had one recorded the
			// entry itself was condemned (Bad) and persisted. An hour-long
			// provider outage therefore left durable damage across the whole
			// library, indistinguishable afterwards from content that really had
			// died.
			//
			// The asymmetry decides it: wrongly condemning good content costs a
			// re-download plus an indexer search, wrongly keeping dead content
			// costs one more failed read. So only an UNAMBIGUOUS content verdict
			// — the provider stating the bytes are gone or legally removed —
			// earns the marker or the Bad flag. Everything else is a failed
			// attempt and nothing more; the next sweep or the next read retries
			// from the same state.
			inconclusive = true
			f.manager.logger.Warn().Err(err).
				Str("debrid", debridName).
				Str("infohash", entry.InfoHash).
				Msg("Re-insertion failed without a content verdict; treating as transient and leaving the entry unmarked")
			continue
		}
		// The provider stated the content itself is gone. THAT is durable, and
		// only that: the marker means "this debrid has confirmed the content is
		// dead", not "an attempt against this debrid failed once".
		f.failedToReinsert.Store(failureMarkerKey(entry.InfoHash, debridName), struct{}{})
	}

	if totalAttempts == 0 {
		// Nothing was even tried — every candidate was already marked, or the
		// attempt order was empty (skipCurrent with no other provider
		// configured). Zero attempts is not "every debrid says this is broken",
		// and the block below must not read it as such.
		result := &FixResult{
			Success:       false,
			Error:         fmt.Errorf("no debrid was available to re-insert %s", entry.InfoHash),
			AttemptsCount: 0,
		}
		req.result <- result
		return result, result.Error
	}

	if inconclusive {
		// NOT A VERDICT, SO NOT A VERDICT ANYWHERE.
		//
		// Entry.Bad short-circuits every subsequent read of this entry before any
		// provider call (link.badEntryError), and nothing clears it except a
		// re-insertion that succeeds. Setting it because a provider was too slow
		// to answer, or because it answered "not right now", would convert a
		// backend flap into a library that serves errors until somebody notices —
		// the precise failure mode this codebase has already been burned by, and
		// the reason NewBackendTimeoutError is retryable and explicitly not
		// permanent.
		//
		// So: fail this attempt, say why, and leave the entry's health untouched.
		// The next sweep or the next read retries from the same state.
		//
		// The error TYPE names the cause honestly. Our ceiling firing and a
		// provider refusing are both retryable and both non-permanent, but they
		// are not the same event, and an operator reading a log during an
		// incident is entitled to know which one happened.
		var resultErr error
		if ceilingFired {
			resultErr = customerror.NewBackendTimeoutError(
				fmt.Errorf("re-insertion of %s was inconclusive: %w", entry.InfoHash, lastErr))
		} else {
			resultErr = fmt.Errorf("%w: re-insertion of %s reached no verdict: %w",
				customerror.HosterUnavailableError, entry.InfoHash, lastErr)
		}
		f.manager.logger.Warn().
			Err(lastErr).
			Str("infohash", entry.InfoHash).
			Int("attempts", totalAttempts).
			Bool("ceiling_fired", ceilingFired).
			Msg("Re-insertion inconclusive: no provider reached a verdict about the content; entry left unmarked")

		result := &FixResult{
			Success:       false,
			Error:         resultErr,
			AttemptsCount: totalAttempts,
		}
		req.result <- result
		return result, result.Error
	}

	// EVERY DEBRID THAT WAS TRIED RETURNED A CONTENT VERDICT.
	//
	// Reaching here now means something much stronger than it used to: not "the
	// attempts failed" but "every provider asked stated the bytes are gone or
	// legally removed". Nothing else gets this far — an answered transient and
	// our own ceiling both set `inconclusive` above and returned already. That
	// is what makes the durable Bad flag below a correct verdict rather than a
	// record of a bad afternoon.
	f.manager.logger.Error().
		Err(lastErr).
		Str("infohash", entry.InfoHash).
		Int("attempts", totalAttempts).
		Msg("Every debrid confirmed the content is gone; condemning the entry")

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
		Error:         fmt.Errorf("every debrid confirmed the content is gone: %w", lastErr),
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

// failureMarkerKey is the ONE place the marker key shape is spelled out.
//
// It exists because the shape used to be open-coded at three sites and one of
// them got it wrong: markers were written and read as "infohash:debrid" while
// ResetFailureState deleted the bare "infohash". Reading and writing agreed, so
// the marker worked; only the clearing silently missed, and a bare-key deletion
// cannot fail loudly. A single constructor makes that class of bug impossible
// to reintroduce.
func failureMarkerKey(infohash, debrid string) string {
	return fmt.Sprintf("%s:%s", infohash, debrid)
}

// IsFailedToReinsert checks if a torrent has been marked as failed to re-insert
func (f *Fixer) IsFailedToReinsert(infohash, debrid string) bool {
	_, failed := f.failedToReinsert.Load(failureMarkerKey(infohash, debrid))
	return failed
}

// ResetFailureState clears every per-debrid failure marker recorded for
// infohash.
//
// THE KEY SHAPES HAD TO MATCH AND DID NOT. Markers are stored under
// "infohash:debrid" and read back under the same key, but this function deleted
// the bare "infohash" — a key nothing writes and nothing reads. Every per-debrid
// marker was therefore permanent for the lifetime of the process: a debrid that
// failed once was excluded from repairing that entry forever, and the one event
// that is supposed to wipe the slate (a re-insertion that SUCCEEDED, which is
// the only caller of this function) wiped nothing. The exclusion could not even
// clear itself, because clearing required a success on a debrid the marker had
// already removed from the attempt order.
//
// The prefix scan covers every debrid without needing the caller to know which
// ones were tried, and the exact-match arm cleans up any bare key left in the
// map by an older build.
func (f *Fixer) ResetFailureState(infohash string) {
	if infohash == "" {
		return
	}
	prefix := infohash + ":"
	f.failedToReinsert.Range(func(key string, _ struct{}) bool {
		if key == infohash || strings.HasPrefix(key, prefix) {
			f.failedToReinsert.Delete(key)
		}
		return true
	})
}

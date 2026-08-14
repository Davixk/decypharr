package manager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// AddNewTorrent submits a torrent to debrid before entering the active-download queue.
func (m *Manager) AddNewTorrent(ctx context.Context, importReq *ImportRequest) error {
	if importReq == nil || importReq.Magnet == nil {
		return fmt.Errorf("magnet is required")
	}
	if importReq.Arr == nil {
		return fmt.Errorf("arr is required")
	}

	torrent := newTorrentQueueEntry(importReq, debridTypes.TorrentStatusQueued)
	if err := m.queue.Add(torrent); err != nil {
		return fmt.Errorf("failed to reserve torrent queue entry: %w", err)
	}

	var debridTorrent *debridTypes.Torrent
	err := func() error {
		admissionCtx, release, err := m.queue.BeginAction(ctx, torrent)
		if err != nil {
			return err
		}
		defer release()
		debridTorrent, err = m.SendToDebrid(admissionCtx, importReq)
		if err != nil {
			return err
		}
		torrent.DownloadUncached = debridTorrent.DownloadUncached
		applyDebridTorrentToEntry(torrent, debridTorrent)
		return m.queue.Update(torrent)
	}()
	if err != nil {
		// WHY a capacity failure is held rather than failed.
		//
		// A CONTENT refusal is refused: this release is unservable, another
		// candidate may not be, and the arr is still holding its result set so
		// taking the next one costs nothing. That is the whole economics —
		// failing a grab costs one candidate, failing AFTER a successful grab
		// costs a whole new search across every indexer.
		//
		// A TEMPORARY INABILITY is different in kind. It says nothing about this
		// release, only that the provider is busy, rate limiting, or its
		// allowance is spent. decypharr's own queue is not bounded by the
		// provider's, so the entry is accepted and held until capacity exists.
		// The hold has no deadline: stall pruning continuously reclaims provider
		// slots, so this is a queue with a working drain, and failing a held
		// entry on a timer would fail work that was going to succeed.
		//
		// The at-cap stored-item case is HELD too, by operator ruling — a full
		// account is not a verdict about the release. See classifyQuotaRefusal
		// for what that costs if the drain ever stops.
		refusal := m.classifyAddRefusal(err)
		// LOGGED BEFORE THE BRANCH, because a standing condition now co-exists
		// with a HOLD. It used to be reachable only on the refuse path, so
		// moving the at-cap case to hold would have silently deleted the one log
		// line that tells an operator why a queue is growing and never draining.
		if refusal.standingCondition != "" {
			m.logger.Error().
				Str("provider", refusal.provider).
				Str("hash", torrent.InfoHash).
				Msg(refusal.standingCondition)
		}
		if refusal.hold {
			return m.holdTorrentForCapacity(importReq, torrent, refusal, err)
		}
		// THE REFUSAL REASON IS THE RESPONSE BODY, so it has to lead with
		// something a human reads in an arr's log rather than with the innermost
		// wrapping. The qBittorrent shim answers this error's text verbatim as
		// the 400 body, and "most as silent 400s the arr can't distinguish from
		// anything else" is the exact complaint being answered — twelve
		// consecutive manual grabs on one title, every one indistinguishable
		// from every other kind of failure.
		reason := refusalReason(err)
		if deleted, deleteErr := m.queue.DeleteCurrent(torrent, nil); deleteErr != nil {
			return errors.Join(fmt.Errorf("%s", reason), fmt.Errorf("delete failed reservation: %w", deleteErr), err)
		} else if !deleted {
			return fmt.Errorf("%s (the queue reservation was already gone): %w", reason, err)
		}
		return fmt.Errorf("%s: %w", reason, err)
	}

	job := NewJob(JobTypeTorrent, importReq)
	job.ID = torrent.InfoHash
	job.Entry = torrent
	job.DebridTorrent = debridTorrent
	if err := m.SubmitJob(job); err != nil {
		// GRAB-TIME-KNOWABLE, SO IT FAILS THE GRAB — IT DOES NOT PARK A CORPSE.
		//
		// This used to MarkAsError and keep the row, while also returning an
		// error. The arr therefore got both: a failed add AND a queue entry sat
		// in `error`, which the arr renders as a dead-end warning it can neither
		// retry nor clear. That is the exact shape the operator ruled out —
		// a synchronous refusal must resolve synchronously, leaving nothing
		// behind, because the arr is still holding its ranked candidate list and
		// can take the next release for free. A parked error row instead costs a
		// full indexer re-search later.
		//
		// The placement goes with it. SendToDebrid already succeeded here, so the
		// provider is holding a transfer for a grab we are about to refuse;
		// dropping the row alone would strand it as an orphan burning a slot that
		// no local record can ever release. Release first, then delete.
		if _, deleteErr := m.queue.DeleteCurrent(torrent, m.RemoveTorrentPlacements); deleteErr != nil {
			m.logger.Error().Err(deleteErr).
				Str("infohash", torrent.InfoHash).
				Str("name", torrent.Name).
				Msg("Could not withdraw the reservation for a torrent that failed to queue; " +
					"the row may be left behind in a state the arr cannot act on")
			return errors.Join(
				fmt.Errorf("failed to queue torrent: %w", err),
				fmt.Errorf("withdraw failed reservation: %w", deleteErr),
			)
		}
		m.logger.Error().Err(err).
			Str("infohash", torrent.InfoHash).
			Str("name", torrent.Name).
			Msg("Could not queue the torrent for download; refusing the grab and withdrawing the reservation " +
				"so the arr can take its next candidate immediately")
		return fmt.Errorf("failed to queue torrent: %w", err)
	}
	return nil
}

func (m *Manager) processTorrentJob(ctx context.Context, job *Job) error {
	if job == nil || job.Entry == nil {
		return fmt.Errorf("invalid torrent job")
	}
	current, err := m.queue.RefreshSnapshot(job.Entry)
	if err != nil {
		return fmt.Errorf("refresh torrent queue generation: %w", err)
	}
	if !current {
		return nil
	}
	if job.ResumeExisting {
		job.Entry.Status = debridTypes.TorrentStatusDownloading
		job.Entry.IsDownloading = false
		if err := m.queue.Update(job.Entry); err != nil {
			return err
		}
		m.processingEntries.Store(job.Entry.InfoHash, struct{}{})
		m.processQueuedTorrent(job.Entry)
		return nil
	}
	if job.DebridTorrent == nil {
		if job.Request == nil {
			m.waitForDownloadCompletion(ctx, job.Entry)
			return nil
		}
		debridTorrent, err := m.SendToDebrid(ctx, job.Request)
		if err != nil {
			return fmt.Errorf("failed to submit torrent to debrid: %w", err)
		}
		current, refreshErr := m.queue.RefreshSnapshot(job.Entry)
		if refreshErr != nil || !current {
			m.cleanupStaleSubmittedTorrent(debridTorrent)
			if refreshErr != nil {
				return fmt.Errorf("refresh torrent queue after provider submission: %w", refreshErr)
			}
			return nil
		}
		job.DebridTorrent = debridTorrent
	}

	job.Entry.Status = debridTypes.TorrentStatusDownloading
	job.Entry.DownloadUncached = job.DebridTorrent.DownloadUncached
	if job.Request != nil {
		job.Request.Status = "started"
	}
	if err := m.processNewTorrent(job.Entry, job.DebridTorrent); err != nil {
		if errors.Is(err, storage.ErrStaleEntryGeneration) || errors.Is(err, storage.ErrEntryNotFound) {
			m.cleanupStaleSubmittedTorrent(job.DebridTorrent)
			return nil
		}
		return err
	}
	return nil
}

func (m *Manager) cleanupStaleSubmittedTorrent(torrent *debridTypes.Torrent) {
	if torrent == nil || torrent.Id == "" {
		return
	}
	// Provider submissions may deduplicate and return a pre-existing remote ID.
	// Once the queue generation changed there is no durable proof that this job
	// exclusively owns that ID, even if the hash is momentarily absent locally.
	// Retaining a possible orphan is safer than deleting a replacement's data.
	m.logger.Warn().
		Str("infohash", torrent.InfoHash).
		Str("debrid", torrent.Debrid).
		Str("torrent_id", torrent.Id).
		Msg("Retained stale submission because exclusive remote ownership cannot be proven")
}

func newTorrentQueueEntry(importReq *ImportRequest, status debridTypes.TorrentStatus) *storage.Entry {
	now := time.Now()
	// A magnet carries its display name in the optional "dn" parameter, so a
	// bare magnet:?xt=urn:btih:<hash> parses to an empty Name -- and the real
	// name only arrives later, from the provider. An empty Name makes
	// DownloadPath() clean back to SavePath, which for an *arr entry is the
	// shared category directory: every path derived from this entry then points
	// at a directory owned by all its siblings. The delete path already refuses
	// to act on that, but the entry should never be built that way to begin
	// with. Substitute the infohash, which is unique and filesystem-safe; the
	// real name replaces it once the provider resolves it.
	name := importReq.Magnet.Name
	if !utils.IsUsableName(name) {
		name = importReq.Magnet.InfoHash
	}
	torrent := &storage.Entry{
		InfoHash:         importReq.Magnet.InfoHash,
		Name:             name,
		OriginalFilename: importReq.Magnet.Name,
		Protocol:         config.ProtocolTorrent,
		Size:             importReq.Magnet.Size,
		Bytes:            importReq.Magnet.Size,
		Magnet:           importReq.Magnet.Link,
		Category:         importReq.Arr.Name,
		SavePath:         filepath.Join(importReq.DownloadFolder, importReq.Arr.Name),
		Status:           status,
		State:            storage.EntryStateDownloading,
		Progress:         0,
		Action:           importReq.Action,
		CallbackURL:      importReq.CallBackUrl,
		SkipMultiSeason:  importReq.SkipMultiSeason,
		CreatedAt:        now,
		UpdatedAt:        now,
		AddedOn:          now,
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}
	torrent.ContentPath = torrent.DownloadPath()
	return torrent
}

// admitToProvider decides whether this provider can accept another item right
// now, using whatever the provider itself will tell us.
//
// Two shapes, because providers differ and neither is a fallback for a missing
// feature:
//
//	PROSPECTIVE   RealDebrid answers GET /torrents/activeCount with
//	              {nb, limit}, so capacity is knowable before we spend an add.
//	RETROSPECTIVE AllDebrid publishes no such endpoint, so it tells us by
//	              refusing (MAGNET_TOO_MANY_ACTIVE). That refusal is
//	              authoritative and current; a local constant is neither.
//
// A provider that reports nothing returns ErrAvailableSlotsUnknown and is
// admitted here — its own refusal is the gate. Any other error asking is OUR
// failure, not a verdict about capacity, so it must not manufacture a refusal:
// declining an add because we could not reach an endpoint is the same mistake
// as condemning a release because a probe timed out.
func admitToProvider(db common.Client, providerName string) error {
	slots, err := db.GetAvailableSlots()
	switch {
	case errors.Is(err, debridTypes.ErrAvailableSlotsUnknown):
		return nil
	case err != nil:
		// Could not ask. Proceed and let the provider answer for itself.
		logger := db.Logger()
		logger.Debug().Err(err).
			Str("Provider", providerName).
			Msg("Could not read provider capacity; proceeding and relying on the provider to refuse if full")
		return nil
	case slots <= 0:
		return fmt.Errorf("%w: provider %q reports no free slots", customerror.TooManyActiveDownloadsError, providerName)
	default:
		return nil
	}
}

// isTooManyActiveDownloads reports provider CONCURRENCY exhaustion — slots that
// free as active work finishes.
//
// Matched by sentinel identity rather than by Code string: a fallback chain
// joins one error per provider, and errors.As returns the first *customerror.Error
// in that tree, which may belong to a different provider that failed for an
// unrelated reason. errors.Is tests the whole tree for THIS condition.
func isTooManyActiveDownloads(err error) bool {
	return errors.Is(err, customerror.TooManyActiveDownloadsError)
}

// isProviderAddQuotaExhausted reports an add/storage allowance being spent,
// which our own completions do NOT release. Kept distinct from slot exhaustion
// so the two cannot share a retry cadence.
func isProviderAddQuotaExhausted(err error) bool {
	return errors.Is(err, customerror.ProviderAddQuotaExhaustedError)
}

// isRateClassRefusal reports the third capacity condition: the provider is
// refusing because of the REQUEST RATE or a traffic allowance, not because it
// has no room.
//
// ⚠️ TYPED ONLY — NEVER isRateLimitSignal, which is right next door and looks
// interchangeable. That one is an unanchored substring scan of err.Error() for
// "429"/"503"/"slow down". It is the correct tool for feeding the pacer's
// backoff, where a false positive costs one slower minute, and an unsafe one for
// deciding whether to ACCEPT A GRAB, where a false positive parks an entry as
// queuedDL against a condition that will never clear.
//
// The failure is not hypothetical and it is not exotic: "429" and "503" are
// three digits that appear inside infohashes, release names, byte counts and
// segment offsets. `torrent 429f0ab… is not cached` is a permanent content
// refusal that this scan calls a rate limit. And the shared client's give-up
// string spells the status in words — `status 503` — so every provider outage
// that exhausts retries matches it too.
//
// Sentinel identity cannot be spoofed by a filename:
//
//	RateLimitedError     HTTP 429 from any provider, typed at the one place that
//	                     still knows the status (internal/request's give-up
//	                     handler) — 429 is retried to exhaustion, so it never
//	                     reaches a provider's own status switch.
//	TrafficExceededError RealDebrid codes 23 / 34 / 36 — traffic exhausted, too
//	                     many requests, fair-usage limit. Every one is an
//	                     allowance that returns on the provider's own boundary.
func isRateClassRefusal(err error) bool {
	return errors.Is(err, customerror.RateLimitedError) ||
		errors.Is(err, customerror.TrafficExceededError)
}

func (m *Manager) processQueuedEntries() {
	queueEntries := m.queue.ListFilter("", config.ProtocolAll, storage.EntryStateDownloading, nil, "", true)
	if len(queueEntries) == 0 {
		return
	}
	for _, entry := range queueEntries {
		// Parse only active downloading torrents
		if entry.State != storage.EntryStateDownloading {
			continue
		}
		if entry.Status == debridTypes.TorrentStatusQueued {
			continue
		}
		// Skip entries that are actively being downloading
		if entry.IsDownloading {
			continue
		}
		// Skip if a previous tick's goroutine hasn't finished yet for this hash.
		if _, loaded := m.processingEntries.LoadOrStore(entry.InfoHash, struct{}{}); loaded {
			continue
		}
		if entry.IsTorrent() {
			if entry.ActiveProvider != "" {
				go m.processQueuedTorrent(entry)
			} else {
				m.processingEntries.Delete(entry.InfoHash)
			}
		} else if entry.IsNZB() {
			go m.processQueuedNZB(entry)
		} else {
			m.processingEntries.Delete(entry.InfoHash)
		}
	}
}

// failQueuedEntry marks a queued entry failed, persists it, and emits ONE line
// that can actually be found afterwards.
//
// Every site below used to open-code the same three steps, and they had drifted
// in the way that matters most: the Error line carried `name` and NEVER the
// infohash. A hash-scoped trace of a row that died therefore showed the grab and
// then nothing at all — the marking WAS logged, just not under the key anyone
// searches by, which reads as silence and sent an investigation looking for a
// silent writer that does not exist. A line that cannot be correlated is barely
// better than no line. Two of these sites (NZB generation, metadata mismatch)
// really were silent as well.
//
// source names WHICH verdict marked the row, so "what failed this?" is answered
// by the line itself rather than by matching timestamps against source.
func (m *Manager) failQueuedEntry(entry *storage.Entry, source string, cause error) {
	if entry == nil {
		return
	}
	entry.MarkAsError(cause)
	m.logger.Error().Err(cause).
		Str("source", source).
		Str("infohash", entry.InfoHash).
		Str("name", entry.Name).
		Msg("Marked a queued entry failed; it stays parked for the arr to collect")
	if err := m.queue.Update(entry); err != nil {
		// Benign in the common case: a stale generation means the row was
		// deleted or superseded while this verdict was being reached, so there
		// is nothing left to mark.
		m.logger.Debug().Err(err).
			Str("source", source).
			Str("infohash", entry.InfoHash).
			Msg("Did not persist the failure; the queue row had already moved on")
	}
}

func (m *Manager) processQueuedNZB(entry *storage.Entry) {
	defer m.processingEntries.Delete(entry.InfoHash)
	current, err := m.queue.RefreshSnapshot(entry)
	if err != nil || !current {
		if err != nil {
			m.logger.Warn().Err(err).Str("name", entry.Name).Msg("Failed to refresh queued NZB generation")
		}
		return
	}
	generation, err := m.ensureNZBGeneration(entry)
	if err != nil {
		m.failQueuedEntry(entry, "nzb-generation", err)
		return
	}
	// Check if the nzb is already processed. Only header fields (status, file
	// list) are needed here; processNZB does not touch the segment map.
	metadata, err := m.usenet.GetNZBHeader(entry.InfoHash)
	if err != nil {
		m.failQueuedEntry(entry, "nzb-metadata-load", err)
		return
	}
	if metadata == nil {
		m.failQueuedEntry(entry, "nzb-metadata-missing", fmt.Errorf("nzb metadata not found"))
		return
	}
	if metadata.Generation != generation {
		m.failQueuedEntry(entry, "nzb-generation-mismatch",
			fmt.Errorf("%w: queued generation %q, metadata generation %q", usenet.ErrStaleNZBGeneration, generation, metadata.Generation))
		return
	}
	switch metadata.Status {
	case usenet.NZBStatusFailed:
		m.failQueuedEntry(entry, "nzb-parse-failed", fmt.Errorf("nzb processing failed"))
		return
	case usenet.NZBStatusParsing, usenet.NZBStatusDownloading:
		// Still processing, skip for now
		return
	case usenet.NZBStatusCompleted:
		if err := m.processNZB(context.Background(), entry, metadata); err != nil {
			m.failQueuedEntry(entry, "nzb-completion", err)
			return
		}
	default:
		m.failQueuedEntry(entry, "nzb-unknown-status", fmt.Errorf("unknown nzb status: %s", metadata.Status))
		return
	}
}

func (m *Manager) processQueuedTorrent(entry *storage.Entry) {
	defer m.processingEntries.Delete(entry.InfoHash)
	current, err := m.queue.RefreshSnapshot(entry)
	if err != nil {
		m.logger.Warn().Err(err).Str("name", entry.Name).Msg("Failed to refresh queued torrent generation")
		return
	}
	if !current {
		return
	}
	placement := entry.GetActiveProvider()
	if placement == nil {
		m.failQueuedEntry(entry, "no-active-placement", fmt.Errorf("no active placement found"))
		return
	}

	client := m.ProviderClient(entry.ActiveProvider)
	if client == nil {
		m.failQueuedEntry(entry, "provider-client-missing",
			fmt.Errorf("debrid client not found: %s", entry.ActiveProvider))
		return
	}

	magnet, err := utils.GetMagnetInfo(entry.Magnet, m.config.AlwaysRmTrackerUrls)
	if err != nil {
		magnet = utils.ConstructMagnet(entry.InfoHash, entry.Name)
	}

	arr := m.arr.GetOrCreate(entry.Category)

	debridTorrent := &debridTypes.Torrent{
		Id:               placement.ID,
		InfoHash:         entry.InfoHash,
		Magnet:           magnet,
		Name:             magnet.Name,
		Arr:              arr,
		Size:             entry.Size,
		Files:            make(map[string]debridTypes.File),
		DownloadUncached: entry.DownloadUncached,
	}

	dbT, err := client.CheckStatus(debridTorrent)
	if err != nil {
		// THE PROVIDER DROPPED A TRANSFER IT HAD ALREADY ACCEPTED.
		//
		// A measured case: RealDebrid took the grab, started downloading, then
		// answered 404 for the very torrent id it had just issued, 18 seconds
		// later. That is a genuinely deferred death — nothing at grab time could
		// have predicted it — so failing the row here is correct and the arr
		// learns by polling. It carries the provider and its id because "the
		// provider forgot an id it gave us" is a provider-side fault worth being
		// able to count, not a decypharr one.
		m.failQueuedEntry(entry, "provider-status-check:"+entry.ActiveProvider,
			fmt.Errorf("provider %s rejected a status check for its own transfer %s: %w",
				entry.ActiveProvider, placement.ID, err))
		return
	}

	debridTorrent = dbT

	if debridTorrent == nil {
		m.failQueuedEntry(entry, "provider-entry-missing:"+entry.ActiveProvider,
			fmt.Errorf("debrid entry not found"))
		return
	}

	if debridTorrent.Status == debridTypes.TorrentStatusError {
		m.failQueuedEntry(entry, "provider-error-state:"+debridTorrent.Debrid,
			fmt.Errorf("entry in error state on debrid: %s", debridTorrent.Debrid))
		return
	}

	// Update entry progress
	entry.Progress = debridTorrent.Progress / 100.0
	entry.Speed = debridTorrent.Speed
	entry.Size = debridTorrent.GetSize()
	entry.Seeders = debridTorrent.Seeders
	entry.UpdatedAt = time.Now()

	// Update placement progress
	if placement := entry.GetActiveProvider(); placement != nil {
		placement.Progress = entry.Progress
	}

	if err := m.queue.Update(entry); err != nil {
		m.logger.Debug().Err(err).Str("name", entry.Name).Msg("Stopped stale queued torrent progress update")
		return
	}
	// Check if done or failed
	if debridTorrent.Status == debridTypes.TorrentStatusDownloaded {
		// Hand the detached action its own snapshot: this function still reads
		// the entry after spawning, and sharing the pointer would race with the
		// action's snapshot refreshes.
		actionEntry := *entry
		go m.processAction(&actionEntry)
	}
}

func (m *Manager) processAction(entry *storage.Entry) {
	if entry == nil {
		return
	}
	if !m.beginActionInflight(entry.InfoHash) {
		m.logger.Debug().Str("name", entry.Name).Msg("Post-download action already pending in this process")
		return
	}
	defer m.endActionInflight(entry.InfoHash)
	claimed, err := m.queue.ClaimPostDownload(entry)
	if err != nil {
		m.logger.Debug().Err(err).Str("name", entry.Name).Msg("Stopped stale completed-download workflow")
		return
	}
	if !claimed {
		m.logger.Debug().Str("name", entry.Name).Msg("Post-download action already claimed or no longer current")
		return
	}
	// The claim is durable and the worker that observed it has already
	// released its active-download slot (see waitForDownloadCompletion). Gate
	// the actual action so a backlog of claims drains progressively instead
	// of stampeding the mount.
	if !m.acquireActionSlot() {
		// Shutdown: the durable claim is resumed by restore on next boot.
		return
	}
	defer m.releaseActionSlot()
	m.runClaimedAction(entry)
}

func (m *Manager) runClaimedAction(entry *storage.Entry) {
	if m.claimedActionTestHook != nil {
		m.claimedActionTestHook(entry)
		return
	}
	m.logger.Info().
		Str("name", entry.Name).
		Str("action", string(entry.Action)).
		Msg("Download completed, processing action")

	// Merge and persist under the main row's mutation lock. This tolerates an
	// unrelated size/provider revision without overwriting it, and retries the
	// narrow absent->concurrent-create race.
	if err := m.persistCompletedEntry(entry); err != nil {
		m.logger.Error().Err(err).Str("name", entry.Name).Msg("Failed to persist completed download")
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)
		return
	}
	err := m.downloader.download(entry)
	if err != nil {
		m.logger.Error().
			Err(err).
			Str("name", entry.Name).
			Msg("Error running post-download action")
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)
		return
	}
}

// processTorrent handles the complete torrent lifecycle
func (m *Manager) processNewTorrent(torrent *storage.Entry, debridTorrent *debridTypes.Torrent) error {
	// Update status to submitting
	torrent.UpdatedAt = time.Now()
	applyDebridTorrentToEntry(torrent, debridTorrent)
	if err := m.queue.Update(torrent); err != nil {
		return err
	}

	if debridTorrent.Status != debridTypes.TorrentStatusDownloaded {
		m.logger.Info().
			Str("debrid", debridTorrent.Debrid).
			Str("name", debridTorrent.Name).
			Msg("Started downloading torrent")
		return nil
	}

	// Parse post-download action. The action goroutine gets its own snapshot:
	// the calling worker returns into waitForDownloadCompletion with the same
	// pointer, and the two must not refresh a shared Entry concurrently.
	actionEntry := *torrent
	go m.processAction(&actionEntry)
	return nil
}

func (m *Manager) persistCompletedEntry(entry *storage.Entry) error {
	// A completed provider placement can share a remote ID with a folder COPY
	// alias. Serialize adoption with reference-aware cleanup.
	m.copyEntryMu.Lock()
	defer m.copyEntryMu.Unlock()
	for range 3 {
		updated, present, err := m.storage.MutateEntryIfPresent(entry.InfoHash, func(current *storage.Entry) (bool, error) {
			if entry.IsNZB() {
				mergeWorkflowSnapshot(current, entry)
			} else {
				merged := storage.HandleExistingEntryMerge(current, entry)
				*current = *merged
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		if present {
			*entry = *updated
			m.refreshEntryCache()
			return nil
		}

		if err := m.storage.AddOrUpdate(entry); err == nil {
			m.refreshEntryCache()
			return nil
		} else if !errors.Is(err, storage.ErrStaleEntryGeneration) {
			return err
		}
	}
	return fmt.Errorf("persist completed entry %s after concurrent creates", entry.InfoHash)
}

func applyDebridTorrentToEntry(torrent *storage.Entry, debridTorrent *debridTypes.Torrent) {
	_ = torrent.AddTorrentProvider(debridTorrent)
	torrent.ActiveProvider = debridTorrent.Debrid
	torrent.Bytes = debridTorrent.GetSize()
	torrent.Size = debridTorrent.GetSize()
	torrent.Name = debridTorrent.Name
	torrent.OriginalFilename = debridTorrent.OriginalFilename
	torrent.UpdatedAt = time.Now()

	for _, file := range debridTorrent.Files {
		tFile := &storage.File{
			Name:      file.Name,
			Size:      file.Size,
			ByteRange: file.ByteRange,
			Deleted:   file.Deleted,
			InfoHash:  torrent.InfoHash,
			AddedOn:   torrent.AddedOn,
		}
		torrent.Files[file.Name] = tFile
	}

	if debridTorrent.Status != debridTypes.TorrentStatusDownloaded {
		return
	}
	if placement := torrent.GetActiveProvider(); placement != nil {
		now := time.Now()
		placement.DownloadedAt = &now
		placement.Progress = 1.0
	}
}

// SendToDebrid submits a magnet to debrid service(s) - replaces debrid.Parse
func (m *Manager) SendToDebrid(ctx context.Context, importRequest *ImportRequest) (*debridTypes.Torrent, error) {
	if importRequest == nil || importRequest.Magnet == nil {
		return nil, fmt.Errorf("failed to process torrent: magnet is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	clients, selectedFound := m.debridClientsForRequest(importRequest.SelectedDebrid, importRequest.FallbackOnFailure)
	errs := make([]error, 0, len(clients)+1)
	if importRequest.SelectedDebrid != "" && !selectedFound {
		errs = append(errs, fmt.Errorf("provider %q is not configured", importRequest.SelectedDebrid))
	}
	if len(clients) == 0 {
		if len(errs) == 0 {
			errs = append(errs, errors.New("no debrid clients available"))
		}
		return nil, joinDebridErrors(errs)
	}

	for _, db := range clients {
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("debrid request canceled: %w", err))
			break
		}

		dbConfig := db.Config()
		providerName := dbConfig.Name
		if providerName == "" {
			providerName = dbConfig.Provider
		}
		// Provider admission: ask whether this provider has room BEFORE
		// spending an add on it. A provider that cannot answer is not guessed
		// at — it falls through and refuses for itself, which is the only
		// signal AllDebrid has.
		//
		// A full provider is treated like any other per-provider decline: the
		// chain advances, so a full AllDebrid does not stop a RealDebrid that
		// has room. Only if EVERY provider declines does the joined error reach
		// the caller — and it keeps its type through joinDebridErrors, so
		// processJob requeues the job instead of failing it.
		//
		// This never blocks. Waiting here for capacity would rebuild, one level
		// down, exactly the head-of-line blocking this layer exists to remove.
		if err := admitToProvider(db, providerName); err != nil {
			errs = append(errs, err)
			logDebridAttemptFailure(db.Logger(), providerName, "admission", importRequest.Magnet.InfoHash, err)
			continue
		}

		downloadUncached, uncachedVetoed := resolveDownloadUncached(dbConfig.DownloadUncached, importRequest.DownloadUncached)
		debridTorrent := newDebridAttempt(importRequest, downloadUncached)

		_logger := db.Logger()
		arrName := ""
		if importRequest.Arr != nil {
			arrName = importRequest.Arr.Name
		}
		if uncachedVetoed {
			// The whole failure mode this guards against is silence: without
			// this line, "why is this provider never taking uncached grabs?"
			// has no answer anywhere in the logs.
			_logger.Info().
				Str("Provider", providerName).
				Str("Arr", arrName).
				Str("Hash", debridTorrent.InfoHash).
				Msgf("Provider %q has download_uncached=false; ignoring the Arr's download_uncached=true and probing its cache only", providerName)
		}
		_logger.Info().
			Str("Provider", providerName).
			Str("Arr", arrName).
			Str("Hash", debridTorrent.InfoHash).
			Str("Name", debridTorrent.Name).
			Str("Action", string(importRequest.Action)).
			Msg("Processing torrent")

		// A hash the provider just refused is parked, so the admission
		// controller stops re-offering it on every tick. Skipping here rather
		// than at admission keeps the decision next to the error that caused
		// it, and still costs nothing — no request goes out.
		if cooling, why := m.declines.cooling(providerName, debridTorrent.InfoHash, time.Now()); cooling {
			// 🔴 %w, NEVER %s. This line read `%s` of the reason TEXT, which
			// destroyed every sentinel in the original error and made the
			// cool-off skip unclassifiable to classifyAddRefusal — so a
			// temporary quota exhaustion re-emerged from the hold as a
			// chain-exhausted permanent failure and parked the row as `error`.
			// 13 of 14 new parked rows in two hours were that shape.
			//
			// The classification matches by sentinel identity precisely so a
			// wrapper cannot lie about the class. It only holds if every wrapper
			// on the way actually carries the sentinel.
			coolErr := fmt.Errorf("cooling off after an earlier decline: %w", why)
			errs = append(errs, providerStageError(providerName, "submit", coolErr))
			// SAY SO. This branch skipped a provider in total silence, which made
			// a grab that resolved entirely inside its own second look like the
			// provider had simply never been asked — "Processing torrent" and
			// then nothing at all, with the row's fate decided elsewhere.
			logDebridAttemptFailure(_logger, providerName, "decline cooldown", debridTorrent.InfoHash, coolErr)
			continue
		}

		// RECORDED BEFORE THE REQUEST GOES OUT. A crash between this and the
		// POST leaves a harmless stale ledger entry; the reverse order leaves a
		// transfer nobody can find.
		m.pendingAdds.begin(providerName, debridTorrent.InfoHash)

		dbt, err := db.SubmitMagnet(debridTorrent)
		if err != nil {
			// AMBIGUOUS OUTCOME. The provider may have created the transfer and
			// lost the response on the way back — no retry required for that,
			// one attempt is enough. Ask whether it has the hash we just sent
			// before concluding nothing happened.
			//
			// This is NOT adoption: it is scoped to a hash THIS process
			// submitted seconds ago, keyed by that exact hash. A transfer
			// decypharr did not start can never match, because it was never
			// written to the ledger.
			if recovered := m.reconcileAmbiguousAdd(providerName, debridTorrent.InfoHash); recovered != "" {
				m.pendingAdds.resolve(providerName, debridTorrent.InfoHash)
				m.declines.clear(providerName, debridTorrent.InfoHash)
				dbt = debridTorrent
				dbt.Id = recovered
				dbt.Debrid = providerName
				err = nil
			}
		}
		if err != nil {
			m.pendingAdds.resolve(providerName, debridTorrent.InfoHash)
			// THE PROVIDER SAID WE ARE ASKING TOO FAST. Back the whole lane off,
			// not just this hash — a rate limit is a statement about our request
			// rate, so parking one release would leave the rate unchanged and the
			// next admission would hit the same wall. On RealDebrid this matters
			// doubly: refused requests count against the same budget, so an
			// unadjusted rate actively shrinks the room to recover.
			if isRateLimitSignal(err) {
				interval := m.addPace.penalise(providerName, time.Now())
				budget, current := m.addPace.rates(providerName)
				m.logger.Warn().
					Str("provider", providerName).
					Dur("now_one_add_every", interval).
					Float64("rate_per_min", current).
					Float64("budget_per_min", budget).
					Msg("Provider signalled a rate limit; halving the add rate for this provider")
			}
			cooldown := m.declines.record(providerName, debridTorrent.InfoHash,
				classifyDecline(err), err, time.Now())
			attemptErr := providerStageError(providerName, "submit", err)
			if dbt != nil && dbt.Id != "" {
				attemptErr = errors.Join(attemptErr, cleanupDebridAttempt(db, providerName, dbt.Id))
			}
			errs = append(errs, attemptErr)
			logDebridAttemptFailure(_logger, providerName, "submit", debridTorrent.InfoHash, err)
			_logger.Debug().
				Str("Provider", providerName).
				Str("Hash", debridTorrent.InfoHash).
				Dur("cooling_off", cooldown).
				Msg("Parked this release on this provider after a decline")
			continue
		}
		m.pendingAdds.resolve(providerName, debridTorrent.InfoHash)
		m.declines.clear(providerName, debridTorrent.InfoHash)
		// An add landed, so climb back toward the configured budget. Gradual by
		// design: one success does not prove a provider that just throttled us
		// has recovered, and jumping straight back to full rate would oscillate.
		m.addPace.reward(providerName)
		if dbt == nil {
			nilErr := errors.New("provider returned a nil torrent")
			errs = append(errs, providerStageError(providerName, "submit", nilErr))
			logDebridAttemptFailure(_logger, providerName, "submit", debridTorrent.InfoHash, nilErr)
			continue
		}
		if dbt.Id == "" {
			emptyErr := errors.New("provider returned an empty torrent id")
			errs = append(errs, providerStageError(providerName, "submit", emptyErr))
			logDebridAttemptFailure(_logger, providerName, "submit", debridTorrent.InfoHash, emptyErr)
			continue
		}
		dbt.Arr = importRequest.Arr
		_logger.Info().Str("id", dbt.Id).Msgf("Entry: %s submitted to %s", dbt.Name, providerName)

		if err := ctx.Err(); err != nil {
			errs = append(errs, errors.Join(
				fmt.Errorf("debrid request canceled: %w", err),
				cleanupDebridAttempt(db, providerName, dbt.Id),
			))
			logDebridAttemptFailure(_logger, providerName, "canceled after submit", debridTorrent.InfoHash, err)
			break
		}

		torrent, err := db.CheckStatus(dbt)
		if err != nil {
			cleanupID := dbt.Id
			if torrent != nil && torrent.Id != "" {
				cleanupID = torrent.Id
			}
			errs = append(errs, errors.Join(
				providerStageError(providerName, "status check", err),
				cleanupDebridAttempt(db, providerName, cleanupID),
			))
			logDebridAttemptFailure(_logger, providerName, "status check", debridTorrent.InfoHash, err)
			continue
		}
		if torrent == nil {
			nilStatusErr := errors.New("provider returned a nil torrent")
			errs = append(errs, errors.Join(
				providerStageError(providerName, "status check", nilStatusErr),
				cleanupDebridAttempt(db, providerName, dbt.Id),
			))
			logDebridAttemptFailure(_logger, providerName, "status check", debridTorrent.InfoHash, nilStatusErr)
			continue
		}
		if !downloadUncached && torrent.Status != debridTypes.TorrentStatusDownloaded {
			cleanupID := torrent.Id
			if cleanupID == "" {
				cleanupID = dbt.Id
			}
			uncachedErr := errors.New("torrent is not cached and uncached downloads are disabled")
			errs = append(errs, errors.Join(
				providerStageError(providerName, "status check", uncachedErr),
				cleanupDebridAttempt(db, providerName, cleanupID),
			))
			logDebridAttemptFailure(_logger, providerName, "status check", debridTorrent.InfoHash, uncachedErr)
			m.parkPostSubmitRefusal(providerName, debridTorrent.InfoHash, uncachedErr)
			continue
		}
		// SEEDER GATE. The provider took an UNCACHED torrent and started it, so
		// this is the last moment at which refusing is still cheap: we are
		// inside the arr's blocking add, it still holds its ranked candidate
		// list, and an error here makes it try the next release immediately
		// with no indexer traffic. After we return, the same verdict costs a
		// full re-search across every indexer.
		//
		// A cached hit never reaches this branch, and neither does a provider
		// with download_uncached=false — that case is refused above.
		if downloadUncached && torrent.Status != debridTypes.TorrentStatusDownloaded {
			magnetLink := ""
			if importRequest.Magnet != nil {
				magnetLink = importRequest.Magnet.Link
			}
			if reason := m.seederGateRefusal(ctx, debridTorrent.InfoHash, magnetLink, torrent.Seeders); reason != "" {
				cleanupID := torrent.Id
				if cleanupID == "" {
					cleanupID = dbt.Id
				}
				gateErr := errors.New(reason)
				// Prune the transfer we just created: refusing the grab while
				// still holding the slot would spend it on a release we have
				// declined.
				errs = append(errs, errors.Join(
					providerStageError(providerName, "seeder gate", gateErr),
					cleanupDebridAttempt(db, providerName, cleanupID),
				))
				logDebridAttemptFailure(_logger, providerName, "seeder gate", debridTorrent.InfoHash, gateErr)
				m.parkPostSubmitRefusal(providerName, debridTorrent.InfoHash, gateErr)
				continue
			}
		}
		if err := ctx.Err(); err != nil {
			// The provider accepted the torrent and the status check
			// succeeded; deleting it now would remove content the user asked
			// for. Keep the remote torrent and surface only the cancellation.
			errs = append(errs, fmt.Errorf("debrid request canceled: %w", err))
			logDebridAttemptFailure(_logger, providerName, "canceled after status check", debridTorrent.InfoHash, err)
			break
		}
		return torrent, nil
	}
	return nil, joinDebridErrors(errs)
}

func (m *Manager) debridClientsForRequest(selected string, fallbackOnFailure bool) ([]common.Client, bool) {
	clients := m.FilterDebrid(func(common.Client) bool { return true })
	if selected == "" {
		return clients, true
	}

	selectedIndex := -1
	for i, client := range clients {
		if client.Config().Name == selected {
			selectedIndex = i
			break
		}
	}
	if selectedIndex == -1 {
		if fallbackOnFailure {
			return clients, false
		}
		return nil, false
	}
	if !fallbackOnFailure {
		return []common.Client{clients[selectedIndex]}, true
	}

	ordered := make([]common.Client, 0, len(clients))
	ordered = append(ordered, clients[selectedIndex])
	ordered = append(ordered, clients[:selectedIndex]...)
	ordered = append(ordered, clients[selectedIndex+1:]...)
	return ordered, true
}

func newDebridAttempt(importRequest *ImportRequest, downloadUncached bool) *debridTypes.Torrent {
	return &debridTypes.Torrent{
		InfoHash:         importRequest.Magnet.InfoHash,
		Magnet:           importRequest.Magnet,
		Name:             importRequest.Magnet.Name,
		Arr:              importRequest.Arr,
		Size:             importRequest.Magnet.Size,
		Files:            make(map[string]debridTypes.File),
		DownloadUncached: downloadUncached,
	}
}

// resolveDownloadUncached decides whether an attempt may start an uncached
// download, and reports whether a provider-level veto is what said no.
//
// Both inputs are tri-state on purpose, and the provider's nil is the load-
// bearing one:
//
//	provider explicit false -> hard veto. No Arr-level value can lift it, on
//	                           any path. A per-provider "no" that a client can
//	                           override is not a per-provider setting.
//	provider nil            -> no opinion. The Arr decides; with no Arr value
//	                           either, the historical default (false) stands.
//	provider explicit true  -> permitted. The Arr still decides, so an Arr
//	                           false blocks.
//
// Do NOT "simplify" this to providerAllows && requestOverride using
// Debrid.DownloadsUncached(): that collapses nil to false, and every
// long-standing config that sets download_uncached only on the Arr — with no
// debrids[].download_uncached key at all — would silently stop downloading
// uncached releases. That failure presents as absence, which is the kind
// nobody notices for weeks.
//
// This resolver is deliberately path-independent. It used to take a
// fallbackChain flag and apply the veto only while walking a multi-provider
// chain, which meant a provider's "no" survived only as a side effect of the
// Arr's unrelated fallback_on_failure toggle.
func resolveDownloadUncached(providerSetting, requestOverride *bool) (allowed, vetoed bool) {
	if providerSetting != nil && !*providerSetting {
		// Only worth reporting as a veto when something actually asked for
		// uncached; otherwise nothing was overridden.
		return false, requestOverride != nil && *requestOverride
	}
	if requestOverride != nil {
		return *requestOverride, false
	}
	return providerSetting != nil && *providerSetting, false
}

// providerError carries WHICH provider failed as structured data rather than
// only in the message text.
//
// The name matters to more than logging now: classifying an AllDebrid
// MAGNET_TOO_MANY requires resolving that provider's configured cap against
// that provider's fill, and a chain error joins every provider's refusal
// together. Recovering the name by parsing the formatted string would be the
// same mistake as reading AllDebrid's own message to decide which limit it hit
// — the message said "1000 accross all tabs" while the binding constraint was
// the 5,000 stored cap.
type providerError struct {
	provider string
	stage    string
	err      error
}

func (e *providerError) Error() string {
	return fmt.Sprintf("provider %q %s failed: %v", e.provider, e.stage, e.err)
}

func (e *providerError) Unwrap() error { return e.err }

func providerStageError(providerName, stage string, err error) error {
	if err == nil {
		err = errors.New("unknown provider error")
	}
	return &providerError{provider: providerName, stage: stage, err: err}
}

func cleanupDebridAttempt(client common.Client, providerName, torrentID string) error {
	if torrentID == "" {
		return nil
	}
	if err := client.DeleteTorrent(torrentID); err != nil {
		return providerStageError(providerName, "cleanup", err)
	}
	return nil
}

// singleLineError renders a joined error on one line while leaving unwrapping
// intact.
//
// errors.Join separates with newlines, so a multi-provider failure arrives at an
// *arr as a multi-line body — and an *arr logs only the first line. The result
// is that a fallback chain which tried every provider is indistinguishable from
// one that tried a single provider: the operator sees the first provider's error
// and nothing else. That ambiguity cost this investigation three dead
// mechanisms before anyone counted the attempt log lines.
type singleLineError struct{ err error }

func (e singleLineError) Error() string {
	return strings.ReplaceAll(e.err.Error(), "\n", "; ")
}

func (e singleLineError) Unwrap() error { return e.err }

// logDebridAttemptFailure records a single provider declining an attempt.
//
// Without this the chain is silent: failures are accumulated into the returned
// error and the caller logs that at Debug, so at INFO level a fallback that
// tried every provider and one that tried none look identical. Every provider
// that declines now says so, at WARN, naming itself and its reason.
func logDebridAttemptFailure(l zerolog.Logger, providerName, stage, infoHash string, err error) {
	l.Warn().
		Str("Provider", providerName).
		Str("Stage", stage).
		Str("Hash", infoHash).
		Err(err).
		Msg("Provider declined this torrent; continuing to the next provider in the chain")
}

func joinDebridErrors(errs []error) error {
	joined := errors.Join(errs...)
	if joined == nil {
		joined = errors.New("no debrid clients available")
	}
	return fmt.Errorf("failed to process torrent: %w", singleLineError{joined})
}

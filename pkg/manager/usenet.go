package manager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

// AddNewNZB parses an NZB before entering the active-download queue.
func (m *Manager) AddNewNZB(ctx context.Context, req *ImportRequest) (string, error) {
	if m.usenet == nil {
		return "", fmt.Errorf("usenet not configured")
	}
	if req == nil || len(req.NZBContent) == 0 {
		return "", fmt.Errorf("NZB content is empty")
	}
	if req.Arr == nil {
		return "", fmt.Errorf("arr is required")
	}

	m.logger.Info().
		Str("name", req.Name).
		Str("category", req.Arr.Name).
		Msg("Adding new NZB to usenet")
	generation := usenet.NewNZBGeneration()
	m.nzbAdmissionMu.Lock()
	stagedPath, err := m.usenet.StageNZBForGeneration(req.Id, generation, req.NZBContent)
	if err != nil {
		m.nzbAdmissionMu.Unlock()
		return "", fmt.Errorf("stage NZB source: %w", err)
	}

	entry := &storage.Entry{
		InfoHash:         req.Id,
		Name:             req.Name,
		OriginalFilename: req.Name,
		Protocol:         config.ProtocolNZB,
		Magnet:           stagedPath,
		NZBGeneration:    generation,
		Category:         req.Arr.Name,
		SavePath:         filepath.Join(req.DownloadFolder, req.Arr.Name),
		Status:           debridTypes.TorrentStatusQueued,
		State:            storage.EntryStateDownloading,
		Progress:         0,
		Action:           req.Action,
		CallbackURL:      req.CallBackUrl,
		SkipMultiSeason:  req.SkipMultiSeason,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
		AddedOn:          time.Now(),
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}
	entry.ContentPath = entry.DownloadPath()
	if err := m.queue.Add(entry); err != nil {
		m.usenet.RemoveStagedNZB(stagedPath)
		m.nzbAdmissionMu.Unlock()
		return "", fmt.Errorf("failed to reserve NZB queue entry: %w", err)
	}
	m.nzbAdmissionMu.Unlock()

	var meta *storage.NZB
	var groups map[string]*parser.FileGroup
	err = func() error {
		admissionCtx, release, err := m.queue.BeginAction(ctx, entry)
		if err != nil {
			return err
		}
		defer release()
		meta, groups, err = m.usenet.ParseWithGeneration(admissionCtx, req.Id, generation, req.Name, req.NZBContent, req.Arr.Name)
		if err != nil {
			return err
		}
		entry.InfoHash = meta.ID
		entry.Name = meta.Name
		entry.OriginalFilename = meta.Name
		entry.Size = meta.TotalSize
		entry.Bytes = meta.TotalSize
		entry.Status = debridTypes.TorrentStatusDownloading
		entry.ActiveProvider = "usenet"
		if meta.Generation != entry.NZBGeneration {
			return fmt.Errorf("parsed NZB generation %q does not match reserved generation %q", meta.Generation, entry.NZBGeneration)
		}
		if entry.AddUsenetProvider(meta) == nil {
			return fmt.Errorf("failed to add Usenet provider metadata")
		}
		stagedPath := entry.Magnet
		entry.Magnet = ""
		if err := m.queue.Update(entry); err != nil {
			entry.Magnet = stagedPath
			return err
		}
		m.usenet.RemoveStagedNZB(stagedPath)
		return nil
	}()
	if err != nil {
		if errors.Is(err, parser.ErrProbeInfrastructure) && ctx.Err() == nil {
			// The NZB parsed structurally but its availability probe failed on
			// the NNTP substrate itself (connection/timeout/auth/no acquirable
			// provider connection) — there is no verdict about the articles.
			// Recording this as a failure would surface a Failed history entry
			// and the arr would blocklist a release that may be perfectly
			// healthy. Keep the reserved entry queued, accept the add (200 +
			// nzo_id), and let the job queue re-parse it with backoff once the
			// substrate recovers.
			if deferErr := m.deferInfraRetry(entry); deferErr == nil {
				m.logger.Warn().
					Err(err).
					Str("nzo_id", entry.InfoHash).
					Str("name", req.Name).
					Msg("NZB availability probe hit an infrastructure failure; keeping entry queued for retry")
				return entry.InfoHash, nil
			} else {
				// The deferral could not be persisted; fall through to the
				// delete path so the reservation is not leaked.
				m.logger.Error().
					Err(deferErr).
					Str("nzo_id", entry.InfoHash).
					Msg("Failed to keep infrastructure-deferred NZB queued; falling back to rejecting the add")
			}
		}
		if errors.Is(err, parser.ErrArticlesUnavailable) && ctx.Err() == nil {
			// The NZB parsed structurally but its articles failed the parse-time
			// availability probe. Rejecting the add here surfaces as a raw HTTP
			// error to Sonarr/Radarr ("Failed to connect to SABnzbd") and the
			// release is never blocklisted. Accept the add instead and record the
			// failure on the reserved queue entry so the SABnzbd history reports
			// it as Failed with a clear fail_message — the same UX the async
			// processing pipeline provides for post-admission failures.
			entry.MarkAsError(err)
			updateErr := m.queue.Update(entry)
			if updateErr == nil {
				m.logger.Warn().
					Err(err).
					Str("nzo_id", entry.InfoHash).
					Str("name", req.Name).
					Msg("NZB accepted but failed parse-time availability probe; recorded as failed")
				return entry.InfoHash, nil
			}
			// The failure could not be persisted; fall through to the delete path
			// so the reservation is not leaked.
			m.logger.Error().
				Err(updateErr).
				Str("nzo_id", entry.InfoHash).
				Msg("Failed to persist parse-time NZB failure; falling back to rejecting the add")
		}
		deleted, deleteErr := m.queue.DeleteCurrent(entry, func(*storage.Entry) error {
			return m.usenet.DeleteForGeneration(req.Id, generation)
		})
		if deleteErr != nil {
			return "", errors.Join(fmt.Errorf("usenet parse failed: %w", err), fmt.Errorf("delete failed reservation: %w", deleteErr))
		}
		if !deleted {
			return "", fmt.Errorf("usenet parse failed after reservation was removed: %w", err)
		}
		return "", fmt.Errorf("usenet parse failed: %w", err)
	}

	req.Status = "started"
	job := NewJob(JobTypeNZB, req)
	job.ID = entry.InfoHash
	job.Entry = entry
	job.NZBMeta = meta
	job.NZBGroups = groups
	if err := m.SubmitJob(job); err != nil {
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)
		return "", fmt.Errorf("failed to queue NZB: %w", err)
	}
	return meta.ID, nil
}

// nzbInfraRetryBaseDelay/nzbInfraRetryMaxDelay bound the backoff used when an
// NZB's availability probe fails on the NNTP substrate (no article verdict).
// Vars for tests.
var (
	nzbInfraRetryBaseDelay = 30 * time.Second
	nzbInfraRetryMaxDelay  = 5 * time.Minute
)

// nzbInfraFastRetryCap bounds how many cumulative infrastructure-class attempts
// an NZB entry may take on the FAST job-queue retry loop before it is parked for
// the slow, globally-bounded revival sweep (resweepParkedInfraNZBs). Without
// this cap an entry whose Process keeps failing on the substrate re-parses
// forever, pinning every job-queue worker so completions never get enqueued —
// the livelock. The count is tracked on the durable ErrorCount, so it survives
// process restarts and revival/sweep re-feeds: a parked entry that is
// re-submitted gets at most one more processing attempt per slow sweep instead
// of a fresh fast burst, bounding the aggregate re-processing rate to the
// sweep's budget rather than the fast loop's. Var for tests.
var nzbInfraFastRetryCap = 5

func nzbInfraRetryDelay(attempt int) time.Duration {
	delay := nzbInfraRetryBaseDelay
	for i := 0; i < attempt && delay < nzbInfraRetryMaxDelay; i++ {
		delay *= 2
	}
	if delay > nzbInfraRetryMaxDelay {
		delay = nzbInfraRetryMaxDelay
	}
	return delay
}

// scheduleQueuedNZBRetry routes a queued NZB entry whose availability probe
// hit an infrastructure failure back through the job queue with backoff.
// Queued NZB entries have no other runtime pickup path (the periodic queue
// processor skips Status=queued rows and boot restore pass-2 only runs once),
// so without this the entry would strand until the next reboot.
func (m *Manager) scheduleQueuedNZBRetry(entry *storage.Entry, attempt int) {
	if m.jobQueue == nil || entry == nil {
		return
	}
	job := &Job{
		ID:            entry.InfoHash,
		Type:          JobTypeNZB,
		Entry:         entry,
		RebuildQueued: true,
		RetryCount:    attempt,
		CreatedAt:     time.Now(),
	}
	m.jobQueue.Retry(job, nzbInfraRetryDelay(attempt))
}

// deferInfraRetry records one infrastructure-class retry for a queued NZB entry
// and decides whether it may take another FAST job-queue retry or must be parked
// for the slow revival sweep. Every infrastructure-class deferral (admission,
// worker loop, restore rebuild) funnels through here so the cap is enforced
// uniformly.
//
// The cumulative attempt count lives on the durable ErrorCount, so it survives
// restarts and revival/sweep re-feeds: once it exceeds nzbInfraFastRetryCap the
// entry stops fast-requeuing and rests parked in the queued state, where
// resweepParkedInfraNZBs re-feeds it at a globally bounded rate. The entry is
// always kept OUT of the terminal-error state (State stays downloading, Status
// stays queued, LastError cleared): an infrastructure failure carries no verdict
// about the articles, so surfacing it as Failed would make the arr blocklist a
// possibly healthy release.
//
// Returns an error only when the durable state could not be persisted, so the
// admission path can fall back to rejecting the add rather than leaking a
// half-updated reservation.
func (m *Manager) deferInfraRetry(entry *storage.Entry) error {
	if entry == nil {
		return fmt.Errorf("nil entry")
	}
	if m.queue == nil {
		return fmt.Errorf("queue not initialized")
	}
	infraCount := 0
	updated, err := m.queue.Mutate(entry.InfoHash, func(current *storage.Entry) bool {
		// ErrorCount is the durable cumulative infra-attempt counter. Reused
		// (rather than a new field) so the cap survives restarts without a
		// storage-schema change; revival deliberately never resets it.
		current.ErrorCount++
		current.State = storage.EntryStateDownloading
		current.Status = debridTypes.TorrentStatusQueued
		current.IsDownloading = false
		// No article verdict: keep it out of the Failed history projection.
		current.LastError = ""
		current.UpdatedAt = time.Now()
		infraCount = current.ErrorCount
		return true
	})
	if err != nil {
		return err
	}
	if updated != nil {
		*entry = *updated
	}
	if infraCount > nzbInfraFastRetryCap {
		// Fast-loop budget exhausted: park the entry. It stays queued (out of the
		// Failed history) and the bounded revival sweep re-feeds it, so a
		// permanent Process-infrastructure failure can no longer pin the worker
		// pool with unbounded re-parses.
		m.logger.Warn().
			Str("infohash", entry.InfoHash).
			Str("name", entry.Name).
			Int("infra_attempts", infraCount).
			Int("cap", nzbInfraFastRetryCap).
			Msg("NZB availability probe kept failing on the NNTP substrate; parking entry for the slow revival sweep instead of fast-requeuing")
		return nil
	}
	// Base the backoff on the durable count so a re-fed entry does not restart
	// from the base delay.
	m.scheduleQueuedNZBRetry(entry, infraCount-1)
	return nil
}

func (m *Manager) processNZBJob(ctx context.Context, job *Job) error {
	if job == nil || job.Entry == nil {
		return fmt.Errorf("invalid NZB job")
	}
	current, err := m.queue.RefreshSnapshot(job.Entry)
	if err != nil {
		return fmt.Errorf("refresh NZB queue generation: %w", err)
	}
	if !current {
		return nil
	}
	if job.RebuildQueued {
		// The parse for this queued NZB was deferred (admission- or
		// restore-time infrastructure failure) or must be redone. Rebuild
		// re-parses from the staged source, or resumes completed metadata
		// without any network work.
		rebuilt, rebuildErr := m.rebuildQueuedNZBJob(job.Entry)
		if rebuildErr != nil {
			return rebuildErr
		}
		job.RebuildQueued = false
		job.NZBMeta = rebuilt.NZBMeta
		job.NZBGroups = rebuilt.NZBGroups
		job.ResumeExisting = rebuilt.ResumeExisting
		if job.Request == nil {
			job.Request = rebuilt.Request
		}
	}
	generation, err := m.ensureNZBGeneration(job.Entry)
	if err != nil {
		return err
	}
	if job.NZBMeta == nil {
		if job.Request == nil {
			m.waitForDownloadCompletion(ctx, job.Entry)
			return nil
		}
		return fmt.Errorf("parsed NZB metadata missing")
	}
	if job.NZBMeta.Generation != generation {
		return fmt.Errorf("%w: queued generation %q, job metadata generation %q", usenet.ErrStaleNZBGeneration, generation, job.NZBMeta.Generation)
	}
	if job.Request != nil {
		job.Request.Status = "started"
	}
	if job.ResumeExisting && job.NZBMeta.Status == usenet.NZBStatusCompleted {
		return m.processNZB(ctx, job.Entry, job.NZBMeta)
	}
	return m.processNewNzb(ctx, job.Entry, job.NZBMeta, job.NZBGroups)
}

func (m *Manager) processNZB(ctx context.Context, entry *storage.Entry, metadata *storage.NZB) error {
	if entry == nil || metadata == nil {
		return fmt.Errorf("NZB entry and metadata are required")
	}
	if entry.NZBGeneration == "" || metadata.Generation != entry.NZBGeneration {
		return fmt.Errorf("%w: queued generation %q, completion metadata generation %q", usenet.ErrStaleNZBGeneration, entry.NZBGeneration, metadata.Generation)
	}
	if metadata.ID != entry.InfoHash {
		return fmt.Errorf("NZB completion metadata ID %q does not match queued entry %q", metadata.ID, entry.InfoHash)
	}
	rebuildNZBCompletionFiles(entry, metadata)
	// Add files using the authoritative logical streamable file list.
	// Membership and sizes come from this exact metadata generation; durable
	// user/file state is copied only for names that still exist.

	// Mark as complete
	if placement := entry.GetActiveProvider(); placement != nil {
		now := time.Now()
		placement.DownloadedAt = &now
		placement.Progress = 1.0
	}
	entry.Size = metadata.TotalSize
	entry.Bytes = metadata.TotalSize
	entry.Progress = 1.0
	entry.UpdatedAt = time.Now()
	if err := m.queue.UpdateNZBCompletion(entry); err != nil {
		return err
	}

	if len(entry.Files) == 0 {
		return fmt.Errorf("nzb has no files")
	}

	// Hand the detached action its own snapshot: the calling worker returns
	// into waitForDownloadCompletion with the same pointer, and the two must
	// not refresh a shared Entry concurrently.
	actionEntry := *entry
	go m.processAction(&actionEntry)
	return nil
}

func rebuildNZBCompletionFiles(entry *storage.Entry, metadata *storage.NZB) {
	previous := entry.Files
	files := make(map[string]*storage.File, len(metadata.Files))
	for _, file := range metadata.Files {
		tFile := &storage.File{
			Name:     file.Name,
			Size:     file.Size,
			InfoHash: entry.InfoHash,
			AddedOn:  entry.AddedOn,
		}
		if durable := previous[file.Name]; durable != nil {
			// These fields represent explicit durable/user state rather than a
			// parser snapshot. Preserve them only for files still present in the
			// authoritative metadata; absent names are intentionally dropped.
			preserveDurableNZBFileState(tFile, durable)
		}
		files[file.Name] = tFile
	}
	entry.Files = files
}

// processNewNzb processes a new NZB entry after it has been added to the usenet client
func (m *Manager) processNewNzb(parentCtx context.Context, entry *storage.Entry, metadata *storage.NZB, groups map[string]*parser.FileGroup) error {
	// Bound heavy Process/availability concurrency to the provider pool. The
	// gate is acquired BEFORE the processing-timeout clock starts, so time spent
	// waiting for a slot never counts against the per-job deadline. This is the
	// root cure for the livelock: with the pool fitted, each admitted Process
	// call gets enough connections to answer within processing_timeout and
	// succeed, instead of a starved pool timing out and re-parsing forever.
	if m.processSem != nil {
		select {
		case m.processSem <- struct{}{}:
			defer func() { <-m.processSem }()
		case <-parentCtx.Done():
			return parentCtx.Err()
		}
		if m.processGateObserver != nil {
			m.processGateObserver(1)
			defer m.processGateObserver(-1)
		}
	}

	// Create context with timeout for processing
	ctx, cancel := context.WithTimeout(parentCtx, m.usenetTimeout)
	defer cancel()

	updatedNZB, err := m.usenet.Process(ctx, metadata, groups)
	if err != nil {
		if parentErr := parentCtx.Err(); parentErr != nil {
			// Shutdown or job cancellation: there is no verdict about the
			// content. Return the bare context error so processJob's ctx guard
			// leaves the entry in its prior state instead of terminally
			// error-marking it.
			return parentErr
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			// Only the per-job processing deadline elapsed: the substrate was
			// too slow to answer, which is an infrastructure-class outcome
			// with no content verdict. Carry the sentinel so processJob keeps
			// the entry queued and retries with backoff instead of parking it
			// in a terminal error.
			return fmt.Errorf("%w: usenet processing timed out after %s: %w", parser.ErrProbeInfrastructure, m.usenetTimeout, err)
		}
		return fmt.Errorf("failed to process nzb: %w", err)
	}

	metadata = updatedNZB
	return m.processNZB(ctx, entry, metadata)
}

// HasUsenet returns true if usenet is configured
func (m *Manager) HasUsenet() bool {
	return m.usenet != nil
}

// ensureNZBGeneration adopts one exact token for legacy rows and metadata,
// then persists it in both manager stores. New rows already carry the token;
// this path is idempotent across crashes between metadata and Entry writes.
func (m *Manager) ensureNZBGeneration(entry *storage.Entry) (string, error) {
	if entry == nil || !entry.IsNZB() {
		return "", fmt.Errorf("NZB entry is required")
	}
	if m.usenet == nil {
		return "", fmt.Errorf("usenet not configured")
	}
	header, err := m.usenet.GetNZBHeader(entry.InfoHash)
	if err != nil {
		return "", fmt.Errorf("load NZB generation for %s: %w", entry.InfoHash, err)
	}
	if header == nil {
		return "", fmt.Errorf("NZB metadata %s not found", entry.InfoHash)
	}
	generation := entry.NZBGeneration
	if generation == "" {
		generation = header.Generation
		if generation == "" {
			generation = usenet.NewNZBGeneration()
		}
	}
	if err := m.usenet.NZBStorage().AssertGeneration(entry.InfoHash, generation); err != nil {
		if !errors.Is(err, usenet.ErrStaleNZBGeneration) || entry.NZBGeneration != "" {
			return "", err
		}
		// Another adopter may have won while both legacy values were blank.
		header, reloadErr := m.usenet.GetNZBHeader(entry.InfoHash)
		if reloadErr != nil || header == nil || header.Generation == "" {
			return "", err
		}
		generation = header.Generation
		if assertErr := m.usenet.NZBStorage().AssertGeneration(entry.InfoHash, generation); assertErr != nil {
			return "", assertErr
		}
	}

	apply := func(current *storage.Entry) (bool, error) {
		if current.NZBGeneration != "" && current.NZBGeneration != generation {
			return false, fmt.Errorf("%w: entry generation %q, expected %q", usenet.ErrStaleNZBGeneration, current.NZBGeneration, generation)
		}
		if current.NZBGeneration == generation {
			return false, nil
		}
		current.NZBGeneration = generation
		return true, nil
	}
	_, mainPresent, err := m.storage.MutateEntryIfPresent(entry.InfoHash, apply)
	if err != nil {
		return "", fmt.Errorf("persist main NZB generation: %w", err)
	}
	_, queuePresent, err := m.storage.MutateQueuedIfPresent(entry.InfoHash, apply)
	if err != nil {
		return "", fmt.Errorf("persist queue NZB generation: %w", err)
	}
	if !mainPresent && !queuePresent {
		return "", fmt.Errorf("entry %s was deleted during NZB generation adoption", entry.InfoHash)
	}
	entry.NZBGeneration = generation
	return generation, nil
}

// UsenetStats returns usenet client statistics
func (m *Manager) UsenetStats() map[string]any {
	if m.usenet == nil {
		return nil
	}
	return m.usenet.Stats()
}

// SpeedTestRequest represents a speed test request payload
type SpeedTestRequest struct {
	Protocol string `json:"protocol"` // "nntp" or "debrid"
	Provider string `json:"provider"` // provider host/identifier
}

// SpeedTestResponse represents a speed test result
type SpeedTestResponse struct {
	Provider  string  `json:"provider"`
	Protocol  string  `json:"protocol"`
	SpeedMBps float64 `json:"speed_mbps"`
	LatencyMs int64   `json:"latency_ms"`
	BytesRead int64   `json:"bytes_read"`
	TestedAt  string  `json:"tested_at"`
	Error     string  `json:"error,omitempty"`
}

// SpeedTest runs a speed test for a specific provider based on protocol
func (m *Manager) SpeedTest(ctx context.Context, req SpeedTestRequest) SpeedTestResponse {
	switch req.Protocol {
	case "nntp":
		if m.usenet == nil {
			return SpeedTestResponse{
				Provider: req.Provider,
				Protocol: req.Protocol,
				Error:    "usenet not configured",
			}
		}
		result := m.usenet.SpeedTest(ctx, req.Provider)
		return SpeedTestResponse{
			Provider:  result.Provider,
			Protocol:  req.Protocol,
			SpeedMBps: result.SpeedMBps,
			LatencyMs: result.LatencyMs,
			BytesRead: result.BytesRead,
			TestedAt:  result.TestedAt.Format("2006-01-02T15:04:05Z07:00"),
			Error:     result.Error,
		}
	case "debrid":
		// Look up debrid client by provider name
		client, exists := m.clients.Load(req.Provider)
		if !exists {
			return SpeedTestResponse{
				Provider: req.Provider,
				Protocol: req.Protocol,
				Error:    "debrid provider not found: " + req.Provider,
			}
		}
		result := client.SpeedTest(ctx)

		// Store the result for persistence (so it shows up in stats)
		if result.Error == "" {
			m.debridSpeedTestResults.Store(req.Provider, result)
		}

		return SpeedTestResponse{
			Provider:  result.Provider,
			Protocol:  req.Protocol,
			SpeedMBps: result.SpeedMBps,
			LatencyMs: result.LatencyMs,
			BytesRead: result.BytesRead,
			TestedAt:  result.TestedAt.Format("2006-01-02T15:04:05Z07:00"),
			Error:     result.Error,
		}
	default:
		return SpeedTestResponse{
			Provider: req.Provider,
			Protocol: req.Protocol,
			Error:    "unknown protocol: " + req.Protocol,
		}
	}
}

func (m *Manager) syncNZBs(ctx context.Context) error {
	if m.usenet == nil {
		return nil
	}

	m.nzbSyncMu.Lock()
	defer m.nzbSyncMu.Unlock()

	pendingNZBs, err := m.usenet.ClaimNewNZBs()
	if err != nil {
		return fmt.Errorf("failed to claim new NZBs from usenet client: %w", err)
	}

	for _, pending := range pendingNZBs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		req := NewNZBRequest(
			pending.Name,
			m.config.DownloadFolder,
			pending.Content,
			m.arr.GetOrCreate(""),
			config.DownloadActionNone,
			"",
			ImportTypeWatch,
			false,
		)
		if _, err := m.AddNewNZB(ctx, req); err != nil {
			m.logger.Error().Err(err).Str("name", pending.Name).Msg("Failed to queue watched NZB")
			continue
		}
		m.usenet.RemoveClaimedNZB(pending.Path)
	}
	return nil
}

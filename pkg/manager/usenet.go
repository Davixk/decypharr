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
	// Create context with timeout for processing
	ctx, cancel := context.WithTimeout(parentCtx, m.usenetTimeout)
	defer cancel()

	updatedNZB, err := m.usenet.Process(ctx, metadata, groups)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return fmt.Errorf("usenet processing timed out after %s: %w", m.usenetTimeout, err)
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

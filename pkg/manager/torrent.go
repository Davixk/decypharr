package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func (m *Manager) syncTorrents(ctx context.Context) {
	// First time syncTorrents debrid -> storage
	m.logger.Info().
		Int("debrids", m.clients.Size()).
		Msg("Performing initial sync of torrents from debrid clients...")
	var wg sync.WaitGroup
	m.clients.Range(func(name string, client debrid.Client) bool {
		wg.Go(func() {
			if err := m.refreshTorrents(ctx, name, client); err != nil {
				m.logger.Error().Err(err).Str("debrid", name).Msg("Initial torrent sync failed")
			}
			m.RefreshEntries(false)
		})
		return true
	})
	wg.Wait()
	m.logger.Info().
		Msg("Initial sync of torrents from debrid clients completed")
}

// Refresh configuration constants
const (
	refreshBatchSize      = 500
	refreshMaxWorkers     = 50 // Capped to avoid overwhelming debrid APIs
	refreshMinWorkers     = 5
	refreshDeleteWorkers  = 10
	refreshWorkChanBuffer = 100
)

type providerRemovalCandidate struct {
	snapshot    *storage.Entry
	provider    string
	placementID string
}

type providerRefreshCandidate struct {
	remote   *types.Torrent
	snapshot *storage.Entry
}

var errProviderBecameLastPlacement = errors.New("provider became the last placement")

// refreshTorrents refreshes torrents from a specific debrid service.
// Returns an error if the refresh fails.
func (m *Manager) refreshTorrents(ctx context.Context, provider string, debridClient debrid.Client) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Use singleflight to prevent concurrent refreshes for the same debrid
	_, err, _ := m.refreshSG.Do(provider, func() (any, error) {
		return nil, m.doRefreshTorrents(ctx, provider, debridClient)
	})

	return err
}

// providerPresence records every hash and ID a provider holds in ANY state.
//
// It exists to answer one question honestly: "is this item still on the
// provider at all?" That is a different question from "is this item usable",
// and conflating the two is what made the removal path wrong. See
// detectTorrentChanges.
type providerPresence struct {
	hashes map[string]struct{}
	ids    map[string]struct{}
}

func (p providerPresence) holds(hash, id string) bool {
	if id != "" {
		if _, ok := p.ids[id]; ok {
			return true
		}
	}
	if hash == "" {
		return false
	}
	_, ok := p.hashes[strings.ToLower(hash)]
	return ok
}

// doRefreshTorrents performs the actual refresh logic.
//
// ONE ENUMERATION, TWO VIEWS. This used to call GetTorrents (downloaded only)
// while the fill cache, ENUMERATE and restore reconciliation each called
// GetAllTorrents — the same account, listed twice on unrelated schedules, one a
// strict superset of the other. It now lists once and derives both views:
//
//	LIBRARY view   downloaded rows only. Feeds refreshes/creates, so what
//	               becomes a library entry is unchanged: an in-progress torrent
//	               has no files yet and must not be projected into the mount.
//	PRESENCE view  every row in any state. Feeds the removal guard ONLY.
//
// The split matters beyond saving a request. Removal candidates were selected
// from the downloaded-only listing, so an entry whose provider copy was merely
// mid-download read as "gone from the provider" and had its placement removed —
// and, when that was its only placement, the entry deleted outright. A status
// filter must never be able to mean deletion; absence now means absent.
func (m *Manager) doRefreshTorrents(_ context.Context, provider string, debridClient debrid.Client) error {
	startedAt := time.Now()
	remote, err := debridClient.GetAllTorrents()
	if err != nil {
		m.logger.Error().Err(err).Str("debrid", provider).Msg("Failed to get remote")
		return err
	}

	// The account was just counted, so the fill snapshot is free here. It is
	// the same quantity the cache would otherwise enumerate for itself.
	m.fillCache.observe(provider, len(remote), time.Now())

	// The other direction of the same listing: items the provider holds that no
	// local record claims. Reported only — see provider_orphans.go for why this
	// must never delete on its own inference.
	m.reportProviderOrphans(provider, remote)

	if len(remote) == 0 {
		m.logger.Debug().Str("debrid", provider).Msg("No remote found")
	}

	// Build map of current remote by infohash
	remoteTorrentsByHash := make(map[string]*types.Torrent, len(remote))
	remoteTorrentsByID := make(map[string]*types.Torrent, len(remote))
	presence := providerPresence{
		hashes: make(map[string]struct{}, len(remote)),
		ids:    make(map[string]struct{}, len(remote)),
	}
	downloaded := 0
	for _, t := range remote {
		if t == nil || t.InfoHash == "" {
			continue
		}
		if t.Debrid == "" {
			t.Debrid = provider
		}
		remoteHash := strings.ToLower(t.InfoHash)

		// PRESENCE first, and unconditionally: a row in any state, including a
		// terminally dead one, is still an item the provider holds. Dropping our
		// record of it would not remove it from the account; culling a dead copy
		// is ENUMERATE's job, which deletes on the provider rather than only
		// forgetting locally.
		presence.hashes[remoteHash] = struct{}{}
		if t.Id != "" {
			presence.ids[t.Id] = struct{}{}
		}

		if t.Status != types.TorrentStatusDownloaded {
			continue
		}
		downloaded++
		if t.Id != "" {
			remoteTorrentsByID[t.Id] = t
		}
		old, exists := remoteTorrentsByHash[remoteHash]
		if !exists {
			remoteTorrentsByHash[remoteHash] = t
		}
		if exists && t.Added.After(old.Added) {
			remoteTorrentsByHash[remoteHash] = t
		}
	}

	// Detect changes by streaming through cached entries
	refreshes, removals, err := m.detectTorrentChanges(provider, remoteTorrentsByHash, remoteTorrentsByID, presence)
	if err != nil {
		return err
	}

	removalErr := m.handleProviderRemovals(removals)

	var refreshErr error
	if len(refreshes) > 0 {
		if processErr := m.processNewTorrents(provider, refreshes); processErr != nil {
			m.logger.Error().Err(processErr).Str("debrid", provider).Msg("Failed to process some torrents")
			refreshErr = processErr
		}
	}

	// A successful refresh used to log NOTHING, which made "the job never ran"
	// and "the job ran and had nothing to do" indistinguishable from the
	// outside. An operator reading 19,587 log lines over 2h47m of uptime found
	// zero matches for it and reasonably concluded it was not scheduled. State
	// what happened, at a level that is actually on.
	m.logger.Info().
		Str("debrid", provider).
		Int("remote", len(remote)).
		Int("downloaded", downloaded).
		Int("refreshed", len(refreshes)).
		Int("removed", len(removals)).
		Dur("took", time.Since(startedAt)).
		Msg("Torrent refresh completed")

	return errors.Join(removalErr, refreshErr)
}

// detectTorrentChanges streams through cached entries and detects what changed
func (m *Manager) detectTorrentChanges(
	provider string,
	remoteTorrentsByHash map[string]*types.Torrent,
	remoteTorrentsByID map[string]*types.Torrent,
	presence providerPresence,
) (
	refreshes []providerRefreshCandidate,
	removals []providerRemovalCandidate,
	err error,
) {
	refreshes = make([]providerRefreshCandidate, 0, 100)
	removals = make([]providerRemovalCandidate, 0, 10)
	cachedInfoHashes := make(map[string]bool, len(remoteTorrentsByHash))
	representedRemoteIDs := make(map[string]bool, len(remoteTorrentsByID))

	err = m.storage.ForEachBatch(refreshBatchSize, func(batch []*storage.Entry) error {
		for _, entry := range batch {
			entryHash := strings.ToLower(entry.InfoHash)
			cachedInfoHashes[entryHash] = true

			oldPlacement, placementOnDebrid := entry.Providers[provider]
			currentTorrent, onRemote := remoteTorrentsByHash[entryHash]
			if placementOnDebrid && oldPlacement != nil && oldPlacement.ID != "" {
				if byID, exists := remoteTorrentsByID[oldPlacement.ID]; exists {
					currentTorrent, onRemote = byID, true
				}
			}
			// Folder aliases have a synthetic storage key but retain the content
			// magnet. Fall back to that immutable torrent hash if the provider
			// rotated its placement ID.
			if placementOnDebrid && !onRemote {
				if contentHash := utils.ExtractInfoHash(entry.Magnet); contentHash != "" {
					if byContent, exists := remoteTorrentsByHash[strings.ToLower(contentHash)]; exists {
						currentTorrent, onRemote = byContent, true
					}
				}
			}
			if onRemote && currentTorrent != nil && currentTorrent.Id != "" {
				representedRemoteIDs[currentTorrent.Id] = true
			}

			if placementOnDebrid && oldPlacement != nil {
				if !onRemote {
					// Not in the LIBRARY view. Before concluding the provider
					// dropped it, ask whether the provider holds it at all: an
					// item that is merely mid-download, queued, or dead is
					// absent from the downloaded-only view while very much
					// still on the account. Removing its placement here would
					// delete our only record of something we are still paying a
					// slot for — and, when this is the last placement, delete
					// the entry itself.
					if presence.holds(entryHash, oldPlacement.ID) {
						continue
					}
					removals = append(removals, providerRemovalCandidate{
						snapshot:    entry,
						provider:    provider,
						placementID: oldPlacement.ID,
					})
				} else if oldPlacement.NeedsUpdate(currentTorrent) {
					// currentTorrent has changes for this provider - update placement info
					// But the issue is that currentTorrent may not have all the metadata we need to update the placement (e.g. downloadedAt, files etc)
					// So we need to fetch the full torrent info from debrid to ensure we have all the metadata to update the placement correctly
					// So let's just add it to the newTorrents list and let processNewTorrents handle the update logic - it will be smart enough to only update the placement info without overwriting other metadata
					refreshes = append(refreshes, providerRefreshCandidate{remote: currentTorrent, snapshot: entry})
				}
			} else if onRemote {
				refreshes = append(refreshes, providerRefreshCandidate{remote: currentTorrent, snapshot: entry})
			}
		}
		return nil
	})

	if err != nil {
		m.logger.Error().Err(err).Msg("Failed to stream cached remote")
		return nil, nil, err
	}

	// Check for brand new torrents (not in cache at all)
	for infohash, t := range remoteTorrentsByHash {
		if !cachedInfoHashes[infohash] && (t.Id == "" || !representedRemoteIDs[t.Id]) {
			refreshes = append(refreshes, providerRefreshCandidate{remote: t})
		}
	}

	return refreshes, removals, nil
}

// handleProviderRemovals independently removes placements that disappeared
// from one provider. Every candidate is generation-fenced and rechecked; one
// stale candidate or write error cannot prevent the remaining entries from
// being reconciled.
func (m *Manager) handleProviderRemovals(candidates []providerRemovalCandidate) error {
	if len(candidates) == 0 {
		return nil
	}

	var deleteWg sync.WaitGroup
	deleteChan := make(chan providerRemovalCandidate, len(candidates))
	errChan := make(chan error, len(candidates))

	deleteWorkers := min(refreshDeleteWorkers, len(candidates))
	for range deleteWorkers {
		deleteWg.Go(func() {
			for candidate := range deleteChan {
				if err := m.removeProviderPlacement(candidate); err != nil {
					errChan <- err
				}
			}
		})
	}

	for _, candidate := range candidates {
		deleteChan <- candidate
	}
	close(deleteChan)
	deleteWg.Wait()
	close(errChan)

	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (m *Manager) removeProviderPlacement(candidate providerRemovalCandidate) error {
	for range 8 {
		current, err := m.storage.Get(candidate.snapshot.InfoHash)
		if err != nil {
			return nil
		}
		if !storage.SameMainGeneration(candidate.snapshot, current) {
			return nil
		}
		placement := current.Providers[candidate.provider]
		if placement == nil || placement.ID != candidate.placementID {
			return nil
		}

		if len(current.Providers) == 1 {
			deleted, deleteErr := m.storage.DeleteIfCurrent(current)
			if deleteErr != nil {
				return fmt.Errorf("delete remote-only entry %s: %w", current.InfoHash, deleteErr)
			}
			if deleted {
				return nil
			}
			continue
		}

		_, present, mutateErr := m.storage.MutateEntrySnapshot(candidate.snapshot, func(entry *storage.Entry) (bool, error) {
			placement := entry.Providers[candidate.provider]
			if placement == nil || placement.ID != candidate.placementID {
				return false, nil
			}
			if len(entry.Providers) == 1 {
				return false, errProviderBecameLastPlacement
			}
			entry.RemoveProvider(candidate.provider, nil)
			return true, nil
		})
		if errors.Is(mutateErr, errProviderBecameLastPlacement) {
			continue
		}
		if errors.Is(mutateErr, storage.ErrStaleEntryGeneration) || !present {
			return nil
		}
		if mutateErr != nil {
			return fmt.Errorf("remove %s placement %s from %s: %w", candidate.provider, candidate.placementID, candidate.snapshot.InfoHash, mutateErr)
		}
		return nil
	}
	return fmt.Errorf("provider removal for %s did not stabilize", candidate.snapshot.InfoHash)
}

// processNewTorrents processes new torrents with worker pool and batch writing
func (m *Manager) processNewTorrents(provider string, refreshes []providerRefreshCandidate) error {
	workChan := make(chan providerRefreshCandidate, min(refreshWorkChanBuffer, len(refreshes)))
	errChan := make(chan error, len(refreshes))

	var processWg sync.WaitGroup
	var processed atomic.Int64
	totalTorrents := len(refreshes)

	// Scale workers based on torrent count, but cap to avoid overwhelming APIs
	workers := min(refreshMaxWorkers, max(refreshMinWorkers, len(refreshes)/10))

	for range workers {
		processWg.Go(func() {
			for candidate := range workChan {
				if _, err := m.processSyncTorrentSnapshot(candidate.remote, candidate.snapshot); err != nil {
					m.logger.Error().Err(err).Str("debrid", provider).Msgf("Failed to process torrent %s", candidate.remote.Id)
					errChan <- fmt.Errorf("process %s torrent %s: %w", provider, candidate.remote.InfoHash, err)
				}
				count := processed.Add(1)
				if count%50 == 0 {
					m.logger.Debug().Str("debrid", provider).Msgf("Processed %d / %d new torrents", count, totalTorrents)
				}
			}
		})
	}

	// Send torrents to workers
	for _, candidate := range refreshes {
		workChan <- candidate
	}

	close(workChan)
	processWg.Wait()
	close(errChan)

	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (m *Manager) processSyncTorrentSnapshot(t *types.Torrent, expected *storage.Entry) (*storage.Entry, error) {
	if t == nil || t.InfoHash == "" || t.Debrid == "" {
		return nil, fmt.Errorf("remote torrent is missing identity or provider")
	}
	// GetReader the debrid client
	client := m.ProviderClient(t.Debrid)
	if client == nil {
		return nil, fmt.Errorf("debrid client %s not found", t.Debrid)
	}
	if expected != nil && expected.InfoHash != t.InfoHash {
		placement := expected.Providers[t.Debrid]
		placementMatches := placement != nil && placement.ID != "" && placement.ID == t.Id
		contentMatches := strings.EqualFold(utils.ExtractInfoHash(expected.Magnet), t.InfoHash)
		if !placementMatches && !contentMatches {
			return nil, fmt.Errorf("remote torrent %s/%s does not belong to alias entry %s", t.Debrid, t.Id, expected.InfoHash)
		}
	}

	// Check if files are complete - only make API call if needed
	needsUpdate := len(t.Files) == 0 || !isComplete(t.Files)
	if needsUpdate {
		// This is the main bottleneck - API call per torrent
		// Consider: Could we batch UpdateTorrent calls? Depends on debrid API
		if err := client.UpdateTorrent(t); err != nil {
			return nil, err
		}
	}
	if t.Id == "" {
		return nil, fmt.Errorf("remote %s torrent %s is missing a placement id", t.Debrid, t.InfoHash)
	}

	// Serialize provider-ID adoption with folder alias cleanup. Remote calls
	// remain outside this mutex; the exact snapshot fence rejects a lifecycle
	// that changed while the provider was queried.
	m.copyEntryMu.Lock()
	defer m.copyEntryMu.Unlock()

	addedOn := t.Added
	if addedOn.IsZero() {
		addedOn = time.Now()
	}

	if expected == nil {
		var magnet *utils.Magnet
		if t.Magnet == nil || t.Magnet.Link == "" {
			magnet = utils.ConstructMagnet(t.InfoHash, t.Name)
		} else {
			magnet = t.Magnet
		}
		size := t.Size
		if size == 0 {
			size = t.Bytes
		}
		entry := &storage.Entry{
			Protocol:         config.ProtocolTorrent,
			InfoHash:         t.InfoHash,
			Name:             t.Name,
			OriginalFilename: t.OriginalFilename,
			Size:             size,
			Bytes:            size,
			Magnet:           magnet.Link,
			ActiveProvider:   t.Debrid,
			Providers:        make(map[string]*storage.ProviderEntry),
			Files:            make(map[string]*storage.File),
			Status:           t.Status,
			Progress:         t.Progress,
			Speed:            t.Speed,
			Seeders:          t.Seeders,
			IsComplete:       len(t.Files) > 0,
			Bad:              false,
			AddedOn:          addedOn,
			CreatedAt:        addedOn,
			UpdatedAt:        time.Now(),
		}
		applyProviderTorrent(entry, t)
		if err := m.storage.AddOrUpdate(entry); err != nil {
			return nil, fmt.Errorf("create remote torrent %s: %w", t.InfoHash, err)
		}
		return entry, nil
	}
	updated, present, err := m.storage.MutateEntrySnapshot(expected, func(current *storage.Entry) (bool, error) {
		applyProviderTorrent(current, t)
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("merge %s provider response for %s: %w", t.Debrid, t.InfoHash, err)
	}
	if !present {
		return nil, fmt.Errorf("%w for deleted main entry %s", storage.ErrStaleEntryGeneration, t.InfoHash)
	}
	return updated, nil
}

// refreshTorrent refreshes a single torrent from its active debrid
func (m *Manager) refreshTorrent(expected *storage.Entry) (*storage.Entry, error) {
	if expected == nil {
		return nil, fmt.Errorf("entry is nil")
	}

	if expected.ActiveProvider == "" {
		return expected, nil
	}

	client := m.ProviderClient(expected.ActiveProvider)
	if client == nil {
		return nil, fmt.Errorf("debrid client %s not found", expected.ActiveProvider)
	}

	placement := expected.GetActiveProvider()
	if placement == nil {
		return nil, fmt.Errorf("active placement %s not found", expected.ActiveProvider)
	}

	// GetReader updated torrent info from debrid
	debridTorrent, err := client.GetTorrent(placement.ID)
	if err != nil {
		return nil, err
	}
	if debridTorrent == nil {
		return nil, fmt.Errorf("provider %s returned an empty torrent", expected.ActiveProvider)
	}
	if debridTorrent.InfoHash == "" {
		debridTorrent.InfoHash = expected.InfoHash
	}
	if debridTorrent.Debrid == "" {
		debridTorrent.Debrid = expected.ActiveProvider
	}
	return m.processSyncTorrentSnapshot(debridTorrent, expected)
}

// refreshDebridDownloadLinks refreshes download links for a specific debrid service
func (m *Manager) refreshDebridDownloadLinks(ctx context.Context, debridName string, client debrid.Client) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	if client == nil {
		m.logger.Warn().Str("debrid", debridName).Msg("Provider client is nil, skipping download link refresh")
		return
	}

	if err := client.RefreshDownloadLinks(); err != nil {
		m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to refresh download links")
	}
}

// isComplete checks if all files in a torrent have download links
func isComplete(files map[string]types.File) bool {
	if len(files) == 0 {
		return false
	}
	for _, file := range files {
		if file.Link == "" {
			return false
		}
	}
	return true
}

package manager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

func (m *Manager) restoreActiveDownloadJobs() {
	entries := m.queue.ListFilter("", config.ProtocolAll, storage.EntryStateDownloading, nil, "", false)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].AddedOn.Before(entries[j].AddedOn)
	})

	// Existing active downloads reserve slots before queued imports are resumed.
	for _, entry := range entries {
		current, err := m.queue.RefreshSnapshot(entry)
		if err != nil {
			m.logger.Warn().Err(err).Str("infohash", entry.InfoHash).Msg("Failed to refresh active queue entry during restore")
			continue
		}
		if !current {
			continue
		}
		if entry.IsNZB() {
			job, rebuild, restoreErr := m.restoredActiveNZBJob(entry)
			if restoreErr != nil {
				entry.MarkAsError(restoreErr)
				if updateErr := m.queue.Update(entry); updateErr != nil {
					m.logger.Debug().Err(updateErr).Str("infohash", entry.InfoHash).Msg("Skipped stale NZB restore error update")
				}
				continue
			}
			if rebuild {
				continue
			}
			if job != nil {
				if err := m.SubmitJob(job); err != nil {
					entry.MarkAsError(err)
					_ = m.queue.Update(entry)
				}
			}
			continue
		}
		if entry.Status == debridTypes.TorrentStatusQueued || m.nzbNeedsReprocessing(entry) {
			continue
		}
		_ = m.SubmitJob(&Job{
			ID:           entry.InfoHash,
			Type:         jobTypeForEntry(entry),
			Entry:        entry,
			ResumeAction: entry.IsDownloading && entry.Status == debridTypes.TorrentStatusDownloaded,
		})
	}

	for _, entry := range entries {
		current, err := m.queue.RefreshSnapshot(entry)
		if err != nil {
			m.logger.Warn().Err(err).Str("infohash", entry.InfoHash).Msg("Failed to refresh queued entry during restore")
			continue
		}
		if !current {
			continue
		}
		if entry.Status != debridTypes.TorrentStatusQueued && !m.nzbNeedsReprocessing(entry) {
			continue
		}
		job, err := m.rebuildQueuedJob(entry)
		if err != nil {
			entry.MarkAsError(err)
			if updateErr := m.queue.Update(entry); updateErr != nil {
				m.logger.Debug().Err(updateErr).Str("infohash", entry.InfoHash).Msg("Skipped stale restore error update")
			}
			continue
		}
		if job.DebridTorrent == nil && job.NZBMeta == nil {
			entry.Status = debridTypes.TorrentStatusQueued
		}
		if err := m.queue.Update(entry); err != nil {
			m.logger.Debug().Err(err).Str("infohash", entry.InfoHash).Msg("Stopped stale queued-job restore")
			continue
		}
		job.Entry = entry
		if err := m.SubmitJob(job); err != nil {
			entry.MarkAsError(err)
			if updateErr := m.queue.Update(entry); updateErr != nil {
				m.logger.Debug().Err(updateErr).Str("infohash", entry.InfoHash).Msg("Skipped stale restore submission error")
			}
		}
	}
}

func (m *Manager) restoredActiveNZBJob(entry *storage.Entry) (*Job, bool, error) {
	if entry.IsDownloading && entry.Status == debridTypes.TorrentStatusDownloaded {
		return &Job{ID: entry.InfoHash, Type: JobTypeNZB, Entry: entry, ResumeAction: true}, false, nil
	}
	if entry.Status == debridTypes.TorrentStatusQueued {
		return nil, true, nil
	}
	meta, err := m.usenet.GetNZBHeader(entry.InfoHash)
	if err != nil {
		if errors.Is(err, usenet.ErrNZBNotFound) && entry.Magnet != "" {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("load active NZB metadata: %w", err)
	}
	if meta == nil {
		if entry.Magnet != "" {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("active NZB metadata is missing")
	}
	if entry.NZBGeneration != "" && meta.Generation != "" && entry.NZBGeneration != meta.Generation {
		return nil, true, nil
	}
	switch meta.Status {
	case usenet.NZBStatusCompleted:
		generation, err := m.ensureNZBGeneration(entry)
		if err != nil {
			return nil, false, err
		}
		if meta.Generation == "" {
			meta, err = m.usenet.GetNZBHeader(entry.InfoHash)
			if err != nil {
				return nil, false, err
			}
		}
		if meta.Generation != generation {
			return nil, false, fmt.Errorf("%w: queued generation %q, metadata generation %q", usenet.ErrStaleNZBGeneration, generation, meta.Generation)
		}
		return &Job{ID: entry.InfoHash, Type: JobTypeNZB, Entry: entry, NZBMeta: meta, ResumeExisting: true}, false, nil
	case usenet.NZBStatusFailed:
		return nil, false, fmt.Errorf("NZB processing failed: %s", meta.FailMessage)
	case usenet.NZBStatusParsing, usenet.NZBStatusDownloading, usenet.NZBStatusPending:
		return nil, true, nil
	default:
		return nil, false, fmt.Errorf("unknown NZB status during restore: %s", meta.Status)
	}
}

func jobTypeForEntry(entry *storage.Entry) JobType {
	if entry != nil && entry.IsNZB() {
		return JobTypeNZB
	}
	return JobTypeTorrent
}

func (m *Manager) nzbNeedsReprocessing(entry *storage.Entry) bool {
	if entry == nil || !entry.IsNZB() || m.usenet == nil {
		return false
	}
	meta, err := m.usenet.GetNZBHeader(entry.InfoHash)
	if err != nil || meta == nil {
		return entry.Magnet != ""
	}
	if entry.NZBGeneration != "" && meta.Generation != "" && meta.Generation != entry.NZBGeneration {
		return true
	}
	return meta.Status == usenet.NZBStatusParsing || meta.Status == usenet.NZBStatusDownloading || meta.Status == usenet.NZBStatusPending
}

func (m *Manager) rebuildQueuedJob(entry *storage.Entry) (*Job, error) {
	if entry.IsNZB() {
		return m.rebuildQueuedNZBJob(entry)
	}
	return m.rebuildQueuedTorrentJob(entry)
}

func (m *Manager) rebuildQueuedTorrentJob(entry *storage.Entry) (*Job, error) {
	if entry.ActiveProvider != "" && entry.GetActiveProvider() != nil {
		return &Job{
			ID:             entry.InfoHash,
			Type:           JobTypeTorrent,
			Entry:          entry,
			ResumeExisting: true,
		}, nil
	}

	magnet, err := utils.GetMagnetInfo(entry.Magnet, m.config.AlwaysRmTrackerUrls)
	if err != nil {
		magnet = utils.ConstructMagnet(entry.InfoHash, entry.Name)
	}

	downloadUncached := entry.DownloadUncached
	req := NewTorrentRequest(
		entry.ActiveProvider,
		downloadFolderForEntry(m.config.DownloadFolder, entry),
		magnet,
		m.arr.GetOrCreate(entry.Category),
		entry.Action,
		&downloadUncached,
		entry.CallbackURL,
		ImportTypeAPI,
		entry.SkipMultiSeason,
	)
	req.Id = entry.InfoHash
	job := NewJob(JobTypeTorrent, req)
	job.ID = entry.InfoHash
	job.Entry = entry
	return job, nil
}

func (m *Manager) rebuildQueuedNZBJob(entry *storage.Entry) (*Job, error) {
	if m.usenet == nil {
		return nil, fmt.Errorf("usenet is not configured")
	}
	sourcePath := entry.Magnet
	existingMeta, metaErr := m.usenet.GetNZBHeader(entry.InfoHash)
	if metaErr != nil && !errors.Is(metaErr, usenet.ErrNZBNotFound) {
		return nil, fmt.Errorf("load queued NZB metadata: %w", metaErr)
	}
	if entry.NZBGeneration == "" {
		if existingMeta != nil && existingMeta.Generation != "" {
			entry.NZBGeneration = existingMeta.Generation
		} else {
			entry.NZBGeneration = usenet.NewNZBGeneration()
		}
		if err := m.queue.Update(entry); err != nil {
			return nil, fmt.Errorf("persist restored NZB generation: %w", err)
		}
	}
	if existingMeta != nil {
		if existingMeta.Generation != "" && existingMeta.Generation != entry.NZBGeneration {
			return nil, fmt.Errorf("%w: queued generation %q, metadata generation %q", usenet.ErrStaleNZBGeneration, entry.NZBGeneration, existingMeta.Generation)
		}
		if existingMeta.Generation == entry.NZBGeneration && existingMeta.Status == usenet.NZBStatusCompleted {
			if err := m.commitRestoredNZBMetadata(entry, existingMeta); err != nil {
				return nil, err
			}
			return &Job{
				ID:             entry.InfoHash,
				Type:           JobTypeNZB,
				Entry:          entry,
				NZBMeta:        existingMeta,
				ResumeExisting: true,
			}, nil
		}
		if existingMeta.Path != "" {
			sourcePath = existingMeta.Path
		}
	}
	if sourcePath == "" {
		return nil, fmt.Errorf("NZB source is unavailable for generation %s", entry.NZBGeneration)
	}
	content, err := os.ReadFile(sourcePath)
	if err != nil {
		return nil, err
	}

	name := entry.OriginalFilename
	if name == "" {
		name = entry.Name
	}
	meta, groups, err := m.usenet.ParseWithGeneration(context.Background(), entry.InfoHash, entry.NZBGeneration, name, content, entry.Category)
	if err != nil {
		return nil, fmt.Errorf("usenet parse failed: %w", err)
	}
	if meta.Generation != entry.NZBGeneration {
		return nil, fmt.Errorf("restored NZB generation %q does not match queue generation %q", meta.Generation, entry.NZBGeneration)
	}
	if err := m.commitRestoredNZBMetadata(entry, meta); err != nil {
		return nil, err
	}

	req := NewNZBRequest(
		meta.Name,
		downloadFolderForEntry(m.config.DownloadFolder, entry),
		content,
		m.arr.GetOrCreate(entry.Category),
		entry.Action,
		entry.CallbackURL,
		ImportTypeSABnzbd,
		entry.SkipMultiSeason,
	)
	req.Id = entry.InfoHash
	job := NewJob(JobTypeNZB, req)
	job.ID = entry.InfoHash
	job.Entry = entry
	job.NZBMeta = meta
	job.NZBGroups = groups
	return job, nil
}

func (m *Manager) commitRestoredNZBMetadata(entry *storage.Entry, meta *storage.NZB) error {
	stagedPath := entry.Magnet
	entry.Magnet = ""
	entry.Name = meta.Name
	entry.OriginalFilename = meta.Name
	entry.Size = meta.TotalSize
	entry.Bytes = meta.TotalSize
	entry.Status = debridTypes.TorrentStatusDownloading
	entry.ActiveProvider = "usenet"
	if entry.AddUsenetProvider(meta) == nil {
		entry.Magnet = stagedPath
		return fmt.Errorf("failed to add restored Usenet provider metadata")
	}

	// Persist the source-free state before unlinking. A crash after this commit
	// leaves an unreferenced managed .queued file, which startup reconciliation
	// safely removes; the reverse order leaves an unrecoverable live reference.
	if err := m.queue.Update(entry); err != nil {
		entry.Magnet = stagedPath
		return fmt.Errorf("persist restored NZB metadata: %w", err)
	}
	m.usenet.RemoveStagedNZB(stagedPath)
	return nil
}

func downloadFolderForEntry(fallback string, entry *storage.Entry) string {
	if entry != nil && entry.SavePath != "" {
		return filepath.Dir(entry.SavePath)
	}
	return fallback
}

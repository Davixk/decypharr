package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

//go:fix inline
func ptrTime(t time.Time) *time.Time {
	return new(t)
}

// This is in-charge of moving torrents between different debrid services

// SwitchTorrent moves a torrent from one debrid to another
func (m *Manager) SwitchTorrent(ctx context.Context, infohash, target string, keepOld, waitComplete bool) (*storage.SwitcherJob, error) {
	// GetReader the entry
	entry, err := m.GetEntry(infohash)
	if err != nil {
		return nil, fmt.Errorf("failed to get entry: %w", err)
	}

	// Check if already on target debrid
	if entry.ActiveProvider == target {
		return nil, storage.ErrAlreadyOnDebrid
	}

	// Need to actually migrate - create job
	job := &storage.SwitcherJob{
		ID:             uuid.New().String(),
		InfoHash:       infohash,
		SourceProvider: entry.ActiveProvider,
		TargetProvider: target,
		Status:         storage.SwitcherStatusPending,
		Progress:       0,
		CreatedAt:      time.Now(),
		KeepOld:        keepOld,
		WaitComplete:   waitComplete,
	}

	// Store job
	m.migrationJobs.Store(job.ID, job)

	// Start migration in background
	go m.executeMigration(job, entry)

	return job, nil
}

// executeMigration performs the actual torrent migration - COMPLETE IMPLEMENTATION
func (m *Manager) executeMigration(job *storage.SwitcherJob, torrent *storage.Entry) {
	m.logger.Info().
		Str("job_id", job.ID).
		Str("torrent", torrent.Name).
		Str("source", job.SourceProvider).
		Str("target", job.TargetProvider).
		Msg("Starting torrent migration")
	job.Status = storage.SwitcherStatusInProgress

	if m.ProviderClient(job.TargetProvider) == nil {
		job.Status = storage.SwitcherStatusFailed
		job.Error = fmt.Sprintf("target debrid %s not found", job.TargetProvider)
		job.CompletedAt = new(time.Now())
		return
	}

	var sourcePlacement *storage.ProviderEntry
	if placement := torrent.Providers[job.SourceProvider]; placement != nil {
		copyPlacement := *placement
		sourcePlacement = &copyPlacement
	}

	job.Progress = 10

	success, err := m.fixer.MoveTorrent(torrent, job.TargetProvider, false) // false = don't force re-download

	if !success {
		job.Status = storage.SwitcherStatusFailed
		job.Error = fmt.Sprintf("failed to move torrent to target debrid: %v", err)
		job.CompletedAt = new(time.Now())
		m.logger.Error().
			Err(err).
			Str("job_id", job.ID).
			Msg("Failed to move torrent to target debrid")
		return
	}
	if err != nil {
		// The target is already committed. A cleanup warning must not report the
		// ownership move itself as failed or trigger another target submission.
		m.logger.Warn().Err(err).Str("job_id", job.ID).Msg("Migration target committed with cleanup warning")
		job.Error = err.Error()
	}

	if !job.KeepOld && sourcePlacement != nil && sourcePlacement.ID != "" {
		updated, present, cleanupErr := func() (*storage.Entry, bool, error) {
			m.copyEntryMu.Lock()
			defer m.copyEntryMu.Unlock()
			return m.storage.MutateEntrySnapshot(torrent, func(current *storage.Entry) (bool, error) {
				placement := current.Providers[job.SourceProvider]
				if placement == nil {
					return false, nil
				}
				if placement.ID != sourcePlacement.ID {
					return false, fmt.Errorf("source placement changed from %s to %s", sourcePlacement.ID, placement.ID)
				}
				if current.ActiveProvider != job.TargetProvider || current.Providers[job.TargetProvider] == nil {
					return false, fmt.Errorf("target provider %s is no longer active", job.TargetProvider)
				}
				// copyEntryMu serializes alias creation and last-reference cleanup;
				// the main-row generation lock additionally fences same-hash replacement.
				if err := m.removeProviderPlacementIfUnreferenced(current.InfoHash, placement); err != nil {
					return false, err
				}
				for key, candidate := range current.Providers {
					if candidate != nil && candidate.Provider == job.SourceProvider && candidate.ID == sourcePlacement.ID {
						delete(current.Providers, key)
					}
				}
				return true, nil
			})
		}()
		if cleanupErr != nil || !present {
			if cleanupErr == nil {
				cleanupErr = fmt.Errorf("entry was deleted before source cleanup")
			}
			job.Status = storage.SwitcherStatusFailed
			job.Error = fmt.Sprintf("target committed but failed to remove source placement: %v", cleanupErr)
			job.CompletedAt = new(time.Now())
			m.logger.Error().Err(cleanupErr).Str("job_id", job.ID).Msg("Migration source cleanup failed")
			return
		}
		copyProviderView(torrent, updated)
	}

	if current, getErr := m.storage.Get(torrent.InfoHash); getErr != nil {
		job.Status = storage.SwitcherStatusFailed
		job.Error = fmt.Sprintf("failed to verify migrated torrent: %v", getErr)
		m.logger.Error().Err(getErr).Msg("Failed to verify torrent after migration")
	} else {
		copyProviderView(torrent, current)
		job.Status = storage.SwitcherStatusCompleted
		job.Progress = 100
		if m.entry != nil {
			m.RefreshEntries(false)
		}
	}

	job.CompletedAt = new(time.Now())

	m.logger.Info().
		Str("job_id", job.ID).
		Str("status", string(job.Status)).
		Msg("Migration completed")
}

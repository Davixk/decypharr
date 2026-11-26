package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

//go:fix inline
func ptrTime(t time.Time) *time.Time {
	return new(t)
}

var (
	// ErrSwitchTargetUnknown reports a switch to a provider that is not configured.
	ErrSwitchTargetUnknown = errors.New("target provider is not configured")
	// ErrSwitchUnsupported reports an entry that cannot move between providers.
	ErrSwitchUnsupported = errors.New("entry cannot be moved between providers")
)

// This is in-charge of moving torrents between different debrid services

// SwitchTorrent moves a torrent from one debrid to another.
//
// It keeps the SAME entry — same infohash, same Name and OriginalFilename — so
// the mount folder, and every library symlink into it, survives the move.
// commitMove only adds the target placement and its files; it never renames.
//
// ⚠️ CACHED CONTENT ONLY, IN PRACTICE. commitMove refuses to activate a target
// copy that is not `downloaded` after the provider's status poll, and deletes
// the copy it just added. An uncached release therefore fails the job and the
// entry stays exactly where it was — which is the safe outcome, not a bug.
// waitComplete is recorded on the job but nothing reads it; do not offer it as a
// knob until something does.
//
// Validation that can be answered locally happens HERE, before a job exists, so
// a caller is told synchronously rather than finding a failed job later.
func (m *Manager) SwitchTorrent(ctx context.Context, infohash, target string, keepOld, waitComplete bool) (*storage.SwitcherJob, error) {
	if exists, _ := m.EntryExists(infohash); !exists {
		return nil, fmt.Errorf("%w: %s", storage.ErrEntryNotFound, infohash)
	}
	entry, err := m.GetEntry(infohash)
	if err != nil {
		return nil, fmt.Errorf("failed to get entry: %w", err)
	}

	// Check if already on target debrid
	if entry.ActiveProvider == target {
		return nil, storage.ErrAlreadyOnDebrid
	}
	if m.ProviderClient(target) == nil {
		return nil, fmt.Errorf("%w: %s", ErrSwitchTargetUnknown, target)
	}
	if !entry.CanBeMoved() {
		return nil, fmt.Errorf("%w: %s is not a torrent", ErrSwitchUnsupported, infohash)
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

	// Snapshot BEFORE the goroutine owns job: it mutates the struct as it goes,
	// so handing the caller the live pointer would be a data race.
	accepted := snapshotJob(job)
	m.publishJob(job)

	// Start migration in background
	go m.executeMigration(job, entry)

	return accepted, nil
}

// snapshotJob returns an independent copy of job, deep enough that nothing the
// migration goroutine later writes can reach it.
func snapshotJob(job *storage.SwitcherJob) *storage.SwitcherJob {
	snap := *job
	if job.CompletedAt != nil {
		completed := *job.CompletedAt
		snap.CompletedAt = &completed
	}
	return &snap
}

// publishJob stores an immutable snapshot of job.
//
// 🔴 THE STORED VALUE IS NEVER MUTATED AFTER IT IS STORED. executeMigration
// writes Status, Progress, Error and CompletedAt on its own goroutine while
// GetMigrationJob and the stats Range read jobs from request goroutines. Storing
// the live pointer made every such read a data race; it went unnoticed only
// because nothing called SwitchTorrent until an API route was added. Readers now
// only ever see a finished snapshot.
func (m *Manager) publishJob(job *storage.SwitcherJob) {
	m.migrationJobs.Store(job.ID, snapshotJob(job))
}

// executeMigration performs the actual torrent migration - COMPLETE IMPLEMENTATION
func (m *Manager) executeMigration(job *storage.SwitcherJob, torrent *storage.Entry) {
	// Every return path below publishes the job's final state.
	defer m.publishJob(job)

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
	m.publishJob(job)

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

package manager

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// revivableErrorSignatures identify LastError values recorded for
// infrastructure/availability failures — the classes that are safe to revive
// because the content itself was never proven bad. A parse-time probe wrapped
// in ErrArticlesUnavailable ("articles missing on provider") is included:
// genuine misses are still capped by ErrorCount. The archive-processing
// signatures cover the dominant 2026-07-19 incident cohort: 1,891 entries
// were stamped 'failed to process nzb: failed to process NZB archives: no
// valid files found in NZB' when every file group was dropped on a collapsed
// substrate and the parser swallowed the real cause behind the generic
// verdict (fixed since, but the parked rows still carry that text). Those
// entries parsed successfully at add-time, so "invalid NZB" was never a
// credible verdict for them.
var revivableErrorSignatures = []string{
	"articles missing on provider",
	"availability probe failed",
	"timeout waiting for mount files",
	"failed to process NZB archives",
	"no valid files found in NZB",
}

// mountTimeoutSignature marks failures where the download itself completed
// and only the post-download action timed out waiting for mount visibility.
const mountTimeoutSignature = "timeout waiting for mount files"

// reviveSweepLimit caps how many entries one periodic revival sweep resubmits,
// so a backlog drains progressively instead of stampeding the action gate and
// job queue. Var for tests.
var reviveSweepLimit = 50

// reviveSweepInterval is how often the runtime revival job runs.
const reviveSweepInterval = "5m"

func (m *Manager) retryLimit() int {
	if m.config != nil && m.config.Retries > 0 {
		return m.config.Retries
	}
	return 3
}

func matchesRevivableSignature(lastError string) bool {
	for _, signature := range revivableErrorSignatures {
		if strings.Contains(lastError, signature) {
			return true
		}
	}
	return false
}

// isRevivableErrorEntry reports whether entry may be reset back into the
// active pipeline. force (explicit user retry via SABnzbd mode=retry) skips
// the ErrorCount cap and signature match but never revives Bad entries or
// entries that are not terminal-error NZBs.
func (m *Manager) isRevivableErrorEntry(entry *storage.Entry, force bool) bool {
	if entry == nil || !entry.IsNZB() || entry.State != storage.EntryStateError || entry.Bad {
		return false
	}
	if force {
		return true
	}
	if entry.ErrorCount >= m.retryLimit() {
		return false
	}
	return matchesRevivableSignature(entry.LastError)
}

// reviveErrorEntry atomically resets a terminal-error NZB entry back into the
// active pipeline, mirroring the offline recovery tool:
//   - "timeout waiting for mount files" failures completed their download and
//     only the post-download action wedged, so they get the durably claimed
//     mid-action triple (State=downloading, Status=downloaded, IsDownloading)
//     and resume through the action gate;
//   - every other revivable failure re-enters the queued path
//     (State=downloading, Status=queued), where restore pass-2 or a
//     RebuildQueued job re-parses from the staged source or resumes completed
//     metadata.
//
// ErrorCount and LastError are deliberately untouched: the count grew when the
// failure was recorded, so config `retries` naturally caps revival attempts.
// The returned bool reports whether the reset was applied.
func (m *Manager) reviveErrorEntry(infohash string, force bool) (*storage.Entry, bool, bool, error) {
	applied := false
	resumeAction := false
	updated, err := m.queue.Mutate(infohash, func(current *storage.Entry) bool {
		if !m.isRevivableErrorEntry(current, force) {
			return false
		}
		applied = true
		current.State = storage.EntryStateDownloading
		if strings.Contains(current.LastError, mountTimeoutSignature) {
			current.Status = debridTypes.TorrentStatusDownloaded
			current.IsDownloading = true
			resumeAction = true
		} else {
			current.Status = debridTypes.TorrentStatusQueued
			current.IsDownloading = false
		}
		current.UpdatedAt = time.Now()
		return true
	})
	if err != nil {
		return nil, false, false, err
	}
	return updated, resumeAction, applied, nil
}

// resubmitRevivedEntry routes a freshly revived entry back into processing at
// runtime. Boot restore skips this: its own passes pick up both shapes.
func (m *Manager) resubmitRevivedEntry(entry *storage.Entry, resumeAction bool) {
	if entry == nil {
		return
	}
	if resumeAction {
		if m.isActionInflight(entry.InfoHash) || m.queue.HasActionLease(entry.InfoHash) {
			return
		}
		m.submitResumeAction(entry)
		return
	}
	// Queued NZB entries have no other runtime pickup path; rebuild through
	// the job queue (re-parse from staged source or resume completed meta).
	job := &Job{
		ID:            entry.InfoHash,
		Type:          JobTypeNZB,
		Entry:         entry,
		RebuildQueued: true,
		CreatedAt:     time.Now(),
	}
	if err := m.SubmitJob(job); err != nil {
		m.logger.Warn().Err(err).Str("infohash", entry.InfoHash).Msg("Failed to submit revived NZB entry for processing")
	}
}

// reviveErrorEntries scans terminal-error NZB entries and revives every one
// whose failure signature is infrastructure/availability class and whose
// ErrorCount is still below the configured retries. limit == 0 means
// unlimited (boot restore, explicit retry_all); the periodic sweep passes
// reviveSweepLimit. When resubmit is false the caller owns pickup (boot
// restore's passes run right after).
func (m *Manager) reviveErrorEntries(ctx context.Context, limit int, resubmit bool) int {
	if m.queue == nil {
		return 0
	}
	entries := m.queue.ListFilter("", config.ProtocolNZB, storage.EntryStateError, nil, "added_on", false)
	revived := 0
	for _, entry := range entries {
		if ctx != nil && ctx.Err() != nil {
			break
		}
		if limit > 0 && revived >= limit {
			break
		}
		if !m.isRevivableErrorEntry(entry, false) {
			continue
		}
		updated, resumeAction, applied, err := m.reviveErrorEntry(entry.InfoHash, false)
		if err != nil {
			m.logger.Debug().Err(err).Str("infohash", entry.InfoHash).Msg("Skipped revival of failed entry")
			continue
		}
		if !applied {
			continue
		}
		revived++
		m.logger.Info().
			Str("infohash", updated.InfoHash).
			Str("name", updated.Name).
			Int("error_count", updated.ErrorCount).
			Str("last_error", updated.LastError).
			Bool("resume_action", resumeAction).
			Msg("Revived failed entry for retry")
		if resubmit {
			m.resubmitRevivedEntry(updated, resumeAction)
		}
	}
	if revived > 0 {
		m.logger.Info().Int("revived", revived).Msg("Failed-entry revival sweep completed")
	}
	return revived
}

// ReviveErrorEntry revives one failed NZB entry on explicit user intent
// (SABnzbd mode=retry with a value). It bypasses the ErrorCount/signature
// eligibility gates but never revives Bad entries or non-error entries.
func (m *Manager) ReviveErrorEntry(nzoID string) error {
	if strings.TrimSpace(nzoID) == "" {
		return fmt.Errorf("nzo_id is required")
	}
	updated, resumeAction, applied, err := m.reviveErrorEntry(nzoID, true)
	if err != nil {
		return err
	}
	if !applied {
		return fmt.Errorf("entry %s is not a failed NZB eligible for retry", nzoID)
	}
	m.logger.Info().
		Str("infohash", updated.InfoHash).
		Str("name", updated.Name).
		Bool("resume_action", resumeAction).
		Msg("Revived failed entry via retry request")
	m.resubmitRevivedEntry(updated, resumeAction)
	return nil
}

// ReviveEligibleErrorEntries revives every eligible failed NZB entry (SABnzbd
// mode=retry_all) and returns the number revived.
func (m *Manager) ReviveEligibleErrorEntries() int {
	return m.reviveErrorEntries(context.Background(), 0, true)
}

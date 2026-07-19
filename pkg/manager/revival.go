package manager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
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

// isDoomedQueuedRebuild reports whether reviving entry into the queued path
// would only churn a rebuild that is GUARANTEED to fail:
// rebuildQueuedNZBJob needs either completed metadata to resume or a readable
// NZB source to re-parse. When the durable meta is not completed and carries
// no parsed segments (empty or segmentless Files — the on-disk shape the
// 2026-07-19 quick-parse persist left behind before markAsFailed cemented it)
// and no source survives (entry.Magnet, meta.Path — the two paths
// rebuildQueuedNZBJob actually resolves — or a staged
// usenet/nzbs/<id>.*.source/.queued artifact), the rebuild can only fail
// again: ErrorCount would grow and the forensic LastError the offline
// recovery tool selects on would be overwritten.
//
// Mount-timeout revivals resume the post-download action instead of
// rebuilding, so callers must not apply this check to them. Any uncertainty
// (usenet not configured, undecodable meta, stat errors) keeps the entry
// eligible exactly as before.
func (m *Manager) isDoomedQueuedRebuild(entry *storage.Entry) bool {
	if m.usenet == nil || entry == nil {
		return false
	}
	meta, err := m.usenet.GetNZBHeader(entry.InfoHash)
	if err != nil && !errors.Is(err, usenet.ErrNZBNotFound) {
		return false
	}
	if meta != nil {
		if meta.Status == usenet.NZBStatusCompleted {
			// Completed metadata resumes without any source; eligible as today.
			return false
		}
		hasSegments, segErr := m.usenet.NZBStorage().HasSegmentedFiles(entry.InfoHash)
		if segErr != nil || hasSegments {
			// Populated segment maps stay eligible (conservative: only the
			// empty/segmentless shape is provably unrebuildable offline).
			return false
		}
	}
	// Mirror rebuildQueuedNZBJob's source resolution (entry.Magnet, overridden
	// by meta.Path), plus the staged .source/.queued artifacts a claim scan
	// could re-import. Any surviving file keeps the entry eligible.
	if usableNZBSourceFile(entry.Magnet) {
		return false
	}
	if meta != nil && usableNZBSourceFile(meta.Path) {
		return false
	}
	nzbDir := filepath.Join(config.GetMainPath(), "usenet", "nzbs")
	for _, suffix := range []string{".source", ".queued"} {
		matches, globErr := filepath.Glob(filepath.Join(nzbDir, entry.InfoHash+".*"+suffix))
		if globErr == nil && len(matches) > 0 {
			return false
		}
	}
	return true
}

func usableNZBSourceFile(path string) bool {
	if path == "" {
		return false
	}
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

// reviveErrorEntries scans terminal-error NZB entries and revives every one
// whose failure signature is infrastructure/availability class and whose
// ErrorCount is still below the configured retries. limit == 0 means
// unlimited (boot restore, explicit retry_all); the periodic sweep passes
// reviveSweepLimit. When resubmit is false the caller owns pickup (boot
// restore's passes run right after).
//
// Entries whose queued rebuild is guaranteed to fail (see
// isDoomedQueuedRebuild) are skipped — reviving them cannot succeed and each
// doomed cycle would increment ErrorCount and overwrite the forensic
// LastError. The skip WARNs once per entry per boot (deduped via
// revivalDoomWarned) and names the recourse. Explicit single-entry retries
// (ReviveErrorEntry, SABnzbd mode=retry) still bypass this via force.
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
		if !strings.Contains(entry.LastError, mountTimeoutSignature) && m.isDoomedQueuedRebuild(entry) {
			if _, warned := m.revivalDoomWarned.LoadOrStore(entry.InfoHash, struct{}{}); !warned {
				m.logger.Warn().
					Str("infohash", entry.InfoHash).
					Str("name", entry.Name).
					Str("last_error", entry.LastError).
					Int("error_count", entry.ErrorCount).
					Msg("Skipping revival: rebuild is guaranteed to fail (metadata has no parsed segments and no NZB source survives on disk); unrecoverable offline; requires re-grab")
			}
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

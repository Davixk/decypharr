package manager

import (
	"context"
	"os"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// missingDownloadSweepLimit caps how many orphaned completed entries one sweep
// resets, so a mass category-directory wipe recovers progressively instead of
// stampeding the action gate and the mount. Var for tests.
var missingDownloadSweepLimit = 100

// reconcileMissingDownloads recovers completed (State pausedUP) entries whose
// on-disk DownloadPath() has disappeared — the fingerprint of the category-dir
// data-loss incident, where deleting one malformed entry wiped every sibling's
// symlinks under downloads/<category>. Each recoverable entry is reset back to
// the durable post-download claim shape (State downloading, Status downloaded,
// IsDownloading) so the existing claimed-action path re-creates its symlinks
// from the still-intact provider/mount content.
//
// Selection is conservative:
//   - only pausedUP (completed) rows are considered;
//   - the Name must be usable (non-collapsing) so the recreated DownloadPath is
//     a real child directory — empty/"." names can only be re-grabbed, never
//     re-symlinked, so they are skipped;
//   - DownloadPath() must be absent on disk (os.Stat reports NotExist) — a
//     present folder means the symlinks are intact and there is nothing to
//     recover; any other stat error is treated conservatively and skipped.
//
// limit caps resets per sweep (0 = unlimited). When resubmit is false the
// caller owns pickup: boot restore resets before it lists EntryStateDownloading
// so its own passes resume the claimed shape. When true (the periodic sweep)
// the reset is resubmitted through the gated, idempotent submitResumeAction.
//
// No-thrash/idempotency: a reset moves the row out of pausedUP, so a later
// sweep cannot re-select it; if the resumed action fails the row lands in error
// state (still not pausedUP). submitResumeAction's in-flight registration, the
// live-lease check, and the queue's generation fencing prevent double
// submission across concurrent passes.
func (m *Manager) reconcileMissingDownloads(ctx context.Context, limit int, resubmit bool) int {
	if m.queue == nil {
		return 0
	}
	entries := m.queue.ListFilter("", config.ProtocolAll, storage.EntryStatePausedUP, nil, "added_on", false)
	recovered := 0
	for _, entry := range entries {
		if ctx != nil && ctx.Err() != nil {
			break
		}
		if limit > 0 && recovered >= limit {
			break
		}
		if !utils.IsUsableName(entry.Name) {
			// Empty/collapsed name: DownloadPath() would resolve to the category
			// directory. Such entries are for deletion + re-grab, never re-symlink.
			continue
		}
		downloadPath := entry.DownloadPath()
		if downloadPath == "" {
			continue
		}
		if _, err := os.Stat(downloadPath); err == nil {
			continue // folder present: symlinks intact, nothing to recover
		} else if !os.IsNotExist(err) {
			continue // stat failed for another reason (I/O, permissions): be conservative
		}
		if m.isActionInflight(entry.InfoHash) || m.queue.HasActionLease(entry.InfoHash) {
			continue
		}
		updated, applied, err := m.resetMissingDownload(entry.InfoHash)
		if err != nil {
			m.logger.Debug().Err(err).Str("infohash", entry.InfoHash).Msg("Skipped recovery of orphaned completed entry")
			continue
		}
		if !applied {
			continue
		}
		recovered++
		m.logger.Warn().
			Str("infohash", updated.InfoHash).
			Str("name", updated.Name).
			Str("download_path", downloadPath).
			Msg("Recovering completed entry whose download folder disappeared; re-running the post-download action to rebuild its symlinks")
		if resubmit {
			m.submitResumeAction(updated)
		}
	}
	if recovered > 0 {
		m.logger.Info().Int("recovered", recovered).Msg("Missing-download recovery sweep completed")
	}
	return recovered
}

// resetMissingDownload atomically flips a completed (pausedUP) entry that lost
// its download folder back into the durable post-download claim shape. It
// re-validates under the queue mutation lock (still pausedUP, still a usable
// name) so a concurrent deletion or a racing sweep cannot double-apply it.
func (m *Manager) resetMissingDownload(infohash string) (*storage.Entry, bool, error) {
	applied := false
	updated, err := m.queue.Mutate(infohash, func(current *storage.Entry) bool {
		if current.State != storage.EntryStatePausedUP || !utils.IsUsableName(current.Name) {
			return false
		}
		applied = true
		current.State = storage.EntryStateDownloading
		current.Status = debridTypes.TorrentStatusDownloaded
		current.IsDownloading = true
		current.IsComplete = false
		current.UpdatedAt = time.Now()
		return true
	})
	if err != nil {
		return nil, false, err
	}
	return updated, applied, nil
}

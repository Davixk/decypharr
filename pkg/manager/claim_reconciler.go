package manager

import (
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// orphanedClaimGrace is how long a durably claimed post-download action may go
// without a queue write before the reconciler may resubmit it. Every queue
// write (including ClaimPostDownload itself) stamps UpdatedAt, so a fresh
// claim always gets at least this long to reach BeginAction before it can be
// considered orphaned. Var for tests.
var orphanedClaimGrace = 2 * time.Minute

// reconcileOrphanedClaims finds queue rows whose post-download action was
// durably claimed (State downloading + Status downloaded + IsDownloading) but
// that no live goroutine owns anymore. Such rows are invisible to the periodic
// scheduler (it skips IsDownloading entries) and to the SAB history (which
// only projects terminal states), and before this reconciler they could only
// be recovered by a restart's restore pass. Resubmitting them through the
// action gate self-heals at runtime.
//
// Idempotency: submitResumeAction registers the hash in the in-process
// actionInflight table before spawning, and every live action registers there
// for its whole lifetime (claim -> gate wait -> BeginAction lease -> release),
// so a claim with either a live lease or an in-flight registration is never
// resubmitted, and concurrent reconcile passes cannot double-submit. The
// resumed action itself re-validates the claim via RefreshSnapshot and runs
// under the queue's generation fencing.
func (m *Manager) reconcileOrphanedClaims() {
	if m.queue == nil {
		return
	}
	cutoff := time.Now().Add(-orphanedClaimGrace)
	entries := m.queue.ListFilter("", config.ProtocolAll, storage.EntryStateDownloading, nil, "", false)
	for _, entry := range entries {
		if entry.Status != debridTypes.TorrentStatusDownloaded || !entry.IsDownloading {
			continue
		}
		if !entry.UpdatedAt.Before(cutoff) {
			continue
		}
		if m.isActionInflight(entry.InfoHash) {
			// Claimed in this process: executing or waiting on the action gate.
			continue
		}
		if m.queue.HasActionLease(entry.InfoHash) {
			// A live action lease owns this entry.
			continue
		}
		m.logger.Warn().
			Str("infohash", entry.InfoHash).
			Str("name", entry.Name).
			Time("updated_at", entry.UpdatedAt).
			Msg("Reconciling orphaned post-download claim; resubmitting action through the gate")
		m.submitResumeAction(entry)
	}
}

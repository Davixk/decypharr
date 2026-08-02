package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// stallPruneSweepLimit caps how many stalled entries one pass may delete.
// Deletion is destructive and irreversible, so a misconfigured window drains
// progressively and visibly rather than wiping an account in one tick — the
// same discipline as the repair sweep's per-run deletion cap.
const stallPruneSweepLimit = 25

// stallPruneSweepInterval is how often the pass runs. Well below any sane
// window, so the effective threshold is the window rather than the cadence.
const stallPruneSweepInterval = "5m"

// stalledDownloadReason reports why an entry counts as stalled, or "" if it
// does not.
//
// THE PREDICATE IS ZERO BYTES OVER A WINDOW, AND IT DELIBERATELY DOES NOT LOOK
// AT SEEDERS.
//
// The original design tested sustained seeders==0 AND sustained progress==0,
// which needs a sampling buffer because seeder counts fluctuate — a swarm can
// report 0 at one instant and 3 five minutes later, and one observation of a
// time-varying signal proves nothing.
//
// But progress is MONOTONIC. It cannot go down. So "progress is 0 now, and the
// entry was added an hour ago" already proves zero bytes across that entire
// hour, with no samples, no buffer and no window state to keep.
//
// And seeders adds nothing on top: if seeders had been present and useful,
// bytes would have moved. Progress measures the outcome; the seeder count is a
// proxy for it. Testing the outcome is both simpler and strictly more reliable,
// so the fluctuating input is not sampled — it is dropped.
//
// Restricted to torrents. A usenet entry has no swarm, and its own add-time
// gate already refuses releases no provider can serve.
func stalledDownloadReason(e *storage.Entry, window time.Duration, now time.Time) string {
	if e == nil || window <= 0 {
		return ""
	}
	if !e.IsTorrent() {
		return ""
	}
	// Only entries the PROVIDER considers actively downloading. A queued entry
	// has not been given a chance to move bytes yet, and pruning it would
	// punish provider-side queueing rather than a stall.
	if e.Status != debridTypes.TorrentStatusDownloading {
		return ""
	}
	if e.Progress > 0 {
		return ""
	}
	if e.AddedOn.IsZero() {
		// No clock to measure against. Absence of data is not a verdict.
		return ""
	}
	elapsed := now.Sub(e.AddedOn)
	if elapsed < window {
		return ""
	}
	return fmt.Sprintf("no bytes transferred in %s", elapsed.Round(time.Minute))
}

// stallPruneWindow resolves the configured window. Empty or unparseable
// disables the sweep entirely — this deletes data, so an unreadable setting
// must mean "do nothing", never "use a default".
func stallPruneWindow() time.Duration {
	raw := config.Get().StallPruneAfter
	if raw == "" {
		return 0
	}
	window, err := utils.ParseDuration(raw)
	if err != nil || window <= 0 {
		return 0
	}
	return window
}

// pruneStalledDownloads deletes torrents that have transferred nothing for the
// configured window, freeing the provider slot each one was holding.
//
// This exists because provider slots are finite and a stalled torrent holds one
// indefinitely. Once admission is metered against provider-reported capacity,
// a pool slowly filling with entries that will never finish quietly converts a
// working account into a full one — metering access to a wasted resource.
//
// Deletion goes through DeleteEntry(removePlacements: true) precisely so the
// PROVIDER placement is removed, not just the local record. A prune that leaves
// the remote torrent behind converts a stalled slot into a leaked one, which is
// strictly worse than doing nothing.
func (m *Manager) pruneStalledDownloads(ctx context.Context, limit int) int {
	window := stallPruneWindow()
	if window <= 0 {
		return 0
	}

	entries := m.queue.ListFilter("", config.ProtocolTorrent, storage.EntryStateDownloading, nil, "", true)
	if len(entries) == 0 {
		return 0
	}

	now := time.Now()
	pruned := 0
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return pruned
		}
		if pruned >= limit {
			m.logger.Info().
				Int("pruned", pruned).
				Msg("Stall prune hit its per-sweep cap; the rest will be reconsidered next pass")
			return pruned
		}
		reason := stalledDownloadReason(entry, window, now)
		if reason == "" {
			continue
		}
		// Re-read before deleting: the listing is a snapshot, and an entry that
		// started moving between the scan and now must not be deleted on stale
		// evidence.
		current, err := m.GetEntry(entry.InfoHash)
		if err != nil {
			continue
		}
		if stalledDownloadReason(current, window, time.Now()) == "" {
			continue
		}

		m.logger.Warn().
			Str("infohash", current.InfoHash).
			Str("name", current.Name).
			Str("provider", current.ActiveProvider).
			Str("reason", reason).
			Msg("Stall prune: deleting a torrent that has transferred nothing, and releasing its provider slot")

		if err := m.DeleteEntry(current.InfoHash, true); err != nil {
			m.logger.Error().Err(err).
				Str("infohash", current.InfoHash).
				Msg("Stall prune: delete failed; the provider slot is still held")
			continue
		}
		pruned++
	}
	return pruned
}

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

// stallPruneSweepInterval is how often the pass runs. Well below any sane
// threshold, so the effective behaviour is set by the thresholds rather than by
// the cadence.
const stallPruneSweepInterval = "5m"

// stallPruneSettings is the resolved, validated configuration for one sweep.
// Anything unparseable resolves to "stage disabled" rather than to a default —
// for a destructive feature, an unreadable setting must mean do nothing.
type stallPruneSettings struct {
	noProgressAfter time.Duration // stage 1; 0 = disabled
	maxETA          time.Duration // stage 2; 0 = disabled
	minAge          time.Duration // stage 2 grace period
	maxPerSweep     int
}

func (s stallPruneSettings) enabled() bool {
	return s.noProgressAfter > 0 || s.maxETA > 0
}

func resolveStallPruneSettings(cfg config.StallPruneConfig) stallPruneSettings {
	parse := func(raw string) time.Duration {
		if raw == "" {
			return 0
		}
		d, err := utils.ParseDuration(raw)
		if err != nil || d <= 0 {
			return 0
		}
		return d
	}

	s := stallPruneSettings{
		noProgressAfter: parse(cfg.NoProgressAfter),
		maxETA:          parse(cfg.MaxETA),
		minAge:          parse(cfg.MinAge),
		maxPerSweep:     cfg.MaxPerSweep,
	}
	if s.minAge <= 0 {
		s.minAge = parse(config.DefaultStallPruneMinAge)
	}
	if s.maxPerSweep <= 0 {
		s.maxPerSweep = config.DefaultStallPruneMaxPerSweep
	}
	return s
}

// prunableReason reports why an entry should be deleted, or "" to keep it.
//
// TWO STAGES, AND THE FIRST IS THE ONE TO TRUST.
//
// Stage 1 — zero bytes for a window. Progress is MONOTONIC, so "0 now, added
// an hour ago" already proves zero bytes across that entire hour: no sampling,
// no buffer, no window state. Seeders are deliberately not consulted, because
// if seeders had been present and useful, bytes would have moved. Progress
// measures the outcome; the seeder count is only a proxy for it.
//
// Stage 2 — projected completion beyond MaxETA, computed from the LIFETIME
// AVERAGE. Weaker on purpose: some genuinely slow torrents do finish, and this
// stage cannot distinguish them. It uses the average rather than the
// instantaneous rate so a dead torrent that briefly spikes cannot look healthy,
// and it will not act before MinAge because an average over a few seconds
// projects to an absurd ETA and would delete every new torrent on arrival.
//
// Both stages only ever consider entries the PROVIDER reports as downloading —
// a queued entry has not been given a chance to move bytes, and pruning it
// would punish provider-side queueing rather than a stall.
//
// Torrents only. A usenet entry has no swarm and has its own add-time gate.
func prunableReason(e *storage.Entry, s stallPruneSettings, now time.Time) string {
	if e == nil || !s.enabled() {
		return ""
	}
	if !e.IsTorrent() {
		return ""
	}
	if e.Status != debridTypes.TorrentStatusDownloading {
		return ""
	}
	if e.AddedOn.IsZero() {
		// No clock to measure against. Absence of data is not a verdict.
		return ""
	}
	age := now.Sub(e.AddedOn)

	// Stage 1: zero bytes for the whole window.
	if s.noProgressAfter > 0 && e.Progress <= 0 && age >= s.noProgressAfter {
		return fmt.Sprintf("no bytes transferred in %s", age.Round(time.Minute))
	}

	// Stage 2: projected completion beyond the ceiling, at the average rate.
	if s.maxETA > 0 && age >= s.minAge {
		eta := e.ETAAtAverageSpeed()
		// EtaUnknown means "no rate to extrapolate from". For an entry old
		// enough to judge that is a stall, but it is stage 1's call to make,
		// not stage 2's — stage 2 refuses to invent a projection it does not
		// have. If stage 1 is disabled, such an entry is kept.
		if eta != storage.EtaUnknown && time.Duration(eta)*time.Second > s.maxETA {
			return fmt.Sprintf("projected %s to complete at its average rate of %s/s, over the %s ceiling",
				(time.Duration(eta) * time.Second).Round(time.Minute),
				utils.FormatSize(e.AverageSpeed()), s.maxETA)
		}
	}
	return ""
}

// pruneStalledDownloads deletes torrents that will not finish and releases the
// provider slot each one was holding.
//
// This exists because provider slots are finite and a stalled torrent holds one
// indefinitely. Once admission is metered against provider-reported capacity, a
// pool slowly filling with entries that will never finish quietly converts a
// working account into a full one — so the admission layer ends up carefully
// metering access to a resource that is being wasted.
//
// Deletion goes through DeleteEntry(removePlacements: true) precisely so the
// PROVIDER placement is removed, not just the local record. A prune that leaves
// the remote torrent behind converts a stalled slot into a leaked one, which is
// strictly worse than doing nothing.
func (m *Manager) pruneStalledDownloads(ctx context.Context, settings stallPruneSettings) int {
	if !settings.enabled() {
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
		if pruned >= settings.maxPerSweep {
			m.logger.Info().
				Int("pruned", pruned).
				Msg("Stall prune hit its per-sweep cap; the rest will be reconsidered next pass")
			return pruned
		}
		if prunableReason(entry, settings, now) == "" {
			continue
		}
		// Re-read before deleting: the listing is a snapshot, and an entry that
		// started moving between the scan and now must not die on stale
		// evidence.
		current, err := m.GetEntry(entry.InfoHash)
		if err != nil {
			continue
		}
		reason := prunableReason(current, settings, time.Now())
		if reason == "" {
			continue
		}

		m.logger.Warn().
			Str("infohash", current.InfoHash).
			Str("name", current.Name).
			Str("provider", current.ActiveProvider).
			Str("reason", reason).
			Msg("Stall prune: deleting a torrent that will not finish, and releasing its provider slot")

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

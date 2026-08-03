package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
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

// pruneStalledDownloads FAILS torrents that will not finish, and releases the
// provider slot each one was holding.
//
// It fails them rather than deleting them, and that distinction is the whole
// feature. An earlier version called DeleteEntry, which freed the slot and told
// the *arr nothing — leaving the *arr holding a queue row for a download that no
// longer existed anywhere, believing it was still progressing. It would never
// re-grab, so a stalled torrent became a permanently missing episode instead of
// a retried one. That is WORSE than leaving the stall in place, because a stall
// is at least visible.
//
// So the order is: release the provider placement (the slot is the resource we
// are reclaiming), then mark the entry errored. MarkAsError sets
// EntryStateError, which the qBittorrent shim reports as state "error" — the
// same path every other failure in decypharr takes to reach the *arr. The *arr
// sees a failed download, applies its own policy, and re-searches.
//
// The entry is deliberately NOT deleted here. Deleting it would remove the very
// row the *arr must observe to learn the download failed; cleanup is the *arr's
// and the queue-cleanup policy's job, on their own schedule.
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
	// Entries whose provider slot could not be released. Collected so the sweep
	// REPORTS them: a per-entry error line among thousands is how a permanently
	// unreleasable entry became invisible, skipped silently on every pass with
	// nothing ever summarising that it kept happening.
	var stuck []string
	defer func() {
		if len(stuck) == 0 {
			return
		}
		m.logger.Error().
			Int("stuck", len(stuck)).
			Strs("infohashes", stuck).
			Msg("Stall prune: these torrents could NOT have their provider slot released, so they were left " +
				"untouched rather than failed while still holding it. Their slots stay occupied and they will " +
				"be retried next sweep. If this repeats, the placement needs deleting on the provider directly.")
	}()
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
			Msg("Stall prune: failing a torrent that will not finish, and releasing its provider slot")

		if err := m.PruneEntry(ctx, current, "stall prune: "+reason); err != nil {
			if errors.Is(err, errPruneSlotStillHeld) {
				stuck = append(stuck, current.InfoHash)
			}
			continue
		}
		pruned++
	}
	return pruned
}

// errPruneSlotStillHeld reports that the provider placement could not be
// released, so the entry was deliberately left alone rather than failed.
var errPruneSlotStillHeld = errors.New("provider placement could not be released")

// PruneEntry is THE prune. Both the automatic stall sweep and the manual
// control go through here, so there is exactly one definition of what pruning
// means and the two cannot drift into behaving differently.
//
// The ordering is the whole point and it is load-bearing (1558258):
//
//  1. RELEASE the provider placement first. That is the resource being
//     reclaimed, and it must happen even if the local update later fails.
//  2. THEN fail the entry, so the *arr actually learns. Without this the arr
//     keeps a queue row for a download that no longer exists and never
//     re-searches — which is precisely what the plain delete does, and why it
//     is not a prune.
//
// Never step 2 before step 1: telling the arr to re-grab while we still occupy
// the slot spends a second slot on the replacement.
func (m *Manager) PruneEntry(ctx context.Context, entry *storage.Entry, reason string) error {
	if entry == nil {
		return fmt.Errorf("prune: no entry")
	}
	if err := m.releasePlacementWithRetry(ctx, entry); err != nil {
		return fmt.Errorf("%w: %s: %w", errPruneSlotStillHeld, entry.InfoHash, err)
	}

	entry.MarkAsError(fmt.Errorf("%s", reason))
	if err := m.queue.Update(entry); err != nil {
		m.logger.Error().Err(err).
			Str("infohash", entry.InfoHash).
			Msg("Prune: released the provider slot but could not record the failure; the arr may not see " +
				"this as a failed download")
		return fmt.Errorf("prune %s: released the slot but could not record the failure: %w", entry.InfoHash, err)
	}
	return nil
}

// Placement-release retry policy.
//
// A release failure was previously one undifferentiated `continue`: the entry
// was skipped, and if the condition was permanent it was skipped again on every
// subsequent sweep, forever, with nothing distinguishing "the provider blipped"
// from "this will never succeed".
//
// The two need opposite handling. A transient failure deserves another attempt
// promptly, because the slot it is holding is the resource under contention. A
// terminal one must never be retried silently — retrying cannot change it, and
// the operator has to know a slot is stranded.
const (
	placementReleaseAttempts   = 3
	placementReleaseBackoff    = 2 * time.Second
	placementReleaseBackoffMax = 8 * time.Second
)

// releasePlacementWithRetry releases an entry's provider placements, retrying
// only what is worth retrying.
//
// Returns nil once the slot is genuinely free. A provider reporting the item as
// already absent counts as released — the slot is not occupied by something the
// provider does not have, and treating that as failure would strand the entry
// permanently: its slot free, but the caller refusing to fail it on the grounds
// that the slot is held.
func (m *Manager) releasePlacementWithRetry(ctx context.Context, entry *storage.Entry) error {
	delay := placementReleaseBackoff
	var err error
	for attempt := 1; attempt <= placementReleaseAttempts; attempt++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		err = m.RemoveTorrentPlacements(entry)
		if err == nil {
			return nil
		}

		if !customerror.IsRetriableError(err) {
			// TERMINAL. Loud, identifiable, and named as an operator action —
			// not a line in a sweep summary. Retrying is pointless, so this
			// returns immediately rather than burning the remaining attempts.
			m.logger.Error().Err(err).
				Str("infohash", entry.InfoHash).
				Str("name", entry.Name).
				Str("provider", entry.ActiveProvider).
				Str("placement_id", placementIDOf(entry)).
				Int("attempt", attempt).
				Msg("Stall prune: PERMANENTLY cannot release this provider placement. The slot stays occupied " +
					"and the entry is left untouched (failing it while the slot is held would make the arr " +
					"re-grab into a slot we still hold). This will not fix itself — delete the item on the " +
					"provider, or check the account's credentials.")
			return err
		}

		m.logger.Warn().Err(err).
			Str("infohash", entry.InfoHash).
			Str("provider", entry.ActiveProvider).
			Int("attempt", attempt).
			Int("of", placementReleaseAttempts).
			Dur("retry_in", delay).
			Msg("Stall prune: transient failure releasing the provider placement; retrying")

		if attempt == placementReleaseAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if delay = delay * 2; delay > placementReleaseBackoffMax {
			delay = placementReleaseBackoffMax
		}
	}

	// Transient, but it outlasted the attempt budget. Still not silent: the
	// next sweep retries, and the sweep summary reports that it is stuck.
	m.logger.Error().Err(err).
		Str("infohash", entry.InfoHash).
		Str("name", entry.Name).
		Str("provider", entry.ActiveProvider).
		Int("attempts", placementReleaseAttempts).
		Msg("Stall prune: could not release the provider placement after retries; leaving the entry alone " +
			"rather than failing it while the slot is still held")
	return err
}

func placementIDOf(entry *storage.Entry) string {
	if placement := entry.GetActiveProvider(); placement != nil {
		return placement.ID
	}
	return ""
}

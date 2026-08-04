package manager

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// PROVIDER-SOURCED STALL PRUNE.
//
// THE ARCHITECTURAL POINT: the candidate set is the PROVIDER'S active list, not
// our own records. The provider is the source of truth for what is occupying a
// slot, because it is the thing whose slots they are.
//
// The local-state sweep in stall_prune.go cannot be fixed into this. It asks
// "which of MY entries look stalled", so it is structurally blind to anything
// that fell out of local state — and that is precisely the population that
// needs pruning. Measured on a live account: decypharr could see 4 downloading
// entries while RealDebrid was running 108 transfers, 68 of them dead or
// crawling and 60 over 24 hours old. Fixing the lookup (which was genuinely
// broken, and is now fixed) still leaves 104 of 108 invisible, because they are
// not in the local set at all.
//
// So this pass inverts the question. It asks the provider what it is working
// on, judges each item on the provider's own reported progress and age, and
// releases the slot. A local row is used when one exists — to fail the *arr so
// it re-searches — and its ABSENCE never aborts the prune.
//
// That last point is the same bug twice removed: the first version dropped
// candidates whose local row could not be re-read, which made the whole feature
// inert. Requiring a local row here would reintroduce it in a new costume, on
// the exact population that has none.

// providerPruneSweepInterval matches the local sweep's cadence.
const providerPruneSweepInterval = "5m"

// providerCandidate is one provider-side transfer, as the provider describes it.
type providerCandidate struct {
	provider string
	id       string
	infoHash string
	name     string
	// progress is a 0..1 fraction, normalised from whatever scale the provider
	// reports on.
	progress float64
	size     int64
	added    time.Time
	status   debridTypes.TorrentStatus
	dead     bool
}

// age is how long the provider has had this transfer.
func (c providerCandidate) age(now time.Time) time.Duration {
	if c.added.IsZero() {
		return 0
	}
	return now.Sub(c.added)
}

// averageSpeed is bytes/second over the transfer's whole life.
//
// Derived rather than sampled, and deliberately so: the provider's LIST
// endpoints carry progress and an added timestamp but no instantaneous speed,
// and the lifetime average is the figure the stall predicate wants anyway. A
// torrent that crawled for a day and then briefly spiked has a flattering
// instantaneous rate and an honest average one.
func (c providerCandidate) averageSpeed(now time.Time) int64 {
	elapsed := c.age(now).Seconds()
	if elapsed <= 0 || c.size <= 0 || c.progress <= 0 {
		return 0
	}
	return int64(float64(c.size) * c.progress / elapsed)
}

// providerPrunableReason reports why a provider-side transfer should be killed,
// or "" to keep it.
//
// Mirrors prunableReason's stages against provider-reported values. Only items
// the provider itself calls in-flight are considered: a completed transfer is
// occupying storage, not a download slot, and is not this pass's business.
func providerPrunableReason(c providerCandidate, s stallPruneSettings, now time.Time) string {
	if !s.enabled() {
		return ""
	}
	if c.status == debridTypes.TorrentStatusDownloaded {
		return ""
	}
	// A terminally dead copy is ENUMERATE's to reap, through the health path
	// that records WHY. Killing it here would free the slot while losing the
	// verdict.
	if c.dead {
		return ""
	}
	if c.added.IsZero() {
		// No clock to measure against. Absence of data is not a verdict.
		return ""
	}
	age := c.age(now)

	// Stage 1: zero bytes for the whole window. Progress is monotonic, so
	// "0 now, added an hour ago" already proves zero across that hour.
	if s.noProgressAfter > 0 && c.progress <= 0 && age >= s.noProgressAfter {
		return fmt.Sprintf("provider reports no bytes transferred in %s", age.Round(time.Minute))
	}

	// Stage 2: projected completion beyond the ceiling, at the lifetime average.
	if s.maxETA > 0 && age >= s.minAge {
		speed := c.averageSpeed(now)
		remaining := c.size - int64(float64(c.size)*c.progress)
		if speed > 0 && remaining > 0 {
			eta := time.Duration(remaining/speed) * time.Second
			if eta > s.maxETA {
				return fmt.Sprintf("provider reports %.1f%% after %s, projecting %s to finish at %s/s, over the %s ceiling",
					c.progress*100, age.Round(time.Minute), eta.Round(time.Minute),
					utils.FormatSize(speed), s.maxETA)
			}
		}
	}
	return ""
}

// providerActiveCandidates asks a provider what it is currently working on.
func (m *Manager) providerActiveCandidates(name string, client debrid.Client) ([]providerCandidate, error) {
	torrents, err := client.GetAllTorrents()
	if err != nil {
		return nil, err
	}
	// The listing is the same one the refresh and the orphan check consume, so
	// publish the count while we have it rather than making the fill cache
	// enumerate the account again.
	m.fillCache.observe(name, len(torrents), time.Now())

	candidates := make([]providerCandidate, 0, 32)
	for _, t := range torrents {
		if t == nil || t.Id == "" {
			continue
		}
		if t.Status == debridTypes.TorrentStatusDownloaded {
			continue
		}
		progress := t.Progress
		// Providers disagree about scale: some report 0-100, some 0-1. Treat
		// anything above 1 as a percentage. Guessing wrong in the other
		// direction would read 0.5 as half a percent and prune a healthy
		// transfer, so the normalisation is deliberately one-way.
		if progress > 1 {
			progress /= 100
		}
		size := t.Size
		if size == 0 {
			size = t.Bytes
		}
		candidates = append(candidates, providerCandidate{
			provider: name,
			id:       t.Id,
			infoHash: strings.ToLower(t.InfoHash),
			name:     t.Name,
			progress: progress,
			size:     size,
			added:    t.Added,
			status:   t.Status,
			dead:     t.ProviderDead,
		})
	}
	return candidates, nil
}

// pruneProviderStalled is the sweep.
//
// Returns how many provider slots it released.
func (m *Manager) pruneProviderStalled(ctx context.Context, settings stallPruneSettings) int {
	if !settings.enabled() {
		return 0
	}

	now := time.Now()
	released := 0
	// Counted so a sweep that judges items prunable and then frees none says
	// so. The local sweep spent its whole life silently dropping candidates,
	// and the only reason that was survivable is that somebody eventually
	// measured the provider from outside.
	failed := 0

	m.clients.Range(func(name string, client debrid.Client) bool {
		if client == nil || ctx.Err() != nil {
			return ctx.Err() == nil
		}
		candidates, err := m.providerActiveCandidates(name, client)
		if err != nil {
			// UNKNOWN, not "nothing to do". A failed enumeration must not read
			// as a healthy account.
			m.logger.Warn().Err(err).Str("debrid", name).
				Msg("Provider stall prune: could not enumerate; skipping this provider for this sweep")
			return true
		}

		for _, candidate := range candidates {
			if ctx.Err() != nil {
				return false
			}
			if released >= settings.maxPerSweep {
				m.logger.Info().
					Int("released", released).
					Msg("Provider stall prune hit its per-sweep cap; the rest will be reconsidered next pass")
				return false
			}
			reason := providerPrunableReason(candidate, settings, now)
			if reason == "" {
				continue
			}
			if err := m.pruneProviderCandidate(ctx, candidate, reason); err != nil {
				failed++
				m.logger.Error().Err(err).
					Str("debrid", candidate.provider).
					Str("provider_id", candidate.id).
					Str("name", candidate.name).
					Msg("Provider stall prune: could not release this slot")
				continue
			}
			released++
		}
		return true
	})

	if released > 0 || failed > 0 {
		m.logger.Info().
			Int("released", released).
			Int("failed", failed).
			Msg("Provider stall prune completed")
	}
	return released
}

// pruneProviderCandidate frees one slot, and tells the *arr if we can.
//
// ORDER MATTERS, and it is the same ordering the local prune uses: release the
// provider placement FIRST, then fail the local row. Telling an *arr to
// re-search while we still hold the slot spends a second slot on the
// replacement.
//
// THE LOCAL ROW IS OPTIONAL. 104 of 108 measured candidates had none. The slot
// still has to be freed, so a missing entry is not an error and must never
// abort the prune — requiring one is exactly what made the previous sweep
// inert, and this population is defined by not having one.
func (m *Manager) pruneProviderCandidate(ctx context.Context, c providerCandidate, reason string) error {
	client := m.ProviderClient(c.provider)
	if client == nil {
		return fmt.Errorf("provider %q client not found", c.provider)
	}

	entry, entryErr := m.PrunableEntry(c.infoHash)
	if entryErr == nil && entry != nil && placementIDOf(entry) == c.id {
		// We own this one and hold the matching placement. Go through the
		// shared prune so the *arr learns and the two paths cannot drift.
		m.logger.Warn().
			Str("infohash", c.infoHash).
			Str("name", c.name).
			Str("provider", c.provider).
			Str("reason", reason).
			Msg("Provider stall prune: failing a tracked torrent and releasing its slot")
		return m.PruneEntry(ctx, entry, "provider stall prune: "+reason)
	}

	// No local row, or one that points somewhere else. Free the slot directly.
	// There is no *arr queue row to fail — the *arr already stopped tracking
	// this, which is how it came to be orphaned — so the only thing left to
	// reclaim is the slot.
	m.logger.Warn().
		Str("provider", c.provider).
		Str("provider_id", c.id).
		Str("infohash", c.infoHash).
		Str("name", c.name).
		Str("reason", reason).
		Msg("Provider stall prune: releasing an untracked provider transfer; no local record exists to fail, " +
			"so no arr is told — nothing was tracking it")

	if err := client.DeleteTorrent(c.id); err != nil {
		return err
	}
	// The account just shrank, so a fill snapshot taken before this would still
	// report it full and wrongly refuse the next add as a standing condition.
	m.fillCache.invalidate(c.provider)
	m.releaseHeldForCapacity(1)
	return nil
}

// resolveProviderPruneSettings reads the same knobs as the local sweep.
//
// Deliberately shared: an operator sets ONE set of stall thresholds and expects
// them to mean one thing. Two sweeps with independently configurable
// definitions of "stalled" is a way to make the system unexplainable.
func resolveProviderPruneSettings() stallPruneSettings {
	return resolveStallPruneSettings(config.Get().StallPrune)
}

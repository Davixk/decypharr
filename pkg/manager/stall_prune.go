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
	// sampleWindow is BOTH the warm-up before a verdict is possible and the
	// window the speed is averaged over — they are the same thing. See
	// config.StallPruneConfig.
	sampleWindow time.Duration
	maxETA       time.Duration
	// maxDownloading is the hard failsafe. 0 = none configured.
	maxDownloading time.Duration
	maxPerSweep    int
	// misconfigured records WHY the feature refused to arm, so a sweep can say
	// so rather than looking idle. A destructive feature that silently declines
	// to run is the same class of problem as one that silently runs.
	misconfigured string
}

// enabled requires BOTH the window and the ceiling. The test is meaningless
// without either: no window means no trustworthy speed, no ceiling means
// nothing to judge the ETA against.
func (s stallPruneSettings) enabled() bool {
	return s.misconfigured == "" && s.maxETA > 0 && s.sampleWindow > 0
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
		sampleWindow:   parse(cfg.ETASampleWindow),
		maxETA:         parse(cfg.MaxETA),
		maxDownloading: parse(cfg.MaxDownloadingTime),
		maxPerSweep:    cfg.MaxPerSweep,
	}
	if s.maxPerSweep <= 0 {
		s.maxPerSweep = config.DefaultStallPruneMaxPerSweep
	}

	// THE FAILSAFE MUST NOT CONTRADICT THE TEST IT BACKS UP.
	//
	// A max_downloading_time below sample_window + max_eta would delete
	// transfers that are still inside the ETA they were explicitly allowed —
	// the backstop firing before the rule it exists to catch failures of.
	//
	// Refusing to arm (rather than clamping to something we invented) is the
	// same discipline as everywhere else here: for a destructive feature, a
	// setting we cannot honour must mean DO NOTHING, never "do something
	// adjusted". It disables this feature only — a media stack should not fail
	// to boot over one knob — and the sweep reports the reason every pass, so
	// it cannot be mistaken for an idle account.
	if s.maxDownloading > 0 && s.sampleWindow > 0 && s.maxETA > 0 {
		if minimum := s.sampleWindow + s.maxETA; s.maxDownloading < minimum {
			s.misconfigured = fmt.Sprintf(
				"max_downloading_time (%s) is below eta_sample_window + max_eta (%s); it would prune "+
					"transfers still inside the ETA they were allowed. Raise it to at least %s",
				s.maxDownloading, minimum, minimum)
		}
	}
	return s
}

// prunableReason reports why a locally-tracked entry should be failed, or ""
// to keep it.
//
// ONE TEST: the failsafe, then the ETA.
//
// The stall detector that used to live here is GONE. It tested "has this moved
// zero bytes since it was added", which nobody ever asked for — it was invented
// in the first version of this feature and then reasoned about for days as
// though it were a requirement. A transfer that has stopped is caught by the
// ETA being infinite; it needs no separate rule.
//
// Only entries the PROVIDER reports as downloading are considered — a queued
// entry has not been given a chance to move bytes, and pruning it would punish
// provider-side queueing rather than a genuine failure to progress.
//
// Torrents only. A usenet entry has no swarm and has its own add-time gate.
//
// NOTE this local sweep still projects from the LIFETIME average, because a
// local entry carries no sample history. The provider-sourced sweep measures
// speed over the sample window and is strictly better informed; see
// provider_prune.go.
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

	// The failsafe. Needs no measurement at all, which is why it is first and
	// why it still works when nothing else can be computed.
	if s.maxDownloading > 0 && age >= s.maxDownloading {
		return fmt.Sprintf("has been downloading for %s, over the %s hard limit",
			age.Round(time.Minute), s.maxDownloading)
	}

	// Below the sampling window there is NO VERDICT — not "probably fine", but
	// "we do not yet have the data to compute one".
	if age < s.sampleWindow {
		return ""
	}

	eta := e.ETAAtAverageSpeed()
	if eta == storage.EtaUnknown {
		// Nothing to extrapolate from: no bytes have moved at all. Past the
		// sampling window that IS an infinite ETA, and infinite is over any
		// ceiling.
		return fmt.Sprintf("no measurable rate after %s, so it will not complete",
			age.Round(time.Minute))
	}
	if time.Duration(eta)*time.Second > s.maxETA {
		return fmt.Sprintf("projected %s to complete at %s/s, over the %s ceiling",
			(time.Duration(eta) * time.Second).Round(time.Minute),
			utils.FormatSize(e.AverageSpeed()), s.maxETA)
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
	// Candidates that qualified for pruning but whose authoritative row could
	// not be re-read. This counter exists because its absence hid a total
	// failure of the feature: the re-read used to consult the MAIN store while
	// the listing above comes from the QUEUE, so it missed on EVERY candidate
	// and every one was dropped by a bare `continue`. A sweep that considers
	// hundreds of entries and prunes none must say so.
	skipped := 0
	// Entries whose provider slot could not be released. Collected so the sweep
	// REPORTS them: a per-entry error line among thousands is how a permanently
	// unreleasable entry became invisible, skipped silently on every pass with
	// nothing ever summarising that it kept happening.
	var stuck []string
	defer func() {
		if skipped > 0 {
			m.logger.Error().
				Int("skipped", skipped).
				Int("pruned", pruned).
				Msg("Stall prune: these torrents qualified for pruning but their queue row could not be " +
					"re-read, so they were left alone. A non-zero count here means the sweep is not acting " +
					"on entries it has already judged prunable.")
		}
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
		current, err := m.PrunableEntry(entry.InfoHash)
		if err != nil {
			skipped++
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

// errNotAnActiveEntry reports that a hash has no queue row, so there is no
// in-flight download to prune.
var errNotAnActiveEntry = errors.New("no active queue entry")

// PrunableEntry re-reads the authoritative row for a prune candidate.
//
// IT MUST READ THE QUEUE, AND THAT IS THE ENTIRE POINT OF THIS FUNCTION.
//
// decypharr keeps two stores. The QUEUE holds in-flight workflow rows; the MAIN
// store holds the library, and an entry is only written there by
// persistCompletedEntry — that is, ON COMPLETION. A torrent that is still
// downloading therefore exists in the queue and NOWHERE ELSE.
//
// Both prune callers used to re-read through Manager.GetEntry, which resolves
// the MAIN store. That lookup missed on every candidate the sweep could ever
// act on, because "still downloading" and "present in the main store" are
// mutually exclusive by construction. Each miss hit a bare `continue`, so the
// automatic stall prune ran on schedule, evaluated its thresholds correctly,
// selected the right entries — and then silently dropped all of them. It had
// never pruned anything. The manual control added later inherited the same
// lookup and would have reported "entry not found" for precisely the stalled
// torrents it exists to kill.
//
// The main store is deliberately NOT consulted as a fallback. A completed
// library entry has no arr queue row to fail, so pruning it cannot do the one
// thing a prune is for; "remove it from the provider" is a different action and
// already has its own control. Answering with a typed refusal keeps the two
// distinct instead of half-performing one as the other.
func (m *Manager) PrunableEntry(infohash string) (*storage.Entry, error) {
	entry, err := m.queue.GetTorrent(infohash)
	if err != nil {
		if errors.Is(err, storage.ErrEntryNotFound) {
			return nil, fmt.Errorf("%w for %s", errNotAnActiveEntry, infohash)
		}
		return nil, err
	}
	if entry == nil {
		return nil, fmt.Errorf("%w for %s", errNotAnActiveEntry, infohash)
	}
	return entry, nil
}

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

	// ⚠️ MARK FAILED AND PARK. THE ROW IS NOT OURS TO DELETE.
	//
	// decypharr is the download client here and nothing more. It marks the row
	// failed through the shim and leaves it parked; the *arr polls, sees a failed
	// download, and does whatever IT is configured to do — blocklist, re-search,
	// neither. That decision is the *arr's, and decypharr never reaches into an
	// *arr API to make it.
	//
	// So this function ends here, deliberately. The pull mechanism is the DESIGN,
	// not a workaround: the defect was never that we failed to push, it was that
	// another sweep DELETED these rows within 60 seconds, before any *arr could
	// poll them. Measured: 15,004 rows removed in 24h against 91 downloadFailed
	// events at the *arrs. The fix is upstream of here — reapers no longer
	// delete — and this code was correct all along.
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

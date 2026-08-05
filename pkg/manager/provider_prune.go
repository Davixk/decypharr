package manager

import (
	"context"
	"fmt"
	"strings"
	"sync"
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

// progressTracker remembers where each provider transfer had got to, so a
// stall can be measured as an absence of movement rather than inferred.
//
// Deliberately in-memory and unbounded-by-account-size only: it holds one small
// record per IN-FLIGHT transfer, which is bounded by the provider's own
// concurrency limit (100 on RealDebrid), and entries for anything no longer
// seen are dropped on each sweep. A restart forgets everything, which is the
// safe direction — the first sweep after boot re-baselines and concludes
// nothing.
type progressTracker struct {
	mu   sync.Mutex
	seen map[string]sampleSeries
}

// progressObservation is one reading of how far a transfer had got.
type progressObservation struct {
	progress float64
	at       time.Time
}

func newProgressTracker() *progressTracker {
	return &progressTracker{seen: map[string]sampleSeries{}}
}

// sampleSeries is the readings kept for one transfer, oldest first.
type sampleSeries []progressObservation

// speedOver returns bytes/second measured across the window, and whether the
// series covers enough of it to be trusted.
//
// THE WINDOW IS THE SMOOTHING, and that is the whole reason it exists. Torrent
// speeds and peer counts float constantly, so a delta between two consecutive
// sweeps is noise: a swarm that goes quiet for one five-minute window would
// read as stopped, and under a pure-ETA test that means deleted. Averaged
// across the window, the same lull moves the number instead of zeroing it,
// while a genuinely dead transfer still reads zero across every sample.
//
// Not trusted until the series actually spans the window. A partial series
// would answer from a few minutes of data — precisely the untrustworthy
// reading the window was introduced to prevent.
func (s sampleSeries) speedOver(window time.Duration, size int64, now time.Time) (int64, bool) {
	if len(s) < 2 || size <= 0 || window <= 0 {
		return 0, false
	}
	oldest := s[0]
	newest := s[len(s)-1]
	if now.Sub(oldest.at) < window {
		return 0, false
	}
	elapsed := newest.at.Sub(oldest.at).Seconds()
	if elapsed <= 0 {
		return 0, false
	}
	moved := (newest.progress - oldest.progress) * float64(size)
	if moved <= 0 {
		// Zero or backwards across the whole window. A real reading of zero,
		// not an absence of one — the caller may act on it.
		return 0, true
	}
	return int64(moved / elapsed), true
}

// observe records a reading and returns the series for this transfer, trimmed
// to the window plus one sample either side of it.
//
// Retaining one sample OLDER than the window is deliberate: it is what lets the
// series span the full window as soon as it possibly can, instead of always
// measuring slightly less than asked for.
func (p *progressTracker) observe(key string, progress float64, window time.Duration, now time.Time) sampleSeries {
	if p == nil || key == "" {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	series := append(p.seen[key], progressObservation{progress: progress, at: now})

	// Drop anything older than the window, keeping the last one that is, so a
	// full-width measurement is available at the earliest honest moment.
	cutoff := now.Add(-window)
	trimAt := 0
	for i, sample := range series {
		if sample.at.After(cutoff) {
			break
		}
		trimAt = i
	}
	if trimAt > 0 {
		series = append(sampleSeries(nil), series[trimAt:]...)
	}
	p.seen[key] = series
	return series
}

// retain drops observations for transfers the provider no longer lists, so the
// map tracks the live set rather than growing for the process lifetime.
func (p *progressTracker) retain(keys map[string]struct{}) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for key := range p.seen {
		if _, ok := keys[key]; !ok {
			delete(p.seen, key)
		}
	}
}

// providerPrunableReason reports why a provider-side transfer should be killed,
// or "" to keep it.
//
// ONE TEST — the failsafe, then the ETA. Not a ladder of stages, and not a
// stall detector: a transfer that has stopped prunes because its ETA is
// infinite, and one that trickles prunes because its rate will not finish in
// time. Both fall out of the same question.
//
// samples is this transfer's reading history. When it does not yet span the
// sampling window there is NO VERDICT — the point of the window is that a
// reading taken over less than it is not trustworthy, and acting on one anyway
// would defeat the only thing protecting healthy transfers from a momentary
// lull.
func providerPrunableReason(c providerCandidate, s stallPruneSettings, now time.Time, samples sampleSeries) string {
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

	// THE FAILSAFE, first because it needs no measurement at all.
	//
	// The provider's own `added` timestamp is enough, so this still works after
	// a restart when no samples exist yet — which is exactly when the ETA test
	// cannot answer. It is the backstop for whatever that test gets wrong.
	if s.maxDownloading > 0 && age >= s.maxDownloading {
		return fmt.Sprintf("has been downloading for %s, over the %s hard limit",
			age.Round(time.Minute), s.maxDownloading)
	}

	// Below the sampling window: NO VERDICT. Not "probably healthy" — we do not
	// yet have the data to compute one.
	if age < s.sampleWindow {
		return ""
	}

	speed, trusted := samples.speedOver(s.sampleWindow, c.size, now)
	if !trusted {
		// Our own history does not span the window yet, even though the
		// transfer is old enough. Happens after a restart. Waiting one window
		// costs nothing; guessing from a lifetime average would flatter a
		// transfer that died hours ago, which is the whole reason the window
		// exists.
		return ""
	}

	remaining := c.size - int64(float64(c.size)*c.progress)
	if remaining <= 0 {
		return ""
	}
	if speed <= 0 {
		// A measured zero across the entire window, not an absent reading.
		// That is an infinite ETA, and infinite is over any ceiling.
		return fmt.Sprintf("provider reports %.1f%% and no bytes moved in the last %s, so it will not complete",
			c.progress*100, s.sampleWindow)
	}
	eta := time.Duration(remaining/speed) * time.Second
	if eta > s.maxETA {
		return fmt.Sprintf("provider reports %.1f%%, projecting %s to finish at %s/s measured over %s, over the %s ceiling",
			c.progress*100, eta.Round(time.Minute), utils.FormatSize(speed),
			s.sampleWindow, s.maxETA)
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
	if settings.misconfigured != "" {
		// LOUD, and every sweep. A destructive feature that silently declines
		// to run is the same class of problem as one that silently runs — the
		// operator would see no prunes and no explanation, which is exactly
		// the state that cost a day of investigation.
		m.logger.Error().
			Str("reason", settings.misconfigured).
			Msg("Stall prune refuses to arm: its configuration would make the failsafe contradict the ETA test")
		return 0
	}
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

	considered := 0
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
		considered += len(candidates)

		// Record a reading for each transfer and drop anything the provider no
		// longer lists, so the tracker follows the live set.
		live := make(map[string]struct{}, len(candidates))
		series := make(map[string]sampleSeries, len(candidates))
		for _, candidate := range candidates {
			key := candidate.provider + "\x00" + candidate.id
			live[key] = struct{}{}
			series[key] = m.progress.observe(key, candidate.progress, settings.sampleWindow, now)
		}
		m.progress.retain(live)

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
			reason := providerPrunableReason(candidate, settings, now,
				series[candidate.provider+"\x00"+candidate.id])
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

	// ALWAYS LOGGED, including a sweep that released nothing.
	//
	// This is the third time in this codebase a sweep has been silent on
	// success and therefore indistinguishable from one that never ran. An
	// operator watched 35 minutes of uptime with ~56 transfers they believed
	// prunable, saw zero lines, and reasonably concluded the job was not
	// registered. It was registered and running; it just had nothing to say.
	//
	// `considered` is the load-bearing field: it separates "the sweep is not
	// running" (absent line) from "the sweep ran and the provider had nothing
	// in flight" (considered=0) from "it ran, saw work, and judged none of it
	// prunable" (considered>0, released=0) — three very different situations
	// that previously produced identical silence.
	m.logger.Info().
		Int("considered", considered).
		Int("released", released).
		Int("failed", failed).
		Msg("Provider stall prune completed")
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

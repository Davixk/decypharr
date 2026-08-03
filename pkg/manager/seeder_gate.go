package manager

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/netbind"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/swarm"
)

// THE GRAB-TIME SEEDER GATE.
//
// Refuses an UNCACHED torrent whose swarm is too small to finish, while the arr
// is still blocked inside its add call and still holding its ranked candidate
// list. See config.SeederGateConfig for why that timing is the entire feature
// and why this can never wait for anything.
//
// This file contains POLICY only — the threshold, the confirm-never-condemn
// rule for provider counts, and the fail-open discipline. WHERE the swarm
// reading comes from lives behind swarm.Source, so the backend can be replaced
// without touching any of it. That separation is deliberate: the first version
// of this feature was welded to one backend, and when that backend's data
// turned out to be 58 hours stale there was no seam to swap it at.

type seederGateSettings struct {
	minSeeders int
	timeout    time.Duration
	source     swarm.Source
	// usesScrape / trackerPool exist only to detect a gate that is switched on
	// but has nobody to ask. See warnIfToothless.
	usesScrape  bool
	trackerPool int
}

func (s seederGateSettings) enabled() bool {
	return s.minSeeders > 0 && s.source != nil
}

// warnIfToothless reports a gate that is enabled and cannot possibly act.
//
// Measured: 3,276 of 3,276 magnets on a live deployment carried ZERO tr=
// entries, so a scrape source with no configured pool has nothing to ask on
// every grab and fails open every time. That is IDENTICAL from the outside to a
// gate that is working and finding healthy swarms — same absence of refusals,
// same absence of log lines. An operator would reasonably conclude the feature
// was protecting them.
//
// Rate-limited to once a minute: this is evaluated per grab, and a warning that
// floods is a warning nobody reads.
var toothlessWarned atomic.Int64

func (s seederGateSettings) warnIfToothlessVia(m *Manager, magnetTrackerCount int) {
	if m == nil || !s.enabled() || !s.usesScrape || s.trackerPool > 0 || magnetTrackerCount > 0 {
		return
	}
	now := time.Now().Unix()
	last := toothlessWarned.Load()
	if now-last < 60 || !toothlessWarned.CompareAndSwap(last, now) {
		return
	}
	m.logger.Warn().
		Int("min_seeders", s.minSeeders).
		Msg("Seeder gate is enabled with the udp_scrape source but has NO trackers to ask: this magnet " +
			"carries none and seeder_gate.trackers is empty. Every grab will fail open, which looks " +
			"exactly like a gate that is working. Set seeder_gate.trackers, and note that " +
			"always_rm_tracker_urls strips a magnet's own list before this point.")
}

func resolveSeederGate(cfg config.SeederGateConfig) seederGateSettings {
	s := seederGateSettings{timeout: parseDurationOr(cfg.Timeout, config.DefaultSeederGateTimeout)}

	// TRI-STATE, and absent means OFF. An earlier version pointed absent at 1,
	// so an operator who had never heard of the feature got a live gate that
	// deletes transfers. For anything destructive, silence must mean do nothing.
	if cfg.MinSeeders != nil && *cfg.MinSeeders > 0 {
		s.minSeeders = *cfg.MinSeeders
	}
	if s.minSeeders <= 0 {
		return s
	}

	var chain swarm.Chain
	for _, name := range cfg.Sources {
		switch strings.ToLower(strings.TrimSpace(name)) {
		case config.SwarmSourceUDPScrape:
			perLookup := cfg.TrackersPerLookup
			if perLookup <= 0 {
				perLookup = config.DefaultSeederGateTrackersPerLookup
			}
			chain = append(chain, scrapeFor(cfg, perLookup))
			s.usesScrape = true
			s.trackerPool = len(cfg.Trackers)
		case config.SwarmSourceBitmagnet:
			if endpoint := strings.TrimSpace(cfg.BitmagnetURL); endpoint != "" {
				chain = append(chain, &swarm.Bitmagnet{Endpoint: endpoint})
			}
		}
		// An unrecognised name contributes nothing rather than failing the
		// whole gate. A typo should not silently arm a different backend, and
		// it must not turn the gate into a refuse-everything either — with no
		// usable source the gate is simply off.
	}
	if len(chain) > 0 {
		// Cache the WHOLE chain, not each source: two grabs of the same release
		// should cost one lookup regardless of which backend answered.
		s.source = &swarm.Cache{
			Inner:       chain,
			TTL:         parseDurationOr(cfg.CacheTTL, config.DefaultSeederGateCacheTTL),
			NegativeTTL: parseDurationOr(cfg.CacheNegativeTTL, config.DefaultSeederGateCacheNegativeTTL),
		}
	}
	return s
}

// scrapeFor returns the process-wide UDP scraper.
//
// ONE INSTANCE, deliberately. Per-tracker backoff and pool rotation are state
// about how public trackers are treating US, and rebuilding the scraper on
// every grab — which resolving from config on each call would do — would reset
// that state constantly and re-dial trackers already known to be penalising us.
// The measured penalty is cumulative, so forgetting it is the worst thing we
// could do.
var (
	sharedScrape   *swarm.UDPScrape
	scrapeSettings string
	scrapeMu       sync.Mutex
)

func scrapeFor(cfg config.SeederGateConfig, perLookup int) *swarm.UDPScrape {
	// Identity of the settings that shape the scraper. When they change (a live
	// config apply), the instance is rebuilt — and losing backoff state at that
	// point is correct, because the pool itself may have changed.
	binding := config.Get().NetworkBinding
	key := strings.Join(cfg.Trackers, ",") + "|" + binding.Tracker + "|" + binding.Default + "|" +
		cfg.ScrapeTimeout + "|" + strconv.Itoa(perLookup)

	scrapeMu.Lock()
	defer scrapeMu.Unlock()
	if sharedScrape == nil || scrapeSettings != key {
		binder := netbind.New(classSpecs(binding.Bindings()))
		sharedScrape = &swarm.UDPScrape{
			// Resolved per dial through the tracker class, so a reconnected
			// tunnel is picked up and a vanished one fails the scrape rather
			// than quietly leaving on the ordinary route.
			LocalAddr: func() (net.Addr, error) {
				addr, err := binder.UDPAddr(netbind.ClassTracker)
				if err != nil {
					return nil, err
				}
				if addr == nil {
					return nil, nil
				}
				return addr, nil
			},
			PerTracker: parseDurationOr(cfg.ScrapeTimeout, config.DefaultSeederGateScrapeTimeout),
			Trackers:   cfg.Trackers,
			PerLookup:  perLookup,
		}
		scrapeSettings = key
	}
	return sharedScrape
}

// classSpecs adapts config's string-keyed bindings to netbind's Class keys.
func classSpecs(raw map[string]string) map[netbind.Class]string {
	out := make(map[netbind.Class]string, len(raw))
	for name, spec := range raw {
		out[netbind.Class(name)] = spec
	}
	return out
}

func parseDurationOr(raw, fallback string) time.Duration {
	if d, err := utils.ParseDuration(raw); err == nil && d > 0 {
		return d
	}
	d, _ := utils.ParseDuration(fallback)
	return d
}

// magnetTrackers pulls the announce list out of a magnet link.
//
// ⚠️ EXPECT THIS TO BE EMPTY. Measured on a live deployment, 3,276 of 3,276
// stored magnets carried zero tr= entries — the uniform tracker visible in the
// qBittorrent API is a decypharr-synthesised placeholder, not magnet data. And
// always_rm_tracker_urls strips whatever is there before the gate sees it. So
// seeder_gate.trackers is in practice the only announce list the scrape gets,
// which is why an enabled gate with an empty pool warns instead of quietly
// allowing everything.
func magnetTrackers(magnetLink string) []string {
	if magnetLink == "" {
		return nil
	}
	parsed, err := url.Parse(magnetLink)
	if err != nil {
		return nil
	}
	return parsed.Query()["tr"]
}

// seederGateRefusal reports why an uncached grab should be refused, or "" to
// allow it.
//
// providerSeeders is whatever the provider itself reported on the transfer it
// just created. It may CONFIRM and may never CONDEMN: a provider has not had
// time to discover peers on a transfer that is seconds old, so a zero from it
// is ignorance rather than a verdict. Non-zero is real evidence and
// short-circuits the lookup entirely.
//
// That rule lives HERE and not inside a source, because it is a policy about
// how much to trust a provider's own reading — not a way of looking up a swarm.
func (m *Manager) seederGateRefusal(ctx context.Context, infoHash, magnetLink string, providerSeeders int) string {
	settings := resolveSeederGate(config.Get().SeederGate)
	if !settings.enabled() {
		return ""
	}
	if providerSeeders >= settings.minSeeders {
		return ""
	}
	if !swarm.IsInfoHash(infoHash) {
		return ""
	}

	trackers := magnetTrackers(magnetLink)
	settings.warnIfToothlessVia(m, len(trackers))

	// One budget for the whole lookup, every source and every tracker inside
	// it. The caller is an arr blocked on an add; it must not be able to wait
	// longer than this however many backends are configured.
	ctx, cancel := context.WithTimeout(ctx, settings.timeout)
	defer cancel()

	md, known := settings.source.Lookup(ctx, infoHash, trackers)
	if !known {
		// FAIL OPEN. No source could answer — no record, no tracker, a
		// timeout, a malformed reply. None of those are evidence about the
		// swarm, and refusing on ignorance would reject a large share of
		// everything the arrs ask for.
		return ""
	}
	if md.Seeders >= settings.minSeeders {
		return ""
	}
	return fmt.Sprintf("uncached release has %d seeders per %s, below the minimum of %d",
		md.Seeders, md.Source, settings.minSeeders)
}

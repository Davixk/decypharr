package config

// SeederGateConfig governs the GRAB-TIME refusal of an uncached torrent whose
// swarm is too small to finish.
//
// ⚠️ THIS IS NOT A STALL-PRUNE STAGE, AND THE DIFFERENCE IS THE WHOLE FEATURE.
//
// It runs while the *arr is still blocked inside its add call, holding its
// ranked list of candidate releases. A refusal there costs nothing: the arr
// takes the next release immediately, synchronously, with no indexer traffic.
//
// Once we have answered 200, that option is gone. Any later verdict — however
// correct — is an ASYNCHRONOUS download failure, and an async failure costs a
// FULL NEW SEARCH ACROSS EVERY INDEXER. Those are different orders of cost, not
// the same cost at different times.
//
// So this gate may never wait. There is no such thing as "settle for a moment
// and then judge" inside a live blocking request: a gate that waits has already
// answered, and everything it does afterwards pays the expensive price to avoid
// the cheap one.
type SeederGateConfig struct {
	// MinSeeders is the swarm size an UNCACHED torrent must have to be kept.
	//
	// TRI-STATE:
	//
	//	nil (absent) -> OFF. Silence must never enable something that deletes.
	//	0            -> OFF, stated explicitly.
	//	N            -> require N.
	//
	// Set 1 to turn it on. Measured across 107 live RealDebrid transfers
	// against actual outcomes: 0 seeders stalled 79% of the time, 1-2 stalled
	// 24%, 3+ stalled 27%. The cliff is entirely between 0 and 1, so 1 is the
	// whole signal and 3 would refuse 59% of transfers while predicting no
	// better.
	MinSeeders *int `json:"min_seeders,omitempty"`

	// Sources is the ordered list of swarm-metadata backends to consult:
	// "udp_scrape", "bitmagnet". The first one to answer wins; one that cannot
	// answer contributes nothing and the next is tried. Empty disables the gate.
	//
	// ORDER IS PRIORITY, AND FRESHNESS IS WHY IT MATTERS. A live scrape reports
	// the swarm as it is now. bitmagnet reports what it last saw, measured at a
	// median of 58.3 hours old with nothing inside 24h — good enough to RESCUE
	// a release the scrape could not reach, never good enough to condemn one.
	// Put "udp_scrape" first.
	//
	// The list exists so the backend can be swapped without touching the gate;
	// a Prowlarr-backed source is expected to join it later.
	Sources []string `json:"sources,omitempty"`

	// Timeout bounds the WHOLE lookup, every source and every tracker. On
	// expiry the gate ALLOWS. Defaults to 3s: the arrs tolerate far more, but
	// spending their patience is not free and a scrape that has not answered in
	// three seconds is not going to.
	Timeout string `json:"timeout,omitempty"`

	// BitmagnetURL is the GraphQL endpoint for the "bitmagnet" source.
	BitmagnetURL string `json:"bitmagnet_url,omitempty"`

	// Which interface a scrape egresses from is NetworkBindingConfig's job, on
	// the "tracker" class — it is one instance of a general per-class binding
	// rather than a scrape-specific knob, and lives there so the same mechanism
	// can route other traffic later.
	//
	// ScrapeTimeout bounds ONE tracker's connect+scrape exchange. Trackers are
	// queried in parallel, so this is not multiplied by their number. Defaults
	// to 1s.
	ScrapeTimeout string `json:"scrape_timeout,omitempty"`

	// Trackers is the UDP tracker POOL, used in addition to whatever announce
	// URLs a magnet carries.
	//
	// ⚠️ IN PRACTICE THIS IS USUALLY THE ONLY TRACKER LIST THERE IS. Measured
	// on a live deployment: 3,276 of 3,276 stored magnets carried ZERO tr=
	// entries. The uniform tracker visible in the qBittorrent API is a
	// decypharr-synthesised placeholder, not magnet data. So a gate configured
	// without a pool has nobody to ask on every single grab, fails open every
	// time, and is indistinguishable from a working one — which is why an
	// enabled gate with no reachable tracker WARNS rather than staying quiet.
	//
	// A POOL, NOT A TRACKER. One public tracker answered 2 of 8 back-to-back
	// scrapes and 0 of 5 spaced four seconds apart — spacing made it worse, so
	// the penalty is cumulative rather than rate-based. Under the grab floods
	// this deployment runs deliberately, a single tracker degrades exactly when
	// load is highest. Spread the requests.
	//
	// Also note always_rm_tracker_urls strips a magnet's own list before the
	// gate sees it, which makes this pool the sole source outright.
	Trackers []string `json:"trackers,omitempty"`

	// TrackersPerLookup caps how many pool members one lookup asks. Asking all
	// of them would multiply our rate against every member — the single-tracker
	// limit again, with more configuration. Defaults to 3, rotating.
	TrackersPerLookup int `json:"trackers_per_lookup,omitempty"`

	// CacheTTL is how long a swarm reading is reused for the same infohash.
	//
	// THE HIGHEST-VALUE SETTING HERE. The same release is re-grabbed and
	// re-probed constantly, so one scrape can serve all of it; against a
	// tracker with a cumulative penalty window the fix is to make far fewer
	// requests, not to pace them. Defaults to 15m.
	CacheTTL string `json:"cache_ttl,omitempty"`

	// CacheNegativeTTL is how long an UNANSWERABLE lookup is remembered, so a
	// burst does not re-ask a tracker that has just refused us. Deliberately
	// short — it exists to stop a stampede, not to give up on a hash. A cached
	// unknown still allows. Defaults to 1m.
	CacheNegativeTTL string `json:"cache_negative_ttl,omitempty"`
}

const (
	// DefaultSeederGateTimeout bounds the whole lookup.
	DefaultSeederGateTimeout = "3s"
	// DefaultSeederGateScrapeTimeout bounds one tracker exchange.
	DefaultSeederGateScrapeTimeout = "1s"
	// DefaultSeederGateCacheTTL reuses a positive reading. A swarm does not
	// change much in minutes, and re-scraping is what gets us rate-limited.
	DefaultSeederGateCacheTTL = "15m"
	// DefaultSeederGateCacheNegativeTTL remembers an unanswerable lookup just
	// long enough to stop a burst re-asking a tracker that refused us.
	DefaultSeederGateCacheNegativeTTL = "1m"
	// DefaultSeederGateTrackersPerLookup rotates a small subset of the pool.
	DefaultSeederGateTrackersPerLookup = 3

	SwarmSourceUDPScrape = "udp_scrape"
	SwarmSourceBitmagnet = "bitmagnet"
)

// FAIL OPEN. Absence of data means ALLOW, always.
//
// Every one of these means allow: no source configured, no tracker in the
// magnet, no response, a malformed packet, a null count, a timeout, an
// unparseable infohash. Refusing on ignorance would silently reject a large
// fraction of everything the *arrs ask for, and the stall sweep still catches a
// dead torrent later — just at the async price this gate exists to avoid.
func (s SeederGateConfig) IsZero() bool {
	return s.MinSeeders == nil && len(s.Sources) == 0 && s.Timeout == "" &&
		s.BitmagnetURL == "" && s.ScrapeTimeout == "" &&
		len(s.Trackers) == 0 && s.TrackersPerLookup == 0 &&
		s.CacheTTL == "" && s.CacheNegativeTTL == ""
}

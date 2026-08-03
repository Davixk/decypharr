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

	// ScrapeBindAddr is the LOCAL address the UDP scrape egresses from, e.g.
	// "10.2.0.2:0".
	//
	// ⚠️ A UDP SCRAPE REVEALS THIS HOST'S IP TO EVERY TRACKER IT CONTACTS.
	// Nothing else in decypharr talks to a tracker at all — the point of a
	// debrid service is that the provider faces the swarm and we never do.
	// Enabling "udp_scrape" gives that up for one packet per uncached grab.
	//
	// Set this to a VPN interface's local address to egress the same way other
	// indexing tools do. It is deliberately not defaulted: no address this code
	// could pick is right for someone else's network, and quietly using the
	// container's default route is exactly the leak. An address that does not
	// resolve makes the scrape fail rather than fall back.
	ScrapeBindAddr string `json:"scrape_bind_addr,omitempty"`

	// ScrapeTimeout bounds ONE tracker's connect+scrape exchange. Trackers are
	// queried in parallel, so this is not multiplied by their number. Defaults
	// to 1s.
	ScrapeTimeout string `json:"scrape_timeout,omitempty"`

	// FallbackTrackers are scraped ONLY when a magnet carries no announce list
	// of its own (a DHT-only magnet). Absence of trackers is otherwise an
	// absence, not a licence to invent an announce list.
	//
	// Note this interacts with always_rm_tracker_urls: with that on, magnets
	// reach the gate stripped of their `tr=` list, so EVERY grab falls back to
	// this set — and with it empty, the scrape can never answer.
	FallbackTrackers []string `json:"fallback_trackers,omitempty"`
}

const (
	// DefaultSeederGateTimeout bounds the whole lookup.
	DefaultSeederGateTimeout = "3s"
	// DefaultSeederGateScrapeTimeout bounds one tracker exchange.
	DefaultSeederGateScrapeTimeout = "1s"

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
		s.BitmagnetURL == "" && s.ScrapeBindAddr == "" && s.ScrapeTimeout == "" &&
		len(s.FallbackTrackers) == 0
}

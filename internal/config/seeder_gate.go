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
// the cheap one. A previous attempt at this feature was built as a stage of the
// 5-minute stall sweep with a 10-minute settle window, which inverted it
// perfectly — every refusal it could ever have made would have arrived after the
// response.
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

	// BitmagnetURL is the GraphQL endpoint of a local bitmagnet index, e.g.
	// "http://10.0.0.2:30036/graphql". Empty disables the gate outright.
	//
	// Local by design. The lookup sits inside a request an *arr is blocked on,
	// so the only acceptable source is one that answers in milliseconds; a
	// measured bitmagnet lookup is 1-3ms. Prowlarr has seeder counts but only
	// behind a multi-second indexer fan-out, which cannot go here at all.
	BitmagnetURL string `json:"bitmagnet_url,omitempty"`

	// Timeout bounds the lookup. On expiry the gate ALLOWS — see the fail-open
	// note below. Defaults to 2s, which is ~1000x the measured response time
	// and still nothing against the >135s the arrs tolerate.
	Timeout string `json:"timeout,omitempty"`
}

// DefaultSeederGateTimeout bounds a lookup that must not be felt by the caller.
const DefaultSeederGateTimeout = "2s"

// FAIL OPEN. Absence of data means ALLOW, always.
//
// Coverage was measured at ~53%: of 60 real infohashes, bitmagnet matched 46
// (77%) and only 32 of those carried a seeder count. So roughly half of all
// grabs have no answer available, and refusing on ignorance would silently
// reject half of everything the *arrs ask for.
//
// Every one of these means allow: no record, a null count, an HTTP error, a
// malformed response, a timeout, a disabled endpoint, an unparseable infohash.
func (s SeederGateConfig) IsZero() bool {
	return s.MinSeeders == nil && s.BitmagnetURL == "" && s.Timeout == ""
}

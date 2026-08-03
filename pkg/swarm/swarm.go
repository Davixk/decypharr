// Package swarm answers one question — how big is this torrent's swarm right
// now — behind an interface that hides where the answer came from.
//
// WHY AN INTERFACE AND NOT A FUNCTION. The first implementation of this feature
// was a bitmagnet call site rather than a lookup abstraction, and when the data
// turned out to be unusable (median staleness 58.3 HOURS, nothing inside 24h,
// which would have refused ~18% of grabs on a two-and-a-half-day-old number)
// the source could not be replaced without rewriting the caller. The operator's
// requirement is explicit: swapping how we obtain live torrent metadata must be
// easy, because the shortlist is not settled — UDP tracker scrape now, and a
// Prowlarr-backed lookup is still on the table.
//
// So the gate depends on Source and nothing else. Implementations know how to
// fetch; they do not know what a threshold is or what happens on a refusal.
package swarm

import "context"

// Metadata is a swarm reading at a point in time.
type Metadata struct {
	Seeders   int
	Leechers  int
	Completed int
	// Source names the implementation that answered, for logging. A verdict
	// that deletes a transfer should be able to say what informed it.
	Source string
}

// Source is one way of obtaining a swarm reading.
//
// ⚠️ THE CONTRACT IS THE SECOND RETURN VALUE. It is KNOWN, and false NEVER
// means "zero seeders" — it means the question could not be answered. An
// implementation must return false for every failure it meets: no response, a
// timeout, a malformed packet, an unknown hash, a null count, no trackers to
// ask. Returning a confident zero for an unanswered question is the one bug
// that matters here, because zero is exactly the value that triggers a refusal.
type Source interface {
	// Name identifies the implementation.
	Name() string
	// Lookup reports the swarm reading for infoHash. trackers is the torrent's
	// own announce list, which some implementations need and others ignore.
	Lookup(ctx context.Context, infoHash string, trackers []string) (Metadata, bool)
}

// Chain queries sources in order and takes the first positive answer.
//
// Order is priority: put the freshest source first. A source that cannot answer
// contributes nothing and the next one is tried, so a chain degrades to its
// weakest member rather than to a wrong answer. An exhausted chain returns
// false, which every caller must read as allow.
type Chain []Source

func (c Chain) Name() string { return "chain" }

func (c Chain) Lookup(ctx context.Context, infoHash string, trackers []string) (Metadata, bool) {
	for _, source := range c {
		if source == nil {
			continue
		}
		if ctx.Err() != nil {
			// The budget is spent. Stop rather than let later sources run
			// against an already-cancelled context and report false answers.
			return Metadata{}, false
		}
		if md, ok := source.Lookup(ctx, infoHash, trackers); ok {
			return md, true
		}
	}
	return Metadata{}, false
}

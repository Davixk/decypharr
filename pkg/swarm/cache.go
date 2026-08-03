package swarm

import (
	"context"
	"strings"
	"sync"
	"time"
)

// Cache wraps a Source so the same infohash is scraped once, not once per grab.
//
// THE HIGHEST-VALUE PIECE, measured. A public tracker answered 2 of 8
// back-to-back scrapes, and 0 of 5 spaced four seconds apart — spacing made it
// WORSE, which reads as a cumulative penalty window rather than a rate limit.
// Against an operator who deliberately runs grab floods, the fix is not to
// pace requests but to stop making most of them: the same release is re-grabbed
// and re-probed constantly, and one scrape can serve all of it.
//
// BOTH OUTCOMES ARE CACHED, with different lifetimes and for different reasons.
// A positive reading is cached because a swarm does not change much in minutes.
// An UNKNOWN is cached because re-asking a tracker that just refused us is how
// the penalty window gets deeper — but only briefly, so a transient block does
// not blind the gate for long. A cached unknown still means allow, so this can
// never turn a lookup failure into a refusal.
type Cache struct {
	Inner Source
	// TTL is how long a positive reading is reused.
	TTL time.Duration
	// NegativeTTL is how long an unanswerable lookup is remembered. Short: it
	// exists to stop a stampede, not to give up on a hash.
	NegativeTTL time.Duration

	mu      sync.Mutex
	entries map[string]cacheEntry
}

type cacheEntry struct {
	md      Metadata
	known   bool
	expires time.Time
}

func (c *Cache) Name() string {
	if c.Inner == nil {
		return "cache"
	}
	return "cache(" + c.Inner.Name() + ")"
}

func (c *Cache) Lookup(ctx context.Context, infoHash string, trackers []string) (Metadata, bool) {
	if c.Inner == nil {
		return Metadata{}, false
	}
	key := strings.ToLower(infoHash)
	now := time.Now()

	c.mu.Lock()
	if entry, ok := c.entries[key]; ok && now.Before(entry.expires) {
		c.mu.Unlock()
		return entry.md, entry.known
	}
	c.mu.Unlock()

	// Deliberately NOT single-flighted. Two concurrent grabs of the same hash
	// cost one extra scrape; holding a lock across a network call inside a
	// request an arr is blocked on would cost far more, and the cache fills
	// immediately afterwards anyway.
	md, known := c.Inner.Lookup(ctx, infoHash, trackers)

	ttl := c.NegativeTTL
	if known {
		ttl = c.TTL
	}
	if ttl <= 0 {
		return md, known
	}

	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[string]cacheEntry)
	}
	// Opportunistic eviction. The map is only ever read by key, so a full sweep
	// on write keeps it from growing without bound across a long uptime without
	// needing a background goroutine.
	if len(c.entries) > cacheSweepThreshold {
		for k, v := range c.entries {
			if now.After(v.expires) {
				delete(c.entries, k)
			}
		}
	}
	c.entries[key] = cacheEntry{md: md, known: known, expires: now.Add(ttl)}
	c.mu.Unlock()

	return md, known
}

// cacheSweepThreshold is when a write also evicts expired rows. Well above any
// plausible in-flight grab burst, so the sweep is rare.
const cacheSweepThreshold = 2048

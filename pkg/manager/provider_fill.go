package manager

import (
	"sync"
	"time"

	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
)

// Provider fill tracking.
//
// WHAT THIS DISAMBIGUATES: AllDebrid answers two opposite conditions with the
// SAME error code, MAGNET_TOO_MANY.
//
//	the DAILY add allowance is spent  -> TRANSIENT. Resets on the provider's own
//	                                     boundary; a held item will go through.
//	the STORED-item cap is full       -> PERMANENT. Nothing we finish or wait
//	                                     for frees it; only deleting does.
//
// Neither the code nor the message can tell them apart. The observed text read
// "Magnets limit reached (1000 accross all tabs)" while the binding constraint
// was the 5,000 stored cap — so the number IN the string named the wrong limit.
// The account's own fill level is the only reliable discriminator.
//
// WHY IT IS CACHED: the fill is consulted when an add is refused, and at the
// scale this system runs a refused-add storm would otherwise mean thousands of
// full-account enumerations. Fill moves slowly — a snapshot refreshed every few
// minutes is ample, and one enumeration is a single request answering in under
// a second.
//
// ⚠️ CONTRACT: this reads a COUNT, which is legitimate, unlike inferring
// anything from a particular hash being absent. But a count from a FAILED or
// partial enumeration is not a count — when the probe cannot answer, callers
// get "unknown" and must decline to classify rather than assume room.

// providerFillTTL is how long a fill snapshot is trusted. Short enough that a
// cap freed by a prune is noticed promptly, long enough that a burst of refused
// adds costs one enumeration rather than thousands.
const providerFillTTL = 3 * time.Minute

type providerFillSnapshot struct {
	count int
	known bool
	takenAt time.Time
}

// providerFillCache memoizes per-provider stored-item counts.
type providerFillCache struct {
	mu   sync.Mutex
	byProvider map[string]providerFillSnapshot
	inflight   map[string]*sync.WaitGroup
}

func newProviderFillCache() *providerFillCache {
	return &providerFillCache{
		byProvider: map[string]providerFillSnapshot{},
		inflight:   map[string]*sync.WaitGroup{},
	}
}

// fill returns how many items the provider currently stores.
//
// The second return is KNOWN, and it is the important one: false means the
// enumeration failed, was never attempted, or the provider does not enumerate.
// It never means "zero". A caller that reads !known as an empty account would
// turn a provider outage into "there is plenty of room", which is precisely the
// class of confident wrong answer this codebase keeps having to undo.
func (c *providerFillCache) fill(name string, client debrid.Client, now time.Time) (int, bool) {
	if c == nil || client == nil {
		return 0, false
	}

	c.mu.Lock()
	if snap, ok := c.byProvider[name]; ok && now.Sub(snap.takenAt) < providerFillTTL {
		c.mu.Unlock()
		return snap.count, snap.known
	}
	// Collapse concurrent refreshes: a storm of refused adds must cost ONE
	// enumeration, not one per add.
	if wg, ok := c.inflight[name]; ok {
		c.mu.Unlock()
		wg.Wait()
		c.mu.Lock()
		snap := c.byProvider[name]
		c.mu.Unlock()
		return snap.count, snap.known
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	c.inflight[name] = wg
	c.mu.Unlock()

	count, known := enumerateProviderFill(client)

	c.mu.Lock()
	c.byProvider[name] = providerFillSnapshot{count: count, known: known, takenAt: now}
	delete(c.inflight, name)
	c.mu.Unlock()
	wg.Done()

	return count, known
}

// invalidate drops a provider's snapshot so the next read re-enumerates. Used
// after we delete items on that provider, where waiting out the TTL would keep
// reporting a cap that is no longer full.
func (c *providerFillCache) invalidate(name string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.byProvider, name)
	c.mu.Unlock()
}

func enumerateProviderFill(client debrid.Client) (int, bool) {
	torrents, err := client.GetAllTorrents()
	if err != nil {
		// UNKNOWN, not zero. See the contract note above.
		return 0, false
	}
	return len(torrents), true
}

// providerFill is the manager-level accessor.
func (m *Manager) providerFill(name string) (int, bool) {
	client := m.ProviderClient(name)
	if client == nil {
		return 0, false
	}
	return m.fillCache.fill(name, client, time.Now())
}

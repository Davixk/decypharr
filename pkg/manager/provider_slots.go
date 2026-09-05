package manager

import (
	"sync"
	"time"

	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
)

// Provider free-slot memoization.
//
// 🔴 WHY THIS EXISTS: ONE ADD WAS COSTING TWO RATE-LIMIT TOKENS.
//
// The synchronous add walks the provider chain inside the qBittorrent HTTP
// handler, and every provider it visits made two limiter-gated calls — the slot
// probe, then the submit. Each of those can wait up to the token-wait ceiling,
// so an add cost 2x the ceiling before anything else went wrong. Against a 10s
// ceiling that is 20s, and the *arr gave up at 25s.
//
// That arithmetic is the whole outage. It was reproduced off-production by
// driving the real add path at sustained arrival pressure: an add took 2.02x
// the ceiling, against production's 2.5x. Three builds tried to fix WHERE the
// waiting happened; none of them reduced how many gated waits sit between an
// *arr's request and its answer.
//
// Halving that count is the cheapest available reduction, and the probe is the
// half that can go: it asks a question whose answer barely changes and which
// the provider will answer again anyway by refusing.
//
// ⚠️ A STALE READ IS SAFE HERE, AND THE ADMISSION PATH ALREADY SAYS SO:
//
//	"It is not a race guard. If this reading loses a race with the provider's
//	 own accounting, the provider refuses and the job requeues, which is
//	 already correct without any reserve."
//
// So the only cost of memoizing is admitting an item the provider then declines
// — a case that is handled, classified and cheap. The cost of NOT memoizing was
// a total write-path outage.
//
// UNKNOWN IS NOT ZERO, exactly as in the fill cache. A failed probe reports
// known=false and the caller admits rather than inventing a refusal; declining
// an add because a probe failed is the same mistake as condemning a release
// because a health check timed out.

// providerSlotTTL is how long a free-slot snapshot is trusted.
//
// Deliberately short. Slots move fast — every completed download frees one —
// and the value of this cache is collapsing a burst, not remembering. Five
// seconds turns a storm of concurrent adds into one probe while keeping the
// snapshot fresher than any add it gates.
const providerSlotTTL = 5 * time.Second

type providerSlotSnapshot struct {
	slots   int
	known   bool
	takenAt time.Time
}

type providerSlotCache struct {
	mu         sync.Mutex
	byProvider map[string]providerSlotSnapshot
	inflight   map[string]*sync.WaitGroup
	probes     int
}

func newProviderSlotCache() *providerSlotCache {
	return &providerSlotCache{
		byProvider: map[string]providerSlotSnapshot{},
		inflight:   map[string]*sync.WaitGroup{},
	}
}

// slots returns the provider's free-slot count, probing at most once per TTL
// per provider no matter how many callers ask at once.
func (c *providerSlotCache) slots(name string, client debrid.Client, now time.Time) (int, bool, error) {
	if c == nil || client == nil {
		return 0, false, nil
	}

	c.mu.Lock()
	if snap, ok := c.byProvider[name]; ok && now.Sub(snap.takenAt) < providerSlotTTL {
		c.mu.Unlock()
		return snap.slots, snap.known, nil
	}
	// Collapse concurrent probes. Without this the cache would still cost one
	// gated call per add during exactly the burst it exists to survive.
	if wg, ok := c.inflight[name]; ok {
		c.mu.Unlock()
		wg.Wait()
		c.mu.Lock()
		snap := c.byProvider[name]
		c.mu.Unlock()
		return snap.slots, snap.known, nil
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	c.inflight[name] = wg
	c.probes++
	c.mu.Unlock()

	slots, err := client.GetAvailableSlots()
	known := err == nil

	c.mu.Lock()
	c.byProvider[name] = providerSlotSnapshot{slots: slots, known: known, takenAt: now}
	delete(c.inflight, name)
	c.mu.Unlock()
	wg.Done()

	return slots, known, err
}

// probeCount reports how many real probes have been issued, for tests that need
// to assert the collapsing actually collapses.
func (c *providerSlotCache) probeCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.probes
}

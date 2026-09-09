package manager

import (
	"context"
	"errors"
	"sync"
	"time"

	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// Provider free-slot tracking.
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
// the ceiling, against production's 2.5x.
//
// 🔑 THE ADD PATH NO LONGER PROBES AT ALL. A background poller refreshes each
// provider's capacity on its own schedule and admission reads whatever the last
// reading was. That removes the second gated wait from the synchronous path
// permanently rather than merely making it rarer.
//
// This replaces an on-demand memoizing cache, which was the wrong shape and was
// argued out of the codebase rather than defended:
//
//   - A capacity probe has no business competing with submits for the same
//     token bucket. The cache made that contention less frequent; the poller
//     removes it from the add path entirely.
//   - The cache had to answer "the probe failed" inside the hot path, and the
//     ONE production defect in it lived entirely in that branch: a failed probe
//     stored slots=0, and the caller read that zero as "provider full",
//     manufacturing refusals for a whole TTL with no request going out.
//
// ⚠️ A STALE READ IS SAFE HERE, AND THE ADMISSION PATH ALREADY SAID SO:
//
//	"It is not a race guard. If this reading loses a race with the provider's
//	 own accounting, the provider refuses and the job requeues, which is
//	 already correct without any reserve."
//
// UNKNOWN IS NOT ZERO. A provider that has never answered, or whose reading has
// gone stale, is admitted rather than refused. Declining an add because a probe
// failed is the same mistake as condemning a release because a health check
// timed out.

const (
	// providerSlotPollEvery is the poller's cadence per provider.
	//
	// One request per provider per interval, spent on our own schedule instead
	// of inside an add. At 10s that is 6 req/min against RealDebrid's global
	// 250/min budget — under 3%, and it buys the add path a capacity answer it
	// never has to wait for.
	providerSlotPollEvery = 10 * time.Second

	// providerSlotMaxAge is how old a reading may be and still justify refusing
	// an add.
	//
	// 🛑 THIS IS A CEILING ON CONFIDENCE, NOT A CACHE TTL. Past it the reading
	// is not discarded — it stops being allowed to REFUSE. If the poller has
	// stalled, or the provider has stopped answering, the honest state is "we
	// do not know", and not knowing admits. Six missed polls is a generous
	// allowance for a transient stall and still far short of the horizon where
	// a zero could be badly wrong.
	providerSlotMaxAge = 60 * time.Second
)

type providerSlotSnapshot struct {
	// slots is what the PROVIDER said, unmodified.
	slots int
	// consumed counts the adds WE have accepted since that reading. Kept apart
	// from slots so a log line can distinguish the provider's answer from our
	// own estimate on top of it — which matters when a refusal has to be
	// explained after the fact.
	consumed int
	known    bool
	takenAt  time.Time
}

// effective is the reading adjusted for what we have spent since taking it.
func (s providerSlotSnapshot) effective() int {
	return s.slots - s.consumed
}

type providerSlotCache struct {
	mu         sync.Mutex
	byProvider map[string]providerSlotSnapshot
	probes     int
}

func newProviderSlotCache() *providerSlotCache {
	return &providerSlotCache{byProvider: map[string]providerSlotSnapshot{}}
}

// refresh probes the provider and publishes the result, resetting the consumed
// counter because the new reading already accounts for everything we spent.
//
// Only the poller calls this. It blocks on the provider's rate limiter, which
// is precisely why no request-serving path may.
func (c *providerSlotCache) refresh(name string, client debrid.Client, now time.Time) (int, bool, error) {
	if c == nil || client == nil {
		return 0, false, nil
	}

	c.mu.Lock()
	c.probes++
	c.mu.Unlock()

	slots, err := client.GetAvailableSlots()
	known := err == nil

	c.mu.Lock()
	c.byProvider[name] = providerSlotSnapshot{slots: slots, known: known, takenAt: now}
	c.mu.Unlock()

	return slots, known, err
}

// consume records that the provider just accepted an add from us, so the next
// admission reads a capacity we have not already spent.
//
// 🔴 THE DEFECT THIS FIXES: nothing invalidated the reading on a successful
// submit. Inside one refresh interval every subsequent add read a number that
// was stale by at least one slot — stale because WE made it so — and a burst of
// adds all read the same pre-burst value, so the error compounded with exactly
// the concurrency this machinery exists to survive. At a provider's active
// ceiling that is the difference between admitting what fits and admitting a
// storm the provider then has to refuse one request at a time, each refusal
// costing a token and, on RealDebrid, counting against the same global budget.
//
// ⚠️ takenAt IS DELIBERATELY NOT ADVANCED. Spending a slot is not evidence
// about anything else the provider is doing, so it must not renew the reading's
// claim to being current.
func (c *providerSlotCache) consume(name string) {
	if c == nil || name == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	snap, ok := c.byProvider[name]
	if !ok || !snap.known {
		return
	}
	snap.consumed++
	c.byProvider[name] = snap
}

// reading returns the last known capacity for a provider. It never probes and
// never blocks, which is the entire point: this is what the add path calls.
//
// known=false means no reading has ever landed, or the last probe failed. It
// never means zero.
func (c *providerSlotCache) reading(name string, now time.Time) (slots int, known bool, age time.Duration) {
	if c == nil {
		return 0, false, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	snap, ok := c.byProvider[name]
	if !ok || !snap.known {
		return 0, false, 0
	}
	return snap.effective(), true, now.Sub(snap.takenAt)
}

// probeCount reports how many real probes have been issued, for tests that need
// to assert the add path is not issuing any.
func (c *providerSlotCache) probeCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.probes
}

// pollProviderSlots starts one poller per provider. Serial per provider, so a
// probe that outlasts its interval delays only its own next tick, and slow
// providers cannot hold up fast ones.
func (m *Manager) pollProviderSlots(ctx context.Context) {
	if m.slotCache == nil {
		return
	}
	m.Clients().Range(func(name string, client debrid.Client) bool {
		go m.pollOneProviderSlots(ctx, name, client)
		return true
	})
}

func (m *Manager) pollOneProviderSlots(ctx context.Context, name string, client debrid.Client) {
	ticker := time.NewTicker(providerSlotPollEvery)
	defer ticker.Stop()

	for {
		_, _, err := m.slotCache.refresh(name, client, time.Now())
		if errors.Is(err, debridTypes.ErrAvailableSlotsUnknown) {
			// STRUCTURAL, NOT TRANSIENT. This provider exposes no capacity
			// endpoint at all — AllDebrid is the live case — so asking again
			// will never produce a different answer. Stop, and let admission
			// fall through to the provider's own refusal, which is the only
			// signal it has and is authoritative in a way no local reading is.
			m.logger.Debug().
				Str("provider", name).
				Msg("Provider reports no capacity endpoint; not polling it for free slots")
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

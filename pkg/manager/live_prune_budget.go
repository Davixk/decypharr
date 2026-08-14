package manager

import (
	"sync"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// THE RATE LIMIT ON PRUNES THAT DO NOT HAPPEN INSIDE A RUN.
//
// repairDeletionBudget bounds a sweep. It cannot bound these, because the whole
// point of an event-driven verdict is that it does NOT wait for a sweep: a
// confirmed debrid takedown and a confirmed usenet dead article prune the moment
// a read proves the content is gone, so the library stops lying between sweeps.
// No run, no run boundary, no cap.
//
// The hazard MaxDeletionsPerRun was written for applies unchanged, and for
// usenet it is sharper than for debrid:
//
//	a debrid takedown is one provider's LEGAL statement about one release, and
//	  the takedown path asks the other configured debrids before condemning;
//	a usenet article-not-found is "missing on every configured provider", which
//	  sounds like corroboration and, with one provider configured, is one
//	  server's word about its own index.
//
// A provider that changes retention or loses an index shelf answers 430 for a
// great many articles at once. Every read of every affected file would condemn
// and prune its entry, at READ RATE, until the library was gone.
//
// ⚠️ WHAT THIS DOES NOT DO. It does not decide whether content is dead, and it
// does not keep dead content serving: the mark always happens, so a condemned
// entry stops serving and starts refusing reads whether or not it is pruned.
// Only the DELETION is deferred, and the nightly sweep collects the remainder
// under its own cap. Refusing to prune is therefore always recoverable and
// always visible; pruning a library because a news server had a bad afternoon is
// neither.
//
// 🔻 THE FIRST VERSION OF THIS SHIPPED A BARE 50/HOUR AND THAT WAS WRONG.
//
// Operator's ruling: "50 is a magic, and low, number." Both halves land. The
// derivation was "an order of magnitude above genuine decay, two below a
// runaway" — which measured only ONE of the two things the rail sits between,
// and picked the number from the wrong one. Genuine decay (~5 confirmed
// takedowns a day here) is the FLOOR of what must pass, not the scale to size
// against. A real takedown wave — a distributor clearing a catalogue, an
// indexer's postings expiring together — puts hundreds of entries into the read
// path in an hour, legitimately, and 50/hour would have visibly throttled every
// one of them while the library went on serving content that was already gone.
//
// THE RAIL MUST NEVER THROTTLE A LEGITIMATE OPERATION. It exists for exactly one
// failure: mass deletion driven by a source that is WRONG — the July incident
// here, ~5,000 files lost to one bad listing.
//
// So the bound is PROPORTIONAL, because that is the only form in which
// "catastrophic" means the same thing to every library. Hundreds of deletions in
// an hour is unremarkable in a large library and an emergency in a small one,
// and a percentage says exactly that in both directions without anyone tuning
// it. An absolute number cannot: whatever constant is chosen is simultaneously
// too tight for the big library and too loose for the small one.
//
//	ceiling = max(floor, percent × library entries), per rolling hour
//
// The floor exists so a small or freshly-seeded library is not pinned near zero
// by the percentage, and it is set far above any rate real decay produces.
//
// ⚠️ AND NOT A TOKEN BUCKET — a claim in this comment used to say the opposite.
// It read "a bucket would smooth the rate, and smoothing is precisely wrong
// here, a burst IS the signal this exists to catch". That is now known to be
// backwards: a legitimate wave arrives as a burst, so smoothing it is the
// visible throttle the ruling forbids. A rolling window with a generous ceiling
// passes the whole burst instantly and still stops a sustained runaway, which is
// what a bucket cannot do without metering the wave.
type livePruneBudget struct {
	mu     sync.Mutex
	events []time.Time
}

func newLivePruneBudget() *livePruneBudget {
	return &livePruneBudget{}
}

// livePruneCeiling resolves the bound live, per reservation, so an operator
// watching a runaway can tighten it without restarting the mount.
//
// librarySize is the current entry count — an in-memory index length, so this
// costs nothing to consult and cannot go stale between sweeps.
//
//	MaxLivePrunesPerHour  < 0  -> unlimited, the escape hatch
//	                      > 0  -> an explicit absolute ceiling, overriding the
//	                              proportional term entirely. An operator who has
//	                              decided on a number gets that number.
//	                        0  -> derive it: max(floor, percent × librarySize)
//
//	LivePrunePercent      < 0  -> proportional term off, floor only
//	                        0  -> the default percent
func livePruneCeiling(librarySize int) int {
	repair := config.Get().Repair
	switch v := repair.MaxLivePrunesPerHour; {
	case v < 0:
		return 0 // unlimited
	case v > 0:
		return v
	}

	ceiling := config.DefaultLivePruneFloor
	percent := repair.LivePrunePercent
	if percent == 0 {
		percent = config.DefaultLivePrunePercent
	}
	if percent > 0 && librarySize > 0 {
		if scaled := librarySize * percent / 100; scaled > ceiling {
			ceiling = scaled
		}
	}
	return ceiling
}

// librarySize reports how many entries the bound is measured against, or 0 when
// it cannot be read.
//
// A count we could not take must NOT collapse the ceiling to the floor silently
// — that would be the same "absence of a measurement read as a small
// measurement" mistake the fill check made. It returns 0, and livePruneCeiling
// treats 0 as "no proportional term available", which lands on the floor with
// the reason visible in the log rather than inferred.
func (m *Manager) librarySize() int {
	if m == nil || m.storage == nil {
		return 0
	}
	n, err := m.storage.Count()
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// reserve takes one slot if the last hour has room. It returns the slot count
// used so far so the caller can log a trip with real numbers rather than "cap
// reached".
//
// Reserving is what COMMITS the deletion, so it is called immediately before
// the prune and never speculatively: a reservation that is not spent is a
// deletion another entry could have had.
func (b *livePruneBudget) reserve(now time.Time, librarySize int) (ok bool, used, limit int) {
	limit = livePruneCeiling(librarySize)
	if b == nil {
		// A Manager built without one (test fixtures, mostly) must not silently
		// become UNLIMITED — that is the vacuous-guard shape that has bitten
		// this codebase before. But it must not silently become zero either,
		// which would make every prune test pass by doing nothing. Allowing the
		// prune is the honest answer: no budget was configured, so nothing is
		// being enforced, and the caller's own tests are what prove the prune
		// ran.
		return true, 0, limit
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	cutoff := now.Add(-time.Hour)
	kept := b.events[:0]
	for _, t := range b.events {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	b.events = kept
	used = len(b.events)

	if limit > 0 && used >= limit {
		return false, used, limit
	}
	b.events = append(b.events, now)
	return true, used + 1, limit
}

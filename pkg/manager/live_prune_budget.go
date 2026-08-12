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
// The window is a simple rolling one rather than a token bucket. A bucket would
// smooth the rate, and smoothing is precisely wrong here — a burst is the signal
// this exists to catch.
type livePruneBudget struct {
	mu     sync.Mutex
	events []time.Time
}

func newLivePruneBudget() *livePruneBudget {
	return &livePruneBudget{}
}

// maxLivePrunesPerHour resolves the knob live, so an operator watching a
// runaway can tighten it without restarting the mount — the same reasoning as
// every other threshold read at its decision point.
//
// 0/unset -> the default; negative -> unlimited.
func maxLivePrunesPerHour() int {
	switch v := config.Get().Repair.MaxLivePrunesPerHour; {
	case v < 0:
		return 0 // unlimited
	case v == 0:
		return config.DefaultMaxLivePrunesPerHour
	default:
		return v
	}
}

// reserve takes one slot if the last hour has room. It returns the slot count
// used so far so the caller can log a trip with real numbers rather than "cap
// reached".
//
// Reserving is what COMMITS the deletion, so it is called immediately before
// the prune and never speculatively: a reservation that is not spent is a
// deletion another entry could have had.
func (b *livePruneBudget) reserve(now time.Time) (ok bool, used, limit int) {
	limit = maxLivePrunesPerHour()
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

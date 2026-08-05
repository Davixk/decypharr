package manager

import (
	"errors"
	"fmt"
	"sync"
	"time"

	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// Capacity holds — entries accepted at grab time that no provider had room for
// yet, waiting for capacity.
//
// EVENT-DRIVEN, NOT POLLED, and the distinction is the whole design. An earlier
// version re-attempted every held entry on a 30-second cadence, which at N held
// entries is N/30 provider calls per second, forever, asking a question we
// already know the answer to.
//
// We do know it. decypharr starts every download, observes its completion,
// fails it, and — since stall pruning — deletes it. A provider slot becoming
// free is an event this process WITNESSES, and usually one it CAUSES. There is
// nothing external to discover, so there is nothing to poll:
//
//	a download finishes            -> its active slot frees -> admit one
//	a download fails               -> same
//	a placement is deleted         -> same (prune, user delete, cleanup)
//
// Provider polling cost for the hold is therefore zero, and admission happens
// the instant a slot frees rather than up to a cadence later. It also means
// there is no interval to choose, which is the point: the cadence was a number
// nobody could justify.
//
// ONE EVENT ADMITS ONE ENTRY. That is not a throttle bolted on, it is the
// accounting: one freed slot is room for exactly one entry. A released entry
// that is refused again simply returns here, which is the correct outcome if
// the slot was not really free.
//
// The reconciliation sweep in workers.go covers the residue — capacity that
// changes WITHOUT us witnessing it. See reconcileHeldCapacity for what actually
// falls into that category and why it is minutes rather than seconds.

// capacityHoldQueue is a FIFO of jobs waiting on provider capacity.
//
// FIFO because held entries are otherwise indistinguishable: nothing about
// waiting longer makes an entry more or less likely to succeed, so the only
// defensible order is the one that does not starve anything.
type capacityHoldQueue struct {
	mu   sync.Mutex
	jobs []*Job
	held map[string]struct{}
}

func newCapacityHoldQueue() *capacityHoldQueue {
	return &capacityHoldQueue{held: map[string]struct{}{}}
}

// push enqueues a job, ignoring one already waiting. Returns false when the job
// was already held — a held entry re-refused must not be enqueued twice, or the
// queue would grow by one on every slot-free event.
func (q *capacityHoldQueue) push(job *Job) bool {
	if q == nil || job == nil || job.ID == "" {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.held[job.ID]; ok {
		return false
	}
	q.held[job.ID] = struct{}{}
	q.jobs = append(q.jobs, job)
	return true
}

func (q *capacityHoldQueue) pop() *Job {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.jobs) == 0 {
		return nil
	}
	job := q.jobs[0]
	q.jobs[0] = nil
	q.jobs = q.jobs[1:]
	delete(q.held, job.ID)
	return job
}

// remove drops a job from the hold without submitting it, so an entry deleted
// or failed while waiting does not come back.
func (q *capacityHoldQueue) remove(id string) {
	if q == nil || id == "" {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.held[id]; !ok {
		return
	}
	delete(q.held, id)
	for i, job := range q.jobs {
		if job != nil && job.ID == id {
			q.jobs = append(q.jobs[:i], q.jobs[i+1:]...)
			break
		}
	}
}

func (q *capacityHoldQueue) len() int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs)
}

// holdForCapacity parks a job until capacity exists.
func (m *Manager) holdForCapacity(job *Job) {
	if m.capacityHold == nil || job == nil {
		return
	}
	if m.capacityHold.push(job) {
		m.logger.Debug().
			Str("job_id", job.ID).
			Int("held", m.capacityHold.len()).
			Msg("Held for provider capacity; will be admitted when a slot frees")
	}
}

// releaseHeldForCapacity admits up to n held entries. Called from the points
// where a provider slot demonstrably became free.
//
// Deliberately cheap and non-blocking: it makes no provider calls of its own.
// The released job re-walks the normal add path, where admission decides
// whether the slot is genuinely available.
func (m *Manager) releaseHeldForCapacity(n int) {
	if m.capacityHold == nil || n <= 0 {
		return
	}
	for range n {
		job := m.capacityHold.pop()
		if job == nil {
			return
		}
		if err := m.SubmitJob(job); err != nil {
			// Could not resubmit; put it back rather than dropping the entry on
			// the floor, which would leave the arr with a queue row nothing is
			// working on.
			m.capacityHold.push(job)
			m.logger.Debug().Err(err).Str("job_id", job.ID).Msg("Failed to admit a held entry; leaving it held")
			return
		}
		m.logger.Info().
			Str("job_id", job.ID).
			Int("still_held", m.capacityHold.len()).
			Msg("Provider capacity freed; admitting a held entry")
	}
}

// releaseHeldEntryOnSlotFree is the single call every slot-free site makes. One
// freed slot, one admitted entry.
func (m *Manager) releaseHeldEntryOnSlotFree() {
	m.releaseHeldForCapacity(1)
}

// capacityAdmissionInterval is how often the admission controller asks each
// provider what it has free.
//
// ONE CALL PER PROVIDER, not one per held entry — that distinction is the whole
// point and it is the difference between O(1) and O(N):
//
//	per held entry : N × 1 call / 30s  =  N/30 calls per second, growing with
//	                 the backlog. At ~3,000 held that is ~100 calls/second.
//	per provider   : 1 × 1 call / 30s  =  0.03 calls per second, constant no
//	                 matter how deep the queue gets.
//
// It is a POLL INTERVAL, not a threshold and not a deadline: it bounds how
// stale this process's view of capacity may be, and says nothing about how long
// any entry may wait. Nothing is failed when it elapses.
const capacityAdmissionInterval = "30s"

// admitHeldFromProviderCapacity is the per-provider admission controller.
//
// The event hooks above are the primary mechanism and are free and instant.
// This is the SAFETY NET for capacity that frees from sources this process
// never witnesses, and those genuinely exist:
//
//	AllDebrid's own 30-minute no-peer rule deletes magnets we never asked it to;
//	the operator may delete items directly on the provider;
//	another client may share the account;
//	AllDebrid's daily add allowance resets, which frees NO slot at all — so no
//	  slot-free event can ever exist for it, and a purely event-driven hold
//	  would wait on it indefinitely.
//
// That last one is why the poll cannot simply be deleted once the event hooks
// exist.
func (m *Manager) admitHeldFromProviderCapacity() {
	held := m.capacityHold.len()
	if held == 0 {
		// Nothing is waiting, so there is nothing to admit and no reason to
		// spend a call asking. The controller costs literally zero when the
		// system is healthy.
		return
	}

	// 🛑 SLOT MATH GATES *WHETHER* TO ADMIT. THE PACER SETS *HOW MANY AT ONCE*.
	//
	// These are two different questions and conflating them caused the storm.
	// Free slots say the provider has room to DOWNLOAD; the pacer says how fast
	// we may ASK. A provider can simultaneously have 90 free slots and no
	// willingness to accept 90 requests this second, and it did.
	//
	// Per provider, because the budgets genuinely differ — RealDebrid allows 250
	// requests/minute where AllDebrid allows 600 — and a single shared number
	// would have to be the smaller one, permanently underusing the other.
	free := 0
	var eligible []string
	m.clients.Range(func(name string, client debrid.Client) bool {
		if client == nil {
			return true
		}
		slots, err := client.GetAvailableSlots()
		switch {
		case errors.Is(err, debridTypes.ErrAvailableSlotsUnknown):
			// The provider cannot report free capacity — AllDebrid exposes no
			// such endpoint. Its own refusal is the only authority, so admit
			// exactly ONE entry as a probe and let the add path find out. One
			// is not a tuned batch size: it is the smallest step that makes
			// progress, and a probe that is refused simply returns to the hold
			// at no extra cost.
			free++
			eligible = append(eligible, name)
		case err != nil:
			m.logger.Debug().Err(err).Str("provider", name).
				Msg("Capacity admission: provider could not report free slots this round")
		case slots > 0:
			free += slots
			eligible = append(eligible, name)
		}
		return true
	})

	if free <= 0 || len(eligible) == 0 {
		return
	}

	// ⚠️ THE HOLD QUEUE IS NOT PARTITIONED BY PROVIDER, so a released job may go
	// to ANY eligible provider — the add path chooses, with fallback. The only
	// bound that cannot overspend anyone is therefore the SMALLEST permit among
	// the eligible providers: if every admitted job happened to pick the most
	// restricted one, we would still be inside its budget.
	//
	// The cost is that a permissive provider is paced at a strict one's rate
	// while both are eligible. Acceptable here because the slot ceiling binds
	// first in practice (AllDebrid allows 30 active magnets against a 600
	// req/min allowance), and being conservative errs toward the failure mode
	// that costs nothing — a slightly slower refill — rather than the one that
	// cost 12,123 give-ups.
	admit := -1
	now := time.Now()
	for _, name := range eligible {
		permit := m.addPace.take(name, free, now)
		if admit < 0 || permit < admit {
			admit = permit
		}
	}
	if admit <= 0 {
		// Budget spent for now. Not a failure and nothing is dropped: the next
		// tick will have tokens again.
		return
	}

	ev := m.logger.Debug().
		Int("held", held).
		Int("free", free).
		Int("admitting", admit)
	for _, name := range eligible {
		budget, current := m.addPace.rates(name)
		ev = ev.Str("rate_"+name, fmt.Sprintf("%.0f/%.0f per min", current, budget))
	}
	ev.Msg("Capacity admission: provider capacity available; admitting what the add-rate budget allows")
	m.releaseHeldForCapacity(admit)
}

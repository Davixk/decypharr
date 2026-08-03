package manager

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"context"
	"github.com/puzpuzpuz/xsync/v4"

	"github.com/rs/zerolog"

	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// slotsClient reports a scripted free-slot count and counts how many times it
// was asked — the figure the whole O(1)-vs-O(N) argument turns on.
type slotsClient struct {
	fakeDebridClient
	slots int
	err   error
	calls int
	mu    sync.Mutex
}

func (c *slotsClient) GetAvailableSlots() (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.err != nil {
		return 0, c.err
	}
	return c.slots, nil
}

func (c *slotsClient) asked() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func newCapacityFixture(t *testing.T, clients map[string]debrid.Client) *Manager {
	t.Helper()
	m := newActionLifecycleFixture(t, 2)
	m.clients = xsync.NewMap[string, debrid.Client]()
	for name, c := range clients {
		m.clients.Store(name, c)
	}
	m.capacityHold = newCapacityHoldQueue()
	m.logger = zerolog.Nop()
	// Admission submits released entries to the job queue, and a failed submit
	// deliberately returns them to the hold rather than dropping them — so
	// without a queue here every admission would silently look like "no
	// capacity". The handler is a no-op: these tests are about how many entries
	// leave the hold, not what happens to them afterwards.
	m.jobQueue = NewJobQueue(context.Background(), 4, func(context.Context, *Job) {})
	t.Cleanup(m.jobQueue.Close)
	return m
}

func heldJob(id string) *Job {
	return &Job{ID: id, Type: JobTypeTorrent}
}

// TestAdmissionAsksEachProviderOncePerRound is the correction that matters: the
// defect was never polling, it was polling PER HELD ENTRY. At N held entries a
// per-entry cadence is N/30 calls per second and grows with the backlog; this
// must be one call per provider regardless of depth.
func TestAdmissionAsksEachProviderOncePerRound(t *testing.T) {
	rd := &slotsClient{slots: 0}
	m := newCapacityFixture(t, map[string]debrid.Client{"rd": rd})

	for i := range 500 {
		m.holdForCapacity(heldJob(fmt.Sprintf("held-%d", i)))
	}
	if got := m.capacityHold.len(); got != 500 {
		t.Fatalf("held = %d, want 500", got)
	}

	m.admitHeldFromProviderCapacity()

	if got := rd.asked(); got != 1 {
		t.Fatalf("provider asked %d times for 500 held entries, want exactly 1", got)
	}
}

// TestAdmissionCostsNothingWhenNothingIsHeld: a healthy system must not spend a
// provider call every interval to learn it has no work.
func TestAdmissionCostsNothingWhenNothingIsHeld(t *testing.T) {
	rd := &slotsClient{slots: 10}
	m := newCapacityFixture(t, map[string]debrid.Client{"rd": rd})

	m.admitHeldFromProviderCapacity()

	if got := rd.asked(); got != 0 {
		t.Fatalf("provider asked %d times with an empty hold, want 0", got)
	}
}

// TestAdmissionAdmitsAsManyAsThereAreFreeSlots: the held set is a queue we
// drain against real capacity, not N independent things each asking the same
// question.
func TestAdmissionAdmitsAsManyAsThereAreFreeSlots(t *testing.T) {
	rd := &slotsClient{slots: 3}
	m := newCapacityFixture(t, map[string]debrid.Client{"rd": rd})
	for i := range 10 {
		m.holdForCapacity(heldJob(fmt.Sprintf("held-%d", i)))
	}

	m.admitHeldFromProviderCapacity()

	if got := m.capacityHold.len(); got != 7 {
		t.Fatalf("held = %d after admitting against 3 free slots, want 7", got)
	}
}

// TestAdmissionProbesOnceWhenCapacityIsUnknowable: AllDebrid exposes no
// free-slot endpoint, so its own refusal is the only authority. Admit exactly
// one as a probe; a probe that is refused simply returns to the hold.
func TestAdmissionProbesOnceWhenCapacityIsUnknowable(t *testing.T) {
	ad := &slotsClient{err: debridTypes.ErrAvailableSlotsUnknown}
	m := newCapacityFixture(t, map[string]debrid.Client{"ad": ad})
	for i := range 10 {
		m.holdForCapacity(heldJob(fmt.Sprintf("held-%d", i)))
	}

	m.admitHeldFromProviderCapacity()

	if got := m.capacityHold.len(); got != 9 {
		t.Fatalf("held = %d, want 9 (exactly one probe admitted)", got)
	}
}

// TestAdmissionIgnoresProvidersThatError: a provider that cannot answer must
// contribute no admissions, never a guessed count.
func TestAdmissionIgnoresProvidersThatError(t *testing.T) {
	broken := &slotsClient{err: errors.New("provider down")}
	m := newCapacityFixture(t, map[string]debrid.Client{"broken": broken})
	m.holdForCapacity(heldJob("held-1"))

	m.admitHeldFromProviderCapacity()

	if got := m.capacityHold.len(); got != 1 {
		t.Fatalf("held = %d; a provider that could not answer must not admit anything", got)
	}
}

// TestHoldQueueDoesNotDoubleEnqueue: a released entry refused again returns
// here, and must not stack — otherwise the queue grows by one on every
// slot-free event.
func TestHoldQueueDoesNotDoubleEnqueue(t *testing.T) {
	q := newCapacityHoldQueue()
	job := heldJob("same-id")

	if !q.push(job) {
		t.Fatal("first push rejected")
	}
	if q.push(job) {
		t.Fatal("second push of the same job was accepted; the hold would grow unboundedly")
	}
	if got := q.len(); got != 1 {
		t.Fatalf("len = %d, want 1", got)
	}

	// Once popped it may be held again — that is the normal re-refusal cycle.
	if got := q.pop(); got == nil || got.ID != "same-id" {
		t.Fatalf("pop = %+v, want the held job", got)
	}
	if !q.push(job) {
		t.Fatal("push after pop rejected; a re-refused entry could never be re-held")
	}
}

// TestHoldQueueIsFIFO: nothing about waiting longer makes an entry more likely
// to succeed, so the only defensible order is the one that cannot starve.
func TestHoldQueueIsFIFO(t *testing.T) {
	q := newCapacityHoldQueue()
	for _, id := range []string{"first", "second", "third"} {
		q.push(heldJob(id))
	}
	for _, want := range []string{"first", "second", "third"} {
		got := q.pop()
		if got == nil || got.ID != want {
			t.Fatalf("pop = %+v, want %q", got, want)
		}
	}
	if q.pop() != nil {
		t.Fatal("pop on an empty queue returned a job")
	}
}

// TestHoldQueueRemoveDropsEntry: an entry deleted or failed while waiting must
// not come back when a slot frees.
func TestHoldQueueRemoveDropsEntry(t *testing.T) {
	q := newCapacityHoldQueue()
	q.push(heldJob("keep"))
	q.push(heldJob("drop"))

	q.remove("drop")

	if got := q.len(); got != 1 {
		t.Fatalf("len = %d after remove, want 1", got)
	}
	if got := q.pop(); got == nil || got.ID != "keep" {
		t.Fatalf("pop = %+v, want the surviving job", got)
	}
}

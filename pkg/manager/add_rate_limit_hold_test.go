package manager

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// END TO END: A RATE-LIMITED GRAB IS ACCEPTED, NOT REFUSED.
//
// The operator's ruling, and the whole point of Half A: "quota/rate-class
// refusals NEVER answer the arr's add with 400 and NEVER park-as-error — accept
// the add, hold the row visible as qBittorrent queuedDL, retry submission
// internally on your schedule until a real verdict."
//
// Every clause of that is a separate assertion below, because the shape has
// three ways to be wrong and only one to be right: it can refuse (a lost grab),
// it can accept and park an error row (a corpse the arr cannot act on), or it
// can accept and then forget the entry (the arr waits on a download nobody is
// working on).
func newRateLimitHoldManager(t *testing.T, submitErr error) (*Manager, *fakeDebridClient) {
	t.Helper()
	client := &fakeDebridClient{
		cfg:      config.Debrid{Name: "rd", Provider: "realdebrid"},
		recorder: &fallbackCallRecorder{},
		submitFn: func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
			return nil, submitErr
		},
	}
	m := newSyncRefusalManager(t, client)
	// ⚠️ NOT OPTIONAL, AND NOT COSMETIC. holdForCapacity returns silently when
	// capacityHold is nil, so a fixture without it would let a broken
	// implementation "accept" a grab, drop it on the floor, and pass every
	// assertion about the return value. Wiring it is what makes "the entry is
	// actually waiting" observable.
	m.capacityHold = newCapacityHoldQueue()
	m.jobQueue = NewJobQueue(context.Background(), 1, func(context.Context, *Job) {})
	t.Cleanup(m.jobQueue.Close)
	return m, client
}

func TestRateLimitedGrabIsAcceptedAndHeld(t *testing.T) {
	m, client := newRateLimitHoldManager(t, customerror.RateLimitedError)
	req := fallbackTestRequest("", false, nil)

	// 1. THE ADD IS ACCEPTED. Returning an error here becomes a 400 on
	//    torrents/add, which Radarr answers with `Couldn't add release` and a
	//    dropped grab — the 595-refusals-per-30-minutes shape.
	if err := m.AddNewTorrent(context.Background(), req); err != nil {
		t.Fatalf("a rate-limited add was REFUSED: %v. The arr's grab is lost to a condition that clears in seconds", err)
	}

	// 2. THE ROW SURVIVES, QUEUED. Not `error`: an error row is a dead end the
	//    arr can neither retry nor clear, and the operator ruled those out by
	//    name ("nameless parked error rows accumulating").
	entry, err := m.queue.GetTorrent(req.Magnet.InfoHash)
	if err != nil || entry == nil {
		t.Fatalf("an accepted grab left no queue row (entry=%v err=%v); the arr is waiting on a download "+
			"that no longer exists anywhere", entry, err)
	}
	if entry.Status != debridTypes.TorrentStatusQueued {
		t.Fatalf("held entry status = %q, want %q — this is what the shim converts into qBittorrent's queuedDL, "+
			"and any other value tells the arr something untrue about what is happening",
			entry.Status, debridTypes.TorrentStatusQueued)
	}

	// 3. SOMETHING IS ACTUALLY GOING TO RETRY IT. "Accepted" without this is
	//    just a silent drop with better manners.
	if held := m.capacityHold.len(); held != 1 {
		t.Fatalf("capacity hold holds %d entries, want 1: the grab was accepted but nothing will ever re-attempt it", held)
	}

	// 4. THE PROVIDER COPY IS NOT RELEASED. There is nothing to release — the
	//    submit never succeeded — and a cleanup that fired anyway would be the
	//    fork.44 shape all over again.
	if deleted := client.deleted(); len(deleted) != 0 {
		t.Fatalf("a held grab released provider placements %v", deleted)
	}
}

// The traffic/fair-usage class takes the identical path.
func TestTrafficExceededGrabIsAcceptedAndHeld(t *testing.T) {
	m, _ := newRateLimitHoldManager(t, customerror.TrafficExceededError)
	req := fallbackTestRequest("", false, nil)

	if err := m.AddNewTorrent(context.Background(), req); err != nil {
		t.Fatalf("a traffic-allowance refusal was refused: %v", err)
	}
	if held := m.capacityHold.len(); held != 1 {
		t.Fatalf("capacity hold holds %d entries, want 1", held)
	}
}

// 🛑 THE CONTROL, AND IT IS THE HALF THE OPERATOR EXPLICITLY KEPT: "For OTHER
// failure types, FAIL THE GRAB so the arr moves on, without blocklisting."
//
// Without this test, an implementation that held EVERY failed add would pass
// both tests above while quietly converting every dead release into a permanent
// queuedDL row — the fork.34 spin, rebuilt.
func TestContentFailureStillFailsTheGrabSynchronously(t *testing.T) {
	m, _ := newRateLimitHoldManager(t, customerror.NewContentTakedownError(
		errors.New("realdebrid: infringing_file (code 35)")))
	req := fallbackTestRequest("", false, nil)

	err := m.AddNewTorrent(context.Background(), req)
	if err == nil {
		t.Fatal("a dead release was ACCEPTED and held. The arr records a successful grab, waits on queuedDL " +
			"forever, and never searches for a replacement")
	}

	// AND THE REFUSAL SAYS WHY, IN THE FIRST CLAUSE. The qBittorrent shim writes
	// this error's text verbatim as the 400 body, and that body is all an *arr
	// can show a human. "Most as silent 400s the arr can't distinguish from
	// anything else" is the complaint; leading with the wrapper chain instead of
	// the cause is how it stayed true.
	//
	// Asserted here rather than on refusalReason alone, because a unit test of
	// that function passes whether or not anything calls it — which is exactly
	// what a negative control showed.
	if !strings.HasPrefix(err.Error(), "refused:") {
		t.Fatalf("the error the arr receives does not lead with a reason: %q", err)
	}
	if !strings.Contains(err.Error(), "legal reasons") {
		t.Fatalf("a takedown refusal reached the arr without naming its cause: %q", err)
	}
	if _, err := m.queue.GetTorrent(req.Magnet.InfoHash); err == nil {
		t.Fatal("a synchronously-refused grab left a row behind for the arr to trip over")
	}
	if held := m.capacityHold.len(); held != 0 {
		t.Fatalf("a content failure was placed on the capacity hold (%d entries); nothing about a dead release "+
			"is waiting for capacity", held)
	}
}

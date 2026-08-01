package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// Provider admission asks a provider whether it has room before spending an add
// on it, and treats "I don't report that" as an honest answer rather than a
// number to invent.
//
// The tests below guard the three properties that make it safe:
//
//	1. a full provider does NOT block a worker — the job goes back to the queue
//	2. a full provider does NOT abort the fallback chain
//	3. a provider that cannot report capacity is admitted, not refused
//
// Property 1 is the one to watch. The defect this whole layer exists to remove
// was head-of-line blocking; an implementation that waits here for capacity
// rebuilds it exactly, one level down, and every functional test still passes.

// TestFullProviderDoesNotBlockAndYieldsRequeueableError is the negative control
// for the defect most likely to be reintroduced: waiting for a slot instead of
// handing the job back.
//
// A provider reporting zero slots must return promptly with an error the job
// layer recognises as requeueable. If a future implementation sleeps or polls
// until capacity frees, this test hangs rather than fails — so it is bounded by
// its own deadline and reports the blocking explicitly.
func TestFullProviderDoesNotBlockAndYieldsRequeueableError(t *testing.T) {
	full := &fakeDebridClient{
		cfg:     config.Debrid{Name: "full", DownloadUncached: boolPointer(true)},
		slotsFn: func() (int, error) { return 0, nil },
	}

	done := make(chan error, 1)
	go func() {
		_, err := fallbackTestManager(full).SendToDebrid(context.Background(), fallbackTestRequest("full", false, nil))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a full provider to refuse the add")
		}
		if !isTooManyActiveDownloads(err) {
			t.Fatalf("error = %v, want one isTooManyActiveDownloads recognises. If this fails, the job "+
				"layer will treat provider saturation as a hard failure instead of requeuing", err)
		}
		if submit, _, _ := full.counts(); submit != 0 {
			t.Fatalf("submitted to a provider reporting 0 slots (%d submits): admission ran too late", submit)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SendToDebrid blocked waiting for provider capacity. Admission must NEVER wait — it must " +
			"return so the job goes back to the queue. Blocking here rebuilds the head-of-line blocking " +
			"this layer was built to remove.")
	}
}

// A full provider is a per-provider decline, not a chain-ending failure: with
// fallback enabled the next provider must still be tried. Getting this wrong
// would mean a full AllDebrid stops a RealDebrid that has room.
func TestFullProviderStillAdvancesTheFallbackChain(t *testing.T) {
	full := &fakeDebridClient{
		cfg:     config.Debrid{Name: "full", DownloadUncached: boolPointer(true), Priority: 1},
		slotsFn: func() (int, error) { return 0, nil },
	}
	roomy := &fakeDebridClient{
		cfg:     config.Debrid{Name: "roomy", DownloadUncached: boolPointer(true), Priority: 2},
		slotsFn: func() (int, error) { return 5, nil },
	}

	torrent, err := fallbackTestManager(full, roomy).SendToDebrid(context.Background(), fallbackTestRequest("full", true, nil))
	if err != nil {
		t.Fatalf("SendToDebrid returned error: %v", err)
	}
	if torrent.Debrid != "roomy" {
		t.Fatalf("landed on %q, want the provider with free slots", torrent.Debrid)
	}
	if submit, _, _ := full.counts(); submit != 0 {
		t.Fatalf("full provider received %d submits; it should have been skipped before the add", submit)
	}
}

// A provider that does not report capacity must be ADMITTED. Refusing it would
// disable AllDebrid entirely, since AllDebrid has no slots endpoint and answers
// only by refusing an actual add.
func TestProviderThatCannotReportSlotsIsAdmitted(t *testing.T) {
	silent := &fakeDebridClient{
		cfg:     config.Debrid{Name: "silent", DownloadUncached: boolPointer(true)},
		slotsFn: func() (int, error) { return 0, debridTypes.ErrAvailableSlotsUnknown },
	}

	torrent, err := fallbackTestManager(silent).SendToDebrid(context.Background(), fallbackTestRequest("silent", false, nil))
	if err != nil {
		t.Fatalf("a provider that cannot report slots must still be tried, got: %v", err)
	}
	if torrent.Debrid != "silent" {
		t.Fatalf("unexpected provider: %q", torrent.Debrid)
	}
	if submit, _, _ := silent.counts(); submit != 1 {
		t.Fatalf("submits = %d, want 1: the provider's own refusal is the gate for this case", submit)
	}
}

// Failing to READ capacity is our problem, not a verdict about the provider.
// Manufacturing a refusal from it would repeat the mistake the usenet work
// corrected: condemning a release because a probe could not complete.
func TestUnreadableCapacityDoesNotManufactureARefusal(t *testing.T) {
	flaky := &fakeDebridClient{
		cfg:     config.Debrid{Name: "flaky", DownloadUncached: boolPointer(true)},
		slotsFn: func() (int, error) { return 0, errors.New("connection reset") },
	}

	if _, err := fallbackTestManager(flaky).SendToDebrid(context.Background(), fallbackTestRequest("flaky", false, nil)); err != nil {
		t.Fatalf("an unreadable capacity endpoint must not block the add, got: %v", err)
	}
	if submit, _, _ := flaky.counts(); submit != 1 {
		t.Fatalf("submits = %d, want 1", submit)
	}
}

// The two provider-capacity conditions describe different resources and must
// not share a retry cadence. Slot exhaustion clears as our own work finishes;
// an add allowance does not, so retrying it on the slot cadence is a spin
// against a provider that has already refused.
func TestSlotAndQuotaExhaustionAreDistinctConditions(t *testing.T) {
	slots := customerror.TooManyActiveDownloadsError
	quota := customerror.ProviderAddQuotaExhaustedError

	if !isTooManyActiveDownloads(slots) || isProviderAddQuotaExhausted(slots) {
		t.Fatal("slot exhaustion must classify as slots only")
	}
	if !isProviderAddQuotaExhausted(quota) || isTooManyActiveDownloads(quota) {
		t.Fatal("quota exhaustion must classify as quota only — sharing the slot cadence would spin")
	}
	if providerQuotaRetryDelay <= providerSlotRetryDelay {
		t.Fatalf("quota retry %v must be longer than slot retry %v: our own completions never free a quota",
			providerQuotaRetryDelay, providerSlotRetryDelay)
	}

	// Both must survive being joined with unrelated provider failures, which is
	// how they actually arrive from a fallback chain. Matching by sentinel
	// rather than by "first *customerror.Error in the tree" is what makes this
	// hold.
	joined := joinDebridErrors([]error{
		customerror.HosterUnavailableError,
		errors.New("some other provider failed"),
		quota,
	})
	if !isProviderAddQuotaExhausted(joined) {
		t.Fatal("quota exhaustion lost through a joined multi-provider error; the job would be failed instead of requeued")
	}
}

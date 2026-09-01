package manager

import (
	"errors"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/customerror"
)

// A VERDICT ABOUT THE CONTENT OUTRANKS A FACT ABOUT THE REQUEST.
//
// The ledger recorded class, cause and deadline unconditionally, so the ORDER
// of two unrelated events decided the outcome: a provider refusing the release
// (451, permanent, parked 6h) followed by that provider throttling the next
// attempt (transient) left the hash parked for minutes with the 451 discarded.
// The release was then re-offered, refused again, and never reached a
// conclusion.
//
// Measured on a live deployment over 24h: 16,002 RealDebrid `451
// infringing_file` declines against 1,437 rate-limit declines. The 11:1
// minority event was erasing the majority verdict.
func TestPermanentVerdictSurvivesALaterThrottle(t *testing.T) {
	ledger := newDeclineLedger()
	now := time.Now()

	takedown := customerror.NewContentTakedownError(errors.New("realdebrid: 451 infringing_file"))
	ledger.record("rd", "HASH", declinePermanent, takedown, now)

	// A throttle lands one minute later, on the same hash, same provider.
	throttle := errors.New("realdebrid: 429 slow down")
	ledger.record("rd", "HASH", declineTransient, throttle, now.Add(time.Minute))

	cooling, cause := ledger.cooling("rd", "HASH", now.Add(2*time.Minute))
	if !cooling {
		t.Fatal("the hash is no longer cooling off two minutes after a 6h permanent park; a throttle cancelled " +
			"a takedown")
	}
	if !customerror.IsContentTakedown(cause) {
		t.Fatalf("cooling cause is %v, want the takedown. The verdict that decides hold-vs-refuse is gone, so "+
			"this hash is re-offered and refused forever instead of failing once", cause)
	}

	// And it is still parked well past any transient backoff could reach.
	if cooling, _ := ledger.cooling("rd", "HASH", now.Add(declineBackoffMax+time.Minute)); !cooling {
		t.Fatal("the permanent park was shortened to a transient backoff")
	}
}

// THE OTHER DIRECTION MUST STILL WORK. A transient park is an admission that we
// do not know; a permanent one is a verdict. Arriving second does not make the
// verdict worth less.
func TestPermanentVerdictStillUpgradesATransientPark(t *testing.T) {
	ledger := newDeclineLedger()
	now := time.Now()

	ledger.record("rd", "HASH", declineTransient, errors.New("connection reset"), now)
	takedown := customerror.NewContentTakedownError(errors.New("realdebrid: 451 infringing_file"))
	ledger.record("rd", "HASH", declinePermanent, takedown, now.Add(time.Minute))

	cooling, cause := ledger.cooling("rd", "HASH", now.Add(declineBackoffMax+time.Minute))
	if !cooling {
		t.Fatal("a permanent refusal did not extend a transient park; the release is re-offered within minutes")
	}
	if !customerror.IsContentTakedown(cause) {
		t.Fatalf("cooling cause is %v, want the takedown that arrived second", cause)
	}
}

// 🛑 TRANSIENT MUST NEVER BECOME PERMANENT-BY-ACCUMULATION.
//
// Keeping the ORIGINAL deadline rather than refreshing it is what stops the
// inversion: if each throttle extended the permanent park, a stream of
// transient failures would hold a content verdict open indefinitely. Once the
// 6h lapses the hash is free, and the next decline is classified on its own
// merits.
func TestThrottlesDoNotExtendAPermanentPark(t *testing.T) {
	ledger := newDeclineLedger()
	now := time.Now()

	ledger.record("rd", "HASH", declinePermanent,
		customerror.NewContentTakedownError(errors.New("451")), now)

	// Throttled repeatedly, right up to the edge of the permanent window.
	for i := 1; i <= 10; i++ {
		ledger.record("rd", "HASH", declineTransient, errors.New("429"),
			now.Add(time.Duration(i)*30*time.Minute))
	}

	past := now.Add(declinePermanentCooldown + time.Minute)
	if cooling, cause := ledger.cooling("rd", "HASH", past); cooling {
		t.Fatalf("the park outlived its 6h deadline (cause %v). Ten throttles rolled a bounded verdict into an "+
			"unbounded one, which is the same inversion in the other direction", cause)
	}

	// And after expiry a fresh transient decline is honoured as transient.
	ledger.record("rd", "HASH", declineTransient, errors.New("429"), past)
	if cooling, _ := ledger.cooling("rd", "HASH", past.Add(declineBackoffMax+time.Minute)); cooling {
		t.Fatal("a post-expiry transient decline was still treated as the old permanent verdict")
	}
}

// Providers are independent: one provider's verdict must not park a hash on
// another. "Infringing on RD means nothing on AD" is the operator's rule and
// the ledger key is what enforces it.
func TestVerdictPrecedenceIsPerProvider(t *testing.T) {
	ledger := newDeclineLedger()
	now := time.Now()

	ledger.record("rd", "HASH", declinePermanent,
		customerror.NewContentTakedownError(errors.New("451")), now)

	if cooling, _ := ledger.cooling("ad", "HASH", now.Add(time.Minute)); cooling {
		t.Fatal("a RealDebrid takedown parked the hash on AllDebrid too")
	}
}

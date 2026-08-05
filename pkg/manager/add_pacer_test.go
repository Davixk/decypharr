package manager

import (
	"errors"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/customerror"
)

// THE DEFAULTS MUST TRACK EACH VENDOR'S PUBLISHED LIMIT, not a shared guess.
//
// RealDebrid documents 250 requests/minute; AllDebrid documents 12/second and
// 600/minute. A single constant for both — which is what this replaced — has to
// be sized for the stricter one, permanently underusing the other.
func TestPerProviderDefaultsDiffer(t *testing.T) {
	rd := parseAddRate(defaultAddRateFor("realdebrid"))
	ad := parseAddRate(defaultAddRateFor("alldebrid"))
	unknown := parseAddRate(defaultAddRateFor("somethingelse"))

	if rd <= 0 || ad <= 0 || unknown <= 0 {
		t.Fatalf("a default failed to parse: rd=%v ad=%v unknown=%v", rd, ad, unknown)
	}
	if ad <= rd {
		t.Fatal("AllDebrid publishes a higher limit than RealDebrid; its budget must be higher")
	}
	// An unverified provider must be the most cautious of the three: guessing
	// high on an unknown API is how you find its limit the expensive way.
	if unknown > rd {
		t.Fatal("the unknown-provider default must not exceed the most restrictive researched one")
	}
}

// ⚠️ THE BURST CAP IS THE STORM FIX. Without it an idle period banks tokens and
// the next tick spends them all at once — "admit all 90 free slots" wearing a
// rate limiter's clothes, with a perfectly correct-looking average.
func TestIdleTimeCannotBankAnUnboundedBurst(t *testing.T) {
	p := newAddPacer()
	p.configure("rd", "60/minute")
	start := time.Now()

	// An hour of accumulated entitlement.
	got := p.take("rd", 1000, start.Add(time.Hour))
	if got > addPacerBurst {
		t.Fatalf("granted %d at once after idling; the burst cap (%d) is what stops the storm recurring",
			got, addPacerBurst)
	}
}

func TestBudgetIsSpentThenRefills(t *testing.T) {
	p := newAddPacer()
	p.configure("rd", "60/minute") // one per second
	now := time.Now()

	spent := 0
	for range 20 {
		spent += p.take("rd", 10, now)
	}
	if spent > addPacerBurst {
		t.Fatalf("spent %d without time passing, burst cap is %d", spent, addPacerBurst)
	}
	// Zero is a normal answer, not an error — the caller simply retries later.
	if p.take("rd", 10, now) != 0 {
		t.Fatal("expected the budget to be spent")
	}
	if got := p.take("rd", 10, now.Add(3*time.Second)); got == 0 {
		t.Fatal("budget never refilled; held entries would stall forever")
	}
}

// AIMD: a rate-limit signal must slow the WHOLE LANE, not one hash. Parking a
// single release leaves the request rate exactly where it was.
func TestRateLimitSignalHalvesTheLaneAndRecoveryClimbsBack(t *testing.T) {
	p := newAddPacer()
	p.configure("rd", "60/minute")
	now := time.Now()

	budget, before := p.rates("rd")
	if before != budget {
		t.Fatalf("lane started at %v, want the full budget %v", before, budget)
	}

	p.penalise("rd", now)
	_, after := p.rates("rd")
	if after >= before {
		t.Fatalf("rate %v did not drop below %v after a rate-limit signal", after, before)
	}

	// Recovery is gradual: one success does not prove the provider is healthy.
	p.reward("rd")
	_, climbing := p.rates("rd")
	if climbing <= after {
		t.Fatal("a successful add must let the rate climb back toward the budget")
	}
	if climbing >= budget {
		t.Fatal("one success jumped straight back to the full budget; that oscillates")
	}

	// And it never exceeds the configured ceiling.
	for range 100 {
		p.reward("rd")
	}
	_, recovered := p.rates("rd")
	if recovered > budget {
		t.Fatalf("rate %v exceeded the configured budget %v", recovered, budget)
	}
}

// The backoff must bottom out, not reach zero: a provider refusing everything
// still has to be probed occasionally to notice it has recovered.
func TestBackoffHasAFloor(t *testing.T) {
	p := newAddPacer()
	p.configure("rd", "60/minute")
	now := time.Now()

	for range 50 {
		p.penalise("rd", now)
	}
	_, rate := p.rates("rd")
	if rate <= 0 {
		t.Fatal("the lane throttled to a full stop; it would never discover the provider had recovered")
	}
	// Given the floor, progress must resume within a bounded wait.
	if got := p.take("rd", 1, now.Add(2*addPacerFloor)); got == 0 {
		t.Fatalf("no admission after %v at the floor rate", 2*addPacerFloor)
	}
}

func TestLanesAreIndependentPerProvider(t *testing.T) {
	p := newAddPacer()
	p.configure("rd", "60/minute")
	p.configure("ad", "60/minute")
	now := time.Now()

	p.penalise("rd", now)
	_, rd := p.rates("rd")
	_, ad := p.rates("ad")
	if rd >= ad {
		t.Fatal("throttling one provider must not throttle another — the fallback chain depends on it")
	}
}

func TestAnUnparseableOverrideFallsBackToTheDefault(t *testing.T) {
	p := newAddPacer()
	p.configure("realdebrid", "not a rate")
	budget, _ := p.rates("realdebrid")
	want := parseAddRate(defaultAddRateRealDebrid) * 60
	if budget != want {
		t.Fatalf("budget %v, want the researched default %v — a bad config value must not silently "+
			"pace at some accidental rate", budget, want)
	}
}

// ⚠️ A CAPACITY REFUSAL IS NOT A RATE-LIMIT SIGNAL. "Too many ACTIVE downloads"
// means the account is full, not that we are asking too fast; slowing down would
// not help, and throttling the lane would starve entries merely waiting for a
// slot the hold ledger already handles.
func TestCapacityRefusalIsNotARateLimitSignal(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"slots exhausted", customerror.TooManyActiveDownloadsError, false},
		{"add quota exhausted", customerror.ProviderAddQuotaExhaustedError, false},
		{"http 429", errors.New("POST /torrents/addMagnet gave up after 4 attempt(s): status 429: rate limited"), true},
		{"alldebrid 503", errors.New("alldebrid API error: status 503"), true},
		{"realdebrid slow down", errors.New("realdebrid error 5: slow down"), true},
		{"realdebrid too many requests", errors.New("realdebrid error 34: too many requests"), true},
		{"ordinary timeout", errors.New("context deadline exceeded"), false},
		{"nil", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRateLimitSignal(tc.err); got != tc.want {
				t.Fatalf("isRateLimitSignal = %v, want %v", got, tc.want)
			}
		})
	}
}

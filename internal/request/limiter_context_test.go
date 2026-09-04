package request

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/customerror"
	"go.uber.org/ratelimit"
)

// AN ABANDONED REQUEST MUST STOP WAITING FOR A RATE-LIMIT TOKEN.
//
// go.uber.org/ratelimit's Take() takes no context and cannot be cancelled. The
// old code checked the context once and then blocked unconditionally, so a
// caller whose request was already abandoned still waited its full turn.
//
// That is invisible while demand is under the limiter's rate, and fatal above
// it — the wait queue grows without bound and every caller blocks forever.
// Because the qBittorrent add path runs the provider walk inside the HTTP
// handler, that surfaced as *arrs timing out and acquiring nothing, with the
// process idle at 0.28% CPU: not overloaded, just waiting.
func TestRateLimitWaitIsAbandonedWhenTheCallerGivesUp(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// One request per hour: the second caller can never be served in this test,
	// so anything but context cancellation blocks forever.
	client := New(
		WithRateLimiter(ratelimit.New(1, ratelimit.Per(time.Hour))),
		WithTimeout(30*time.Second),
	)

	// Spend the only token.
	first, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if resp, err := client.Do(first); err != nil {
		t.Fatalf("first request: %v", err)
	} else {
		_ = resp.Body.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	second, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		resp, err := client.Do(second)
		if resp != nil {
			_ = resp.Body.Close()
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the abandoned request somehow succeeded; the limiter should not have had a token for it")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a request whose context expired 200ms in was still waiting for a rate-limit token five seconds " +
			"later. Under load this is the unbounded queue that hangs every caller")
	}
}

// AND THE PACING ITSELF IS UNCHANGED. Moving the wait into a goroutine must not
// let a caller skip the queue — otherwise the fix for a hang becomes the storm
// the limiter exists to prevent.
func TestRateLimitStillPacesCallers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// 20/second -> ~50ms apart. Three calls should span at least two gaps.
	client := New(
		WithRateLimiter(ratelimit.New(20, ratelimit.Per(time.Second))),
		WithTimeout(30*time.Second),
	)

	start := time.Now()
	for range 3 {
		req, err := http.NewRequest(http.MethodGet, server.URL, nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		_ = resp.Body.Close()
	}

	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("three paced requests took %v; the limiter is no longer pacing and the token wait is being "+
			"skipped rather than awaited", elapsed)
	}
}

// 🔴 THE TOKEN QUEUE IS BOUNDED, WHICH IS WHAT CANCELLATION COULD NOT DO.
//
// Most provider calls are built with http.NewRequest and carry
// context.Background(), so the cancellation path above can never fire for them.
// Without a ceiling those callers park forever — which is precisely what fork.77
// did: 151 of 176 workers asleep in Take() against one provider, the pool
// exhausted, the synchronous qBittorrent add handler never serviced, and the
// *arrs reporting a dead download client while the process idled at 0.28% CPU.
//
// The answer has to be TYPED, not just fast. RateLimitedError is what the
// classifier reads to HOLD the entry; an untyped error here would be refused as
// unrecognised and the release spent on our own congestion.
func TestTokenWaitIsBoundedEvenWithNoContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := New(
		// One token per hour: the second caller can never be served.
		WithRateLimiter(ratelimit.New(1, ratelimit.Per(time.Hour))),
		WithRateWaitCeiling(150*time.Millisecond),
		WithTimeout(30*time.Second),
	)

	first, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if resp, err := client.Do(first); err != nil {
		t.Fatalf("first request: %v", err)
	} else {
		_ = resp.Body.Close()
	}

	// NO CONTEXT — exactly how every provider builds its requests today.
	second, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		resp, err := client.Do(second)
		if resp != nil {
			_ = resp.Body.Close()
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the second call succeeded; the limiter had no token to give it")
		}
		if !errors.Is(err, customerror.RateLimitedError) {
			t.Fatalf("give-up error is %v, want RateLimitedError. The classifier reads the TYPE to hold the "+
				"entry; anything else is refused as unrecognised and the release is spent", err)
		}
		// 🔊 AND IT IS COUNTED. A bounded wait that gives up silently just moves
		// the invisibility — both hangs were undetectable from inside the process
		// until an *arr complained.
		if n := client.RateWaitsAbandoned(); n != 1 {
			t.Fatalf("RateWaitsAbandoned() = %d, want 1; a give-up that is not counted cannot be noticed", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a contextless caller was still queued for a token after five seconds. This is the unbounded " +
			"park that exhausted the worker pool and took the write path down")
	}
}

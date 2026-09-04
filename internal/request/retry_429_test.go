package request

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// A 429 MUST COST EXACTLY ONE REQUEST WHEN NOTHING IS MARKED RETRYABLE.
//
// Retrying into a rate limit is not a neutral choice on RealDebrid: its limit is
// global across every endpoint and REFUSED REQUESTS COUNT TOWARD IT, so each
// retry spends the budget that decides the next answer. Measured on the live
// account, the provider answers an add refusal in ~0.15s while decypharr took a
// median 205s to record the verdict — the difference was this retry loop, up to
// five more requests spaced by a backoff capped at 30s.
//
// The worker is the other cost. RealDebrid grants add capacity in short bursts;
// a worker asleep in a 30s backoff sleeps through them, so the retry did not
// merely waste budget, it made us miss the openings we were waiting for.
func TestRateLimitIsNotRetriedWhenNoStatusIsRetryable(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	client := New(
		WithMaxRetries(5),
		WithRetryableStatus(),
		WithTimeout(10*time.Second),
	)

	req, err := http.NewRequest(http.MethodPost, server.URL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if _, err := client.Do(req); err != nil {
		// A give-up error is fine; what matters is how many requests it cost.
		t.Logf("Do returned %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("a 429 cost %d requests, want 1. Every extra one spends the same global budget the provider "+
			"is refusing us on, and holds the worker through the burst it is waiting for", got)
	}
}

// THE MECHANISM STILL WORKS WHEN IT IS ASKED FOR. This is the control: the
// option is what changed, not the client's ability to retry, so a caller that
// genuinely wants 429 retried still gets it.
func TestRateLimitIsRetriedWhenExplicitlyListed(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		// Keep the backoff short so the test is not paced by the retry policy.
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	client := New(
		WithMaxRetries(2),
		WithRetryableStatus(http.StatusTooManyRequests),
		WithTimeout(30*time.Second),
	)

	req, err := http.NewRequest(http.MethodPost, server.URL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if _, err := client.Do(req); err != nil {
		t.Logf("Do returned %v", err)
	}

	if got := calls.Load(); got < 2 {
		t.Fatalf("429 was listed as retryable and cost %d requests; the retry mechanism itself is broken", got)
	}
}

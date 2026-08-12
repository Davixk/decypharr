package request

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
)

// New() reaches config.Get(), which writes a config file on first use. Point it
// somewhere writable and disposable so this package's tests do not litter the
// working tree.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "decypharr-request-test")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// 429 IS RETRIED TO EXHAUSTION, SO THIS HANDLER IS THE ONLY PLACE IT SURFACES.
//
// Because http.StatusTooManyRequests is in retryableStatus, retryablehttp keeps
// retrying and then returns through ErrorHandler with the response drained. The
// provider packages' `switch resp.StatusCode` blocks are simply not on that
// path — every one of them would map a 429 if it ever arrived, and none of them
// ever sees it.
//
// That is why a rate limit reached pkg/manager as an untyped string, why
// classifyAddRefusal could not recognise it as capacity, and why 595 grabs in a
// 30-minute window were answered with 400 and dropped by Radarr. Typing it here
// fixes it for every provider at once.
func TestRateLimitGiveUpIsTyped(t *testing.T) {
	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"too_many_requests"}`))
	}))
	defer server.Close()

	// RetryWaitMin is 1s, so keep the retry count to the minimum that still
	// proves the give-up path was taken rather than the first-response path.
	client := New(WithMaxRetries(1))
	req, err := http.NewRequest(http.MethodPost, server.URL+"/torrents/addMagnet", strings.NewReader("magnet=x"))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	resp, err := client.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err == nil {
		t.Fatal("a 429 that exhausted retries returned no error")
	}
	if attempts < 2 {
		t.Fatalf("server saw %d attempt(s); the give-up handler was not exercised, so this test proves nothing "+
			"about the path a real rate limit takes", attempts)
	}
	if !errors.Is(err, customerror.RateLimitedError) {
		t.Fatalf("a 429 give-up is not typed as RateLimitedError: %v. Untyped, it reaches classifyAddRefusal as "+
			"an unrecognisable string, gets refused, and the *arr's grab is lost to a condition that clears in seconds", err)
	}
	// The diagnostic text must survive the typing — losing the status and body
	// snippet is what made three separate incidents undiagnosable.
	if !strings.Contains(err.Error(), "status 429") || !strings.Contains(err.Error(), "too_many_requests") {
		t.Fatalf("typing the error discarded its diagnostics: %v", err)
	}
}

// THE CONTROL. Only 429 is typed. A 503 give-up carries the same shape and must
// stay untyped here, because a provider outage is not a rate limit and holding a
// grab for one would park it against a condition this process cannot drain.
func TestServiceUnavailableGiveUpIsNotTypedAsRateLimit(t *testing.T) {
	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := New(WithMaxRetries(1))
	req, err := http.NewRequest(http.MethodGet, server.URL+"/torrents/info/x", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	resp, err := client.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err == nil {
		t.Fatal("a 503 that exhausted retries returned no error")
	}
	if attempts < 2 {
		t.Fatalf("server saw %d attempt(s); the give-up handler was not exercised", attempts)
	}
	if errors.Is(err, customerror.RateLimitedError) {
		t.Fatal("a 503 outage was typed as a rate limit; the add path would hold the grab waiting for a limit " +
			"that was never reached")
	}
	if !strings.Contains(err.Error(), "status 503") {
		t.Fatalf("the 503 give-up lost its status: %v", err)
	}
}

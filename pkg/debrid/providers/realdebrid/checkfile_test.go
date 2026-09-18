package realdebrid

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func TestMain(m *testing.M) {
	configDir, err := os.MkdirTemp("", "decypharr-realdebrid-test-")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(configDir)

	code := m.Run()
	_ = os.RemoveAll(configDir)
	os.Exit(code)
}

func newTestRealDebrid(t *testing.T, handler http.HandlerFunc) *RealDebrid {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return newRealDebridAt(server.URL)
}

func newRealDebridAt(host string) *RealDebrid {
	opts := []request.ClientOption{
		request.WithMaxRetries(0),
		request.WithTimeout(2 * time.Second),
	}
	return &RealDebrid{
		Host:         host,
		APIKey:       "test-key",
		client:       request.New(opts...),
		repairClient: request.New(opts...),
		logger:       zerolog.Nop(),
		config:       config.Debrid{Name: "test-realdebrid"},
	}
}

func statusHandler(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}
}

// assertIndeterminate demands the three-way contract: not healthy (nil) and not
// the definitive "gone" verdict that authorises destructive repair.
func assertIndeterminate(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("CheckFile() = nil; an unusable answer must never score healthy")
	}
	if !errors.Is(err, types.ErrAvailabilityIndeterminate) {
		t.Fatalf("CheckFile() error = %v, want types.ErrAvailabilityIndeterminate", err)
	}
	if errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("CheckFile() error = %v; an outage must not read as definitively dead", err)
	}
}

func TestCheckFileAvailable(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusCreated, http.StatusNoContent} {
		r := newTestRealDebrid(t, statusHandler(status))
		if err := r.CheckFile(context.Background(), "hash", "https://real-debrid.com/d/ABCDEF"); err != nil {
			t.Fatalf("CheckFile() with status %d error = %v, want nil", status, err)
		}
	}
}

// 🔴 A 404/410 FROM THIS ENDPOINT IS NOT A DEATH VERDICT, AND CALLING IT ONE
// DELETED LIVE CONTENT.
//
// This test previously asserted the opposite. That assumption was INHERITED
// rather than established: the work the rest of this file documents was about
// 401/429/5xx wrongly scoring healthy, and it carried "404 means definitively
// gone" through untouched.
//
// It is wrong for two independent reasons. `/unrestrict/check` is a fixed API
// route, so a 404 on it describes our request or their infrastructure, not the
// resource — AllDebrid's CheckFile says exactly this about its own endpoint.
// And HosterUnavailableError is the codebase's TRANSIENT class: reinsertReason
// treats it as a re-insertion trigger, Fixer wraps its inconclusive result in
// it, IsContentPermanentlyGone excludes it by name — yet the repair probe
// recorded it as broken, which is destructive-eligible.
//
// Production consequence, measured: a sweep logged "Re-insertion inconclusive:
// no provider reached a verdict about the content; entry left unmarked" and
// PRUNE deleted the entry ONE SECOND LATER. Both audited survivors unlock
// 200 OK on RealDebrid today; one was re-grabbed and fully re-downloaded.
//
// ⚠️ THE ORIGINAL PROPERTY IS PRESERVED AND STILL ASSERTED: a 404/410 must
// never score healthy. It simply reaches "unknown" rather than "dead".
func TestCheckFileNotFoundIsIndeterminateNotDead(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusGone} {
		r := newTestRealDebrid(t, statusHandler(status))
		err := r.CheckFile(context.Background(), "hash", "https://real-debrid.com/d/ABCDEF")
		if err == nil {
			t.Fatalf("CheckFile() with status %d = nil; an unusable answer must never score healthy", status)
		}
		if errors.Is(err, customerror.HosterUnavailableError) {
			t.Fatalf("CheckFile() with status %d error = %v; that sentinel makes the repair probe record "+
				"`broken`, which PRUNE and ARR-DELETE act on. A fixed API route answering 404 says "+
				"nothing about whether the content exists", status, err)
		}
		if !errors.Is(err, types.ErrAvailabilityIndeterminate) {
			t.Fatalf("CheckFile() with status %d error = %v, want types.ErrAvailabilityIndeterminate — "+
				"never healthy, never broken, re-checked soon", status, err)
		}
	}
}

// Before the fix only 404 failed the check, so 401/429/500/503 all scored
// HEALTHY — an outage or a throttle recorded dead entries as fine.
func TestCheckFileIndeterminateStatuses(t *testing.T) {
	statuses := []int{
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
	}
	for _, status := range statuses {
		t.Run(http.StatusText(status), func(t *testing.T) {
			r := newTestRealDebrid(t, statusHandler(status))
			assertIndeterminate(t, r.CheckFile(context.Background(), "hash", "https://real-debrid.com/d/ABCDEF"))
		})
	}
}

func TestCheckFileTransportErrorIsIndeterminate(t *testing.T) {
	server := httptest.NewServer(statusHandler(http.StatusOK))
	deadURL := server.URL
	server.Close()

	r := newRealDebridAt(deadURL)
	assertIndeterminate(t, r.CheckFile(context.Background(), "hash", "https://real-debrid.com/d/ABCDEF"))
}

func TestCheckFileCancelledContextIsIndeterminate(t *testing.T) {
	r := newTestRealDebrid(t, statusHandler(http.StatusOK))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assertIndeterminate(t, r.CheckFile(ctx, "hash", "https://real-debrid.com/d/ABCDEF"))
}

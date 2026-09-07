package manager

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"errors"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"go.uber.org/ratelimit"
)

// 🔴 THE LOAD REPRODUCTION. THE ONLY THING THAT HAS EVER BROKEN PRODUCTION.
//
// Three builds — fork.77, fork.78 and fork.79 — passed CI and then took the
// write path down within two minutes of deploying. CI was green for every one
// of them, because nothing in it exercises THE TOKEN QUEUE UNDER SUSTAINED
// ARRIVAL PRESSURE, and that is the only condition that has ever failed.
//
// Every unit test in this package has ONE caller. The defect needs hundreds
// against one bucket, so no amount of the tests we already had could find it:
//
//	fork.77  removed the retry backoff that was accidentally rationing arrivals
//	         -> every worker piled into AllDebrid's token queue -> add hangs
//	fork.78  skipped AllDebrid at cap
//	         -> the same pile landed on RealDebrid's queue -> add hangs
//	fork.79  bounded each caller's wait at 10s
//	         -> RD active rose 16 -> 74/100, and the add STILL timed out at 25s
//
// That last one is the interesting failure and the reason this harness exists.
// A 10s ceiling cannot produce a 25s timeout unless the bound is not doing what
// its author believed, so the harness has to reproduce the SHAPE — many callers,
// one real limiter, a provider that refuses — rather than any one symptom.
//
// ⚠️ SCALED, AND DELIBERATELY SO. Real ceiling 10s and real limits would make
// this a minute-long test. The structure is what reproduces the bug, not the
// magnitudes: one shared limiter, arrivals faster than it issues, and a
// synchronous add that must answer anyway. Every number below is scaled by the
// same factor and the assertion is stated relative to the ceiling, not in
// absolute seconds.
const (
	// loadReproCeiling stands in for the production 10s token-wait ceiling.
	loadReproCeiling = 300 * time.Millisecond
	// loadReproConcurrency stands in for the worker pool. Production had 176
	// and 367 goroutines in the two dumps.
	loadReproConcurrency = 60
)

// loadDebridClient is a provider whose calls go through a REAL request.Client
// with a REAL rate limiter to a server that refuses — the production stack for
// the part that matters.
type loadDebridClient struct {
	fakeDebridClient
	http    *request.Client
	baseURL string
	calls   atomic.Int64
}

func (c *loadDebridClient) do() error {
	c.calls.Add(1)
	req, err := http.NewRequest(http.MethodPost, c.baseURL, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("provider refused the add")
}

func (c *loadDebridClient) SubmitMagnet(t *debridTypes.Torrent) (*debridTypes.Torrent, error) {
	return nil, c.do()
}

func (c *loadDebridClient) GetAvailableSlots() (int, error) {
	if err := c.do(); err != nil {
		return 0, err
	}
	return 100, nil
}

func newLoadFixture(t *testing.T) (*Manager, *loadDebridClient) {
	return newLoadFixtureWithCeiling(t, loadReproCeiling)
}

// newLoadFixtureWithCeiling lets a test choose how long a token wait may run.
// The async-window test needs a ceiling LONGER than the window, or the resolver
// finishes first and the window bounds nothing — which is exactly how the first
// version of that test passed with the window disabled.
func newLoadFixtureWithCeiling(t *testing.T, ceiling time.Duration) (*Manager, *loadDebridClient) {
	return newLoadFixtureOpts(t, ceiling, 0)
}

// newLoadFixtureOpts adds a provider RESPONSE DELAY.
//
// The window test needs resolution to reliably outlast the window, and the
// token queue cannot be trusted to deliver that: go.uber.org/ratelimit's Take()
// is a mutex and a sleep, not a FIFO queue, so a fresh caller is not reliably
// stuck behind the crowd at test-scale contention. The first version of that
// test passed with the window DISABLED for exactly this reason, and the negative
// control is what exposed it. A slow provider makes "resolution outlasts the
// window" deterministic instead of probabilistic.
func newLoadFixtureOpts(t *testing.T, ceiling, serverDelay time.Duration) (*Manager, *loadDebridClient) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Instant by default: measured against the live account, RealDebrid
		// refuses an add in ~0.15s — it is not slow, and every second the add
		// path spends is ours.
		if serverDelay > 0 {
			time.Sleep(serverDelay)
		}
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)

	client := &loadDebridClient{
		fakeDebridClient: fakeDebridClient{
			cfg:      config.Debrid{Name: "loadrd", Provider: "realdebrid"},
			recorder: &fallbackCallRecorder{},
		},
		baseURL: server.URL,
	}

	// The manager fixture is built FIRST because it establishes the config path;
	// request.New reads the global config and would otherwise try to create one
	// in the working directory.
	m := newSyncRefusalManager(t, &client.fakeDebridClient)

	client.http = request.New(
		// Arrivals must outrun the bucket, which is the whole condition.
		request.WithRateLimiter(ratelimit.New(10, ratelimit.Per(time.Second))),
		request.WithRateWaitCeiling(ceiling),
		request.WithRetryableStatus(),
		request.WithTimeout(30*time.Second),
	)
	m.clients.Store("loadrd", client)
	m.capacityHold = newCapacityHoldQueue()
	m.jobQueue = NewJobQueue(context.Background(), 1, func(context.Context, *Job) {})
	t.Cleanup(m.jobQueue.Close)
	return m, client
}

func loadRequest(hash string) *ImportRequest {
	req := fallbackTestRequest("", false, nil)
	req.Magnet.InfoHash = hash
	req.Arr = &arr.Arr{Name: "sonarr"}
	return req
}

// THE CONTRACT: an add answers, under load, in bounded time.
//
// Not "succeeds" — refused or held are both fine, and under this much pressure
// held is the right answer. The *arr's question is only ever "did you take
// this", and it stops waiting long before it stops caring.
func TestAddStaysResponsiveUnderSustainedArrivalPressure(t *testing.T) {
	m, client := newLoadFixture(t)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range loadReproConcurrency {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for seq := 0; ; seq++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = m.AddNewTorrent(context.Background(), loadRequest(fmt.Sprintf("%040x", n*100000+seq)))
			}
		}(i)
	}
	t.Cleanup(func() { close(stop); wg.Wait() })

	// Let the queue build to the state the dumps captured.
	time.Sleep(2 * time.Second)

	start := time.Now()
	err := m.AddNewTorrent(context.Background(), loadRequest("00000000000000000000000000000000deadbeef"))
	elapsed := time.Since(start)

	if err != nil && elapsed < loadReproCeiling {
		t.Logf("add refused quickly (%v): %v", elapsed, err)
	}

	// 🎯 THE ASSERTION, stated against the ceiling rather than the clock.
	//
	// One add walks one provider. If the token wait is bounded as intended, the
	// worst case is a small multiple of the ceiling. Production saw 2.5x the
	// ceiling and the *arr gave up — so anything at or beyond 2x means the
	// bound is not holding the add path inside the *arr's patience.
	if limit := 2 * loadReproCeiling; elapsed >= limit {
		t.Fatalf("an add took %v under load, at or beyond %v (%dx the %v ceiling).\n"+
			"This is the production failure: fork.79 bounded each caller at 10s and the *arr still timed out "+
			"at 25s, because a caller that gives up leaves its goroutine queued and that goroutine still "+
			"consumes the token when its turn arrives. The waiters are bounded; the WASTE is not, so a live "+
			"caller queues behind a crowd of ghosts. Provider calls made: %d",
			elapsed, limit, 2, loadReproCeiling, client.calls.Load())
	}
}

// 🎯 THE (c) CONTRACT: the handler answers on ITS OWN clock, not the provider's.
//
// The three production hangs all had the same shape — the *arr's connection was
// held open for the provider walk, so provider trouble became *arr-visible
// outage. Bounding the token wait made the walk finish; it did not stop the
// *arr from waiting for it.
//
// With the window in place the add returns regardless of what the providers are
// doing, and resolution continues in the background. The window here is set
// SHORTER than the provider can possibly answer, so this exercises the
// background path specifically rather than happening to be fast.
func TestAddAnswersWithinTheSyncWindowUnderLoad(t *testing.T) {
	originalWindow := addSyncWindow
	addSyncWindow = 50 * time.Millisecond
	t.Cleanup(func() { addSyncWindow = originalWindow })

	// The ceiling must exceed the window, or the resolver answers first and this
	// test proves nothing — the negative control caught exactly that.
	m, _ := newLoadFixtureOpts(t, 2*time.Second, 400*time.Millisecond)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range loadReproConcurrency {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for seq := 0; ; seq++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = m.AddNewTorrent(context.Background(), loadRequest(fmt.Sprintf("%040x", 500000+n*100000+seq)))
			}
		}(i)
	}
	t.Cleanup(func() { close(stop); wg.Wait() })

	time.Sleep(1500 * time.Millisecond)

	start := time.Now()
	err := m.AddNewTorrent(context.Background(), loadRequest("00000000000000000000000000000000cafebabe"))
	elapsed := time.Since(start)

	// Accepted, because no verdict arrived inside the window.
	if err != nil {
		t.Fatalf("the add was refused (%v) despite no provider answering within the window; a verdict we did "+
			"not receive is not a verdict about the release", err)
	}
	if limit := 3 * addSyncWindow; elapsed >= limit {
		t.Fatalf("the add took %v, at or beyond %v. The handler is still waiting on the provider walk, which "+
			"is the shape that took the write path down three times", elapsed, limit)
	}
	// And it is well inside the token ceiling, which is the number the old code
	// was bounded by and the *arr was still timing out against.
	if elapsed >= 2*time.Second {
		t.Fatalf("the add took %v, at or beyond the %v token ceiling; the window is not what is bounding it",
			elapsed, 2*time.Second)
	}
}

// 🛑 A REFUSAL AFTER ACKNOWLEDGEMENT LEAVES A FAILED ROW, NEVER A VANISHED ONE.
//
// The synchronous refusal works by leaving nothing behind — the *arr takes its
// next candidate and there is no corpse. Past the window that becomes the worst
// available outcome: the *arr was told the grab was accepted, so deleting the
// reservation makes its download disappear with no record anywhere.
func TestPostAcknowledgementRefusalLeavesAFailedRow(t *testing.T) {
	originalWindow := addSyncWindow
	addSyncWindow = 30 * time.Millisecond
	t.Cleanup(func() { addSyncWindow = originalWindow })

	// Refuses — a CONTENT refusal, the shape that would normally delete the row
	// — but only after the window has closed.
	client := &fakeDebridClient{
		cfg:      config.Debrid{Name: "primary", Provider: "realdebrid"},
		recorder: &fallbackCallRecorder{},
		submitFn: func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
			time.Sleep(200 * time.Millisecond)
			return nil, errors.New("torrent is not cached and uncached downloads are disabled")
		},
	}
	m := newSyncRefusalManager(t, client)
	m.capacityHold = newCapacityHoldQueue()
	m.jobQueue = NewJobQueue(context.Background(), 1, func(context.Context, *Job) {})
	t.Cleanup(m.jobQueue.Close)

	req := fallbackTestRequest("", false, nil)
	if err := m.AddNewTorrent(context.Background(), req); err != nil {
		t.Fatalf("the add should have been acknowledged: %v", err)
	}

	// Let the background resolution reach its verdict.
	deadline := time.Now().Add(3 * time.Second)
	var entry *storage.Entry
	for time.Now().Before(deadline) {
		e, err := m.queue.GetTorrent(req.Magnet.InfoHash)
		if err == nil && e != nil && e.State == storage.EntryStateError {
			entry = e
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if entry == nil {
		current, err := m.queue.GetTorrent(req.Magnet.InfoHash)
		if err != nil || current == nil {
			t.Fatal("the row was DELETED after the arr was told the grab was accepted. The arr now believes it " +
				"has a download that exists nowhere, and will wait on it forever")
		}
		t.Fatalf("the row never reached a failed state; it is %q/%q, so the arr has no way to learn the "+
			"release was refused", current.State, current.Status)
	}
}

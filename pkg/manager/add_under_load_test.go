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

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
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
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The provider answers instantly. Measured against the live account,
		// RealDebrid refuses an add in ~0.15s — it is not slow, and every
		// second the add path spends is ours.
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
		request.WithRateWaitCeiling(loadReproCeiling),
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

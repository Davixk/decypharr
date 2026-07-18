package reader

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/nntp"
)

func TestFetchTimeoutIncludesLocalSemaphoreWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := DefaultConfig()
	cfg.MaxConnections = 1
	cfg.PrefetchAhead = 0
	cfg.DownloadTimeout = 50 * time.Millisecond

	stats := &ReaderStats{}
	cache := &SegmentCache{
		segments: []SegmentMeta{{
			MessageID:   "missing",
			Number:      1,
			Bytes:       4,
			StartOffset: 0,
			EndOffset:   3,
		}},
		segCount: 1,
		states:   make([]atomic.Uint32, 1),
		ctx:      ctx,
		stats:    stats,
	}

	fetcher := NewSegmentFetcher(ctx, nil, cache, cfg, stats, zerolog.Nop())
	defer fetcher.Close()

	// Occupy the only reader slot without involving the provider client. The
	// fetch must time out while waiting here and restore Fetching -> Empty.
	fetcher.semaphore <- struct{}{}
	started := time.Now()
	err := fetcher.Fetch(context.Background(), 0)
	elapsed := time.Since(started)
	<-fetcher.semaphore

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Fetch error = %v, want context deadline exceeded", err)
	}
	if elapsed > time.Second {
		t.Fatalf("semaphore wait took %s, want a bounded wait", elapsed)
	}
	if state := cache.GetState(0); state != StateEmpty {
		t.Fatalf("segment state after timeout = %s, want Empty", state)
	}
}

func TestValidateSegmentLengthClassifiesShortBodiesAsMissing(t *testing.T) {
	if err := validateSegmentLength(4, 4); err != nil {
		t.Fatalf("exact segment length rejected: %v", err)
	}

	err := validateSegmentLength(3, 4)
	if !nntp.IsArticleNotFoundError(err) {
		t.Fatalf("short segment error = %v, want article-not-found classification", err)
	}
	if !strings.Contains(err.Error(), "got 3 bytes, expected 4") {
		t.Fatalf("short segment error lacks byte counts: %v", err)
	}
}

func TestPrefetchParentCancellationReleasesQueuedAndInflightWork(t *testing.T) {
	fetcher, cache, client := newCancellationTestFetcher(t, 3)

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	fetcher.QueuePrefetchRange(requestCtx, 0, 2)
	waitForTestSignal(t, client.started, "prefetch provider call")

	cancelRequest()
	waitForTestSignal(t, client.stopped, "prefetch provider cancellation")
	waitForTestCondition(t, "prefetch cancellation to release all work", func() bool {
		return cancellationFetcherIdle(fetcher, cache, client)
	})

	if calls := client.calls.Load(); calls != 1 {
		t.Fatalf("provider calls after canceling queued prefetch = %d, want 1", calls)
	}
}

func TestPrefetchCancellationPreservesAnotherRequestInterest(t *testing.T) {
	fetcher, cache, client := newCancellationTestFetcher(t, 1)

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	fetcher.QueuePrefetch(firstCtx, 0)
	waitForTestSignal(t, client.started, "shared prefetch provider call")
	fetcher.QueuePrefetch(secondCtx, 0)

	cancelFirst()
	waitForTestCondition(t, "first prefetch owner to detach", func() bool {
		fetcher.prefetchMu.Lock()
		defer fetcher.prefetchMu.Unlock()
		job := fetcher.prefetchJobs[0]
		return job != nil && len(job.scopes) == 1 && len(fetcher.prefetchScopes) == 1 && job.ctx.Err() == nil
	})
	if active := client.active.Load(); active != 1 {
		t.Fatalf("active provider calls after one owner canceled = %d, want 1", active)
	}
	select {
	case <-client.stopped:
		t.Fatal("shared prefetch was canceled while another request still needed it")
	default:
	}

	cancelSecond()
	waitForTestSignal(t, client.stopped, "final shared prefetch cancellation")
	waitForTestCondition(t, "shared prefetch to release all work", func() bool {
		return cancellationFetcherIdle(fetcher, cache, client)
	})
}

func TestFetchCancellationPreservesAnotherReaderInterest(t *testing.T) {
	fetcher, cache, client := newCancellationTestFetcher(t, 1)

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	firstErr := make(chan error, 1)
	secondErr := make(chan error, 1)
	go func() { firstErr <- fetcher.Fetch(firstCtx, 0) }()
	waitForTestSignal(t, client.started, "shared foreground provider call")
	go func() { secondErr <- fetcher.Fetch(secondCtx, 0) }()

	waitForTestCondition(t, "second reader to join fetch", func() bool {
		fetcher.inFlightMu.Lock()
		defer fetcher.inFlightMu.Unlock()
		promise := fetcher.inFlight[0]
		return promise != nil && promise.users == 2 && promise.ctx.Err() == nil
	})

	cancelFirst()
	if err := waitForTestError(t, firstErr, "first reader cancellation"); !errors.Is(err, context.Canceled) {
		t.Fatalf("first Fetch error = %v, want context canceled", err)
	}
	waitForTestCondition(t, "first reader to release fetch interest", func() bool {
		fetcher.inFlightMu.Lock()
		defer fetcher.inFlightMu.Unlock()
		promise := fetcher.inFlight[0]
		return promise != nil && promise.users == 1 && promise.ctx.Err() == nil
	})
	if active := client.active.Load(); active != 1 {
		t.Fatalf("active provider calls after one reader canceled = %d, want 1", active)
	}

	cancelSecond()
	if err := waitForTestError(t, secondErr, "second reader cancellation"); !errors.Is(err, context.Canceled) {
		t.Fatalf("second Fetch error = %v, want context canceled", err)
	}
	waitForTestSignal(t, client.stopped, "final foreground provider cancellation")
	waitForTestCondition(t, "foreground cancellation to release all work", func() bool {
		return cancellationFetcherIdle(fetcher, cache, client)
	})
	if calls := client.calls.Load(); calls != 1 {
		t.Fatalf("provider calls for shared fetch = %d, want 1", calls)
	}
}

func TestLatePrefetchInterestRequeuesCompletedEmptyAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stats := &ReaderStats{}
	cache := &SegmentCache{
		segments: []SegmentMeta{{
			MessageID:   "late-prefetch",
			Number:      1,
			Bytes:       4,
			StartOffset: 0,
			EndOffset:   3,
		}},
		segCount: 1,
		states:   make([]atomic.Uint32, 1),
		ctx:      ctx,
		stats:    stats,
	}
	client := &cancellationTestClient{
		started: make(chan struct{}, 2),
		stopped: make(chan struct{}, 2),
	}
	cfg := DefaultConfig()
	cfg.MaxConnections = 1 // Construct without a worker; drive the job manually.
	cfg.DownloadTimeout = 10 * time.Millisecond
	fetcher := newSegmentFetcher(ctx, client, cache, cfg, stats, zerolog.Nop())
	fetcher.prefetchWorkers = 1 // Enable queueing while retaining manual control.
	t.Cleanup(func() {
		fetcher.Close()
		cancel()
	})

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	fetcher.QueuePrefetch(firstCtx, 0)
	oldJob := <-fetcher.prefetchCh
	if !fetcher.startPrefetchJob(oldJob) {
		t.Fatal("initial prefetch job was not current")
	}
	fetcher.prefetchOne(oldJob)
	waitForTestCondition(t, "timed-out prefetch flight cleanup", func() bool {
		fetcher.inFlightMu.Lock()
		inFlight := len(fetcher.inFlight)
		fetcher.inFlightMu.Unlock()
		return cache.GetState(0) == StateEmpty && inFlight == 0 &&
			client.active.Load() == 0 && len(fetcher.semaphore) == 0
	})

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	fetcher.QueuePrefetch(secondCtx, 0)
	fetcher.prefetchMu.Lock()
	currentBeforeFinish := fetcher.prefetchJobs[0]
	interestsBeforeFinish := len(oldJob.scopes)
	fetcher.prefetchMu.Unlock()
	if currentBeforeFinish != oldJob || interestsBeforeFinish != 2 {
		t.Fatalf("late interest did not join pending cleanup: current=%p old=%p interests=%d", currentBeforeFinish, oldJob, interestsBeforeFinish)
	}

	newJob := fetcher.finishPrefetchJob(oldJob)
	fetcher.prefetchMu.Lock()
	current := fetcher.prefetchJobs[0]
	newInterests := 0
	if newJob != nil {
		newInterests = len(newJob.scopes)
	}
	fetcher.prefetchMu.Unlock()
	if newJob == nil || current != newJob || newInterests != 1 {
		t.Fatalf("late interest was not retained in a replacement: new=%p current=%p interests=%d", newJob, current, newInterests)
	}
	if !fetcher.startPrefetchJob(newJob) {
		t.Fatal("replacement prefetch job is not runnable")
	}
	if next := fetcher.finishPrefetchJob(newJob); next != nil {
		t.Fatal("replacement without newer interests unexpectedly chained again")
	}
}

type cancellationTestClient struct {
	calls   atomic.Int32
	active  atomic.Int32
	started chan struct{}
	stopped chan struct{}
}

func (c *cancellationTestClient) ExecuteWithFailover(ctx context.Context, _ func(*nntp.Connection) error) error {
	c.calls.Add(1)
	c.active.Add(1)
	c.started <- struct{}{}
	defer func() {
		c.active.Add(-1)
		c.stopped <- struct{}{}
	}()
	<-ctx.Done()
	return ctx.Err()
}

func newCancellationTestFetcher(t *testing.T, segmentCount int) (*SegmentFetcher, *SegmentCache, *cancellationTestClient) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stats := &ReaderStats{}
	segments := make([]SegmentMeta, segmentCount)
	for i := range segments {
		segments[i] = SegmentMeta{
			MessageID:   "cancel-test",
			Number:      i + 1,
			Bytes:       4,
			StartOffset: int64(i * 4),
			EndOffset:   int64(i*4 + 3),
		}
	}
	cache := &SegmentCache{
		segments: segments,
		segCount: segmentCount,
		states:   make([]atomic.Uint32, segmentCount),
		ctx:      ctx,
		stats:    stats,
	}
	client := &cancellationTestClient{
		started: make(chan struct{}, segmentCount+1),
		stopped: make(chan struct{}, segmentCount+1),
	}
	cfg := DefaultConfig()
	cfg.MaxConnections = 2
	cfg.PrefetchAhead = segmentCount
	cfg.DownloadTimeout = 10 * time.Second
	fetcher := newSegmentFetcher(ctx, client, cache, cfg, stats, zerolog.Nop())
	t.Cleanup(func() {
		fetcher.Close()
		cancel()
	})
	return fetcher, cache, client
}

func cancellationFetcherIdle(fetcher *SegmentFetcher, cache *SegmentCache, client *cancellationTestClient) bool {
	if client.active.Load() != 0 || len(fetcher.semaphore) != 0 || len(fetcher.prefetchCh) != 0 {
		return false
	}
	fetcher.inFlightMu.Lock()
	inFlight := len(fetcher.inFlight)
	fetcher.inFlightMu.Unlock()
	if inFlight != 0 {
		return false
	}
	fetcher.prefetchMu.Lock()
	prefetchJobs := len(fetcher.prefetchJobs)
	prefetchScopes := len(fetcher.prefetchScopes)
	fetcher.prefetchMu.Unlock()
	if prefetchJobs != 0 || prefetchScopes != 0 {
		return false
	}
	for i := 0; i < cache.SegmentCount(); i++ {
		if cache.GetState(i) != StateEmpty {
			return false
		}
	}
	return true
}

func waitForTestSignal(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitForTestError(t *testing.T, ch <-chan error, description string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return nil
	}
}

func waitForTestCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

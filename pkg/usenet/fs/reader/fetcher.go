package reader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/nntp"
)

type segmentClient interface {
	ExecuteWithFailover(context.Context, func(*nntp.Connection) error) error
}

// SegmentFetcher handles downloading segments from NNTP with deduplication and retry.
//
// Key features:
//   - Request deduplication: Only one goroutine fetches a segment at a time
//   - Semaphore for connection limiting
//   - Background prefetch queue for read-ahead
//   - Streams directly to disk via cache's StreamWriter
type SegmentFetcher struct {
	client segmentClient
	cache  *SegmentCache
	config Config
	logger zerolog.Logger
	stats  *ReaderStats

	// Concurrency control
	semaphore chan struct{} // Limits concurrent downloads

	// Request deduplication
	inFlight   map[int]*fetchPromise
	inFlightMu sync.Mutex
	fetchWg    sync.WaitGroup
	closing    bool

	// Background prefetch
	prefetchCh      chan *prefetchJob
	prefetchMu      sync.Mutex
	prefetchJobs    map[int]*prefetchJob
	prefetchScopes  map[<-chan struct{}]*prefetchScope
	prefetchWorkers int
	prefetchWg      sync.WaitGroup
	prefetchClosing bool

	// Lifecycle
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeDone chan struct{}
}

// fetchPromise allows callers from independent stream requests to share one
// download. Its operation context is rooted at the reader lifetime, not the
// first caller; it is canceled only when the final interested caller leaves.
type fetchPromise struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	err      error
	users    int
	finished bool
}

// prefetchScope groups every read-ahead hint issued by one request context, so
// one context cancellation can promptly detach that request from all jobs.
type prefetchScope struct {
	key    <-chan struct{}
	jobs   map[*prefetchJob]struct{}
	stop   func() bool
	closed bool
}

// prefetchJob is one segment download shared by all request scopes interested
// in it. A stale canceled pointer may remain buffered in prefetchCh; workers
// validate it against prefetchJobs before doing any work.
type prefetchJob struct {
	segIdx        int
	ctx           context.Context
	cancel        context.CancelFunc
	scopes        map[*prefetchScope]struct{}
	attemptScopes map[*prefetchScope]struct{} // guarded by prefetchMu
}

// NewSegmentFetcher creates a new segment fetcher.
func NewSegmentFetcher(
	ctx context.Context,
	client *nntp.Client,
	cache *SegmentCache,
	config Config,
	stats *ReaderStats,
	logger zerolog.Logger,
) *SegmentFetcher {
	return newSegmentFetcher(ctx, client, cache, config, stats, logger)
}

// newSegmentFetcher accepts the narrow NNTP operation used by the fetcher so
// cancellation and ownership tests can use deterministic clients without
// changing the exported constructor's API.
func newSegmentFetcher(
	ctx context.Context,
	client segmentClient,
	cache *SegmentCache,
	config Config,
	stats *ReaderStats,
	logger zerolog.Logger,
) *SegmentFetcher {
	ctx, cancel := context.WithCancel(ctx)

	maxConns := config.MaxConnections
	if maxConns < 1 {
		maxConns = 8
	}

	sf := &SegmentFetcher{
		client:         client,
		cache:          cache,
		config:         config,
		logger:         logger.With().Str("component", "fetcher").Logger(),
		stats:          stats,
		semaphore:      make(chan struct{}, maxConns),
		inFlight:       make(map[int]*fetchPromise),
		prefetchCh:     make(chan *prefetchJob, 256), // Buffer for prefetch hints
		prefetchJobs:   make(map[int]*prefetchJob),
		prefetchScopes: make(map[<-chan struct{}]*prefetchScope),
		ctx:            ctx,
		cancel:         cancel,
		closeDone:      make(chan struct{}),
	}

	// Start fewer prefetch workers than foreground connection slots. Seeky
	// callers such as ffprobe can jump to the tail while head read-ahead is
	// still running; reserving at least one slot prevents background prefetch
	// from starving the blocking read that the caller is waiting on.
	numPrefetchWorkers := maxConns - 1
	sf.prefetchWorkers = numPrefetchWorkers
	if numPrefetchWorkers > 0 {
		for i := range numPrefetchWorkers {
			sf.prefetchWg.Add(1)
			go sf.prefetchWorker(i)
		}
	}

	return sf
}

// Fetch downloads a segment synchronously, with deduplication.
// Multiple goroutines calling Fetch for the same segment will share the download.
func (sf *SegmentFetcher) Fetch(ctx context.Context, segIdx int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := sf.ctx.Err(); err != nil {
			return err
		}

		// Fast path: already cached, or wait out an in-progress eviction so we
		// don't dedup/fetch against a segment whose disk range is mid-punch.
		state := sf.cache.GetState(segIdx)
		if state == StateEvicting {
			if err := sf.cache.WaitForEvictionRelease(ctx, segIdx); err != nil {
				return err
			}
			continue // slot is Empty now; re-evaluate
		}
		switch state {
		case StateOnDisk:
			return nil
		case StateFailed:
			return sf.cache.GetError(segIdx)
		}

		sf.inFlightMu.Lock()
		if sf.closing {
			err := sf.ctx.Err()
			if err == nil {
				err = context.Canceled
			}
			sf.inFlightMu.Unlock()
			return err
		}
		if promise, ok := sf.inFlight[segIdx]; ok {
			if promise.users == 0 && !promise.finished {
				// The prior owners all canceled and cancellation is propagating
				// through the download. Wait for that generation to release the
				// cache slot, then start/join the replacement generation.
				done := promise.done
				sf.inFlightMu.Unlock()
				select {
				case <-done:
					continue
				case <-ctx.Done():
					return ctx.Err()
				case <-sf.ctx.Done():
					return sf.ctx.Err()
				}
			}
			promise.users++
			sf.inFlightMu.Unlock()
			return sf.waitForFetch(ctx, promise)
		}

		flightCtx, flightCancel := context.WithCancel(sf.ctx)
		promise := &fetchPromise{
			ctx:    flightCtx,
			cancel: flightCancel,
			done:   make(chan struct{}),
			users:  1,
		}
		sf.inFlight[segIdx] = promise
		sf.fetchWg.Add(1)
		sf.inFlightMu.Unlock()

		go sf.runFetch(segIdx, promise)
		return sf.waitForFetch(ctx, promise)
	}
}

func (sf *SegmentFetcher) runFetch(segIdx int, promise *fetchPromise) {
	defer sf.fetchWg.Done()
	err := sf.doFetch(promise.ctx, segIdx)
	promise.cancel()

	sf.inFlightMu.Lock()
	promise.err = err
	promise.finished = true
	if sf.inFlight[segIdx] == promise {
		delete(sf.inFlight, segIdx)
	}
	close(promise.done)
	sf.inFlightMu.Unlock()
}

func (sf *SegmentFetcher) waitForFetch(ctx context.Context, promise *fetchPromise) error {
	var err error
	select {
	case <-promise.done:
		err = promise.err
	case <-ctx.Done():
		err = ctx.Err()
	case <-sf.ctx.Done():
		err = sf.ctx.Err()
	}
	sf.releaseFetchInterest(promise)
	return err
}

func (sf *SegmentFetcher) releaseFetchInterest(promise *fetchPromise) {
	var cancel context.CancelFunc
	sf.inFlightMu.Lock()
	if promise.users > 0 {
		promise.users--
	}
	if promise.users == 0 && !promise.finished {
		cancel = promise.cancel
	}
	sf.inFlightMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// doFetch performs the actual NNTP download.
func (sf *SegmentFetcher) doFetch(ctx context.Context, segIdx int) error {
	seg := sf.cache.GetSegment(segIdx)
	if seg == nil {
		return ErrSegmentNotFound
	}

	// Try to mark as fetching (atomic transition Empty -> Fetching)
	if !sf.cache.MarkFetching(segIdx) {
		// Someone else is fetching or it's already cached
		state := sf.cache.GetState(segIdx)
		switch state {
		case StateOnDisk:
			return nil
		case StateFailed:
			return sf.cache.GetError(segIdx)
		case StateFetching:
			// Wait for the other fetcher
			return sf.cache.WaitForSegment(ctx, segIdx)
		case StateEvicting:
			// An evictor grabbed the slot between Fetch's check and here.
			// Wait for the punch to finish, then retry the fetch into the
			// released range.
			if err := sf.cache.WaitForEvictionRelease(ctx, segIdx); err != nil {
				return err
			}
			return sf.doFetch(ctx, segIdx)
		}
	}

	// Start the attempt deadline before acquiring the local reader slot. This
	// ensures a saturated per-file semaphore cannot wait forever.
	timeout := sf.config.DownloadTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	downloadCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Acquire connection slot
	select {
	case sf.semaphore <- struct{}{}:
		defer func() { <-sf.semaphore }()
	case <-downloadCtx.Done():
		sf.cache.ReleaseFetching(segIdx)
		return downloadCtx.Err()
	case <-sf.ctx.Done():
		sf.cache.ReleaseFetching(segIdx)
		return sf.ctx.Err()
	}

	messageID := seg.MessageID

	// ExecuteWithFailover already retries per provider and across providers —
	// a single call is sufficient.  An outer retry loop would multiply the
	// total attempts by retries×providers, leading to very long failure times.
	err := sf.client.ExecuteWithFailover(downloadCtx, func(conn *nntp.Connection) error {
		stopCancel := context.AfterFunc(downloadCtx, func() {
			_ = conn.Close()
		})
		defer stopCancel()

		// Get the segment writer for the disk cache.
		writer := sf.cache.StreamWriter(segIdx)
		if writer == nil {
			return ErrCacheClosed
		}

		// Stream the decoded body into the chosen tier.
		_, err := conn.StreamBody(messageID, writer)
		if err != nil {
			writer.Discard()
			if ctxErr := downloadCtx.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		}
		if ctxErr := downloadCtx.Err(); ctxErr != nil {
			writer.Discard()
			return ctxErr
		}

		// A BODY response is not usable unless it contains every byte promised
		// by the segment map. Previously a short final segment could Finalize
		// with zero/partial data, leaving the cache in Fetching or causing a
		// tail retry loop forever.
		if err := validateSegmentLength(writer.BytesWritten(), seg.Bytes); err != nil {
			writer.Discard()
			return err
		}

		// Commit (updates cache state to StateOnDisk).
		writer.Finalize()

		return nil
	})

	if err != nil {
		sf.stats.DownloadErrors.Add(1)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			sf.cache.ReleaseFetching(segIdx)
			return err
		}
		sf.cache.MarkFailed(segIdx, err)
		return err
	}

	sf.stats.Downloads.Add(1)
	return nil
}

func validateSegmentLength(written, expected int64) error {
	if written == expected {
		return nil
	}
	return &nntp.Error{
		Type:    nntp.ErrorTypeArticleNotFound,
		Message: fmt.Sprintf("article body is short after decoding: got %d bytes, expected %d", written, expected),
	}
}

// QueuePrefetch adds request-owned interest in a segment to the background
// queue. Multiple request scopes share one job; canceling one scope only
// cancels the job when no other scope remains interested.
func (sf *SegmentFetcher) QueuePrefetch(ctx context.Context, segIdx int) {
	sf.QueuePrefetchRange(ctx, segIdx, segIdx)
}

// QueuePrefetchRange queues multiple segments for prefetch. The context's Done
// channel is the request identity: all hints issued by the same stream share a
// single cancellation callback, while wrappers that preserve Done naturally
// remain part of the same scope.
func (sf *SegmentFetcher) QueuePrefetchRange(ctx context.Context, startSeg, endSeg int) {
	if sf.prefetchWorkers <= 0 || startSeg > endSeg {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil || sf.ctx.Err() != nil {
		return
	}

	var stops []func() bool
	sf.prefetchMu.Lock()
	if sf.prefetchClosing || sf.ctx.Err() != nil || ctx.Err() != nil {
		sf.prefetchMu.Unlock()
		return
	}
	scope := sf.prefetchScopeLocked(ctx)
	for segIdx := startSeg; segIdx <= endSeg; segIdx++ {
		if segIdx < 0 || segIdx >= sf.cache.SegmentCount() || sf.cache.GetState(segIdx) == StateOnDisk {
			continue
		}
		if job := sf.prefetchJobs[segIdx]; job != nil {
			if _, exists := job.scopes[scope]; !exists {
				job.scopes[scope] = struct{}{}
				scope.jobs[job] = struct{}{}
			}
			continue
		}

		jobCtx, jobCancel := context.WithCancel(sf.ctx)
		job := &prefetchJob{
			segIdx: segIdx,
			ctx:    jobCtx,
			cancel: jobCancel,
			scopes: map[*prefetchScope]struct{}{scope: {}},
		}
		sf.prefetchJobs[segIdx] = job
		scope.jobs[job] = struct{}{}
		select {
		case sf.prefetchCh <- job:
		default:
			delete(sf.prefetchJobs, segIdx)
			delete(scope.jobs, job)
			job.cancel()
			sf.stats.PrefetchMisses.Add(1)
		}
	}
	if len(scope.jobs) == 0 {
		delete(sf.prefetchScopes, scope.key)
		scope.closed = true
		if scope.stop != nil {
			stops = append(stops, scope.stop)
		}
	}
	sf.prefetchMu.Unlock()
	for _, stop := range stops {
		stop()
	}
}

func (sf *SegmentFetcher) prefetchScopeLocked(ctx context.Context) *prefetchScope {
	key := ctx.Done()
	if scope := sf.prefetchScopes[key]; scope != nil && !scope.closed {
		return scope
	}
	scope := &prefetchScope{
		key:  key,
		jobs: make(map[*prefetchJob]struct{}),
	}
	sf.prefetchScopes[key] = scope
	if key != nil {
		scope.stop = context.AfterFunc(ctx, func() {
			sf.cancelPrefetchScope(scope)
		})
	}
	return scope
}

func (sf *SegmentFetcher) cancelPrefetchScope(scope *prefetchScope) {
	var cancels []context.CancelFunc
	sf.prefetchMu.Lock()
	if scope.closed || sf.prefetchScopes[scope.key] != scope {
		sf.prefetchMu.Unlock()
		return
	}
	delete(sf.prefetchScopes, scope.key)
	scope.closed = true
	for job := range scope.jobs {
		delete(job.scopes, scope)
		if len(job.scopes) == 0 && sf.prefetchJobs[job.segIdx] == job {
			delete(sf.prefetchJobs, job.segIdx)
			cancels = append(cancels, job.cancel)
		}
	}
	scope.jobs = nil
	sf.prefetchMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// prefetchWorker processes segments from the prefetch queue.
func (sf *SegmentFetcher) prefetchWorker(id int) {
	defer sf.prefetchWg.Done()

	for {
		select {
		case <-sf.ctx.Done():
			return
		case job, ok := <-sf.prefetchCh:
			if !ok {
				return
			}
			// A transiently empty attempt may hand late request scopes to a
			// replacement generation. Run it on this worker directly: the old
			// queue token already provided the worker budget, and an inline handoff
			// cannot deadlock when prefetchCh is full.
			for job != nil {
				if sf.startPrefetchJob(job) {
					sf.prefetchOne(job)
				}
				job = sf.finishPrefetchJob(job)
			}
		}
	}
}

// prefetchOne uses the deduplicated, failover-aware single-segment fetch path.
func (sf *SegmentFetcher) prefetchOne(job *prefetchJob) {
	ctx := job.ctx
	segIdx := job.segIdx
	state := sf.cache.GetState(segIdx)
	if state == StateOnDisk {
		sf.stats.PrefetchHits.Add(1)
		return
	}

	timeout := sf.config.DownloadTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	fetchCtx, cancel := context.WithTimeout(ctx, timeout)
	err := sf.Fetch(fetchCtx, segIdx)
	cancel()

	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		sf.logger.Debug().Err(err).Int("segment", segIdx).Msg("prefetch failed")
	}
}

func (sf *SegmentFetcher) startPrefetchJob(job *prefetchJob) bool {
	if job == nil || job.ctx.Err() != nil {
		return false
	}
	sf.prefetchMu.Lock()
	current := sf.prefetchJobs[job.segIdx] == job && len(job.scopes) > 0
	if current {
		job.attemptScopes = make(map[*prefetchScope]struct{}, len(job.scopes))
		for scope := range job.scopes {
			job.attemptScopes[scope] = struct{}{}
		}
	}
	sf.prefetchMu.Unlock()
	return current
}

// finishPrefetchJob retires one attempt. If a transient attempt left no usable
// cache entry, request scopes that joined after the attempt began are moved to
// a fresh generation rather than being discarded with the completed attempt.
// The caller must run the returned job before accepting another channel item.
func (sf *SegmentFetcher) finishPrefetchJob(job *prefetchJob) *prefetchJob {
	if job == nil {
		return nil
	}
	var stops []func() bool
	var replacement *prefetchJob
	sf.prefetchMu.Lock()
	if sf.prefetchJobs[job.segIdx] == job {
		state := sf.cache.GetState(job.segIdx)
		canRetryLate := !sf.prefetchClosing && sf.ctx.Err() == nil &&
			(state == StateEmpty || state == StateFetching)
		if canRetryLate && len(job.attemptScopes) > 0 {
			for scope := range job.scopes {
				if _, attempted := job.attemptScopes[scope]; attempted || scope.closed {
					continue
				}
				if replacement == nil {
					replacementCtx, replacementCancel := context.WithCancel(sf.ctx)
					replacement = &prefetchJob{
						segIdx: job.segIdx,
						ctx:    replacementCtx,
						cancel: replacementCancel,
						scopes: make(map[*prefetchScope]struct{}),
					}
				}
				delete(job.scopes, scope)
				delete(scope.jobs, job)
				replacement.scopes[scope] = struct{}{}
				scope.jobs[replacement] = struct{}{}
			}
		}
		if replacement != nil {
			sf.prefetchJobs[job.segIdx] = replacement
		} else {
			delete(sf.prefetchJobs, job.segIdx)
		}
	}
	for scope := range job.scopes {
		delete(scope.jobs, job)
		if len(scope.jobs) == 0 && !scope.closed && sf.prefetchScopes[scope.key] == scope {
			delete(sf.prefetchScopes, scope.key)
			scope.closed = true
			if scope.stop != nil {
				stops = append(stops, scope.stop)
			}
		}
	}
	job.scopes = nil
	job.attemptScopes = nil
	sf.prefetchMu.Unlock()
	job.cancel()
	for _, stop := range stops {
		stop()
	}
	return replacement
}

// EnsureSegments fetches all segments in the range, returning when all are
// available. Segments are fetched in order; in steady-state playback the
// background prefetch workers have already downloaded them, so this loop
// usually just confirms cache presence. fetchWithRetry keeps a single
// transient segment failure from tearing down the whole stream.
func (sf *SegmentFetcher) EnsureSegments(ctx context.Context, startSeg, endSeg int) error {
	for i := startSeg; i <= endSeg; i++ {
		state := sf.cache.GetState(i)
		if state != StateOnDisk {
			if err := sf.fetchWithRetry(ctx, i); err != nil {
				return err
			}
		}
	}
	return nil
}

// fetchWithRetry fetches a single segment, retrying transient failures so a
// momentary provider hiccup or stall does not tear down the whole stream.
// Permanent failures (article-not-found) and cancellations return immediately.
func (sf *SegmentFetcher) fetchWithRetry(ctx context.Context, segIdx int) error {
	maxAttempts := sf.config.MaxRetries
	if maxAttempts < 1 {
		maxAttempts = 3
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			// Clear the failed state so the segment can be re-fetched, then
			// back off briefly before retrying. ResetFailed is a CAS: if a
			// concurrent reader fetched the segment meanwhile it stays OnDisk.
			sf.cache.ResetFailed(segIdx)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-sf.ctx.Done():
				return sf.ctx.Err()
			case <-time.After(sf.retryBackoff(attempt)):
			}
		}

		err := sf.Fetch(ctx, segIdx)
		if err == nil {
			return nil
		}
		lastErr = err

		// Don't retry permanent errors or cancellations.
		if nntp.IsArticleNotFoundError(err) || ctx.Err() != nil || sf.ctx.Err() != nil {
			return err
		}
	}
	return lastErr
}

// retryBackoff returns the delay before the given (1-indexed) retry attempt.
func (sf *SegmentFetcher) retryBackoff(attempt int) time.Duration {
	base := sf.config.RetryDelay
	if base <= 0 {
		base = time.Second
	}
	d := base << (attempt - 1)
	if maxDelay := 5 * time.Second; d > maxDelay {
		d = maxDelay
	}
	return d
}

// Close stops all workers and waits for them to finish.
//
// prefetchCh is deliberately never closed: QueuePrefetch can race Close (a
// ReadAtContext already past the reader's closed check), and a send on a
// closed channel panics even inside a select. Workers exit via sf.ctx instead,
// and the channel is garbage-collected with the fetcher.
func (sf *SegmentFetcher) Close() {
	sf.closeOnce.Do(func() {
		// Fence out new flights before Wait begins. Every fetchWg.Add happens
		// while holding inFlightMu after checking this flag.
		var flightCancels []context.CancelFunc
		sf.inFlightMu.Lock()
		sf.closing = true
		for _, promise := range sf.inFlight {
			flightCancels = append(flightCancels, promise.cancel)
		}
		sf.inFlightMu.Unlock()

		sf.cancel()
		for _, cancel := range flightCancels {
			cancel()
		}
		sf.cancelAllPrefetch()
		sf.prefetchWg.Wait()
		sf.fetchWg.Wait()
		close(sf.closeDone)
	})
	<-sf.closeDone
}

func (sf *SegmentFetcher) cancelAllPrefetch() {
	var cancels []context.CancelFunc
	var stops []func() bool
	sf.prefetchMu.Lock()
	sf.prefetchClosing = true
	for segIdx, job := range sf.prefetchJobs {
		delete(sf.prefetchJobs, segIdx)
		cancels = append(cancels, job.cancel)
		job.scopes = nil
	}
	for key, scope := range sf.prefetchScopes {
		delete(sf.prefetchScopes, key)
		scope.closed = true
		scope.jobs = nil
		if scope.stop != nil {
			stops = append(stops, scope.stop)
		}
	}
	sf.prefetchMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	for _, stop := range stops {
		stop()
	}
}

// Error types
var (
	ErrSegmentNotFound = &segmentError{msg: "segment not found"}
	ErrCacheClosed     = &segmentError{msg: "cache closed"}
)

type segmentError struct {
	msg string
}

func (e *segmentError) Error() string {
	return e.msg
}

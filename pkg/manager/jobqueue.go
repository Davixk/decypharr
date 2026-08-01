package manager

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

// JobType represents the type of processing job
type JobType string

const (
	JobTypeTorrent JobType = "torrent"
	JobTypeNZB     JobType = "nzb"
)

// Job represents a unified processing job for both torrents and NZBs
type Job struct {
	ID             string
	Type           JobType
	Request        *ImportRequest               // The original import request
	DebridTorrent  *debridTypes.Torrent         // Torrent placement created before the active-download gate
	NZBMeta        *storage.NZB                 // NZB metadata parsed before the active-download gate
	NZBGroups      map[string]*parser.FileGroup // NZB file groups parsed before the active-download gate
	Entry          *storage.Entry               // Entry created during processing
	ResumeExisting bool                         // Continue an already persisted provider placement
	ResumeAction   bool                         // Resume a durably claimed post-download action after restart
	RebuildQueued  bool                         // Re-parse a queued NZB from its staged source (or resume completed metadata) before processing
	RetryCount     int                          // Infrastructure-retry attempts already made for this job (drives backoff)
	CreatedAt      time.Time
}

// NewJob creates a new job
func NewJob(jobType JobType, req *ImportRequest) *Job {
	id := ""
	if req != nil {
		id = req.Id
	}
	return &Job{
		ID:        id,
		Type:      jobType,
		Request:   req,
		CreatedAt: time.Now(),
	}
}

// JobQueue is a unified, unbounded, thread-safe job queue with a worker pool
// that GROWS ON DEMAND up to maxWorkers. It replaces the separate
// ImportRequest queue, nzbJobQueue, and unbounded goroutine fan-out with a
// single queue that processes both torrent and NZB jobs.
type JobQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	jobs   []*Job
	closed bool

	maxWorkers int
	running    int // workers alive; guarded by mu
	idle       int // workers parked in pop(); guarded by mu
	logger     zerolog.Logger
	wg         sync.WaitGroup
	active     atomic.Int64

	// processFunc is called by workers to process a job
	processFunc func(ctx context.Context, job *Job)
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewJobQueue creates a job queue that will run at most maxWorkers jobs at once.
//
// Workers are started ON DEMAND rather than all at construction. maxWorkers is a
// machine-overhead CEILING — the point past which fan-out threatens the host —
// and a ceiling should bound what may be allocated, not allocate it. Spawning
// the full pool up front made an idle queue pay for its own worst case: at the
// current default that is 500 goroutines parked on a condvar for an instance
// that may never run five jobs.
//
// It also had a sharp edge that only showed under load in CI, where several
// managers exist at once: thousands of goroutines created at startup shifted
// scheduling enough to lose a race that had always been latent. The eager pool
// did not cause that bug, but it made the machine ceiling expensive to raise —
// which is exactly the property a ceiling must not have.
func NewJobQueue(ctx context.Context, maxWorkers int, processFunc func(ctx context.Context, job *Job)) *JobQueue {
	if maxWorkers <= 0 {
		maxWorkers = 5
	}

	ctx, cancel := context.WithCancel(ctx)
	q := &JobQueue{
		jobs:        make([]*Job, 0, 64),
		maxWorkers:  maxWorkers,
		logger:      logger.New("jobqueue"),
		processFunc: processFunc,
		ctx:         ctx,
		cancel:      cancel,
	}
	q.cond = sync.NewCond(&q.mu)

	q.logger.Info().Int("max_workers", maxWorkers).Msg("Job queue started (workers start on demand)")
	return q
}

// Submit adds a job to the queue (never blocks)
func (q *JobQueue) Submit(job *Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return fmt.Errorf("job queue is closed")
	}

	q.jobs = append(q.jobs, job)

	// Grow the pool only when no parked worker can take this job and we are
	// under the ceiling. Workers persist once started, so the pool settles at
	// the high-water mark of real concurrent demand rather than at maxWorkers.
	if q.idle == 0 && q.running < q.maxWorkers {
		q.running++
		q.wg.Add(1)
		go q.worker(q.running)
	}

	q.cond.Signal() // Wake one waiting worker
	q.logger.Debug().
		Str("id", job.ID).
		Str("type", string(job.Type)).
		Int("queued", len(q.jobs)).
		Int("workers", q.running).
		Msg("Job submitted")
	return nil
}

// Workers returns the number of worker goroutines currently alive. Exposed for
// tests asserting that the pool grows on demand rather than up front.
func (q *JobQueue) Workers() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.running
}

// Len returns the current number of pending jobs
func (q *JobQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs)
}

// ActiveCount returns the number of jobs currently holding an active-download slot.
func (q *JobQueue) ActiveCount() int {
	return int(q.active.Load())
}

// Retry submits a job again after a delay without holding an active slot.
func (q *JobQueue) Retry(job *Job, delay time.Duration) {
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-q.ctx.Done():
			return
		case <-timer.C:
			if err := q.Submit(job); err != nil {
				q.logger.Debug().Err(err).Str("job_id", job.ID).Msg("Failed to retry job")
			}
		}
	}()
}

// Context returns the queue's lifecycle context. It is cancelled by Close, so
// long-running restore/retry loops can observe shutdown.
func (q *JobQueue) Context() context.Context {
	return q.ctx
}

// Close signals all workers to stop and waits for them to finish
func (q *JobQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cancel()
	q.cond.Broadcast() // Wake all waiting workers
	q.wg.Wait()
	q.logger.Info().Msg("Job queue stopped")
}

// worker is the main loop for a single worker goroutine
func (q *JobQueue) worker(id int) {
	defer q.wg.Done()

	for {
		job := q.pop()
		if job == nil {
			q.logger.Debug().Int("worker_id", id).Msg("Worker exiting")
			return
		}

		q.logger.Debug().
			Int("worker_id", id).
			Str("job_id", job.ID).
			Str("type", string(job.Type)).
			Int("queued", q.Len()).
			Msg("Processing job")

		q.active.Add(1)
		q.runJob(job)
		q.active.Add(-1)
	}
}

// runJob executes a single job, recovering from panics so that one bad job
// cannot permanently kill a worker goroutine. With a fixed worker pool, an
// unrecovered panic per worker silently drained the pool to zero — leaving
// every queued download stuck at 0% while the healthcheck still passed.
func (q *JobQueue) runJob(job *Job) {
	defer func() {
		if r := recover(); r != nil {
			q.logger.Error().
				Str("job_id", job.ID).
				Str("type", string(job.Type)).
				Interface("panic", r).
				Bytes("stack", debug.Stack()).
				Msg("Recovered from panic while processing job")
		}
	}()
	q.processFunc(q.ctx, job)
}

// pop removes and returns the next job, blocking if queue is empty.
// Returns nil if the queue is closed and empty.
func (q *JobQueue) pop() *Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	// idle is what Submit consults to decide whether growing the pool would
	// actually buy anything. It must bracket the Wait exactly: a worker is only
	// available to take new work while it is parked here.
	for len(q.jobs) == 0 && !q.closed {
		q.idle++
		q.cond.Wait()
		q.idle--
	}

	if q.closed {
		return nil
	}

	job := q.jobs[0]
	q.jobs = q.jobs[1:]
	return job
}

// DeleteJob removes a pending job by ID (before it's picked up by a worker).
// Returns true if the job was found and removed.
func (q *JobQueue) DeleteJob(jobID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	for i, job := range q.jobs {
		if job.ID == jobID {
			q.jobs = append(q.jobs[:i], q.jobs[i+1:]...)
			return true
		}
	}
	return false
}

// FindJob returns a pending job by ID without removing it
func (q *JobQueue) FindJob(jobID string) *Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, job := range q.jobs {
		if job.ID == jobID {
			return job
		}
	}
	return nil
}

// PendingCount returns the count of pending jobs, optionally filtered by type
func (q *JobQueue) PendingCount(jobType JobType) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	if jobType == "" {
		return len(q.jobs)
	}

	count := 0
	for _, job := range q.jobs {
		if job.Type == jobType {
			count++
		}
	}
	return count
}

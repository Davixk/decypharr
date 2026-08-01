package manager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// NewJobQueue builds a logger, which resolves the config singleton. Point it at
// a temp dir first or construction exits the process.
func jobQueueTestConfig(t *testing.T) {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
}

// The job-slot ceiling is a bound on what MAY be allocated, not an allocation.
// It previously started every worker at construction, so an idle queue paid for
// its own worst case up front — at the current default, 500 goroutines parked on
// a condvar for an instance that might never run five jobs.
//
// That made the ceiling expensive to raise, which is the one property a ceiling
// must not have: the whole point of setting it in the hundreds is that doing so
// should cost nothing until the work actually arrives.

func TestWorkersStartOnDemandNotAtConstruction(t *testing.T) {
	jobQueueTestConfig(t)
	q := NewJobQueue(context.Background(), 500, func(context.Context, *Job) {})
	defer q.Close()

	if got := q.Workers(); got != 0 {
		t.Fatalf("workers at construction = %d, want 0: a ceiling of 500 must not cost 500 goroutines "+
			"before a single job exists", got)
	}

	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(1)
	blocking := NewJobQueue(context.Background(), 500, func(context.Context, *Job) {
		started.Done()
		<-release
	})
	defer func() { close(release); blocking.Close() }()

	if err := blocking.Submit(&Job{ID: "one"}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	started.Wait()
	if got := blocking.Workers(); got != 1 {
		t.Fatalf("workers after one job = %d, want exactly 1: the pool must grow to demand, not to the cap", got)
	}
}

// The pool must still reach the concurrency it promises — growing on demand is
// only correct if demand actually grows it. This is the control against a fix
// that under-provisions and quietly serialises everything.
func TestPoolGrowsToConcurrentDemand(t *testing.T) {
	jobQueueTestConfig(t)
	const concurrent = 8

	release := make(chan struct{})
	var running sync.WaitGroup
	running.Add(concurrent)

	q := NewJobQueue(context.Background(), 500, func(context.Context, *Job) {
		running.Done()
		<-release
	})
	defer func() { close(release); q.Close() }()

	for i := 0; i < concurrent; i++ {
		if err := q.Submit(&Job{ID: string(rune('a' + i))}); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	done := make(chan struct{})
	go func() { running.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("only %d workers started for %d concurrent jobs: on-demand growth is under-provisioning "+
			"and jobs are serialising", q.Workers(), concurrent)
	}

	if got := q.Workers(); got != concurrent {
		t.Fatalf("workers = %d, want %d", got, concurrent)
	}
}

// The ceiling still binds: demand beyond it queues rather than spawning.
func TestPoolStopsGrowingAtTheCeiling(t *testing.T) {
	jobQueueTestConfig(t)
	const ceiling = 3

	release := make(chan struct{})
	var running sync.WaitGroup
	running.Add(ceiling)

	q := NewJobQueue(context.Background(), ceiling, func(context.Context, *Job) {
		running.Done()
		<-release
	})
	defer func() { close(release); q.Close() }()

	for i := 0; i < ceiling*4; i++ {
		if err := q.Submit(&Job{ID: string(rune('a' + i))}); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	running.Wait()

	if got := q.Workers(); got != ceiling {
		t.Fatalf("workers = %d, want the ceiling %d — excess demand must queue, not spawn", got, ceiling)
	}
}

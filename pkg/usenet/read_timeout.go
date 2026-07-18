package usenet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

var ErrReadTimeout = errors.New("usenet stream made no progress")

// progressDeadline owns one idle timer for an entire stream operation. The
// context is cancelled when no bytes have been delivered for timeout, so the
// same deadline covers cache waits, reader semaphore waits, provider-pool
// acquisition, NNTP reads, and retry backoff.
type progressDeadline struct {
	context.Context
	cancel   context.CancelCauseFunc
	timeout  time.Duration
	started  time.Time
	lastByte atomic.Int64
	progress chan struct{}
	finished chan struct{}
	close    sync.Once
}

func newProgressDeadline(parent context.Context, timeout time.Duration) *progressDeadline {
	if parent == nil {
		parent = context.Background()
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithCancelCause(parent)
	d := &progressDeadline{
		Context:  ctx,
		cancel:   cancel,
		timeout:  timeout,
		started:  time.Now(),
		progress: make(chan struct{}, 1),
		finished: make(chan struct{}),
	}
	go d.run()
	return d
}

func (d *progressDeadline) run() {
	defer close(d.finished)
	timer := time.NewTimer(d.timeout)
	defer timer.Stop()
	for {
		select {
		case <-d.Context.Done():
			return
		case <-timer.C:
			// Progress() records its timestamp before signalling this goroutine.
			// If the timer and signal become ready together, verify the atomic
			// timestamp so select ordering cannot cause a false timeout.
			idleFor := time.Since(d.started) - time.Duration(d.lastByte.Load())
			if idleFor < d.timeout {
				timer.Reset(d.timeout - idleFor)
				continue
			}
			d.cancel(fmt.Errorf("%w for %s", ErrReadTimeout, d.timeout))
			return
		case <-d.progress:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(d.timeout)
		}
	}
}

func (d *progressDeadline) Progress() {
	d.lastByte.Store(int64(time.Since(d.started)))
	select {
	case d.progress <- struct{}{}:
	default:
	}
}

func (d *progressDeadline) Close() {
	d.close.Do(func() {
		d.cancel(context.Canceled)
		<-d.finished
	})
}

func contextError(ctx context.Context) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return ctx.Err()
}

type progressWriter struct {
	io.Writer
	progress func()
}

func (w progressWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if n > 0 && w.progress != nil {
		w.progress()
	}
	return n, err
}

package usenet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/internal/utils"
)

var ErrReadTimeout = errors.New("usenet stream made no progress")

// parseTimeoutSetting resolves a duration knob that supports an explicit off
// switch. An empty value returns def. "off", "none", and any spelling that
// parses to zero ("0", "0s", ...) return 0, meaning disabled. Invalid or
// negative values return an error; callers keep def and warn.
func parseTimeoutSetting(raw string, def time.Duration) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return def, nil
	}
	switch strings.ToLower(trimmed) {
	case "off", "none":
		return 0, nil
	}
	d, err := utils.ParseDuration(trimmed)
	if err != nil {
		return def, err
	}
	if d < 0 {
		return def, fmt.Errorf("negative duration %q", raw)
	}
	return d, nil
}

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
		// Deadline disabled (read_timeout "0"/"off"/"none"): pure passthrough.
		// No watchdog goroutine, and the caller context is left unwrapped, so a
		// degraded-but-alive provider can stream arbitrarily slowly.
		return &progressDeadline{Context: parent}
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
	if d.progress == nil {
		return // deadline disabled
	}
	d.lastByte.Store(int64(time.Since(d.started)))
	select {
	case d.progress <- struct{}{}:
	default:
	}
}

func (d *progressDeadline) Close() {
	if d.cancel == nil {
		return // deadline disabled; nothing to cancel or join
	}
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

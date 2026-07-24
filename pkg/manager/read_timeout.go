package manager

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

// ErrDebridReadStalled is returned when a debrid HTTP stream delivers zero bytes
// to the client for longer than the configured idle deadline. It is a distinct
// sentinel so a stall is logged and classified as a definitive (non-transient)
// failure rather than a silent context cancellation.
var ErrDebridReadStalled = errors.New("debrid stream made no progress")

// defaultDebridReadTimeout is the fallback idle deadline used when the config
// value is empty (e.g. a lightweight Manager built in tests). It matches the
// "60s" default seeded by config.setDefaults.
const defaultDebridReadTimeout = 60 * time.Second

// parseDebridReadTimeout resolves the debrid_read_timeout knob exactly like the
// usenet read_timeout parser (internal/config + pkg/usenet/read_timeout.go): an
// empty value uses the default; "off", "none", and any spelling that parses to
// zero ("0", "0s", ...) mean disabled (0); negative/invalid values return an
// error and the caller keeps the default.
func parseDebridReadTimeout(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultDebridReadTimeout, nil
	}
	switch strings.ToLower(trimmed) {
	case "off", "none":
		return 0, nil
	}
	d, err := utils.ParseDuration(trimmed)
	if err != nil {
		return defaultDebridReadTimeout, err
	}
	if d < 0 {
		return defaultDebridReadTimeout, fmt.Errorf("negative duration %q", raw)
	}
	return d, nil
}

// debridReadTimeout resolves the effective idle deadline for a debrid stream.
// A parse error falls back to the default and is logged so a typo can never
// silently disable stall protection.
func (m *Manager) debridReadTimeout() time.Duration {
	raw := ""
	if m.config != nil {
		raw = m.config.DebridReadTimeout
	}
	timeout, err := parseDebridReadTimeout(raw)
	if err != nil {
		m.logger.Warn().Err(err).Str("debrid_read_timeout", raw).
			Msg("Invalid debrid_read_timeout; using default")
		return defaultDebridReadTimeout
	}
	return timeout
}

// streamProgressDeadline owns exactly one idle timer for a debrid stream copy.
// The context is cancelled when no bytes have been delivered to the client for
// timeout, so the same deadline covers the wait for the first byte (connection
// + response headers) and mid-stream idle. It mirrors pkg/usenet's
// progressDeadline but is self-contained in the manager package so the two
// stream paths stay decoupled.
type streamProgressDeadline struct {
	context.Context
	cancel   context.CancelCauseFunc
	timeout  time.Duration
	started  time.Time
	lastByte atomic.Int64
	progress chan struct{}
	finished chan struct{}
	close    sync.Once
}

// newStreamProgressDeadline builds an idle deadline around parent. When timeout
// <= 0 the deadline is disabled: no watchdog goroutine is started and parent is
// returned unwrapped, so a degraded-but-alive stream can trickle bytes forever
// (pure passthrough, matching usenet's disabled mode).
func newStreamProgressDeadline(parent context.Context, timeout time.Duration) *streamProgressDeadline {
	if parent == nil {
		parent = context.Background()
	}
	if timeout <= 0 {
		return &streamProgressDeadline{Context: parent}
	}
	ctx, cancel := context.WithCancelCause(parent)
	d := &streamProgressDeadline{
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

func (d *streamProgressDeadline) run() {
	defer close(d.finished)
	timer := time.NewTimer(d.timeout)
	defer timer.Stop()
	for {
		select {
		case <-d.Context.Done():
			return
		case <-timer.C:
			// Progress() records its timestamp before signalling this goroutine.
			// If the timer and a signal become ready together, re-check the atomic
			// timestamp so select ordering cannot cause a false timeout.
			idleFor := time.Since(d.started) - time.Duration(d.lastByte.Load())
			if idleFor < d.timeout {
				timer.Reset(d.timeout - idleFor)
				continue
			}
			d.cancel(fmt.Errorf("%w for %s", ErrDebridReadStalled, d.timeout))
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

// Progress records byte delivery and resets the idle timer. It is non-blocking:
// the timestamp is stored atomically and the wake-up is a buffered
// send-or-drop, so it never blocks or measurably slows the copy hot path and
// cannot deadlock against the watchdog goroutine.
func (d *streamProgressDeadline) Progress() {
	if d.progress == nil {
		return // deadline disabled
	}
	d.lastByte.Store(int64(time.Since(d.started)))
	select {
	case d.progress <- struct{}{}:
	default:
	}
}

// Close cancels the deadline and joins the watchdog goroutine. It is safe to
// call multiple times and is a no-op when the deadline is disabled.
func (d *streamProgressDeadline) Close() {
	if d.cancel == nil {
		return // deadline disabled; nothing to cancel or join
	}
	d.close.Do(func() {
		d.cancel(context.Canceled)
		<-d.finished
	})
}

// stallCause returns the ErrDebridReadStalled cause when THIS deadline fired,
// and nil otherwise (deadline disabled, still running, or the parent context
// was cancelled for another reason such as a client disconnect). Callers use it
// to distinguish an idle-timeout abort from an ordinary cancellation.
func (d *streamProgressDeadline) stallCause() error {
	if d.cancel == nil {
		return nil // deadline disabled
	}
	if cause := context.Cause(d.Context); errors.Is(cause, ErrDebridReadStalled) {
		return cause
	}
	return nil
}

// streamProgressWriter forwards writes to the underlying client writer and
// pulses the idle deadline after any successful delivery (n > 0). Bytes that
// never reach the client (a stalled read) therefore never reset the timer.
type streamProgressWriter struct {
	io.Writer
	progress func()
}

func (w streamProgressWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if n > 0 && w.progress != nil {
		w.progress()
	}
	return n, err
}

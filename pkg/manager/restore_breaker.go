package manager

import (
	"context"
	"time"

	"github.com/rs/zerolog"
)

// Restore circuit-breaker tuning. Vars so tests can shrink the timings.
var (
	// restoreBreakerThreshold is the number of CONSECUTIVE infrastructure-class
	// rebuild failures after which the restore loop pauses.
	restoreBreakerThreshold = 3
	// restoreBreakerBaseDelay is the first pause before a canary probe; each
	// failed probe doubles it up to restoreBreakerMaxDelay.
	restoreBreakerBaseDelay = 30 * time.Second
	restoreBreakerMaxDelay  = 5 * time.Minute
)

// restoreCircuitBreaker pauses the restore loop once consecutive
// infrastructure-class rebuild failures prove the NNTP substrate is down.
// Without it, a boot restore against a collapsed substrate hammers thousands
// of entries through failing connection acquisition — the exact amplification
// that turned one incident into ~1,794 poisoned entries. While paused, the
// loop probes health with a single canary STAT and resumes only when the
// canary succeeds. Any non-infrastructure outcome resets the counter.
type restoreCircuitBreaker struct {
	logger    zerolog.Logger
	threshold int
	baseDelay time.Duration
	maxDelay  time.Duration
	canary    func(ctx context.Context) error
	// sleep waits for the duration or ctx cancellation; injectable for tests.
	sleep func(ctx context.Context, d time.Duration) bool

	consecutive int
	delay       time.Duration
}

func newRestoreCircuitBreaker(logger zerolog.Logger, canary func(ctx context.Context) error) *restoreCircuitBreaker {
	return &restoreCircuitBreaker{
		logger:    logger,
		threshold: restoreBreakerThreshold,
		baseDelay: restoreBreakerBaseDelay,
		maxDelay:  restoreBreakerMaxDelay,
		canary:    canary,
		sleep:     sleepWithContext,
	}
}

// probeNNTPHealth is the restore circuit-breaker canary: a single STAT of a
// known segment through the usenet client (or a pool-saturation check when no
// segment is stored). A genuine 430 answer counts as healthy.
func (m *Manager) probeNNTPHealth(ctx context.Context) error {
	if m.usenet == nil {
		return nil
	}
	return m.usenet.CheckHealth(ctx)
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// recordSuccess resets the consecutive-failure chain. Any successful rebuild —
// or a genuine article verdict, which equally proves the substrate answered —
// qualifies.
func (b *restoreCircuitBreaker) recordSuccess() {
	b.consecutive = 0
	b.delay = 0
}

func (b *restoreCircuitBreaker) recordFailure() {
	b.consecutive++
}

func (b *restoreCircuitBreaker) tripped() bool {
	return b.consecutive >= b.threshold
}

// pauseUntilHealthy blocks while the breaker is tripped, backing off
// exponentially (base -> 2x -> 4x, capped) between single canary probes. It
// returns true once the substrate is healthy (or the breaker never tripped)
// and false when ctx is cancelled (shutdown).
func (b *restoreCircuitBreaker) pauseUntilHealthy(ctx context.Context) bool {
	if !b.tripped() {
		return ctx.Err() == nil
	}
	b.logger.Warn().
		Int("consecutive_failures", b.consecutive).
		Msgf("restore paused: NNTP substrate unhealthy (%d consecutive infrastructure failures)", b.consecutive)
	for {
		if b.delay <= 0 {
			b.delay = b.baseDelay
		} else {
			b.delay *= 2
			if b.delay > b.maxDelay {
				b.delay = b.maxDelay
			}
		}
		if !b.sleep(ctx, b.delay) {
			return false
		}
		var probeErr error
		if b.canary != nil {
			probeErr = b.canary(ctx)
		}
		if ctx.Err() != nil {
			return false
		}
		if probeErr == nil {
			b.logger.Info().Msg("restore resumed: NNTP canary probe succeeded")
			b.recordSuccess()
			return true
		}
		b.logger.Warn().
			Err(probeErr).
			Dur("waited", b.delay).
			Msg("restore still paused: NNTP canary probe failed")
	}
}

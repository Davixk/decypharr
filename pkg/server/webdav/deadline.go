package webdav

import (
	"fmt"
	"net/http"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// defaultMetadataReadTimeout is the fallback ceiling used when the config value
// is empty (e.g. a handler built directly in a test). It matches the "15s"
// default seeded by config.setDefaults.
const defaultMetadataReadTimeout = 15 * time.Second

// transientRetryAfter is the backoff hint attached to every 503 this handler
// emits. It matches the readiness middleware's existing hint so a client sees
// one consistent cadence for "decypharr is not ready to answer this yet",
// whatever the reason.
const transientRetryAfter = "5"

// metadataReadTimeout resolves the ceiling on a listing/HEAD wait, live per
// request so the knob applies without a restart. A parse error falls back to
// the default and is logged: a typo silently restoring an unbounded listing is
// the failure this ceiling exists to prevent.
func (h *Handler) metadataReadTimeout() time.Duration {
	raw := ""
	if cfg := config.Get(); cfg != nil {
		raw = cfg.MetadataReadTimeout
	}
	timeout, err := utils.ParseReadCeiling(raw, defaultMetadataReadTimeout)
	if err != nil {
		h.logger.Rate("metadata-read-timeout-parse").Warn().Err(err).
			Str("metadata_read_timeout", raw).
			Msg("Invalid metadata_read_timeout; using default")
		return defaultMetadataReadTimeout
	}
	return timeout
}

// awaitBounded runs work and returns its result, or reports false once limit
// has elapsed.
//
// WHY A GOROUTINE RATHER THAN A CONTEXT. The metadata preparers take no
// context, and neither do the provider calls beneath them — the debrid clients
// build their requests with http.NewRequest, so there is nothing for a deadline
// to cancel. A context here would compile and bound nothing. Releasing the
// HANDLER is the only mechanism that actually works, and it is the one the
// acceptance criterion asks for: a reader must get an answer, not a wedge.
//
// The abandoned goroutine is not lost work. It runs to completion and its
// result lands in the manager's own caches, so the retry the client makes after
// the 503 is usually served from warm state — the flap gets absorbed on the
// second attempt instead of failing twice. The channel is buffered so that
// goroutine can never block on a reader that has already given up, and it is
// the ONLY writer to the value it sends, so nothing is shared across the
// handover.
//
// A limit <= 0 disables the ceiling and runs work inline, which is both the
// documented "off" semantics and the exact pre-ceiling behaviour.
//
// The cost when enabled is one goroutine and one buffered channel per metadata
// request. That is deliberately paid unconditionally rather than behind a
// "does this need a backend?" fast path: deciding that here would duplicate
// knowledge the preparers own, and duplicated predicates in this codebase have
// drifted before.
func awaitBounded[T any](limit time.Duration, work func() T) (T, bool) {
	if limit <= 0 {
		return work(), true
	}
	done := make(chan T, 1)
	go func() { done <- work() }()

	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case result := <-done:
		return result, true
	case <-timer.C:
		var zero T
		return zero, false
	}
}

// preparedFile and preparedBatch carry a bounded preparation's results across
// the goroutine handover as a single value, so nothing is written through a
// shared variable that the timed-out handler may already be reading.
type preparedFile struct {
	entry *storage.Entry
	info  *manager.FileInfo
	err   error
}

type preparedBatch struct {
	infos  []manager.FileInfo
	errors []error
}

// writeBackendTimeout answers a metadata request whose backend outlived the
// ceiling.
//
// 503 + Retry-After, never 500 and never 410. 500 is the status this codebase
// already learned rclone retries blindly (see link.badEntryError), and 410 is a
// permanent content verdict — the one thing a timeout must never claim, since a
// slow backend says nothing at all about whether the content exists.
func (h *Handler) writeBackendTimeout(w http.ResponseWriter, what string, limit time.Duration) {
	err := customerror.NewBackendTimeoutError(fmt.Errorf("%s exceeded the %s metadata ceiling", what, limit))
	h.logger.Rate("metadata-timeout:"+what).Warn().Err(err).
		Dur("ceiling", limit).
		Msg("Metadata request exceeded its ceiling; answering 503 instead of blocking the reader")
	w.Header().Set("Retry-After", transientRetryAfter)
	http.Error(w, err.Error(), err.StatusCode())
}

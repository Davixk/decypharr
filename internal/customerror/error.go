package customerror

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
)

type Error struct {
	err            error
	silent         bool
	statusCode     int
	Code           string
	HeadersWritten bool // True if response headers were already written (can't send error status)
	retry          bool // True if the operation that caused the error is safe to retry
	permanent      bool // True if the error is permanent and should not be retried
}

func (e *Error) Error() string {
	return e.err.Error()
}

func (e *Error) Unwrap() error {
	return e.err
}

func (e *Error) Retryable() *Error {
	e.retry = true
	return e
}

func (e *Error) Permanent() *Error {
	e.permanent = true
	return e
}

func (e *Error) IsRetryable() bool {
	return e.retry && !e.permanent
}

func (e *Error) IsPermanent() bool {
	return e.permanent
}

func (e *Error) StatusCode() int {
	if e.statusCode == 0 {
		return http.StatusInternalServerError
	}
	return e.statusCode
}

func (e *Error) IsSilent() bool {
	if e.err == nil {
		return false
	}
	if e.silent {
		return true
	}
	return IsSilentError(e.err)
}

func NewError(err error, statusCode int, code string, silent bool, headersWritten bool) *Error {
	return &Error{
		err:            err,
		silent:         silent,
		statusCode:     statusCode,
		HeadersWritten: headersWritten,
	}
}

func NewSilentError(err error) *Error {
	return &Error{
		err:            err,
		silent:         true,
		statusCode:     http.StatusInternalServerError,
		HeadersWritten: false,
	}
}

func NewPermanentError(err error) *Error {
	e := &Error{
		err:            err,
		silent:         false,
		statusCode:     http.StatusInternalServerError,
		HeadersWritten: false,
	}
	return e.Permanent()
}

func FromError(err error) *Error {
	var customErr *Error
	if errors.As(err, &customErr) {
		return customErr
	}

	return &Error{
		err:            err,
		silent:         false,
		statusCode:     http.StatusInternalServerError,
		HeadersWritten: false,
	}
}

func IsSilentError(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, http.ErrHandlerTimeout) ||
		errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		if errors.Is(netErr.Err, syscall.EPIPE) || errors.Is(netErr.Err, syscall.ECONNRESET) {
			return true
		}
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "client disconnected") {
		return true
	}

	// Check for custom error type
	var customErr *Error
	if errors.As(err, &customErr) {
		return customErr.silent
	}

	return false
}

func NewArticleNotFoundError(err error) *Error {
	if err == nil {
		err = errors.New("article not found")
	}
	return (&Error{
		err:        err,
		statusCode: http.StatusGone,
		Code:       "usenet_article_missing",
	}).Permanent()
}

// IsContentPermanentlyGone reports whether err is a DEFINITIVE, durable
// statement that the bytes behind a resource no longer exist — as opposed to a
// failure of the machinery that was asked about them.
//
// This is the SINGLE predicate shared by the two sides that must never disagree:
//
//   - the SERVE path (WebDAV PROPFIND), which drops such a child from a
//     collection listing rather than advertising a resource every read of which
//     will fail; and
//   - the REPAIR probe, which classifies such a file as broken — a verdict that
//     is destructive-eligible under PRUNE.
//
// They drifted once, and the drift is exactly the production bug this exists to
// prevent: PROPFIND hid every child of an entry (410 Gone) while the probe
// recorded "could not reach a verdict" for the very same files, so an entry that
// served an EMPTY directory to every client sat forever in a non-actionable
// state. One predicate, two callers, no room to disagree again.
//
// Membership is deliberately narrow. Only errors that CARRY A CONTENT VERDICT
// qualify:
//
//   - usenet_article_missing — a 430/423 from the provider, or the durable
//     IsDeleted flag that only such a verdict can set.
//   - usenet_segment_missing — sampled segments definitively absent on every
//     configured provider.
//   - debrid_content_gone    — the hoster answered 404/410 for the content.
//   - any other PERMANENT error carrying HTTP 410 Gone.
//
// Everything else is excluded on purpose: authentication (401), permission,
// rate limiting (429), 5xx, timeouts, connection failures, "no connection could
// be acquired", cancellations, invalid-metadata errors, and a MISSING SEGMENT
// MAP (usenet.ErrNZBNotFound). Those describe a broken substrate or lost local
// bookkeeping — they say nothing whatsoever about whether the content is alive,
// and treating any of them as gone would condemn a library on an outage.
func IsContentPermanentlyGone(err error) bool {
	if err == nil {
		return false
	}
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	switch e.Code {
	case "usenet_article_missing", "usenet_segment_missing", "debrid_content_gone":
		return true
	}
	return e.permanent && e.statusCode == http.StatusGone
}

// NewContentGoneError is the debrid analog of NewArticleNotFoundError: the
// download link resolved but the upstream reports the content is definitively
// gone (HTTP 404/410). It carries a permanent 410 so the WebDAV layer maps it
// to 410 Gone before the first byte and the stream retry loop never masks it as
// a transient failure.
func NewContentGoneError(err error) *Error {
	if err == nil {
		err = errors.New("debrid content gone")
	}
	return (&Error{
		err:        err,
		statusCode: http.StatusGone,
		Code:       "debrid_content_gone",
	}).Permanent()
}

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
//   - debrid_content_takedown — the hoster answered a LEGAL removal (RealDebrid
//     code 35 / HTTP 451). Different cause from debrid_content_gone, identical
//     consequence for every consumer here: the bytes are not coming back.
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
	case "usenet_article_missing", "usenet_segment_missing", "debrid_content_gone", "debrid_content_takedown":
		return true
	}
	return e.permanent && e.statusCode == http.StatusGone
}

// NewBackendTimeoutError names decypharr's OWN ceiling firing: a backend did
// not answer within the time a reader is allowed to wait for it.
//
// It is deliberately the exact opposite of NewContentGoneError in every
// attribute that any downstream consumer reads:
//
//   - 503, not 410 or 500. 500 is what this codebase already learned rclone
//     retries blindly (see link.badEntryError), and 410 is a permanent verdict.
//     503 is the only honest answer: the machinery was too slow, the content
//     was never asked about.
//   - Retryable() and NOT Permanent(), so IsContentPermanentlyGone is false for
//     it. That is the load-bearing property: a timeout must never let a
//     collection listing hide a child, let the repair probe call a file broken,
//     or let anything downstream reach a destructive verdict. A backend that is
//     merely slow says NOTHING about whether the content exists.
//   - A fresh error, not a wrapped context.DeadlineExceeded — wrapping one
//     would make IsSilentError true and the timeout would stop being logged,
//     which is precisely the diagnostic an operator needs during a flap.
func NewBackendTimeoutError(err error) *Error {
	if err == nil {
		err = errors.New("backend did not respond in time")
	}
	return (&Error{
		err:        err,
		statusCode: http.StatusServiceUnavailable,
		Code:       "backend_timeout",
	}).Retryable()
}

// IsBackendTimeout reports whether err carries — anywhere in its chain,
// including inside an errors.Join tree — one of decypharr's OWN ceilings
// firing.
//
// It is the mirror image of IsContentPermanentlyGone and exists for the same
// reason: one predicate, so the sides that must not disagree cannot drift. A
// ceiling firing means WE stopped waiting. It is not a statement about the
// content, the provider's answer, or the entry's health, so no caller may
// convert it into a durable verdict — specifically, the repair cascade must not
// set Entry.Bad on it (that flag short-circuits every subsequent read, so a
// single provider flap would otherwise blank a library until something cleared
// it by hand).
//
// The chain walk matters: the re-insertion path joins the status error with its
// compensating provider cleanup, so the timeout is never the top-level error by
// the time a caller sees it.
func IsBackendTimeout(err error) bool {
	if err == nil {
		return false
	}
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	if e.Code == "backend_timeout" {
		return true
	}
	// errors.As stops at the FIRST *Error it finds, which in a join tree may be
	// the cleanup error rather than the timeout. Keep walking by hand so the
	// order of errors.Join's arguments cannot decide the answer.
	switch node := err.(type) {
	case interface{ Unwrap() []error }:
		for _, branch := range node.Unwrap() {
			if IsBackendTimeout(branch) {
				return true
			}
		}
	case interface{ Unwrap() error }:
		return IsBackendTimeout(node.Unwrap())
	}
	return false
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

// NewContentTakedownError names a provider refusing content BECAUSE IT HAS BEEN
// LEGALLY REMOVED — RealDebrid error code 35 (`infringing_file`, served as
// HTTP 451) and any future equivalent.
//
// WHY THIS IS NOT HosterUnavailableError. RealDebrid's codes 19 (hoster
// temporarily unavailable), 24 (file unavailable) and 35 (infringing file) were
// all mapped onto the single HosterUnavailableError, so "the hoster is having a
// bad day" and "this release is legally dead" arrived at every caller as the
// same value. That lumping is wrong in BOTH directions and both directions were
// measured on one production library: a provider flap could drive an entry all
// the way to a permanent Bad verdict, while 5 of 5 sampled entries that HAD
// been condemned were in fact code 35 — genuine takedowns that then stayed
// invisible, re-refusing reads roughly 695 times a day because nothing
// downstream could tell they were dead rather than merely unlucky.
//
// Every attribute here is chosen so the two classes can never re-merge:
//
//   - 451 Unavailable For Legal Reasons, which is literally what the provider
//     answered. An operator reading a log or an HTTP status learns the cause
//     without going back to the provider's error table.
//   - Permanent(), so IsRetriableError refuses it and no retry loop can mask a
//     takedown as a flap. This is the exact opposite of NewBackendTimeoutError,
//     which is Retryable() precisely because it says nothing about the content.
//   - A member of IsContentPermanentlyGone, so the SERVE path and the REPAIR
//     probe reach the same verdict about it. See that function for why those two
//     are not allowed to drift.
func NewContentTakedownError(err error) *Error {
	if err == nil {
		err = errors.New("debrid content removed for legal reasons")
	}
	return (&Error{
		err:        err,
		statusCode: http.StatusUnavailableForLegalReasons,
		Code:       "debrid_content_takedown",
	}).Permanent()
}

// IsContentTakedown reports whether err carries — anywhere in its chain,
// including inside an errors.Join tree — a provider's confirmed LEGAL removal
// of the content.
//
// It is narrower than IsContentPermanentlyGone on purpose. Callers that must
// only ever act on an UNAMBIGUOUS takedown (the re-insertion cascade, which
// re-submits the same magnet to the provider that just refused it, and the
// condemn-and-prune decision that follows) ask this; callers that only need
// "these bytes are not coming back" ask IsContentPermanentlyGone.
//
// The chain walk matters for the same reason it does in IsBackendTimeout: the
// re-insertion path joins a provider error with its compensating cleanup error,
// so errors.As can land on the cleanup branch and answer for the wrong one.
func IsContentTakedown(err error) bool {
	if err == nil {
		return false
	}
	var e *Error
	if errors.As(err, &e) && e.Code == "debrid_content_takedown" {
		return true
	}
	switch node := err.(type) {
	case interface{ Unwrap() []error }:
		for _, branch := range node.Unwrap() {
			if IsContentTakedown(branch) {
				return true
			}
		}
	case interface{ Unwrap() error }:
		return IsContentTakedown(node.Unwrap())
	}
	return false
}

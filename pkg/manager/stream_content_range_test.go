package manager

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// 🔴 A STREAM BOUND MUST REJECT INVALID DATA, NOT ONLY ABSENT DATA.
//
// Measured: an upstream looping `Content-Range: bytes 125-…/125` — a range whose
// start equals its own total, so it can never contain a byte. Every reply
// arrived promptly, so the idle deadline (progress-based, reset on every
// delivered byte) never fired and the client retried forever. Eight ffprobes in
// D-state for six hours on one file; only a restart cleared them.
//
// Absence was already covered by the read timeout. Nonsense arriving ON SCHEDULE
// was covered by nothing: parseContentRange correctly identified it as invalid
// and the caller discarded the verdict, warned, and served from the body anyway.
func newRangeFixture(t *testing.T) *Manager {
	t.Helper()
	m := newActionLifecycleFixture(t, 1)
	m.logger = zerolog.Nop()
	return m
}

func partialResponse(contentRange string) *http.Response {
	return &http.Response{
		StatusCode:    http.StatusPartialContent,
		Header:        http.Header{"Content-Range": []string{contentRange}},
		ContentLength: -1,
	}
}

func rangePlan(start, end int64) httpStreamPlan {
	return httpStreamPlan{
		upstreamRequested: true,
		upstreamStart:     start,
		upstreamEnd:       end,
		logicalStart:      start,
		logicalEnd:        end,
	}
}

func TestUnusableContentRangeIsRejected(t *testing.T) {
	m := newRangeFixture(t)
	plan := rangePlan(0, 1023)

	cases := []struct {
		name  string
		value string
		why   string
	}{
		{
			"start equals total", "bytes 125-125/125",
			"the observed loop: a window starting at the file's own size can never contain a byte",
		},
		{
			"end beyond total", "bytes 0-999/125",
			"a window larger than the file it claims to come from",
		},
		{
			"end before start", "bytes 900-100/1000",
			"a window with negative length",
		},
		{
			"not a byte range at all", "items 0-10/20",
			"a unit decypharr never requested and cannot interpret",
		},
		{
			"garbage", "totally-not-a-range",
			"unparseable is unusable",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := m.resolveHTTPStreamResponse(partialResponse(tc.value), plan, 1024, "rd", "movie.mkv")
			if err == nil {
				t.Fatalf("%q was accepted — %s. Serving from it means guessing, and an upstream that keeps "+
					"answering promptly with nonsense never trips a progress-based deadline", tc.value, tc.why)
			}
			var streamErr StreamError
			if !errors.As(err, &streamErr) {
				t.Fatalf("error is not a StreamError, so the retry chain cannot classify it: %v", err)
			}
			// RETRYABLE: a statement about one RESPONSE, not about the content. A
			// header mangled by a proxy should cost a retry, not a verdict.
			if !streamErr.Retryable {
				t.Fatal("an unusable Content-Range was marked non-retryable; a one-off mangled header would " +
					"then condemn a perfectly good file")
			}
			if !strings.Contains(err.Error(), "Content-Range") {
				t.Fatalf("the error does not name what was wrong: %v", err)
			}
		})
	}
}

// 🛑 THE CONTROL, AND IT IS THE HALF THAT KEEPS THIS SAFE. A range that is
// COHERENT but simply not the one requested must still be served: upstreams are
// loose about honouring Range, the window they describe is internally
// consistent, and the copy loop enforces the exact logical length regardless.
// Rejecting these would turn a working-if-sloppy CDN into an outage.
func TestCoherentButDifferentContentRangeStillServes(t *testing.T) {
	m := newRangeFixture(t)
	plan := rangePlan(0, 1023)

	for _, value := range []string{
		"bytes 0-2047/8192",  // wider than asked
		"bytes 64-1023/8192", // shifted start
		"bytes 0-1023/*",     // unknown total, still a real window
	} {
		if _, err := m.resolveHTTPStreamResponse(partialResponse(value), plan, 1024, "rd", "movie.mkv"); err != nil {
			t.Fatalf("%q was rejected: %v. It describes a real window — different from the request is not the "+
				"same as impossible, and the copy loop already bounds the length", value, err)
		}
	}
}

// And the exact range still serves, which is the case that must never get
// clever.
func TestExactContentRangeServes(t *testing.T) {
	m := newRangeFixture(t)
	if _, err := m.resolveHTTPStreamResponse(partialResponse("bytes 0-1023/8192"), rangePlan(0, 1023), 1024, "rd", "movie.mkv"); err != nil {
		t.Fatalf("the exactly-correct Content-Range was rejected: %v", err)
	}
}

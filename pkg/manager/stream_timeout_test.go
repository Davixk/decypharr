package manager

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// stalledReader blocks until its context is cancelled, then returns the context
// error. It models a dead upstream whose resp.Body.Read only unblocks when the
// request context is cancelled.
type stalledReader struct{ ctx context.Context }

func (r stalledReader) Read(_ []byte) (int, error) {
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

// pacedReader delivers one chunk of data per interval, aborting on ctx.Done. It
// models a slow-but-alive upstream that keeps trickling bytes.
type pacedReader struct {
	ctx       context.Context
	interval  time.Duration
	chunk     []byte
	remaining int
}

func (r *pacedReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	timer := time.NewTimer(r.interval)
	defer timer.Stop()
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	case <-timer.C:
	}
	r.remaining--
	return copy(p, r.chunk), nil
}

// --- Helper-level tests: streamProgressDeadline + streamProgressWriter ---

// A reader that never delivers must trip the deadline within ~timeout, and the
// stall must be attributable to ErrDebridReadStalled.
func TestStreamProgressDeadlineStallFires(t *testing.T) {
	const timeout = 100 * time.Millisecond
	deadline := newStreamProgressDeadline(context.Background(), timeout)
	defer deadline.Close()

	pw := streamProgressWriter{Writer: io.Discard, progress: deadline.Progress}
	start := time.Now()
	_, err := io.CopyBuffer(pw, stalledReader{ctx: deadline.Context}, make([]byte, 4096))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected the copy to fail once the deadline fired")
	}
	stall := deadline.stallCause()
	if stall == nil || !errors.Is(stall, ErrDebridReadStalled) {
		t.Fatalf("stallCause = %v, want ErrDebridReadStalled", stall)
	}
	if elapsed > timeout*5 {
		t.Fatalf("stall aborted after %s, want ~%s", elapsed, timeout)
	}
}

// THE CRITICAL SAFETY TEST: a stream that keeps delivering a small chunk every
// timeout/3 for several multiples of the timeout must complete and must never
// trip the deadline. A slow-but-progressing read must not be killed.
func TestStreamProgressDeadlineSlowButAliveNeverFires(t *testing.T) {
	const timeout = 150 * time.Millisecond
	deadline := newStreamProgressDeadline(context.Background(), timeout)
	defer deadline.Close()

	const chunks = 9 // ~3x the timeout at timeout/3 spacing
	reader := &pacedReader{
		ctx:       deadline.Context,
		interval:  timeout / 3,
		chunk:     []byte("hello"),
		remaining: chunks,
	}
	var buf bytes.Buffer
	pw := streamProgressWriter{Writer: &buf, progress: deadline.Progress}
	n, err := io.CopyBuffer(pw, reader, make([]byte, 4096))
	if err != nil {
		t.Fatalf("slow-but-alive copy failed: %v (stall=%v)", err, deadline.stallCause())
	}
	if deadline.stallCause() != nil {
		t.Fatalf("deadline tripped on a healthy slow stream: %v", deadline.stallCause())
	}
	if want := int64(chunks * len("hello")); n != want {
		t.Fatalf("copied %d bytes, want %d", n, want)
	}
}

// A normal fast copy completes without error and, after Close, the single
// watchdog goroutine has exited (no leak, timer stopped).
func TestStreamProgressDeadlineHealthyCompletesAndCleansUp(t *testing.T) {
	const timeout = 100 * time.Millisecond
	deadline := newStreamProgressDeadline(context.Background(), timeout)

	src := bytes.NewReader(bytes.Repeat([]byte("x"), 64*1024))
	var buf bytes.Buffer
	pw := streamProgressWriter{Writer: &buf, progress: deadline.Progress}
	if _, err := io.CopyBuffer(pw, src, make([]byte, 4096)); err != nil {
		t.Fatalf("healthy copy failed: %v", err)
	}
	if deadline.stallCause() != nil {
		t.Fatalf("healthy copy tripped the deadline: %v", deadline.stallCause())
	}

	deadline.Close()
	select {
	case <-deadline.finished:
		// good: the watchdog goroutine exited and stopped its timer
	default:
		t.Fatal("watchdog goroutine still running after Close")
	}
	// Close is idempotent.
	deadline.Close()
}

// A disabled deadline ("off"/0) allocates no watchdog, passes the parent context
// through unwrapped, and never cancels a long slow copy.
func TestStreamProgressDeadlineDisabledNeverCancels(t *testing.T) {
	parent := context.Background()
	deadline := newStreamProgressDeadline(parent, 0)
	defer deadline.Close()

	if deadline.cancel != nil || deadline.progress != nil || deadline.finished != nil {
		t.Fatal("disabled deadline must not allocate a watchdog goroutine or channels")
	}
	if deadline.Context != parent {
		t.Fatal("disabled deadline must pass the parent context through unwrapped")
	}

	reader := &pacedReader{ctx: deadline.Context, interval: 20 * time.Millisecond, chunk: []byte("data"), remaining: 10}
	var buf bytes.Buffer
	pw := streamProgressWriter{Writer: &buf, progress: deadline.Progress}
	if _, err := io.CopyBuffer(pw, reader, make([]byte, 4096)); err != nil {
		t.Fatalf("disabled copy failed: %v", err)
	}
	if deadline.stallCause() != nil {
		t.Fatal("disabled deadline reported a stall")
	}
}

// parseDebridReadTimeout must mirror the usenet read_timeout semantics.
func TestParseDebridReadTimeout(t *testing.T) {
	cases := []struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{"", defaultDebridReadTimeout, false},
		{"60s", 60 * time.Second, false},
		{"100ms", 100 * time.Millisecond, false},
		{"off", 0, false},
		{"none", 0, false},
		{"OFF", 0, false},
		{"0", 0, false},
		{"0s", 0, false},
		{"-5s", defaultDebridReadTimeout, true},
		{"garbage", defaultDebridReadTimeout, true},
	}
	for _, tc := range cases {
		got, err := parseDebridReadTimeout(tc.raw)
		if (err != nil) != tc.wantErr {
			t.Fatalf("parseDebridReadTimeout(%q) err = %v, wantErr %v", tc.raw, err, tc.wantErr)
		}
		if got != tc.want {
			t.Fatalf("parseDebridReadTimeout(%q) = %s, want %s", tc.raw, got, tc.want)
		}
	}
}

// --- Integration tests: streamHTTPURL end-to-end over a real HTTP server ---

// A stalled upstream (headers, then zero body) is aborted with the distinct
// stall sentinel within roughly the configured timeout.
func TestStreamHTTPURLStallReturnsSentinel(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "10")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Deliver zero body bytes, then hold the connection open until the
		// client's request context is cancelled (deadline fire) or the test ends.
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(server.Close)

	m := &Manager{streamClient: server.Client(), config: &config.Config{DebridReadTimeout: "150ms"}}
	file := &storage.File{Name: "video.mkv", Size: 10}
	var body bytes.Buffer
	start := time.Now()
	err := m.streamHTTPURL(context.Background(), server.URL, "stall-provider", file, file.Name, 0, 9, false, &body, nil)
	elapsed := time.Since(start)

	if err == nil || !errors.Is(err, ErrDebridReadStalled) {
		t.Fatalf("stream error = %v, want ErrDebridReadStalled", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("stall abort took %s, want ~150ms", elapsed)
	}
}

// A slow-but-progressing upstream (one byte every 40ms, whole transfer far
// longer than the 200ms idle deadline) must complete intact. Integration-level
// proof that a healthy slow read on the hot path is never killed.
func TestStreamHTTPURLSlowButAliveCompletes(t *testing.T) {
	source := []byte(strings.Repeat("A", 20))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(source)))
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < len(source); i++ {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(40 * time.Millisecond):
			}
			_, _ = w.Write(source[i : i+1])
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	t.Cleanup(server.Close)

	m := &Manager{streamClient: server.Client(), config: &config.Config{DebridReadTimeout: "200ms"}}
	file := &storage.File{Name: "video.mkv", Size: int64(len(source))}
	var body bytes.Buffer
	err := m.streamHTTPURL(context.Background(), server.URL, "slow-provider", file, file.Name, 0, int64(len(source))-1, false, &body, nil)
	if err != nil {
		t.Fatalf("slow-but-alive stream failed: %v (want success)", err)
	}
	if body.String() != string(source) {
		t.Fatalf("body = %q, want %q", body.String(), source)
	}
}

// With the deadline disabled ("off"), an upstream that idles well past any short
// timeout before delivering must still complete (pure passthrough).
func TestStreamHTTPURLDisabledTimeoutToleratesInitialStall(t *testing.T) {
	source := []byte("0123456789")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(source)))
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(300 * time.Millisecond):
		}
		_, _ = w.Write(source)
	}))
	t.Cleanup(server.Close)

	m := &Manager{streamClient: server.Client(), config: &config.Config{DebridReadTimeout: "off"}}
	file := &storage.File{Name: "video.mkv", Size: int64(len(source))}
	var body bytes.Buffer
	if err := m.streamHTTPURL(context.Background(), server.URL, "disabled-provider", file, file.Name, 0, int64(len(source))-1, false, &body, nil); err != nil {
		t.Fatalf("disabled-timeout stream failed: %v", err)
	}
	if body.String() != string(source) {
		t.Fatalf("body = %q, want %q", body.String(), source)
	}
}

// PART A: a definitive dead status (410 Gone) surfaces as a permanent 410 Gone
// customerror before the first byte and is not retried. 404 is intentionally
// excluded — see TestStreamHTTPURLAmbiguousStatusIsNotContentGone.
func TestStreamHTTPURLContentGoneReturns410(t *testing.T) {
	for _, status := range []int{http.StatusGone} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			t.Cleanup(server.Close)

			m := &Manager{streamClient: server.Client(), config: &config.Config{}}
			file := &storage.File{Name: "video.mkv", Size: 10}
			var body bytes.Buffer
			readyCalled := false
			err := m.streamHTTPURL(context.Background(), server.URL, "gone-provider", file, file.Name, 0, 9, true, &body, func(*StreamMetadata) error {
				readyCalled = true
				return nil
			})
			var customErr *customerror.Error
			if !errors.As(err, &customErr) {
				t.Fatalf("error = %v, want a *customerror.Error", err)
			}
			if customErr.StatusCode() != http.StatusGone {
				t.Fatalf("status = %d, want 410", customErr.StatusCode())
			}
			if !customErr.IsPermanent() {
				t.Fatal("content-gone error must be permanent (non-retryable)")
			}
			if customErr.Code != "debrid_content_gone" {
				t.Fatalf("code = %q, want debrid_content_gone", customErr.Code)
			}
			if readyCalled {
				t.Fatal("ready callback ran for a gone response")
			}
			if body.Len() != 0 {
				t.Fatalf("wrote %d bytes for a gone response", body.Len())
			}
		})
	}
}

// PART A guard: 403 (forbidden/expired) and 404 (not found / expired-refetchable
// link) and other non-2xx statuses keep their existing behavior and must NOT be
// reclassified as a permanent content-gone. Only 410 is a dead verdict — a 404
// can be a merely-expired but still-live link, so treating it as dead would
// violate the read-deadline safety bar.
func TestStreamHTTPURLAmbiguousStatusIsNotContentGone(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			t.Cleanup(server.Close)

			m := &Manager{streamClient: server.Client(), config: &config.Config{}}
			file := &storage.File{Name: "video.mkv", Size: 10}
			var body bytes.Buffer
			err := m.streamHTTPURL(context.Background(), server.URL, "ambiguous-provider", file, file.Name, 0, 9, true, &body, nil)
			if err == nil {
				t.Fatalf("expected an error for a %d response", status)
			}
			var customErr *customerror.Error
			if errors.As(err, &customErr) && customErr.Code == "debrid_content_gone" {
				t.Fatalf("%d must NOT be classified as content-gone (got %v)", status, customErr)
			}
			if !strings.Contains(err.Error(), "unexpected HTTP status") {
				t.Fatalf("%d error = %v, want the unchanged unexpected-status error", status, err)
			}
		})
	}
}

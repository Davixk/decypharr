package link

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// stallingClient models the shape of the production wedge: a provider call that
// does not return, cannot be cancelled (the debrid clients build their requests
// with http.NewRequest, so no caller context ever reaches them), and eventually
// completes with a usable link once the provider recovers.
type stallingClient struct {
	debrid.Client
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
	link    debridTypes.DownloadLink
}

func (c *stallingClient) GetDownloadLink(string, *debridTypes.File) (debridTypes.DownloadLink, error) {
	c.calls.Add(1)
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.release
	return c.link, nil
}

// ceilingHarness wires a Service to a stalled provider and a live validation
// endpoint, so a released resolution can still finish successfully — the
// "absorb" half needs a real success to be observable.
type ceilingHarness struct {
	svc     *Service
	client  *stallingClient
	ceiling atomic.Int64
}

func (h *ceilingHarness) setCeiling(d time.Duration) { h.ceiling.Store(int64(d)) }

func newCeilingHarness(t *testing.T, ceiling time.Duration) *ceilingHarness {
	t.Helper()
	config.SetConfigPath(t.TempDir())

	// Answers the HEAD that validateLink makes, so a resolution that gets past
	// the stalled provider genuinely succeeds instead of failing on validation.
	validator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	client := &stallingClient{
		entered: make(chan struct{}, 64),
		release: make(chan struct{}),
		link: debridTypes.DownloadLink{
			Debrid:       "provider",
			Filename:     "Movie.mkv",
			Link:         "https://example.invalid/id",
			DownloadLink: validator.URL,
			Generated:    time.Now(),
			ExpiresAt:    time.Now().Add(time.Hour),
		},
	}

	harness := &ceilingHarness{client: client}
	harness.setCeiling(ceiling)

	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("provider", client)
	harness.svc = New(clients, nil, nil, nil, nil, nil, validator.Client(), 0,
		func() time.Duration { return time.Duration(harness.ceiling.Load()) }, zerolog.Nop())

	// LIFO: the release runs first so the detached resolution is never left
	// blocked on a server that has already gone away.
	t.Cleanup(validator.Close)
	t.Cleanup(func() {
		select {
		case <-client.release:
		default:
			close(client.release)
		}
	})
	return harness
}

// TestGetLinkReleasesCallerWhenProviderStalls is the whole point of the change.
//
// WITHOUT the ceiling this call never returns until the provider does — the
// singleflight wait is context-blind and the provider call carries no context —
// so the test blocks until its own guard fires and fails. That is the
// production wedge in miniature: a WebDAV GET parked in the handler with a FUSE
// reader in uninterruptible sleep behind it.
func TestGetLinkReleasesCallerWhenProviderStalls(t *testing.T) {
	entry := linkLifecycleEntry(strings.Repeat("1", 40), "provider", "id-1")
	h := newCeilingHarness(t, 80*time.Millisecond)

	done := make(chan error, 1)
	go func() {
		_, err := h.svc.GetLink(context.Background(), entry, "Movie.mkv")
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a stalled provider resolved a link")
		}
		var custom *customerror.Error
		if !errors.As(err, &custom) {
			t.Fatalf("ceiling error = %v (%T); the WebDAV layer needs a typed error or it emits a generic 500", err, err)
		}
		if custom.StatusCode() != http.StatusServiceUnavailable {
			t.Fatalf("ceiling status = %d, want 503", custom.StatusCode())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("GetLink never returned while the provider stalled; the caller is still unbounded")
	}
}

// TestCeilingErrorIsTransientAndNeverCondemns pins the safety bar. A ceiling
// firing says the machinery was slow; it says NOTHING about the content. If it
// ever became a permanent/410-shaped error it would satisfy
// IsContentPermanentlyGone, and from there PROPFIND would hide the child and
// the repair probe would call the file broken — a provider hiccup promoted to a
// destructive verdict on a healthy library.
func TestCeilingErrorIsTransientAndNeverCondemns(t *testing.T) {
	entry := linkLifecycleEntry(strings.Repeat("2", 40), "provider", "id-2")
	h := newCeilingHarness(t, 50*time.Millisecond)

	_, err := h.svc.GetLink(context.Background(), entry, "Movie.mkv")
	if err == nil {
		t.Fatal("a stalled provider resolved a link")
	}
	if customerror.IsContentPermanentlyGone(err) {
		t.Fatalf("a resolve ceiling produced a CONTENT verdict: %v", err)
	}
	var custom *customerror.Error
	if !errors.As(err, &custom) || custom.IsPermanent() || !custom.IsRetryable() {
		t.Fatalf("ceiling error must be transient and retryable, got %#v", custom)
	}
	if entry.Bad {
		t.Fatal("a resolve ceiling permanently condemned the entry")
	}
	if customerror.IsSilentError(err) {
		t.Fatal("ceiling error is silent; the one log line an operator needs during a flap would be suppressed")
	}
}

// TestReleasedResolutionStillDeliversToTheNextReader covers the ABSORB half.
// The released resolution is deliberately NOT cancelled, so when the provider
// recovers the very next reader is served from that same in-flight work — one
// provider call total, not two. Cancelling the abandoned resolution would throw
// the result away and guarantee the retry pays the whole cost again.
func TestReleasedResolutionStillDeliversToTheNextReader(t *testing.T) {
	entry := linkLifecycleEntry(strings.Repeat("3", 40), "provider", "id-3")
	h := newCeilingHarness(t, 50*time.Millisecond)

	if _, err := h.svc.GetLink(context.Background(), entry, "Movie.mkv"); err == nil {
		t.Fatal("a stalled provider resolved a link")
	}
	select {
	case <-h.client.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the provider was never called")
	}

	// The client retries after the 503, this time willing to wait.
	h.setCeiling(10 * time.Second)
	second := make(chan error, 1)
	go func() {
		_, err := h.svc.GetLink(context.Background(), entry, "Movie.mkv")
		second <- err
	}()
	time.Sleep(150 * time.Millisecond) // let the retry join the in-flight work

	close(h.client.release) // provider recovers

	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("the retry after a released resolution failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the retry never returned")
	}
	if got := h.client.calls.Load(); got != 1 {
		t.Fatalf("provider was called %d times, want 1; the released resolution was discarded rather than reused", got)
	}
}

// TestConcurrentReadersShareOneStalledResolution proves the release does not
// amplify load. Readers that pile up against a struggling provider must JOIN
// the in-flight resolution, not each start their own — otherwise the fix would
// convert one stuck reader into a request storm aimed at the backend that is
// already failing.
func TestConcurrentReadersShareOneStalledResolution(t *testing.T) {
	entry := linkLifecycleEntry(strings.Repeat("4", 40), "provider", "id-4")
	h := newCeilingHarness(t, 100*time.Millisecond)

	errs := make(chan error, 8)
	go func() {
		_, err := h.svc.GetLink(context.Background(), entry, "Movie.mkv")
		errs <- err
	}()
	// Only start the rest once the leader is demonstrably inside the provider
	// call, so "they joined" is a fact and not a scheduling accident.
	select {
	case <-h.client.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the provider was never called")
	}
	for range 7 {
		go func() {
			_, err := h.svc.GetLink(context.Background(), entry, "Movie.mkv")
			errs <- err
		}()
	}

	for range 8 {
		select {
		case err := <-errs:
			if err == nil {
				t.Fatal("a stalled provider resolved a link")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a reader never returned while the provider stalled")
		}
	}
	if got := h.client.calls.Load(); got != 1 {
		t.Fatalf("8 readers produced %d provider calls, want exactly 1", got)
	}
}

// TestDisabledCeilingRestoresUnboundedWait pins the escape hatch: "off"/"0"
// must give back exactly the historical behaviour, because an operator who
// suspects the ceiling itself has to be able to remove it without a code change.
func TestDisabledCeilingRestoresUnboundedWait(t *testing.T) {
	entry := linkLifecycleEntry(strings.Repeat("5", 40), "provider", "id-5")
	h := newCeilingHarness(t, 0)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = h.svc.GetLink(context.Background(), entry, "Movie.mkv")
	}()

	select {
	case <-done:
		t.Fatal("a disabled ceiling still released the caller")
	case <-time.After(300 * time.Millisecond):
	}
	close(h.client.release)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("GetLink never returned after the provider recovered")
	}
}

// TestGetLinkHonoursCallerCancellation covers the other release path: a reader
// that hangs up gets its own error immediately instead of waiting out the
// ceiling it no longer cares about.
func TestGetLinkHonoursCallerCancellation(t *testing.T) {
	entry := linkLifecycleEntry(strings.Repeat("6", 40), "provider", "id-6")
	h := newCeilingHarness(t, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := h.svc.GetLink(ctx, entry, "Movie.mkv")
		done <- err
	}()
	select {
	case <-h.client.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the provider was never called")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled reader got %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a cancelled reader was not released")
	}
}

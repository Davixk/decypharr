package webdav

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// stallingPreparer models the shape of a wedged metadata backend: a preparation
// that does not return and carries no context anything could cancel. For Usenet
// entries this is a live per-file article/segment preparation against the NNTP
// providers, which is the real call with no deadline behind a listing.
type stallingPreparer struct {
	release chan struct{}
	entered chan struct{}
}

func newStallingPreparer() *stallingPreparer {
	return &stallingPreparer{
		release: make(chan struct{}),
		entered: make(chan struct{}, 8),
	}
}

func (p *stallingPreparer) stall() {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	<-p.release
}

func (p *stallingPreparer) PrepareFileInfo(entry *storage.Entry, info *manager.FileInfo) (*storage.Entry, *manager.FileInfo, error) {
	p.stall()
	return entry, info, nil
}

func (p *stallingPreparer) PrepareFileInfos(infos []manager.FileInfo) ([]manager.FileInfo, []error) {
	p.stall()
	return infos, make([]error, len(infos))
}

// TestPropfindChildrenIsReleasedByTheMetadataCeiling is the listing half of the
// acceptance criterion.
//
// A listing used to have no deadline of ANY kind — only byte-streams did — so a
// stalled preparation had nothing to trip and the handler waited on it
// indefinitely. WITHOUT the ceiling this test blocks in ServeHTTP until its own
// guard fires. That is a wedged `ls`, a wedged *arr scan and a wedged Plex
// refresh, with no error any of them can act on.
func TestPropfindChildrenIsReleasedByTheMetadataCeiling(t *testing.T) {
	router, _, preparer := newStalledMetadataRouter(t, "60ms")
	t.Cleanup(func() { close(preparer.release) })

	response := servePropfindBounded(t, router, "/"+manager.EntryAllFolder+"/"+populatedEntry+"/", "1")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("stalled listing = %d, want 503\nbody: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Retry-After"); got == "" {
		t.Fatal("stalled listing carried no Retry-After; the client has no backoff hint and hot-loops")
	}
}

// A stalled HEAD must be released the same way. A media player probing a file
// is a metadata request, not a transfer, and must never be the thing that
// wedges on a backend.
func TestHeadIsReleasedByTheMetadataCeiling(t *testing.T) {
	router, _, preparer := newStalledMetadataRouter(t, "60ms")
	t.Cleanup(func() { close(preparer.release) })

	request := httptest.NewRequest(http.MethodHead, "/"+manager.EntryAllFolder+"/"+populatedEntry+"/movie.mkv", nil)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		router.ServeHTTP(response, request)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("HEAD never returned while the preparer stalled; the reader is still unbounded")
	}
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("stalled HEAD = %d, want 503\nbody: %s", response.Code, response.Body.String())
	}
}

// The ceiling must not disturb a healthy listing: a preparer that answers
// promptly still produces the same 207 it always did. Without this the previous
// two tests could be satisfied by a handler that simply answered 503 always.
func TestHealthyListingIsUnaffectedByTheCeiling(t *testing.T) {
	router, _, preparer := newStalledMetadataRouter(t, "10s")
	close(preparer.release) // preparer answers immediately

	response := servePropfindBounded(t, router, "/"+manager.EntryAllFolder+"/"+populatedEntry+"/", "1")
	if response.Code != http.StatusMultiStatus {
		t.Fatalf("healthy listing = %d, want 207\nbody: %s", response.Code, response.Body.String())
	}
}

// "off" must restore the historical unbounded listing, so an operator who
// suspects the ceiling itself can remove it without a code change.
func TestDisabledMetadataCeilingRestoresUnboundedListing(t *testing.T) {
	router, _, preparer := newStalledMetadataRouter(t, "off")

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = servePropfindRaw(router, "/"+manager.EntryAllFolder+"/"+populatedEntry+"/", "1")
	}()
	select {
	case <-done:
		t.Fatal("a disabled ceiling still released the listing")
	case <-time.After(300 * time.Millisecond):
	}
	close(preparer.release)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the listing never completed after the preparer recovered")
	}
}

func newStalledMetadataRouter(t *testing.T, ceiling string) (http.Handler, *manager.Manager, *stallingPreparer) {
	t.Helper()
	handler, mgr := newGroupHandler(t)
	preparer := newStallingPreparer()
	handler.preparer = preparer
	config.Get().MetadataReadTimeout = ceiling
	return routerFor(handler), mgr, preparer
}

// servePropfindBounded drives the request on its own goroutine so a handler
// that never returns fails the test instead of hanging the whole binary.
func servePropfindBounded(t *testing.T, router http.Handler, target, depth string) *httptest.ResponseRecorder {
	t.Helper()
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		request := httptest.NewRequest(PROPFIND, target, nil)
		request.Header.Set("Depth", depth)
		router.ServeHTTP(response, request)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("PROPFIND never returned while the preparer stalled; the listing is still unbounded")
	}
	return response
}

func servePropfindRaw(router http.Handler, target, depth string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	request := httptest.NewRequest(PROPFIND, target, nil)
	request.Header.Set("Depth", depth)
	router.ServeHTTP(response, request)
	return response
}

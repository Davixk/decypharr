package link

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sirrobot01/decypharr/internal/customerror"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A STALE STORED LINK IS NOT DEAD CONTENT, AND BOTH PROVIDERS GOT THAT WRONG.
//
// Resolving a download link POSTs a link WE stored. When the provider rotates
// it, that POST 404s while the content behind it is untouched — measured on two
// production specimens, each returning EIO through the mount at 90% and 99% and
// HTTP 206 at the same offsets read directly from the provider.
//
// Nothing refreshed that input: the link refresher re-mints FROM the stale link,
// and the hourly torrent refresh skips settled entries entirely because
// NeedsUpdate looks only at provider ID, status and file count.
//
// The two providers then failed differently, and the second failure is worse:
//
//	AllDebrid   404 arrives untyped   -> falls through reinsertReason -> raw
//	                                     stream error -> rclone EIO -> spinner
//	RealDebrid  404 arrives as        -> IsContentPermanentlyGone -> hidden from
//	            ContentGoneError         WebDAV listings AND prune-eligible
//
// The RealDebrid shape was audited against 7 days of production deletions: 2 of
// 157 pruned torrents were still `downloaded` on the provider.

// rotatingLinkClient mints only for the link the provider currently holds, and
// 404s for anything else — which is exactly what a rotated link looks like from
// our side.
type rotatingLinkClient struct {
	countingClient
	served      string
	currentLink string
	// goneOn404 mirrors RealDebrid, which types a mint 404 as a definitive
	// content verdict. AllDebrid leaves it untyped; both are reproduced.
	goneOn404 bool
	mints     atomic.Int32
	refreshes atomic.Int32
}

func (c *rotatingLinkClient) GetDownloadLink(_ string, file *debridTypes.File) (debridTypes.DownloadLink, error) {
	c.mints.Add(1)
	if file.Link != c.currentLink {
		if c.goneOn404 {
			return debridTypes.DownloadLink{}, customerror.NewContentGoneError(
				fmt.Errorf("unrestrict returned HTTP 404 for %s", file.Name))
		}
		return debridTypes.DownloadLink{}, fmt.Errorf("404: HTTP 404 Not Found")
	}
	return debridTypes.DownloadLink{
		Debrid:       "provider",
		Link:         file.Link,
		DownloadLink: c.served,
		Filename:     file.Name,
		Size:         file.Size,
	}, nil
}

// GetTorrent is the provider's current truth — the call nothing on the read path
// made before, which is the whole defect.
func (c *rotatingLinkClient) GetTorrent(string) (*debridTypes.Torrent, error) {
	c.refreshes.Add(1)
	return &debridTypes.Torrent{
		Id: "id-1",
		Files: map[string]debridTypes.File{
			"Movie.mkv": {Id: "file-id-1", Link: c.currentLink, Name: "Movie.mkv", Size: 100},
		},
	}, nil
}

// 🔴 THE SPINNER HALF — AllDebrid's shape.
//
// A rotated link must not become a failed read. One re-read of the provider's
// own current links serves the file; without it the viewer gets EIO forever at
// any offset the VFS cache cannot answer from disk.
func TestAStaleStoredLinkIsRefreshedRatherThanFailingTheRead(t *testing.T) {
	entry := linkLifecycleEntry(strings.Repeat("a", 40), "provider", "id-1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	t.Cleanup(srv.Close)
	client := &rotatingLinkClient{currentLink: "https://example.invalid/ROTATED", served: srv.URL}

	var saved atomic.Int32
	svc := newLinkService(t, &client.countingClient, nil, func(*storage.Entry) error {
		saved.Add(1)
		return nil
	})
	svc.clients.Store("provider", client)

	dl, err := svc.GetLink(context.Background(), entry, "Movie.mkv")
	if err != nil {
		t.Fatalf("a rotated provider link failed the read: %v\n\nThe content is untouched — only our cached "+
			"copy of the link went stale. Every read past the cached region returns EIO and the viewer "+
			"sees a permanent spinner", err)
	}
	if dl.DownloadLink == "" {
		t.Fatal("resolution reported success with no download link")
	}
	if client.refreshes.Load() == 0 {
		t.Fatal("the provider was never re-read. The stored link is the MINT'S INPUT, so a mint failure " +
			"cannot be judged without first asking whether our copy of that input is current")
	}

	// The refreshed link must be written back, or every subsequent read pays the
	// same failure and refresh again.
	if got := entry.Providers["provider"].Files["Movie.mkv"].Link; got != client.currentLink {
		t.Fatalf("stored link is still %q after a successful refresh; want the provider's current %q",
			got, client.currentLink)
	}
	if saved.Load() == 0 {
		t.Fatal("the refreshed link was never persisted, so it is lost on restart")
	}
}

// 🔴 THE DESTRUCTIVE HALF — RealDebrid's shape, and the one that deletes.
//
// The identical condition arrives typed as a content verdict. It must not
// survive to the classifier: IsContentPermanentlyGone drops the file from WebDAV
// listings and marks it prune-eligible, so a link we failed to refresh would
// delete content that is alive on the provider.
func TestAStaleLinkNeverBecomesAContentGoneVerdict(t *testing.T) {
	entry := linkLifecycleEntry(strings.Repeat("b", 40), "provider", "id-1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	t.Cleanup(srv.Close)
	client := &rotatingLinkClient{currentLink: "https://example.invalid/ROTATED", goneOn404: true, served: srv.URL}

	svc := newLinkService(t, &client.countingClient, nil, func(*storage.Entry) error { return nil })
	svc.clients.Store("provider", client)

	dl, err := svc.GetLink(context.Background(), entry, "Movie.mkv")
	if customerror.IsContentPermanentlyGone(err) {
		t.Fatalf("a stale stored link produced a PERMANENTLY GONE verdict: %v\n\nThat predicate hides the "+
			"file from WebDAV listings and makes it destructive-eligible under PRUNE. Audited against 7 "+
			"days of production deletions, 2 of 157 pruned torrents were still downloaded on the "+
			"provider — deleted on the strength of a link we never refreshed", err)
	}
	if err != nil {
		t.Fatalf("refreshing the stale link should have served the read: %v", err)
	}
	if dl.DownloadLink == "" {
		t.Fatal("resolution reported success with no download link")
	}
}

// ⚠️ AND THE VERDICT MUST STILL BE REACHABLE WHEN IT IS EARNED.
//
// The discriminator is whether the link actually CHANGED. If the provider hands
// back the same link it already had, our state was never stale and its refusal
// is a real answer — the fix must not launder genuinely dead content into
// healthy, which would be a strictly worse bug than the one it replaces.
func TestAnUnchangedLinkStillYieldsTheProvidersVerdict(t *testing.T) {
	entry := linkLifecycleEntry(strings.Repeat("c", 40), "provider", "id-1")
	stored := entry.Providers["provider"].Files["Movie.mkv"].Link

	// The provider hands back exactly the link our record already holds, and
	// still refuses to mint it.
	client := &unchangedLinkClient{stored: stored}
	svc := newLinkService(t, &client.countingClient, nil, func(*storage.Entry) error { return nil })
	svc.clients.Store("provider", client)

	_, err := svc.GetLink(context.Background(), entry, "Movie.mkv")
	if err == nil {
		t.Fatal("a provider refusing to mint its OWN current link was reported as success; the refresh " +
			"path has laundered a real content verdict into a healthy read")
	}
	if !customerror.IsContentPermanentlyGone(err) {
		t.Fatalf("error = %v; when the stored link is confirmed current and the provider still refuses "+
			"it, the content verdict is earned and must survive", err)
	}
}

type unchangedLinkClient struct {
	countingClient
	stored    string
	refreshes atomic.Int32
}

func (c *unchangedLinkClient) GetTorrent(string) (*debridTypes.Torrent, error) {
	c.refreshes.Add(1)
	return &debridTypes.Torrent{
		Id: "id-1",
		Files: map[string]debridTypes.File{
			"Movie.mkv": {Id: "file-id-1", Link: c.stored, Name: "Movie.mkv", Size: 100},
		},
	}, nil
}

func (c *unchangedLinkClient) GetDownloadLink(_ string, file *debridTypes.File) (debridTypes.DownloadLink, error) {
	return debridTypes.DownloadLink{}, customerror.NewContentGoneError(
		fmt.Errorf("unrestrict returned HTTP 404 for %s", file.Name))
}

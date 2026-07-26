package webdav

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A one-segment PROPFIND naming a group that does not exist used to answer
// `207 Multi-Status` with a self-entry and zero children, while the SAME
// nonexistent name one segment deeper correctly answered 404:
//
//	GET      /webdav/__all__/<stale>/<file>   -> 404
//	PROPFIND /webdav/__all__/<stale>/         -> 404
//	PROPFIND /webdav/<stale>/   (ONE segment) -> 207, 0 children   <-- the bug
//
// A client cannot tell that apart from a real, empty group. In production an
// operator probed a suspected-missing path exactly this way, read the 207 as
// "the directory exists and is empty", and an investigation went the wrong way
// for a while on the strength of it.
//
// These tests drive the production route table (registerRoutes) behind the same
// StripSlashes the server installs, so the trailing-slash form the operator
// actually sent is the form under test.

const (
	// A configured custom folder whose filter matches nothing: REAL, and empty.
	// This is the case that must keep answering 207 with zero children — if the
	// fix collapsed "empty" into "missing" it would hide legitimately empty
	// groups, which is the same class of wrong answer in the other direction.
	emptyCustomFolder = "empty-folder"
	populatedEntry    = "PopulatedRelease"
)

func newGroupRouter(t *testing.T) (http.Handler, *manager.Manager) {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	cfg := config.Get()
	cfg.CustomFolders = map[string]config.CustomFolders{
		emptyCustomFolder: {Filters: map[string]string{"regex": "^matches-nothing$"}},
	}

	mgr := manager.New()
	t.Cleanup(func() { _ = mgr.Stop() })

	now := time.Unix(1_700_000_000, 0).UTC()
	hash := "hash-" + populatedEntry
	if err := mgr.Storage().AddOrUpdate(&storage.Entry{
		Protocol:   config.ProtocolTorrent,
		InfoHash:   hash,
		Name:       populatedEntry,
		Status:     debridTypes.TorrentStatusDownloaded,
		IsComplete: true,
		AddedOn:    now,
		Files: map[string]*storage.File{
			"movie.mkv": {Name: "movie.mkv", InfoHash: hash, Size: 4096, AddedOn: now},
		},
	}); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}

	router := chi.NewRouter()
	// The server mounts the WebDAV routes behind StripSlashes, which is why a
	// request for "/<group>/" reaches the "/{group}" route at all.
	router.Use(middleware.StripSlashes)
	NewHandler(mgr).registerRoutes(router)
	return router, mgr
}

func propfind(t *testing.T, router http.Handler, target, depth string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(PROPFIND, target, nil)
	request.Header.Set("Depth", depth)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}

// countResponses counts <d:response> elements. A collection with no children
// still has exactly one: its own self-entry.
func countResponses(body string) int {
	return strings.Count(body, "<d:response>")
}

func TestOneSegmentPropfindOnUnknownGroupIsNotFound(t *testing.T) {
	router, _ := newGroupRouter(t)

	for _, target := range []string{"/stale-group", "/stale-group/"} {
		for _, depth := range []string{"0", "1"} {
			t.Run(target+"@depth"+depth, func(t *testing.T) {
				response := propfind(t, router, target, depth)
				if response.Code != http.StatusNotFound {
					t.Fatalf("PROPFIND %s Depth:%s = %d with %d responses, want 404\nbody: %s",
						target, depth, response.Code, countResponses(response.Body.String()), response.Body.String())
				}
			})
		}
	}
}

// A real group that happens to be empty must STILL answer 207 with zero
// children. Regressing this would hide legitimately empty groups.
func TestOneSegmentPropfindOnRealEmptyGroupIsMultiStatus(t *testing.T) {
	router, _ := newGroupRouter(t)

	// __bad__ is a built-in group and no entry in this fixture is bad;
	// emptyCustomFolder is configured but its filter matches nothing.
	for _, group := range []string{manager.EntryBadFolder, emptyCustomFolder} {
		t.Run(group, func(t *testing.T) {
			response := propfind(t, router, "/"+group, "1")
			if response.Code != http.StatusMultiStatus {
				t.Fatalf("PROPFIND /%s = %d, want 207; a REAL but empty group must not 404\nbody: %s",
					group, response.Code, response.Body.String())
			}
			if got := countResponses(response.Body.String()); got != 1 {
				t.Fatalf("PROPFIND /%s returned %d responses, want 1 (the self-entry only)\nbody: %s",
					group, got, response.Body.String())
			}
			if !strings.Contains(response.Body.String(), "<d:displayname>"+group+"</d:displayname>") {
				t.Fatalf("PROPFIND /%s did not advertise itself\nbody: %s", group, response.Body.String())
			}
		})
	}
}

func TestOneSegmentPropfindOnPopulatedGroupListsChildren(t *testing.T) {
	router, _ := newGroupRouter(t)

	response := propfind(t, router, "/"+manager.EntryAllFolder, "1")
	if response.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND /%s = %d, want 207\nbody: %s", manager.EntryAllFolder, response.Code, response.Body.String())
	}
	body := response.Body.String()
	if got := countResponses(body); got != 2 {
		t.Fatalf("PROPFIND /%s returned %d responses, want 2 (self + one entry)\nbody: %s",
			manager.EntryAllFolder, got, body)
	}
	if !strings.Contains(body, "<d:displayname>"+populatedEntry+"</d:displayname>") {
		t.Fatalf("PROPFIND /%s did not list %q\nbody: %s", manager.EntryAllFolder, populatedEntry, body)
	}
}

// The depth-2 behaviour was already correct and must stay untouched.
func TestTwoSegmentPropfindOnUnknownEntryStillNotFound(t *testing.T) {
	router, _ := newGroupRouter(t)

	for _, depth := range []string{"0", "1"} {
		t.Run("depth"+depth, func(t *testing.T) {
			response := propfind(t, router, "/"+manager.EntryAllFolder+"/StaleEntry/", depth)
			if response.Code != http.StatusNotFound {
				t.Fatalf("PROPFIND /%s/StaleEntry/ Depth:%s = %d, want 404\nbody: %s",
					manager.EntryAllFolder, depth, response.Code, response.Body.String())
			}
		})
	}
	// ...while a real entry two segments down still enumerates.
	response := propfind(t, router, "/"+manager.EntryAllFolder+"/"+populatedEntry+"/", "1")
	if response.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND of a live entry = %d, want 207\nbody: %s", response.Code, response.Body.String())
	}
}

// version.txt is the one non-directory node at the mount root; a one-segment
// lookup of it must keep resolving.
func TestOneSegmentLookupOfVersionFileStillResolves(t *testing.T) {
	router, _ := newGroupRouter(t)

	response := propfind(t, router, "/"+manager.EntryVersionFile, "0")
	if response.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND /%s = %d, want 207\nbody: %s", manager.EntryVersionFile, response.Code, response.Body.String())
	}

	getRequest := httptest.NewRequest(http.MethodGet, "/"+manager.EntryVersionFile, nil)
	getResponse := httptest.NewRecorder()
	router.ServeHTTP(getResponse, getRequest)
	if getResponse.Code != http.StatusOK || getResponse.Body.Len() == 0 {
		t.Fatalf("GET /%s = %d body %q", manager.EntryVersionFile, getResponse.Code, getResponse.Body.String())
	}
}

// Every name the root listing advertises must resolve on its own one-segment
// PROPFIND. This is the invariant that keeps the existence check and
// GetEntries() from drifting apart: if a group is advertised it is navigable,
// and if it is not advertised it is 404 — never an empty phantom collection.
func TestEveryAdvertisedRootNameResolves(t *testing.T) {
	router, mgr := newGroupRouter(t)

	advertised := mgr.GetEntries()
	if len(advertised) == 0 {
		t.Fatal("the mount root advertised nothing")
	}
	for _, entry := range advertised {
		response := propfind(t, router, "/"+entry.Name(), "1")
		if response.Code != http.StatusMultiStatus {
			t.Fatalf("root advertises %q but PROPFIND /%s = %d; the listing and the lookup disagree",
				entry.Name(), entry.Name(), response.Code)
		}
	}
}

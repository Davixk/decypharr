package webdav

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestStreamPreparedResponseSetsNoEntityHeadersBeforeFinalReady(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	mgr := manager.New()
	t.Cleanup(func() { _ = mgr.Stop() })
	handler := NewHandler(mgr)

	// This represents a file whose first WebDAV preparation succeeded. The
	// Manager.Stream revalidation then fails before onReady because no Usenet
	// client is configured. Entity headers must still be absent on the error.
	entry := &storage.Entry{
		Protocol: config.ProtocolNZB,
		InfoHash: "second-preparation-failure",
		Files: map[string]*storage.File{
			"": {Name: "", Size: 1},
		},
	}
	request := httptest.NewRequest(http.MethodGet, "/file", nil)
	response := httptest.NewRecorder()

	err := handler.streamPreparedResponse(entry, &manager.FileInfo{}, response, request)
	if err == nil {
		t.Fatal("streamPreparedResponse succeeded, want final preparation failure")
	}
	for _, header := range []string{"ETag", "Last-Modified", "Content-Type", "Content-Length", "Accept-Ranges"} {
		if got := response.Header().Get(header); got != "" {
			t.Errorf("failure inherited %s=%q", header, got)
		}
	}
}

func TestGetRangeRejectsInvalidOrUnsatisfiableRequests(t *testing.T) {
	tests := []string{
		"bytes=100-199",
		"bytes=200-",
		"bytes=-0",
		"bytes=bad",
		"bytes=0-1,2-3",
	}
	for _, header := range tests {
		t.Run(header, func(t *testing.T) {
			request := httptest.NewRequest("GET", "/", nil)
			request.Header.Set("Range", header)
			if _, _, err := getRange(100, request); err == nil {
				t.Fatalf("getRange(%q) succeeded, want range error", header)
			}
		})
	}
}

func TestPrepareRangeResponseReturns416WithCorrectedSize(t *testing.T) {
	request := httptest.NewRequest("GET", "/", nil)
	request.Header.Set("Range", "bytes=100-199")
	response := httptest.NewRecorder()

	_, _, err := prepareRangeResponse(response, 100, request)
	var rangeErr *customerror.Error
	if !errors.As(err, &rangeErr) {
		t.Fatalf("range error = %v, want *customerror.Error", err)
	}
	if rangeErr.StatusCode() != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want 416", rangeErr.StatusCode())
	}
	if got := response.Header().Get("Content-Range"); got != "bytes */100" {
		t.Fatalf("Content-Range = %q, want bytes */100", got)
	}
	if got := response.Header().Get("ETag"); got != "" {
		t.Fatalf("unsatisfiable response inherited ETag %q", got)
	}
	if got := response.Header().Get("Content-Type"); got != "" {
		t.Fatalf("unsatisfiable response inherited Content-Type %q", got)
	}
}

func TestGetRangeUsesReconciledSize(t *testing.T) {
	request := httptest.NewRequest("GET", "/", nil)
	request.Header.Set("Range", "bytes=90-150")
	start, end, err := getRange(100, request)
	if err != nil {
		t.Fatalf("getRange: %v", err)
	}
	if start != 90 || end != 99 {
		t.Fatalf("range = %d-%d, want 90-99", start, end)
	}

	request.Header.Set("Range", "bytes=-10")
	start, end, err = getRange(100, request)
	if err != nil {
		t.Fatalf("suffix getRange: %v", err)
	}
	if start != 90 || end != 99 {
		t.Fatalf("suffix range = %d-%d, want 90-99", start, end)
	}
}

func TestGetRangeKeepsClientCoordinatesLogical(t *testing.T) {
	request := httptest.NewRequest("GET", "/", nil)
	request.Header.Set("Range", "bytes=10-19")
	start, end, err := getRange(100, request)
	if err != nil {
		t.Fatalf("getRange: %v", err)
	}
	if start != 10 || end != 19 {
		t.Fatalf("logical range = %d-%d, want 10-19", start, end)
	}
}

func TestGetRangeWithoutHeaderPreservesFullRequest(t *testing.T) {
	request := httptest.NewRequest("GET", "/", nil)
	start, end, err := getRange(100, request)
	if err != nil {
		t.Fatalf("getRange: %v", err)
	}
	if start != 0 || end != -1 {
		t.Fatalf("full range = %d-%d, want 0--1", start, end)
	}
}

func TestPropfindDepthZeroSkipsChildren(t *testing.T) {
	request := httptest.NewRequest("PROPFIND", "/", nil)
	request.Header.Set("Depth", "0")
	if propfindIncludesChildren(request) {
		t.Fatal("Depth: 0 unexpectedly includes children")
	}

	request.Header.Set("Depth", "1")
	if !propfindIncludesChildren(request) {
		t.Fatal("Depth: 1 unexpectedly skips children")
	}
}

func TestDepthZeroResolutionNeverEnumeratesColdChildren(t *testing.T) {
	resolver := &countingMetadataResolver{}
	handler := &Handler{metadata: resolver}

	request := httptest.NewRequest(PROPFIND, "/group", nil)
	request.Header.Set("Depth", "0")
	handler.resolveGroupMetadata("__all__", requestNeedsChildren(request))
	handler.resolveTorrentMetadata("release", requestNeedsChildren(request))
	if resolver.entryChildrenCalls != 0 || resolver.torrentChildrenCalls != 0 {
		t.Fatalf("Depth:0 enumerated children: group=%d torrent=%d", resolver.entryChildrenCalls, resolver.torrentChildrenCalls)
	}
	if resolver.entryNodeCalls != 1 || resolver.entryInfoCalls != 1 {
		t.Fatalf("Depth:0 current lookups: group=%d torrent=%d, want 1/1", resolver.entryNodeCalls, resolver.entryInfoCalls)
	}

	request.Header.Set("Depth", "1")
	handler.resolveGroupMetadata("__all__", requestNeedsChildren(request))
	handler.resolveTorrentMetadata("release", requestNeedsChildren(request))
	if resolver.entryChildrenCalls != 1 || resolver.torrentChildrenCalls != 1 {
		t.Fatalf("Depth:1 child lookups: group=%d torrent=%d, want 1/1", resolver.entryChildrenCalls, resolver.torrentChildrenCalls)
	}
}

func TestCurrentOnlyVersionFileSupportsGetAndDepthZero(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	mgr := manager.New()
	t.Cleanup(func() { _ = mgr.Stop() })
	handler := NewHandler(mgr)
	info, children := handler.resolveGroupMetadata("version.txt", false)
	if info == nil || info.IsDir() || len(info.Content()) == 0 || info.Size() != int64(len(info.Content())) {
		t.Fatalf("current-only version info = %#v", info)
	}
	if len(children) != 0 {
		t.Fatalf("current-only version lookup returned %d children", len(children))
	}

	getRequest := httptest.NewRequest(http.MethodGet, "/version.txt", nil)
	getResponse := httptest.NewRecorder()
	handler.handler(info, nil, getResponse, getRequest)
	if getResponse.Code != http.StatusOK || getResponse.Body.String() != string(info.Content()) {
		t.Fatalf("GET version.txt = status %d body %q", getResponse.Code, getResponse.Body.String())
	}

	propfindRequest := httptest.NewRequest(PROPFIND, "/version.txt", nil)
	propfindRequest.Header.Set("Depth", "0")
	propfindResponse := httptest.NewRecorder()
	handler.handler(info, nil, propfindResponse, propfindRequest)
	if propfindResponse.Code != http.StatusMultiStatus {
		t.Fatalf("Depth:0 version.txt status = %d, want 207", propfindResponse.Code)
	}
}

type countingMetadataResolver struct {
	entryNodeCalls       int
	entryInfoCalls       int
	entryChildrenCalls   int
	torrentChildrenCalls int
}

func (*countingMetadataResolver) RootInfo() *manager.FileInfo { return nil }
func (*countingMetadataResolver) GetEntries() []manager.FileInfo {
	return nil
}
func (r *countingMetadataResolver) GetEntryNode(string) *manager.FileInfo {
	r.entryNodeCalls++
	return nil
}
func (r *countingMetadataResolver) GetEntryChildren(string) (*manager.FileInfo, []manager.FileInfo) {
	r.entryChildrenCalls++
	return nil, nil
}
func (r *countingMetadataResolver) GetEntryInfo(string) (*manager.FileInfo, error) {
	r.entryInfoCalls++
	return nil, nil
}
func (r *countingMetadataResolver) GetTorrentChildren(string) (*manager.FileInfo, []manager.FileInfo) {
	r.torrentChildrenCalls++
	return nil, nil
}
func (*countingMetadataResolver) GetTorrentFile(string, string) (*manager.FileInfo, error) {
	return nil, nil
}

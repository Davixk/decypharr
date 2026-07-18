package manager

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

func TestStreamHTTPURLTranslatesBackingRangesAndPreservesClientSemantics(t *testing.T) {
	source := []byte("0123456789")
	tests := []struct {
		name              string
		file              *storage.File
		start             int64
		end               int64
		rangeRequested    bool
		upstreamStart     int64
		upstreamEnd       int64
		wantUpstreamRange string
		wantBody          string
		wantStatus        int
		wantContentRange  string
	}{
		{
			name:          "ordinary full request",
			file:          &storage.File{Name: "video.mkv", Size: 10},
			start:         0,
			end:           9,
			upstreamStart: 0,
			upstreamEnd:   9,
			wantBody:      "0123456789",
			wantStatus:    http.StatusOK,
		},
		{
			name:              "explicit full range",
			file:              &storage.File{Name: "video.mkv", Size: 10},
			start:             0,
			end:               9,
			rangeRequested:    true,
			upstreamStart:     0,
			upstreamEnd:       9,
			wantUpstreamRange: "bytes=0-9",
			wantBody:          "0123456789",
			wantStatus:        http.StatusPartialContent,
			wantContentRange:  "bytes 0-9/10",
		},
		{
			name:              "logical partial range",
			file:              &storage.File{Name: "video.mkv", Size: 10},
			start:             2,
			end:               4,
			rangeRequested:    true,
			upstreamStart:     2,
			upstreamEnd:       4,
			wantUpstreamRange: "bytes=2-4",
			wantBody:          "234",
			wantStatus:        http.StatusPartialContent,
			wantContentRange:  "bytes 2-4/10",
		},
		{
			name:              "stored full request",
			file:              &storage.File{Name: "video.mkv", Size: 4, ByteRange: &[2]int64{3, 6}},
			start:             0,
			end:               3,
			upstreamStart:     3,
			upstreamEnd:       6,
			wantUpstreamRange: "bytes=3-6",
			wantBody:          "3456",
			wantStatus:        http.StatusOK,
		},
		{
			name:              "stored logical partial range",
			file:              &storage.File{Name: "video.mkv", Size: 4, ByteRange: &[2]int64{3, 6}},
			start:             1,
			end:               2,
			rangeRequested:    true,
			upstreamStart:     4,
			upstreamEnd:       5,
			wantUpstreamRange: "bytes=4-5",
			wantBody:          "45",
			wantStatus:        http.StatusPartialContent,
			wantContentRange:  "bytes 1-2/4",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			seenRange := make(chan string, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seenRange <- r.Header.Get("Range")
				if r.Header.Get("Range") == "" {
					w.Header().Set("Content-Length", strconv.Itoa(len(source)))
					_, _ = w.Write(source)
					return
				}
				body := source[tc.upstreamStart : tc.upstreamEnd+1]
				w.Header().Set("Content-Length", strconv.Itoa(len(body)))
				w.Header().Set("Content-Range", buildContentRange(tc.upstreamStart, tc.upstreamEnd, int64(len(source))))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(body)
			}))
			t.Cleanup(server.Close)

			m := &Manager{streamClient: server.Client(), config: &config.Config{}}
			var body bytes.Buffer
			var meta StreamMetadata
			readyCalled := false
			err := m.streamHTTPURL(context.Background(), server.URL, tc.file, tc.file.Name, tc.start, tc.end, tc.rangeRequested, &body, func(got *StreamMetadata) error {
				readyCalled = true
				meta = *got
				meta.Header = got.Header.Clone()
				return nil
			})
			if err != nil {
				t.Fatalf("streamHTTPURL: %v", err)
			}
			if !readyCalled {
				t.Fatal("ready callback was not called")
			}
			if got := <-seenRange; got != tc.wantUpstreamRange {
				t.Fatalf("upstream Range = %q, want %q", got, tc.wantUpstreamRange)
			}
			if got := body.String(); got != tc.wantBody {
				t.Fatalf("body = %q, want %q", got, tc.wantBody)
			}
			if meta.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", meta.StatusCode, tc.wantStatus)
			}
			if got := meta.Header.Get("Content-Range"); got != tc.wantContentRange {
				t.Fatalf("downstream Content-Range = %q, want %q", got, tc.wantContentRange)
			}
			if got, want := meta.Header.Get("Content-Type"), utils.GetContentType(tc.file.Name); got != want {
				t.Fatalf("downstream Content-Type = %q, want %q", got, want)
			}
			if meta.ContentLength != int64(len(tc.wantBody)) {
				t.Fatalf("content length = %d, want %d", meta.ContentLength, len(tc.wantBody))
			}
		})
	}
}

func TestStreamHTTPURLRejectsUpstreamThatIgnoresRangeBeforeReady(t *testing.T) {
	seenRange := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenRange <- r.Header.Get("Range")
		w.Header().Set("Content-Length", "10")
		_, _ = w.Write([]byte("0123456789"))
	}))
	t.Cleanup(server.Close)

	m := &Manager{streamClient: server.Client(), config: &config.Config{}}
	file := &storage.File{Name: "inside.mkv", Size: 4, ByteRange: &[2]int64{3, 6}}
	var body bytes.Buffer
	readyCalled := false
	err := m.streamHTTPURL(context.Background(), server.URL, file, file.Name, 0, 3, false, &body, func(*StreamMetadata) error {
		readyCalled = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "ignored requested byte range") {
		t.Fatalf("stream error = %v, want ignored-range error", err)
	}
	if got := <-seenRange; got != "bytes=3-6" {
		t.Fatalf("upstream Range = %q, want bytes=3-6", got)
	}
	if readyCalled {
		t.Fatal("ready callback ran before ignored range was rejected")
	}
	if body.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting ignored range", body.Len())
	}
}

func TestUsenetMetadataPreservesExplicitFullRangeIntent(t *testing.T) {
	info := usenet.StreamReadyInfo{Start: 0, End: 3, Size: 4}

	full := newUsenetStreamMetadata(info, "video.mkv", false)
	if full.StatusCode != http.StatusOK || full.Header.Get("Content-Range") != "" {
		t.Fatalf("ordinary full metadata = status %d Content-Range %q", full.StatusCode, full.Header.Get("Content-Range"))
	}

	ranged := newUsenetStreamMetadata(info, "video.mkv", true)
	if ranged.StatusCode != http.StatusPartialContent {
		t.Fatalf("explicit full status = %d, want 206", ranged.StatusCode)
	}
	if got := ranged.Header.Get("Content-Range"); got != "bytes 0-3/4" {
		t.Fatalf("explicit full Content-Range = %q, want bytes 0-3/4", got)
	}
}

func TestStreamRejectsSameHashReplacementBeforeSecondUsenetPrepare(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	const infohash = "manager-stream-aba"
	old := &storage.Entry{
		Protocol:      config.ProtocolNZB,
		InfoHash:      infohash,
		Name:          "old release",
		NZBGeneration: "old-generation",
		Size:          4,
		Bytes:         4,
		Files: map[string]*storage.File{
			"video.mkv": {Name: "video.mkv", Size: 4, InfoHash: infohash},
		},
	}
	if err := store.AddOrUpdate(old); err != nil {
		t.Fatalf("add old entry: %v", err)
	}
	staleSnapshot, err := store.Get(infohash)
	if err != nil {
		t.Fatalf("load old snapshot: %v", err)
	}
	if err := store.Delete(infohash); err != nil {
		t.Fatalf("delete old entry: %v", err)
	}
	replacement := &storage.Entry{
		Protocol:      config.ProtocolNZB,
		InfoHash:      infohash,
		Name:          "replacement release",
		NZBGeneration: "replacement-generation",
		Size:          4,
		Bytes:         4,
		Files: map[string]*storage.File{
			"video.mkv": {Name: "video.mkv", Size: 4, InfoHash: infohash},
		},
	}
	if err := store.AddOrUpdate(replacement); err != nil {
		t.Fatalf("add replacement entry: %v", err)
	}

	m := &Manager{storage: store, usenet: &usenet.Usenet{}, config: &config.Config{}, logger: zerolog.Nop()}
	var body bytes.Buffer
	readyCalled := false
	err = m.Stream(context.Background(), staleSnapshot, "video.mkv", 0, -1, false, &body, func(*StreamMetadata) error {
		readyCalled = true
		return nil
	}, "test")
	if !errors.Is(err, usenet.ErrStaleNZBGeneration) {
		t.Fatalf("stream error = %v, want ErrStaleNZBGeneration", err)
	}
	if readyCalled {
		t.Fatal("ready callback ran for stale NZB generation")
	}
	if body.Len() != 0 {
		t.Fatalf("stale stream wrote %d bytes", body.Len())
	}
}

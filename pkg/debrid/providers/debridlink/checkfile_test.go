package debridlink

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func TestMain(m *testing.M) {
	configDir, err := os.MkdirTemp("", "decypharr-debridlink-test-")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(configDir)

	code := m.Run()
	_ = os.RemoveAll(configDir)
	os.Exit(code)
}

func newTestDebridLink() *DebridLink {
	opts := []request.ClientOption{
		request.WithMaxRetries(0),
		request.WithTimeout(2 * time.Second),
	}
	return &DebridLink{
		Host:         "https://debrid-link.com/api/v2",
		APIKey:       "test-key",
		client:       request.New(opts...),
		repairClient: request.New(opts...),
		logger:       zerolog.Nop(),
		config:       config.Debrid{Name: "test-debridlink"},
	}
}

// newLinkServer serves the CDN object the probe ranges over. DebridLink checks
// the download link directly, so the link under test is the server URL.
func newLinkServer(t *testing.T, status int) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)
	return server.URL + "/file.mkv"
}

func assertCheckIndeterminate(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("CheckFile() = nil; an unusable answer must never score healthy")
	}
	if !errors.Is(err, types.ErrAvailabilityIndeterminate) {
		t.Fatalf("CheckFile() error = %v, want types.ErrAvailabilityIndeterminate", err)
	}
	if errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("CheckFile() error = %v; an outage must not read as definitively dead", err)
	}
}

func TestCheckFileAvailable(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusPartialContent} {
		dl := newTestDebridLink()
		if err := dl.CheckFile(context.Background(), "", newLinkServer(t, status)); err != nil {
			t.Fatalf("CheckFile() with status %d error = %v, want nil", status, err)
		}
	}
}

func TestCheckFileDefinitivelyUnavailable(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusGone} {
		dl := newTestDebridLink()
		err := dl.CheckFile(context.Background(), "", newLinkServer(t, status))
		if !errors.Is(err, customerror.HosterUnavailableError) {
			t.Fatalf("CheckFile() with status %d error = %v, want customerror.HosterUnavailableError", status, err)
		}
		if errors.Is(err, types.ErrAvailabilityIndeterminate) {
			t.Fatalf("CheckFile() with status %d must be a definitive verdict, got indeterminate", status)
		}
	}
}

// Previously these returned a bare error the caller could not tell apart from
// any other failure; now they are explicitly "unknown".
func TestCheckFileIndeterminateStatuses(t *testing.T) {
	statuses := []int{
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
	}
	for _, status := range statuses {
		t.Run(http.StatusText(status), func(t *testing.T) {
			dl := newTestDebridLink()
			assertCheckIndeterminate(t, dl.CheckFile(context.Background(), "", newLinkServer(t, status)))
		})
	}
}

func TestCheckFileTransportErrorIsIndeterminate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	deadLink := server.URL + "/file.mkv"
	server.Close()

	dl := newTestDebridLink()
	assertCheckIndeterminate(t, dl.CheckFile(context.Background(), "", deadLink))
}

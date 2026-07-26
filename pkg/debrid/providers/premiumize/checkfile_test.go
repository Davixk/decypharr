package premiumize

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
	configDir, err := os.MkdirTemp("", "decypharr-premiumize-test-")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(configDir)

	code := m.Run()
	_ = os.RemoveAll(configDir)
	os.Exit(code)
}

func newTestPremiumize(host string) *Premiumize {
	opts := []request.ClientOption{
		request.WithMaxRetries(0),
		request.WithTimeout(2 * time.Second),
	}
	return &Premiumize{
		Host:   host,
		APIKey: "test-key",
		client: request.New(opts...),
		logger: zerolog.Nop(),
		config: config.Debrid{Name: "test-premiumize"},
	}
}

func newStatusServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if body != "" {
			_, _ = w.Write([]byte(body))
		}
	}))
	t.Cleanup(server.Close)
	return server
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

func TestCheckFileDirectLinkAvailable(t *testing.T) {
	server := newStatusServer(t, http.StatusOK, "")
	pm := newTestPremiumize(server.URL)
	if err := pm.CheckFile(context.Background(), "hash", server.URL+"/file.mkv"); err != nil {
		t.Fatalf("CheckFile() error = %v, want nil", err)
	}
}

func TestCheckFileDirectLinkDefinitivelyUnavailable(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusGone} {
		server := newStatusServer(t, status, "")
		pm := newTestPremiumize(server.URL)
		err := pm.CheckFile(context.Background(), "hash", server.URL+"/file.mkv")
		if !errors.Is(err, customerror.HosterUnavailableError) {
			t.Fatalf("CheckFile() with status %d error = %v, want customerror.HosterUnavailableError", status, err)
		}
		if errors.Is(err, types.ErrAvailabilityIndeterminate) {
			t.Fatalf("CheckFile() with status %d must be a definitive verdict, got indeterminate", status)
		}
	}
}

func TestCheckFileDirectLinkIndeterminateStatuses(t *testing.T) {
	statuses := []int{
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
	}
	for _, status := range statuses {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := newStatusServer(t, status, "")
			pm := newTestPremiumize(server.URL)
			assertCheckIndeterminate(t, pm.CheckFile(context.Background(), "hash", server.URL+"/file.mkv"))
		})
	}
}

func TestCheckFileTransportErrorIsIndeterminate(t *testing.T) {
	server := newStatusServer(t, http.StatusOK, "")
	deadURL := server.URL
	server.Close()

	pm := newTestPremiumize(deadURL)
	assertCheckIndeterminate(t, pm.CheckFile(context.Background(), "hash", deadURL+"/file.mkv"))
}

func TestCheckFileEmptyFileIDIsDefinitivelyUnavailable(t *testing.T) {
	pm := newTestPremiumize("https://www.premiumize.me")
	err := pm.CheckFile(context.Background(), "hash", "")
	if !errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("CheckFile() error = %v, want customerror.HosterUnavailableError", err)
	}
}

func TestCheckFileItemDetailsAvailable(t *testing.T) {
	server := newStatusServer(t, http.StatusOK, `{"id":"item-1","name":"movie.mkv","size":100,"link":"https://cdn.example/movie.mkv"}`)
	pm := newTestPremiumize(server.URL)
	if err := pm.CheckFile(context.Background(), "hash", "item-1"); err != nil {
		t.Fatalf("CheckFile() error = %v, want nil", err)
	}
}

// /api/item/details is a fixed API route: its failures describe our access to
// Premiumize, never the file, so they can only ever be indeterminate.
func TestCheckFileItemDetailsFailureIsIndeterminate(t *testing.T) {
	statuses := []int{
		http.StatusUnauthorized,
		http.StatusNotFound,
		http.StatusInternalServerError,
	}
	for _, status := range statuses {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := newStatusServer(t, status, `{"status":"error","message":"nope"}`)
			pm := newTestPremiumize(server.URL)
			assertCheckIndeterminate(t, pm.CheckFile(context.Background(), "hash", "item-1"))
		})
	}
}

func TestCheckFileItemDetailsErrorEnvelopeIsIndeterminate(t *testing.T) {
	server := newStatusServer(t, http.StatusOK, `{"status":"error","message":"not logged in","code":"unauthorized"}`)
	pm := newTestPremiumize(server.URL)
	assertCheckIndeterminate(t, pm.CheckFile(context.Background(), "hash", "item-1"))
}

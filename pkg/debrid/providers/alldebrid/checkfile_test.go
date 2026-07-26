package alldebrid

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

const testCheckLink = "https://alldebrid.com/f/movie"

func newAllDebridAt(host string) *AllDebrid {
	opts := []request.ClientOption{
		request.WithMaxRetries(0),
		request.WithTimeout(2 * time.Second),
	}
	return &AllDebrid{
		Host:         host,
		APIKey:       "test-key",
		client:       request.New(opts...),
		repairClient: request.New(opts...),
		logger:       zerolog.Nop(),
		config:       config.Debrid{Name: "test-alldebrid"},
	}
}

// newCheckAllDebrid points a provider at a server that answers /v4.1/link/infos
// with the supplied status and body.
func newCheckAllDebrid(t *testing.T, status int, body string) *AllDebrid {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != allDebridLinkInfosEndpoint {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if body != "" {
			_, _ = io.WriteString(w, body)
		}
	}))
	t.Cleanup(server.Close)

	return newAllDebridAt(server.URL)
}

// assertCheckIndeterminate demands the three-way contract: not healthy (nil) and
// not the definitive "gone" verdict that authorises destructive repair.
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
	ad := newCheckAllDebrid(t, http.StatusOK, `{"status":"success","data":{"infos":[{}]}}`)
	if err := ad.CheckFile(context.Background(), "", testCheckLink); err != nil {
		t.Fatalf("CheckFile() error = %v, want nil", err)
	}
}

// AllDebrid reports a dead link through the per-link error inside a successful
// 200 envelope, never through the HTTP status of this fixed API route.
func TestCheckFileDefinitivelyUnavailable(t *testing.T) {
	ad := newCheckAllDebrid(t, http.StatusOK,
		`{"status":"success","data":{"infos":[{"error":{"code":"LINK_DOWN","message":"link is dead"}}]}}`)

	err := ad.CheckFile(context.Background(), "", testCheckLink)
	if !errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("CheckFile() error = %v, want customerror.HosterUnavailableError", err)
	}
	if errors.Is(err, types.ErrAvailabilityIndeterminate) {
		t.Fatalf("CheckFile() error = %v, want a definitive verdict, got indeterminate", err)
	}
}

func TestCheckFileIndeterminateStatuses(t *testing.T) {
	statuses := []int{
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
	}
	for _, status := range statuses {
		t.Run(http.StatusText(status), func(t *testing.T) {
			ad := newCheckAllDebrid(t, status, `{"status":"error","error":{"code":"OOPS","message":"nope"}}`)
			assertCheckIndeterminate(t, ad.CheckFile(context.Background(), "", testCheckLink))
		})
	}
}

// An auth failure arrives as HTTP 200 with an error envelope. It says nothing
// about the file, so it must not score healthy nor dead.
func TestCheckFileAuthErrorEnvelopeIsIndeterminate(t *testing.T) {
	ad := newCheckAllDebrid(t, http.StatusOK,
		`{"status":"error","error":{"code":"AUTH_BAD_APIKEY","message":"The auth apikey is invalid"}}`)
	assertCheckIndeterminate(t, ad.CheckFile(context.Background(), "", testCheckLink))
}

func TestCheckFileMalformedBodyIsIndeterminate(t *testing.T) {
	ad := newCheckAllDebrid(t, http.StatusOK, `{"status":"success","data":{"infos":`)
	assertCheckIndeterminate(t, ad.CheckFile(context.Background(), "", testCheckLink))
}

func TestCheckFileUnexpectedInfoCountIsIndeterminate(t *testing.T) {
	ad := newCheckAllDebrid(t, http.StatusOK, `{"status":"success","data":{"infos":[]}}`)
	assertCheckIndeterminate(t, ad.CheckFile(context.Background(), "", testCheckLink))
}

func TestCheckFileTransportErrorIsIndeterminate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	deadURL := server.URL
	server.Close()

	ad := newAllDebridAt(deadURL)
	assertCheckIndeterminate(t, ad.CheckFile(context.Background(), "", testCheckLink))
}

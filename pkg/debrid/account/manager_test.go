package account

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func TestMain(m *testing.M) {
	configDir, err := os.MkdirTemp("", "decypharr-account-test-")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(configDir)

	code := m.Run()
	_ = os.RemoveAll(configDir)
	os.Exit(code)
}

const testDebridName = "test-debrid"

// newTestManager builds a Manager over the supplied tokens, in order. The first
// token becomes the current account.
func newTestManager(tokens ...string) *Manager {
	m := &Manager{
		debrid:   testDebridName,
		accounts: xsync.NewMap[string, *Account](),
		logger:   zerolog.Nop(),
	}
	var first *Account
	for i, token := range tokens {
		acc := &Account{
			Debrid: testDebridName,
			Token:  token,
			Index:  i,
			links:  xsync.NewMap[string, types.DownloadLink](),
			httpClient: request.New(
				request.WithMaxRetries(0),
				request.WithTimeout(2*time.Second),
			),
		}
		m.accounts.Store(token, acc)
		if first == nil {
			first = acc
		}
	}
	m.current.Store(first)
	return m
}

// newLinkServer answers /unrestrict with the status configured for the calling
// account's token. Any token without an entry gets 200.
func newLinkServer(t *testing.T, statusByToken map[string]int) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Query().Get("token")
		status, ok := statusByToken[token]
		if !ok {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		if status == http.StatusOK {
			_, _ = io.WriteString(w, "https://cdn.example/"+token+"/movie.mkv")
		}
	}))
	t.Cleanup(server.Close)
	return server
}

// newTestFetcher mirrors what a real provider fetchDownloadLink does: it talks to
// the provider over the account's own client and maps the answer onto the
// provider error vocabulary. 404 is wrapped so the tests also prove a wrapped
// sentinel survives the account-manager layer.
func newTestFetcher(serverURL string) LinkFetcher {
	return func(acc *Account, id string, file *types.File) (types.DownloadLink, error) {
		req, err := http.NewRequest(http.MethodGet, serverURL+"/unrestrict?token="+url.QueryEscape(acc.Token), nil)
		if err != nil {
			return types.DownloadLink{}, err
		}
		resp, err := acc.Client().Do(req)
		if err != nil {
			return types.DownloadLink{}, err
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return types.DownloadLink{}, err
		}

		switch resp.StatusCode {
		case http.StatusOK:
			now := time.Now()
			return types.DownloadLink{
				Debrid:       acc.Debrid,
				Token:        acc.Token,
				Filename:     file.Name,
				Link:         file.Link,
				DownloadLink: strings.TrimSpace(string(body)),
				Generated:    now,
				ExpiresAt:    now.Add(time.Hour),
			}, nil
		case http.StatusNotFound:
			return types.DownloadLink{}, fmt.Errorf("%w: link is dead", customerror.HosterUnavailableError)
		case http.StatusForbidden:
			return types.DownloadLink{}, customerror.TrafficExceededError
		default:
			return types.DownloadLink{}, fmt.Errorf("provider status %d", resp.StatusCode)
		}
	}
}

func testFile() *types.File {
	return &types.File{
		Name: "movie.mkv",
		Size: 1024,
		Link: "https://provider.example/f/movie",
	}
}

// The account manager used to discard the fetch error and return (dl, nil)
// unconditionally, so no provider failure could ever reach a caller. Every
// distinct failure mode arrived as the single symptom "empty download link".
func TestGetDownloadLinkReturnsProviderError(t *testing.T) {
	server := newLinkServer(t, map[string]int{"only": http.StatusNotFound})
	m := newTestManager("only")

	dl, err := m.GetDownloadLink("torrent-1", testFile(), newTestFetcher(server.URL))
	if err == nil {
		t.Fatal("GetDownloadLink() error = nil, want the provider error to be returned, not swallowed")
	}
	if !errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("GetDownloadLink() error = %v, want it to wrap customerror.HosterUnavailableError", err)
	}
	if !dl.Empty() {
		t.Fatalf("GetDownloadLink() link = %q, want empty on failure", dl.DownloadLink)
	}
}

func TestGetDownloadLinkSucceedsOnCurrentAccount(t *testing.T) {
	server := newLinkServer(t, nil)
	m := newTestManager("primary", "secondary")

	dl, err := m.GetDownloadLink("torrent-1", testFile(), newTestFetcher(server.URL))
	if err != nil {
		t.Fatalf("GetDownloadLink() error = %v, want nil", err)
	}
	if dl.Token != "primary" {
		t.Fatalf("GetDownloadLink() token = %q, want primary", dl.Token)
	}
	if dl.DownloadLink != "https://cdn.example/primary/movie.mkv" {
		t.Fatalf("GetDownloadLink() link = %q, unexpected", dl.DownloadLink)
	}
}

// Multi-account fallback must survive the fix: a failure on the current account
// followed by a success elsewhere is still a success with a nil error.
func TestGetDownloadLinkFallbackSucceedsWithNilError(t *testing.T) {
	server := newLinkServer(t, map[string]int{"primary": http.StatusNotFound})
	m := newTestManager("primary", "secondary")

	dl, err := m.GetDownloadLink("torrent-1", testFile(), newTestFetcher(server.URL))
	if err != nil {
		t.Fatalf("GetDownloadLink() error = %v, want nil once a fallback account succeeds", err)
	}
	if dl.Empty() {
		t.Fatal("GetDownloadLink() returned an empty link despite a successful fallback")
	}
	if dl.Token != "secondary" {
		t.Fatalf("GetDownloadLink() token = %q, want secondary", dl.Token)
	}

	// The current account must not have been switched by the fallback.
	if current := m.Current(); current == nil || current.Token != "primary" {
		t.Fatalf("Current() = %v, want the primary account to stay current", current)
	}
}

// Only once every active account has failed does an error surface, and it must
// still carry the primary account's error type so callers can route on it.
func TestGetDownloadLinkErrorTypePreservedWhenFallbackExhausted(t *testing.T) {
	server := newLinkServer(t, map[string]int{
		"primary":   http.StatusNotFound,
		"secondary": http.StatusForbidden,
	})
	m := newTestManager("primary", "secondary")

	_, err := m.GetDownloadLink("torrent-1", testFile(), newTestFetcher(server.URL))
	if err == nil {
		t.Fatal("GetDownloadLink() error = nil, want an error once all accounts failed")
	}
	if !errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("GetDownloadLink() error = %v, want the current account's HosterUnavailableError", err)
	}
	// The fallback account's incidental error must not be the reported verdict.
	if errors.Is(err, customerror.TrafficExceededError) {
		t.Fatalf("GetDownloadLink() error = %v, want the primary verdict, not the fallback's", err)
	}
}

func TestGetDownloadLinkPreservesTrafficExceeded(t *testing.T) {
	server := newLinkServer(t, map[string]int{"only": http.StatusForbidden})
	m := newTestManager("only")

	_, err := m.GetDownloadLink("torrent-1", testFile(), newTestFetcher(server.URL))
	if !errors.Is(err, customerror.TrafficExceededError) {
		t.Fatalf("GetDownloadLink() error = %v, want customerror.TrafficExceededError", err)
	}
	// Distinct failure modes must stay distinguishable from one another.
	if errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("GetDownloadLink() error = %v, must not also read as hoster-unavailable", err)
	}
}

// A disabled account is not a fallback candidate, so its failure cannot mask the
// current account's verdict.
func TestGetDownloadLinkSkipsDisabledFallback(t *testing.T) {
	server := newLinkServer(t, map[string]int{"primary": http.StatusNotFound})
	m := newTestManager("primary", "secondary")

	secondary, err := m.GetAccount("secondary")
	if err != nil {
		t.Fatalf("GetAccount() error = %v", err)
	}
	secondary.MarkDisabled()

	_, err = m.GetDownloadLink("torrent-1", testFile(), newTestFetcher(server.URL))
	if err == nil {
		t.Fatal("GetDownloadLink() error = nil, want the primary error with no usable fallback")
	}
	if !errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("GetDownloadLink() error = %v, want customerror.HosterUnavailableError", err)
	}
}

func TestGetDownloadLinkNoActiveAccount(t *testing.T) {
	server := newLinkServer(t, nil)
	m := newTestManager()

	_, err := m.GetDownloadLink("torrent-1", testFile(), newTestFetcher(server.URL))
	if err == nil {
		t.Fatal("GetDownloadLink() error = nil, want an error when no account exists")
	}
	if !strings.Contains(err.Error(), "no active account") {
		t.Fatalf("GetDownloadLink() error = %v, want a no-active-account error", err)
	}
}

// A transport failure is an error too — it must not be laundered into an empty
// link with a nil error.
func TestGetDownloadLinkReturnsTransportError(t *testing.T) {
	server := newLinkServer(t, nil)
	deadURL := server.URL
	server.Close()

	m := newTestManager("only")
	_, err := m.GetDownloadLink("torrent-1", testFile(), newTestFetcher(deadURL))
	if err == nil {
		t.Fatal("GetDownloadLink() error = nil, want the transport failure to be returned")
	}
}

package torbox

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func TestMain(m *testing.M) {
	configDir, err := os.MkdirTemp("", "decypharr-torbox-test-")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(configDir)

	code := m.Run()
	_ = os.RemoveAll(configDir)
	os.Exit(code)
}

func newTorboxAt(host string) *Torbox {
	opts := []request.ClientOption{
		request.WithMaxRetries(0),
		request.WithTimeout(2 * time.Second),
	}
	return &Torbox{
		Host:   host,
		APIKey: "test-key",
		client: request.New(opts...),
		logger: zerolog.Nop(),
		config: config.Debrid{Name: "test-torbox"},
	}
}

// newListServer answers /api/torrents/mylist. The first page carries the given
// body; every later page is empty so loadDownloadPresent terminates.
func newListServer(t *testing.T, status int, firstPage string) *Torbox {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status < 200 || status >= 300 {
			return
		}
		if r.URL.Query().Get("offset") == "0" {
			_, _ = io.WriteString(w, firstPage)
			return
		}
		_, _ = io.WriteString(w, `{"success":true,"data":[]}`)
	}))
	t.Cleanup(server.Close)

	return newTorboxAt(server.URL)
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

const listWithPresentAndMissing = `{"success":true,"data":[
	{"id":11,"download_present":true},
	{"id":22,"download_present":false}
]}`

func TestCheckFileAvailable(t *testing.T) {
	tb := newListServer(t, http.StatusOK, listWithPresentAndMissing)
	if err := tb.CheckFile(context.Background(), "hash", "11"); err != nil {
		t.Fatalf("CheckFile() error = %v, want nil", err)
	}
}

func TestCheckFileAcceptsTorboxURIForm(t *testing.T) {
	tb := newListServer(t, http.StatusOK, listWithPresentAndMissing)
	if err := tb.CheckFile(context.Background(), "hash", "torbox://11/movie.mkv"); err != nil {
		t.Fatalf("CheckFile() error = %v, want nil", err)
	}
}

func TestCheckFileDefinitivelyUnavailable(t *testing.T) {
	tb := newListServer(t, http.StatusOK, listWithPresentAndMissing)
	err := tb.CheckFile(context.Background(), "hash", "22")
	if !errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("CheckFile() error = %v, want customerror.HosterUnavailableError", err)
	}
	if errors.Is(err, types.ErrAvailabilityIndeterminate) {
		t.Fatalf("CheckFile() error = %v, want a definitive verdict, got indeterminate", err)
	}
}

// A snapshot the probe could not build says nothing about the file. Before the
// fix this surfaced as a bare error indistinguishable from a real verdict.
func TestCheckFileSnapshotFailureIsIndeterminate(t *testing.T) {
	statuses := []int{
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
	}
	for _, status := range statuses {
		t.Run(http.StatusText(status), func(t *testing.T) {
			tb := newListServer(t, status, "")
			assertCheckIndeterminate(t, tb.CheckFile(context.Background(), "hash", "11"))
		})
	}
}

func TestCheckFileTransportErrorIsIndeterminate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	deadURL := server.URL
	server.Close()

	tb := newTorboxAt(deadURL)
	assertCheckIndeterminate(t, tb.CheckFile(context.Background(), "hash", "11"))
}

// seedSnapshot installs a snapshot with an explicit age, standing in for one
// loaded some time ago.
func seedSnapshot(tb *Torbox, age time.Duration, present map[string]bool) {
	tb.downloadPresent.Store(&downloadPresentSnapshot{
		present:  present,
		loadedAt: time.Now().Add(-age),
	})
}

// The false-dead this fix exists to kill: a torrent added AFTER the snapshot was
// built is absent from it, and used to be reported definitively dead — the
// verdict that authorises destructive repair on healthy content.
func TestCheckFileStaleMissIsConfirmedByRefresh(t *testing.T) {
	tb := newListServer(t, http.StatusOK, `{"success":true,"data":[{"id":99,"download_present":true}]}`)
	seedSnapshot(tb, 2*downloadPresentTTL, map[string]bool{"11": true})

	if err := tb.CheckFile(context.Background(), "hash", "99"); err != nil {
		t.Fatalf("CheckFile() error = %v, want nil for a torrent added after the stale snapshot", err)
	}
}

// A stale miss whose refresh FAILS must never be reported as dead. Under the
// old code this exact case returned HosterUnavailableError.
func TestCheckFileStaleMissWithFailedRefreshIsIndeterminate(t *testing.T) {
	tb := newListServer(t, http.StatusServiceUnavailable, "")
	seedSnapshot(tb, 2*downloadPresentTTL, map[string]bool{"11": true})

	err := tb.CheckFile(context.Background(), "hash", "99")
	if errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatal("CheckFile() reported a stale-snapshot miss as definitively dead; that authorises destructive repair on content we never verified")
	}
	assertCheckIndeterminate(t, err)
}

// Same, when the provider is unreachable entirely.
func TestCheckFileStaleMissWithUnreachableProviderIsIndeterminate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	deadURL := server.URL
	server.Close()

	tb := newTorboxAt(deadURL)
	seedSnapshot(tb, 2*downloadPresentTTL, map[string]bool{"11": true})

	err := tb.CheckFile(context.Background(), "hash", "99")
	if errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatal("CheckFile() reported an unverifiable absence as definitively dead")
	}
	assertCheckIndeterminate(t, err)
}

// True-positive detection is preserved: once a refresh confirms the torrent is
// absent from the live account listing, dead is the right answer.
func TestCheckFileStaleMissConfirmedAbsentIsDead(t *testing.T) {
	tb := newListServer(t, http.StatusOK, `{"success":true,"data":[{"id":11,"download_present":true}]}`)
	seedSnapshot(tb, 2*downloadPresentTTL, map[string]bool{"11": true})

	err := tb.CheckFile(context.Background(), "hash", "99")
	if !errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("CheckFile() error = %v, want customerror.HosterUnavailableError after a fresh listing confirmed the absence", err)
	}
	if errors.Is(err, types.ErrAvailabilityIndeterminate) {
		t.Fatalf("CheckFile() error = %v, want a definitive verdict, got indeterminate", err)
	}
}

// A miss against an already-fresh snapshot needs no extra provider call.
func TestCheckFileFreshMissIsDeadWithoutRefresh(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"success":true,"data":[]}`)
	}))
	t.Cleanup(server.Close)

	tb := newTorboxAt(server.URL)
	seedSnapshot(tb, time.Second, map[string]bool{"11": true})

	if err := tb.CheckFile(context.Background(), "hash", "99"); !errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("CheckFile() error = %v, want customerror.HosterUnavailableError", err)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("provider was called %d times for a fresh-snapshot miss, want 0", got)
	}
}

// Many concurrent misses must share a single list walk, not one per file.
func TestCheckFileConcurrentStaleMissesShareOneRefresh(t *testing.T) {
	var pages int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if r.URL.Query().Get("offset") == "0" {
			atomic.AddInt32(&pages, 1)
			_, _ = io.WriteString(w, `{"success":true,"data":[{"id":11,"download_present":true}]}`)
			return
		}
		_, _ = io.WriteString(w, `{"success":true,"data":[]}`)
	}))
	t.Cleanup(server.Close)

	tb := newTorboxAt(server.URL)
	seedSnapshot(tb, 2*downloadPresentTTL, map[string]bool{"11": true})

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = tb.CheckFile(context.Background(), "hash", "99")
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&pages); got != 1 {
		t.Fatalf("refresh walked the torrent list %d times, want exactly 1 shared walk", got)
	}
}

// A reload replaces the snapshot rather than merging into it, so a torrent
// removed from the account stops reporting healthy.
func TestCheckFileRefreshReplacesRatherThanMerges(t *testing.T) {
	tb := newListServer(t, http.StatusOK, `{"success":true,"data":[{"id":11,"download_present":true}]}`)
	seedSnapshot(tb, 2*downloadPresentTTL, map[string]bool{"11": true, "22": true})

	// 22 is gone from the live listing; the miss on 99 forces the refresh.
	if err := tb.CheckFile(context.Background(), "hash", "99"); !errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("CheckFile(99) error = %v, want customerror.HosterUnavailableError", err)
	}
	if err := tb.CheckFile(context.Background(), "hash", "22"); !errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("CheckFile(22) error = %v, want the removed torrent to stop reporting healthy", err)
	}
}

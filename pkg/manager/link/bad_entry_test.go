package link

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// countingClient records every provider call so a test can assert a rejected
// read costs ZERO provider API budget.
type countingClient struct {
	debrid.Client
	downloadLinkCalls atomic.Int32
	torrentCalls      atomic.Int32
	link              debridTypes.DownloadLink
	err               error
}

func (c *countingClient) GetDownloadLink(_ string, _ *debridTypes.File) (debridTypes.DownloadLink, error) {
	c.downloadLinkCalls.Add(1)
	return c.link, c.err
}

func (c *countingClient) GetTorrent(string) (*debridTypes.Torrent, error) {
	c.torrentCalls.Add(1)
	return nil, nil
}

func newLinkService(t *testing.T, client *countingClient, repairer EntryRepairer, saver EntrySaver) *Service {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("provider", client)
	return New(clients, nil, repairer, saver, http.DefaultClient, 0, zerolog.Nop())
}

// TestBadEntryReadReturnsPermanentGoneWithoutProviderCalls covers FIX X and
// FIX Y together: the durable Bad flag must surface as a typed permanent 410
// (a bare error became a generic 500, which rclone retries forever instead of
// converting to EIO) and must be decided BEFORE any provider work, since every
// such read is guaranteed to fail and its GetDownloadLink call is repeated
// across every active account.
func TestBadEntryReadReturnsPermanentGoneWithoutProviderCalls(t *testing.T) {
	entry := linkLifecycleEntry(strings.Repeat("d", 40), "provider", "id-1")
	entry.Bad = true

	client := &countingClient{}
	repairs := atomic.Int32{}
	svc := newLinkService(t, client, func(context.Context, *storage.Entry) error {
		repairs.Add(1)
		return nil
	}, nil)

	_, err := svc.GetLink(context.Background(), entry, "Movie.mkv")
	if err == nil {
		t.Fatal("a bad-marked entry resolved a link")
	}

	var custom *customerror.Error
	if !errors.As(err, &custom) {
		t.Fatalf("bad-entry error = %v (%T); errors.As must find *customerror.Error or the WebDAV layer emits a generic 500", err, err)
	}
	if custom.StatusCode() != http.StatusGone {
		t.Fatalf("bad-entry status = %d, want 410 Gone (500 is retryable and never terminates the reader)", custom.StatusCode())
	}
	if !custom.IsPermanent() {
		t.Fatal("bad-entry error must be permanent")
	}
	// Operators grep for this exact phrase.
	if !strings.Contains(err.Error(), "since it's been marked as bad") {
		t.Fatalf("bad-entry message lost its greppable text: %q", err.Error())
	}

	if got := client.downloadLinkCalls.Load(); got != 0 {
		t.Fatalf("bad-entry read made %d GetDownloadLink calls, want 0", got)
	}
	if got := client.torrentCalls.Load(); got != 0 {
		t.Fatalf("bad-entry read made %d GetTorrent calls, want 0", got)
	}
	if got := repairs.Load(); got != 0 {
		t.Fatalf("bad-entry read triggered %d re-insertions, want 0", got)
	}
}

// TestBadEntryErrorSurvivesStreamWrap pins that %w wrapping on the way to the
// WebDAV layer preserves the typed 410.
func TestBadEntryErrorSurvivesStreamWrap(t *testing.T) {
	entry := linkLifecycleEntry(strings.Repeat("e", 40), "provider", "id-2")
	entry.Bad = true
	svc := newLinkService(t, &countingClient{}, nil, nil)

	_, err := svc.GetLink(context.Background(), entry, "Movie.mkv")
	// Mirrors streamHTTP: fmt.Errorf("failed to get download link: %w", err).
	wrapped := errors.Join(errors.New("failed to get download link"), err)
	var custom *customerror.Error
	if !errors.As(wrapped, &custom) || custom.StatusCode() != http.StatusGone {
		t.Fatalf("wrapped bad-entry error lost its 410: %v", wrapped)
	}
}

// TestEmptyLinkErrorStillTriggersReinsertion covers FIX Z1. The account manager
// no longer returns (empty link, nil error), so the downloadLink.Empty() branch
// that used to drive the re-insertion recovery is unreachable. The equivalent
// typed error must reach the same recovery or the mechanism silently stops.
func TestEmptyLinkErrorStillTriggersReinsertion(t *testing.T) {
	entry := linkLifecycleEntry(strings.Repeat("f", 40), "provider", "id-3")
	client := &countingClient{err: debridTypes.EmptyDownloadLinkError}

	repairs := atomic.Int32{}
	svc := newLinkService(t, client, func(context.Context, *storage.Entry) error {
		repairs.Add(1)
		return nil
	}, func(*storage.Entry) error { return nil })

	_, err := svc.GetLink(context.Background(), entry, "Movie.mkv")
	if err == nil {
		t.Fatal("an empty provider link resolved successfully")
	}
	if got := repairs.Load(); got != MaxReinsertionAttempt {
		t.Fatalf("empty-link error drove %d re-insertions, want %d", got, MaxReinsertionAttempt)
	}
	if !entry.Bad {
		t.Fatal("entry was not marked bad after exhausting re-insertion attempts")
	}
}

// TestHosterUnavailableRetriesWithRealFilename covers FIX Z2: the provider
// returns an EMPTY DownloadLink alongside its error, so retrying with
// dl.Filename resolved "" and produced a bogus file-not-found (and a blank
// filename in the markEntryBad log).
func TestHosterUnavailableRetriesWithRealFilename(t *testing.T) {
	entry := linkLifecycleEntry(strings.Repeat("a", 40), "provider", "id-4")
	client := &countingClient{err: customerror.HosterUnavailableError}

	var seen []string
	svc := newLinkService(t, client, func(context.Context, *storage.Entry) error { return nil }, nil)
	svc.logger = zerolog.Nop()

	// fetchLink is the only place the filename is consumed; a wrong filename
	// short-circuits with a permanent "file_not_found" before the provider is
	// ever called, so counting provider calls proves the filename survived.
	before := client.downloadLinkCalls.Load()
	_, err := svc.GetLink(context.Background(), entry, "Movie.mkv")
	if err == nil {
		t.Fatal("hoster-unavailable resolved successfully")
	}
	attempts := client.downloadLinkCalls.Load() - before
	if attempts != MaxReinsertionAttempt+1 {
		t.Fatalf("provider was asked %d times, want %d; a blank filename would short-circuit before the provider call",
			attempts, MaxReinsertionAttempt+1)
	}
	if strings.Contains(err.Error(), `file  `) || strings.Contains(err.Error(), `file "" `) {
		t.Fatalf("terminal error carries a blank filename: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "Movie.mkv") {
		t.Fatalf("terminal error lost the filename: %q", err.Error())
	}
	_ = seen
}

// TestNonReinsertErrorsDoNotCondemnTheEntry pins the peer's behaviour change:
// a throttle or auth failure must no longer drive re-insertions or permanently
// mark the entry bad.
func TestNonReinsertErrorsDoNotCondemnTheEntry(t *testing.T) {
	entry := linkLifecycleEntry(strings.Repeat("b", 40), "provider", "id-5")
	client := &countingClient{err: errors.New("429 too many requests")}

	repairs := atomic.Int32{}
	svc := newLinkService(t, client, func(context.Context, *storage.Entry) error {
		repairs.Add(1)
		return nil
	}, func(*storage.Entry) error { return nil })

	if _, err := svc.GetLink(context.Background(), entry, "Movie.mkv"); err == nil {
		t.Fatal("a throttled request resolved successfully")
	}
	if got := repairs.Load(); got != 0 {
		t.Fatalf("a transient throttle drove %d re-insertions, want 0", got)
	}
	if entry.Bad {
		t.Fatal("a transient throttle permanently condemned the entry")
	}
}

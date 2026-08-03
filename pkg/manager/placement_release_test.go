package manager

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/sirrobot01/decypharr/internal/customerror"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// releaseClient fails DeleteTorrent a scripted number of times before
// succeeding, and counts attempts.
type releaseClient struct {
	fakeDebridClient
	failWith  error
	failTimes int
	calls     int
	mu        sync.Mutex
}

func (c *releaseClient) DeleteTorrent(string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls <= c.failTimes {
		return c.failWith
	}
	return nil
}

func (c *releaseClient) attempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func newReleaseFixture(t *testing.T, client debrid.Client) (*Manager, *storage.Entry) {
	t.Helper()
	m := newActionLifecycleFixture(t, 2)
	m.clients = xsync.NewMap[string, debrid.Client]()
	m.clients.Store("prov", client)

	entry := probeTorrentEntry("stuckhash", "Stuck.Entry")
	entry.Status = debridTypes.TorrentStatusDownloading
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	return m, entry
}

func transientErr() error {
	return customerror.NewError(errors.New("realdebrid delete x: status 503"), http.StatusServiceUnavailable, "", false, false).Retryable()
}

func terminalErr() error {
	return customerror.NewPermanentError(errors.New("realdebrid delete x: status 403"))
}

// TestReleaseRetriesTransientFailures: a transient failure is retried rather
// than deferred to a sweep that may hit the same condition forever. The slot
// being held is the resource under contention.
func TestReleaseRetriesTransientFailures(t *testing.T) {
	client := &releaseClient{failWith: transientErr(), failTimes: 2}
	m, entry := newReleaseFixture(t, client)

	if err := m.releasePlacementWithRetry(context.Background(), entry); err != nil {
		t.Fatalf("release should have succeeded on retry: %v", err)
	}
	if got := client.attempts(); got != 3 {
		t.Fatalf("attempts = %d, want 3 (two transient failures then success)", got)
	}
}

// TestReleaseDoesNotRetryTerminalFailures: retrying cannot change a 403, so
// burning the attempt budget on it only delays the operator finding out.
func TestReleaseDoesNotRetryTerminalFailures(t *testing.T) {
	client := &releaseClient{failWith: terminalErr(), failTimes: 99}
	m, entry := newReleaseFixture(t, client)

	err := m.releasePlacementWithRetry(context.Background(), entry)
	if err == nil {
		t.Fatal("terminal release failure reported success")
	}
	if got := client.attempts(); got != 1 {
		t.Fatalf("attempts = %d, want 1; a terminal failure must not be retried", got)
	}
}

// TestReleaseGivesUpAfterTransientBudget: a transient condition that outlasts
// the budget still returns an error, so the caller leaves the entry alone
// rather than failing it while the slot is held.
func TestReleaseGivesUpAfterTransientBudget(t *testing.T) {
	client := &releaseClient{failWith: transientErr(), failTimes: 99}
	m, entry := newReleaseFixture(t, client)

	if err := m.releasePlacementWithRetry(context.Background(), entry); err == nil {
		t.Fatal("exhausted transient retries reported success")
	}
	if got := client.attempts(); got != placementReleaseAttempts {
		t.Fatalf("attempts = %d, want %d", got, placementReleaseAttempts)
	}
}

// TestStallPruneDoesNotFailEntryWhileSlotHeld is the invariant from 1558258:
// telling the arr to re-grab while we still occupy the provider slot spends a
// second slot on the replacement.
func TestStallPruneDoesNotFailEntryWhileSlotHeld(t *testing.T) {
	client := &releaseClient{failWith: terminalErr(), failTimes: 99}
	m, entry := newReleaseFixture(t, client)

	if err := m.releasePlacementWithRetry(context.Background(), entry); err == nil {
		t.Fatal("expected the release to fail")
	}

	current, err := m.GetEntry(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if current.Status == debridTypes.TorrentStatusError || current.ErrorCount > 0 {
		t.Fatalf("entry was failed while its provider slot is still held: status=%q errors=%d",
			current.Status, current.ErrorCount)
	}
}

// TestReleaseHonoursContextCancellation: a shutdown mid-backoff must stop
// rather than sleep out the remaining attempts.
func TestReleaseHonoursContextCancellation(t *testing.T) {
	client := &releaseClient{failWith: transientErr(), failTimes: 99}
	m, entry := newReleaseFixture(t, client)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := m.releasePlacementWithRetry(ctx, entry); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got := client.attempts(); got != 0 {
		t.Fatalf("attempts = %d on an already-cancelled context, want 0", got)
	}
}

// TestAlreadyAbsentPlacementCountsAsReleased: a provider reporting the item as
// gone is a SATISFIED delete. Treating it as failure would strand the entry
// forever — slot actually free, but the caller refusing to fail it on the
// grounds that the slot is held.
func TestAlreadyAbsentPlacementCountsAsReleased(t *testing.T) {
	// A nil error is what a 404 is mapped to at the provider boundary; assert
	// the manager path treats that as success end to end.
	client := &releaseClient{}
	m, entry := newReleaseFixture(t, client)

	if err := m.releasePlacementWithRetry(context.Background(), entry); err != nil {
		t.Fatalf("an already-absent placement must count as released: %v", err)
	}
}

// TestReleaseClassificationMatchesProviderErrors guards the wiring: if the
// provider's typed errors ever stop being classifiable, every failure silently
// becomes "terminal" and transient blips would stop being retried.
func TestReleaseClassificationMatchesProviderErrors(t *testing.T) {
	if !customerror.IsRetriableError(transientErr()) {
		t.Error("a 503 from the provider must classify as retriable")
	}
	if customerror.IsRetriableError(terminalErr()) {
		t.Error("a 403 from the provider must NOT classify as retriable")
	}
	// An unclassified error defaults to terminal, which is the right direction:
	// the defect being fixed is invisibility, so an unknown failure should be
	// escalated rather than retried in silence forever.
	if customerror.IsRetriableError(errors.New("something nobody classified")) {
		t.Error("an unclassified error must default to terminal so it becomes visible")
	}
}

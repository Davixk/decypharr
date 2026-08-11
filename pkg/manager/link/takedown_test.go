package link

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// withPrune turns the destructive consent knob on for one test and puts it back.
// PRUNE is the operator's existing "decypharr may delete dead content locally"
// switch; the takedown path deliberately reuses it rather than inventing a
// second one that could default to deleting things.
func withPrune(t *testing.T, enabled bool) {
	t.Helper()
	// The config singleton is loaded on first Get, which may be this line when
	// the takedown tests are run on their own. Point it somewhere writable
	// first, or the load aborts the process.
	config.SetConfigPath(t.TempDir())
	cfg := config.Get()
	previous := cfg.Repair.Prune
	cfg.Repair.Prune = enabled
	t.Cleanup(func() { cfg.Repair.Prune = previous })
}

func withTakedownThreshold(t *testing.T, threshold int) {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	cfg := config.Get()
	previous := cfg.DebridTakedownThreshold
	cfg.DebridTakedownThreshold = threshold
	t.Cleanup(func() { cfg.DebridTakedownThreshold = previous })
}

func takedownEntry(hash string, filenames ...string) *storage.Entry {
	entry := linkLifecycleEntry(hash, "provider", "id-takedown")
	placement := entry.Providers["provider"]
	entry.Files = make(map[string]*storage.File, len(filenames))
	placement.Files = make(map[string]*storage.ProviderFile, len(filenames))
	for _, name := range filenames {
		entry.Files[name] = &storage.File{Name: name, Size: 100, InfoHash: hash}
		placement.Files[name] = &storage.ProviderFile{Id: "file-" + name, Link: "https://example.invalid/" + name}
	}
	return entry
}

// TestOutageClassNeverCondemnsNoMatterHowManyCyclesFail is the read-path half of
// the outage guarantee.
//
// RealDebrid codes 19 and 24 arrive as HosterUnavailableError and drive the
// inline re-insertion recovery. Exhausting that recovery used to set the durable
// Bad flag — so an entry could be permanently condemned by a provider having a
// bad hour, and the LONGER the outage ran the more certain the condemnation
// looked, because more cycles had failed. Bad short-circuits every later read to
// a permanent 410 and only a successful re-insertion clears it, so the damage
// outlived the outage indefinitely.
func TestOutageClassNeverCondemnsNoMatterHowManyCyclesFail(t *testing.T) {
	entry := takedownEntry(strings.Repeat("1", 40), "Movie.mkv")
	client := &countingClient{err: customerror.HosterUnavailableError}

	repairs := atomic.Int32{}
	saves := atomic.Int32{}
	svc := newLinkService(t, client,
		func(context.Context, *storage.Entry) error { repairs.Add(1); return nil },
		func(*storage.Entry) error { saves.Add(1); return nil },
	)

	// Several full read attempts, each running the recovery to exhaustion —
	// what an outage actually looks like from the mount.
	for round := 1; round <= 3; round++ {
		_, err := svc.GetLink(context.Background(), entry, "Movie.mkv")
		if err == nil {
			t.Fatalf("round %d: hoster-unavailable resolved successfully", round)
		}
		if entry.Bad {
			t.Fatalf("round %d: a transient provider failure condemned the entry", round)
		}
	}

	if got := repairs.Load(); got != int32(3*MaxReinsertionAttempt) {
		t.Fatalf("re-insertion recovery ran %d times, want %d — the recovery itself must survive the fix",
			got, 3*MaxReinsertionAttempt)
	}
	if got := saves.Load(); got != 0 {
		t.Fatalf("a transient failure persisted entry state %d times, want 0", got)
	}
}

// TestExhaustedTransientSurfacesRetryable pins the status the reader gets. A
// bare error becomes a generic 500 in the WebDAV layer, and 500 is retryable, so
// rclone retries it forever instead of converting to EIO. The provider said 503;
// the reader gets 503.
func TestExhaustedTransientSurfacesRetryable(t *testing.T) {
	entry := takedownEntry(strings.Repeat("2", 40), "Movie.mkv")
	client := &countingClient{err: customerror.HosterUnavailableError}
	svc := newLinkService(t, client,
		func(context.Context, *storage.Entry) error { return nil },
		func(*storage.Entry) error { return nil },
	)

	_, err := svc.GetLink(context.Background(), entry, "Movie.mkv")
	if err == nil {
		t.Fatal("hoster-unavailable resolved successfully")
	}
	if customerror.IsContentPermanentlyGone(err) {
		t.Fatalf("a transient provider failure surfaced as a permanent content verdict: %v", err)
	}
	if !customerror.IsRetriableError(err) {
		t.Fatalf("exhausted transient error is not retryable: %v", err)
	}
}

// TestConfirmedTakedownCondemnsAndPrunes is the behaviour ORDER 1(b) asks for,
// end to end: a takedown is a correct Bad verdict AND it must not stay invisible.
//
// Bad alone only stops the content being SERVED; the entry stays in the mount
// listing, so the library symlink still resolves and the arr never notices. PRUNE
// is what makes it stop being LISTED — the same DeleteEntry the repair sweep
// performs, no arr calls anywhere, symlink left dangling for the arr's own scan.
func TestConfirmedTakedownCondemnsAndPrunes(t *testing.T) {
	entry := takedownEntry(strings.Repeat("3", 40), "Movie.mkv")
	client := &countingClient{err: customerror.NewContentTakedownError(fmt.Errorf("infringing_file (code 35)"))}
	withPrune(t, true)
	withTakedownThreshold(t, 1)

	reinsertions := atomic.Int32{}
	pruned := atomic.Int32{}
	svc := newLinkServiceWith(t, client,
		func(context.Context, *storage.Entry) error { reinsertions.Add(1); return nil },
		func(context.Context, *storage.Entry) error { return fmt.Errorf("no other debrid has it") },
		func(*storage.Entry) error { return nil },
		func(*storage.Entry) error { pruned.Add(1); return nil },
	)

	_, err := svc.GetLink(context.Background(), entry, "Movie.mkv")
	if err == nil {
		t.Fatal("a taken-down file resolved successfully")
	}
	if !entry.Bad {
		t.Fatal("a confirmed legal takedown did not condemn the entry")
	}
	if got := pruned.Load(); got != 1 {
		t.Fatalf("PRUNE ran %d times for a confirmed takedown, want 1 — without it the entry stays listed and the arr never re-searches", got)
	}
	// Re-submitting the magnet to the provider that legally removed the release
	// cannot work, and doing it on every read is the refusal storm this exists
	// to stop.
	if got := reinsertions.Load(); got != 0 {
		t.Fatalf("a takedown drove %d re-insertions on the provider that issued it, want 0", got)
	}
	if got := client.downloadLinkCalls.Load(); got != 1 {
		t.Fatalf("a takedown cost %d provider link calls, want 1", got)
	}
}

// TestTakedownWithPruneDisabledStillCondemnsButDoesNotDelete pins the honest
// degradation. The destructive step stays behind the operator's existing
// destructive consent; nothing new defaults to deleting content.
func TestTakedownWithPruneDisabledStillCondemnsButDoesNotDelete(t *testing.T) {
	entry := takedownEntry(strings.Repeat("4", 40), "Movie.mkv")
	client := &countingClient{err: customerror.NewContentTakedownError(nil)}
	withPrune(t, false)
	withTakedownThreshold(t, 1)

	pruned := atomic.Int32{}
	svc := newLinkServiceWith(t, client, nil,
		func(context.Context, *storage.Entry) error { return fmt.Errorf("no other debrid has it") },
		func(*storage.Entry) error { return nil },
		func(*storage.Entry) error { pruned.Add(1); return nil },
	)

	if _, err := svc.GetLink(context.Background(), entry, "Movie.mkv"); err == nil {
		t.Fatal("a taken-down file resolved successfully")
	}
	if !entry.Bad {
		t.Fatal("a confirmed takedown did not condemn the entry")
	}
	if got := pruned.Load(); got != 0 {
		t.Fatalf("PRUNE deleted an entry %d times with repair.prune off, want 0", got)
	}
}

// TestPartiallyTakenDownEntryIsNotCondemned pins the whole-entry rule. It is the
// same policy pruneIneligibleReason applies in the repair sweep — refusing to
// delete a multi-file release because some of its files died — enforced here
// because the read path only ever sees one file at a time.
func TestPartiallyTakenDownEntryIsNotCondemned(t *testing.T) {
	entry := takedownEntry(strings.Repeat("5", 40), "S01E01.mkv", "S01E02.mkv")
	client := &countingClient{err: customerror.NewContentTakedownError(nil)}
	withPrune(t, true)
	withTakedownThreshold(t, 1)

	pruned := atomic.Int32{}
	svc := newLinkServiceWith(t, client, nil,
		func(context.Context, *storage.Entry) error { return fmt.Errorf("no other debrid has it") },
		func(*storage.Entry) error { return nil },
		func(*storage.Entry) error { pruned.Add(1); return nil },
	)

	if _, err := svc.GetLink(context.Background(), entry, "S01E01.mkv"); err == nil {
		t.Fatal("a taken-down file resolved successfully")
	}
	if entry.Bad {
		t.Fatal("one taken-down file condemned a two-file entry; its surviving file still serves")
	}
	if got := pruned.Load(); got != 0 {
		t.Fatalf("PRUNE deleted a partially-dead entry %d times, want 0", got)
	}

	// Once every live file has been confirmed, the entry as a whole IS dead.
	if _, err := svc.GetLink(context.Background(), entry, "S01E02.mkv"); err == nil {
		t.Fatal("a taken-down file resolved successfully")
	}
	if !entry.Bad {
		t.Fatal("an entry whose every file was taken down was not condemned")
	}
	if got := pruned.Load(); got != 1 {
		t.Fatalf("PRUNE ran %d times once the whole entry was dead, want 1", got)
	}
}

// TestTakedownReacquiredOnAnotherDebridIsNotCondemned pins the asymmetry. A
// takedown is provider-scoped; content alive on another debrid is good content,
// and condemning it would cost a re-download plus an indexer search for nothing.
func TestTakedownReacquiredOnAnotherDebridIsNotCondemned(t *testing.T) {
	hash := strings.Repeat("6", 40)
	entry := takedownEntry(hash, "Movie.mkv")
	client := &countingClient{err: customerror.NewContentTakedownError(nil)}
	withPrune(t, true)
	withTakedownThreshold(t, 1)

	// Answers the HEAD that validateLink makes, so a read served by the other
	// debrid genuinely succeeds instead of failing on validation.
	validator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(validator.Close)

	config.SetConfigPath(t.TempDir())
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("provider", client)

	pruned := atomic.Int32{}
	svc := New(clients, nil, nil,
		func(_ context.Context, e *storage.Entry) error {
			// Stands in for FixTorrent(skipCurrent=true) landing the entry on a
			// debrid that still serves it.
			client.err = nil
			client.link = debridTypes.DownloadLink{
				Debrid:       "provider",
				Filename:     "Movie.mkv",
				Link:         "https://example.invalid/id",
				DownloadLink: validator.URL,
				Generated:    time.Now(),
				ExpiresAt:    time.Now().Add(time.Hour),
			}
			e.Bad = false
			return nil
		},
		func(*storage.Entry) error { return nil },
		func(*storage.Entry) error { pruned.Add(1); return nil },
		validator.Client(), 0,
		func() time.Duration { return 30 * time.Second }, zerolog.Nop(),
	)

	if _, err := svc.GetLink(context.Background(), entry, "Movie.mkv"); err != nil {
		t.Fatalf("content re-acquired on another debrid did not resolve: %v", err)
	}
	if entry.Bad {
		t.Fatal("content that another debrid still serves was condemned")
	}
	if got := pruned.Load(); got != 0 {
		t.Fatalf("PRUNE deleted %d entries that another debrid still serves, want 0", got)
	}
}

// TestTakedownThresholdDemandsCorroboration covers the knob: an operator who
// distrusts a provider's classification can require more than one refusal.
func TestTakedownThresholdDemandsCorroboration(t *testing.T) {
	entry := takedownEntry(strings.Repeat("7", 40), "Movie.mkv")
	client := &countingClient{err: customerror.NewContentTakedownError(nil)}
	withPrune(t, true)
	withTakedownThreshold(t, 2)

	pruned := atomic.Int32{}
	svc := newLinkServiceWith(t, client, nil,
		func(context.Context, *storage.Entry) error { return fmt.Errorf("no other debrid has it") },
		func(*storage.Entry) error { return nil },
		func(*storage.Entry) error { pruned.Add(1); return nil },
	)

	if _, err := svc.GetLink(context.Background(), entry, "Movie.mkv"); err == nil {
		t.Fatal("a taken-down file resolved successfully")
	}
	if entry.Bad {
		t.Fatal("one refusal condemned the entry with the threshold set to two")
	}
	if _, err := svc.GetLink(context.Background(), entry, "Movie.mkv"); err == nil {
		t.Fatal("a taken-down file resolved successfully")
	}
	if !entry.Bad {
		t.Fatal("two refusals did not reach the configured threshold")
	}
	if got := pruned.Load(); got != 1 {
		t.Fatalf("PRUNE ran %d times, want 1", got)
	}
}

// TestNegativeTakedownThresholdDisablesTheVerdict covers the escape hatch: the
// refusal still reaches the reader with its real cause, but nothing is ever
// condemned or deleted because of it.
func TestNegativeTakedownThresholdDisablesTheVerdict(t *testing.T) {
	entry := takedownEntry(strings.Repeat("8", 40), "Movie.mkv")
	client := &countingClient{err: customerror.NewContentTakedownError(nil)}
	withPrune(t, true)
	withTakedownThreshold(t, -1)

	pruned := atomic.Int32{}
	svc := newLinkServiceWith(t, client, nil,
		func(context.Context, *storage.Entry) error { return fmt.Errorf("no other debrid has it") },
		func(*storage.Entry) error { return nil },
		func(*storage.Entry) error { pruned.Add(1); return nil },
	)

	_, err := svc.GetLink(context.Background(), entry, "Movie.mkv")
	if !customerror.IsContentTakedown(err) {
		t.Fatalf("GetLink error = %v, want the provider's real cause to survive", err)
	}
	if entry.Bad || pruned.Load() != 0 {
		t.Fatalf("takedown verdict acted with the knob disabled: bad=%t pruned=%d", entry.Bad, pruned.Load())
	}
}

// TestTakedownIsAPermanentContentVerdict pins the typed contract the serve path
// and the repair probe both read. They must agree, and 451 must not be
// retryable — a retry loop that masks a takedown as a flap is how a dead entry
// re-refuses reads hundreds of times a day.
func TestTakedownIsAPermanentContentVerdict(t *testing.T) {
	err := customerror.NewContentTakedownError(fmt.Errorf("infringing_file"))
	if !customerror.IsContentTakedown(err) {
		t.Fatal("IsContentTakedown does not recognise its own constructor")
	}
	if !customerror.IsContentPermanentlyGone(err) {
		t.Fatal("a takedown is not a permanent content verdict; PROPFIND would keep advertising it and the probe would not condemn it")
	}
	if customerror.IsRetriableError(err) {
		t.Fatal("a legal takedown was reported as retryable")
	}
	if got := customerror.FromError(err).StatusCode(); got != http.StatusUnavailableForLegalReasons {
		t.Fatalf("takedown status = %d, want 451", got)
	}
	// The chain walk matters: the re-insertion path joins a provider error with
	// its compensating cleanup, so errors.As can land on the wrong branch.
	joined := errors.Join(fmt.Errorf("clean up placement: %w", errors.New("boom")), err)
	if !customerror.IsContentTakedown(joined) {
		t.Fatal("IsContentTakedown missed a takedown buried in an errors.Join tree")
	}
	// And it must stay narrower than IsContentPermanentlyGone.
	if customerror.IsContentTakedown(customerror.NewContentGoneError(nil)) {
		t.Fatal("IsContentTakedown answered true for a plain content-gone error")
	}
}

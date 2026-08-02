package manager

import (
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// The predicate is ZERO BYTES OVER A WINDOW, and it deliberately does not read
// seeders.
//
// The design it replaces tested sustained seeders==0 AND sustained progress==0,
// which needs a sampling buffer because seeder counts fluctuate. Progress does
// not: it is monotonic, so "0 now, added an hour ago" already proves zero bytes
// across the whole hour with no samples at all. And seeders adds nothing on
// top — if seeders had been present and useful, bytes would have moved.
//
// These tests exist mostly to stop seeders being reintroduced as a condition,
// and to stop the window being dropped as an optimisation.

func stallEntry(mods func(*storage.Entry)) *storage.Entry {
	e := &storage.Entry{
		InfoHash: "abc",
		Name:     "Some.Release",
		Protocol: config.ProtocolTorrent,
		Status:   debridTypes.TorrentStatusDownloading,
		State:    storage.EntryStateDownloading,
		Size:     1000,
		Progress: 0,
		AddedOn:  time.Now().Add(-2 * time.Hour),
	}
	if mods != nil {
		mods(e)
	}
	return e
}

func TestStalledWhenNoBytesMovedForTheWindow(t *testing.T) {
	got := stalledDownloadReason(stallEntry(nil), time.Hour, time.Now())
	if got == "" {
		t.Fatal("a torrent the provider calls 'downloading' with zero bytes after 2h must be stalled")
	}
}

// Seeders must not be consulted in either direction. A stalled torrent
// reporting seeders is still stalled — bytes are what matter — and a healthy
// torrent reporting none is not.
func TestSeedersAreNotPartOfThePredicate(t *testing.T) {
	withSeeders := stallEntry(func(e *storage.Entry) { e.Seeders = 12 })
	if stalledDownloadReason(withSeeders, time.Hour, time.Now()) == "" {
		t.Fatal("seeders must not rescue an entry that has moved zero bytes for the window: " +
			"if those seeders were useful, bytes would have moved")
	}

	movingNoSeeders := stallEntry(func(e *storage.Entry) {
		e.Seeders = 0
		e.Progress = 0.01
	})
	if stalledDownloadReason(movingNoSeeders, time.Hour, time.Now()) != "" {
		t.Fatal("an entry that is transferring must never be pruned, whatever its seeder count says")
	}
}

func TestNotStalledBeforeTheWindowElapses(t *testing.T) {
	fresh := stallEntry(func(e *storage.Entry) { e.AddedOn = time.Now().Add(-5 * time.Minute) })
	if got := stalledDownloadReason(fresh, time.Hour, time.Now()); got != "" {
		t.Fatalf("reason = %q, want empty: zero progress is the NORMAL state of a recent add, and the "+
			"window is the entire diagnostic content of this test", got)
	}
}

func TestNeverPrunesWhatItCannotJudge(t *testing.T) {
	cases := []struct {
		name  string
		entry *storage.Entry
	}{
		{"any progress at all", stallEntry(func(e *storage.Entry) { e.Progress = 0.0001 })},
		{"provider has it queued, not downloading", stallEntry(func(e *storage.Entry) {
			e.Status = debridTypes.TorrentStatusQueued
		})},
		{"already downloaded", stallEntry(func(e *storage.Entry) {
			e.Status = debridTypes.TorrentStatusDownloaded
			e.Progress = 1
		})},
		{"no added timestamp to measure against", stallEntry(func(e *storage.Entry) { e.AddedOn = time.Time{} })},
		{"usenet: no swarm, own add-time gate", stallEntry(func(e *storage.Entry) {
			e.Protocol = config.ProtocolNZB
		})},
		{"nil entry", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := stalledDownloadReason(tc.entry, time.Hour, time.Now()); got != "" {
				t.Fatalf("reason = %q, want empty — this entry must not be deleted", got)
			}
		})
	}
}

// A zero or unset window disables the predicate outright. This deletes data, so
// an unreadable setting must mean "do nothing" and never "fall back to a
// default" — the opposite of how most config defaults behave, and deliberate.
func TestDisabledWindowNeverPrunes(t *testing.T) {
	for _, window := range []time.Duration{0, -time.Hour} {
		if got := stalledDownloadReason(stallEntry(nil), window, time.Now()); got != "" {
			t.Fatalf("window %v produced %q, want empty: a disabled or invalid window must delete nothing", window, got)
		}
	}
}

func TestStallPruneWindowDefaultsToDisabled(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	if got := stallPruneWindow(); got != 0 {
		t.Fatalf("stallPruneWindow() = %v with no configuration, want 0 (disabled)", got)
	}

	config.Get().StallPruneAfter = "not-a-duration"
	if got := stallPruneWindow(); got != 0 {
		t.Fatalf("stallPruneWindow() = %v for an unparseable value, want 0: a misconfigured destructive "+
			"knob must disable itself rather than pick a threshold on the operator's behalf", got)
	}

	config.Get().StallPruneAfter = "45m"
	if got := stallPruneWindow(); got != 45*time.Minute {
		t.Fatalf("stallPruneWindow() = %v, want 45m", got)
	}
}

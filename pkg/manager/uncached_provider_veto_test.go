package manager

import (
	"context"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// A provider's download_uncached is a THREE-state setting, and the middle
// state carries the weight:
//
//	explicit false -> hard veto on every path
//	absent (nil)   -> no opinion; the Arr decides
//	explicit true  -> permitted; the Arr still decides
//
// The tests below are the negative controls for that change. The first proves
// the veto now holds where it previously did not; the second proves it did not
// over-correct into a blanket AND, which would have silently switched off
// uncached downloads for every config that only ever set the Arr-level flag.

// TestProviderNilKeepsArrUncachedWorking is the over-correction control. It
// passes both before and after the veto change, on purpose: a provider with no
// download_uncached key at all has expressed no opinion, and the historical
// config shape — Arr-level true, nothing set per-provider — must keep working
// exactly as it always has. If this ever fails, the fix has been "simplified"
// into providerAllows && requestOverride and every such setup has stopped
// downloading uncached releases without saying so.
func TestProviderNilKeepsArrUncachedWorking(t *testing.T) {
	allowed, vetoed := resolveDownloadUncached(nil, boolPointer(true))
	if !allowed {
		t.Fatal("provider with no download_uncached key must not block an Arr that asks for uncached: " +
			"nil means 'no opinion', not 'no'")
	}
	if vetoed {
		t.Fatal("nothing was vetoed: the provider expressed no opinion")
	}

	// The mirror, so the nil case is pinned in both directions.
	if allowed, _ := resolveDownloadUncached(nil, nil); allowed {
		t.Fatal("with neither the provider nor the Arr expressing a preference, the historical default (false) must stand")
	}
}

// TestProviderVetoIsIndependentOfFallbackOnFailure is the regression guard for
// the actual defect. The veto used to apply only while walking a
// multi-provider fallback chain, so a provider-level "no" survived purely as a
// side effect of the Arr's fallback_on_failure toggle — a flag with no visible
// relationship to uncached policy. Flipping it silently re-enabled uncached
// downloads on a provider configured to refuse them.
//
// Pre-fix this FAILS on the fallback=false run: the pinned provider was handed
// downloadUncached=true.
func TestProviderVetoIsIndependentOfFallbackOnFailure(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		name := "fallback_disabled"
		if fallback {
			name = "fallback_enabled"
		}
		t.Run(name, func(t *testing.T) {
			cacheOnly := &fakeDebridClient{
				cfg: config.Debrid{Name: "primary", DownloadUncached: boolPointer(false), Priority: 1},
				checkFn: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
					torrent.Status = debridTypes.TorrentStatusDownloading
					return torrent, nil
				},
			}
			other := &fakeDebridClient{
				cfg: config.Debrid{Name: "secondary", DownloadUncached: boolPointer(false), Priority: 2},
				checkFn: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
					torrent.Status = debridTypes.TorrentStatusDownloading
					return torrent, nil
				},
			}

			manager := fallbackTestManager(cacheOnly, other)
			if _, err := manager.SendToDebrid(context.Background(), fallbackTestRequest("primary", fallback, boolPointer(true))); err == nil {
				t.Fatal("expected every cache-only provider to refuse the uncached release")
			}

			snapshots := cacheOnly.snapshots()
			if len(snapshots) != 1 {
				t.Fatalf("pinned provider attempts = %d, want 1: %+v", len(snapshots), snapshots)
			}
			if snapshots[0].downloadUncached {
				t.Fatalf("provider download_uncached=false was overridden by the Arr with fallback_on_failure=%v. "+
					"The veto must not depend on an unrelated routing toggle", fallback)
			}
		})
	}
}

// TestSelectedCacheOnlyProviderRoutesUncachedToTheNextProvider pins the shape
// of the deployed stack that prompted this change: both Arrs ask for uncached,
// the pinned provider refuses it, and the fallback provider permits it. The
// desired outcome is that uncached grabs reach the permissive provider and
// never start on the cache-only one.
func TestSelectedCacheOnlyProviderRoutesUncachedToTheNextProvider(t *testing.T) {
	uncachedProbe := func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
		torrent.Status = debridTypes.TorrentStatusDownloading
		return torrent, nil
	}
	cacheOnly := &fakeDebridClient{
		cfg:     config.Debrid{Name: "cache-only", DownloadUncached: boolPointer(false), Priority: 1},
		checkFn: uncachedProbe,
	}
	permissive := &fakeDebridClient{
		cfg:     config.Debrid{Name: "permissive", DownloadUncached: boolPointer(true), Priority: 2},
		checkFn: uncachedProbe,
	}

	manager := fallbackTestManager(cacheOnly, permissive)
	torrent, err := manager.SendToDebrid(context.Background(), fallbackTestRequest("cache-only", true, boolPointer(true)))
	if err != nil {
		t.Fatalf("SendToDebrid returned error: %v", err)
	}
	if torrent.Debrid != "permissive" {
		t.Fatalf("uncached release landed on %q, want the provider that permits uncached downloads", torrent.Debrid)
	}

	cacheOnlyAttempts := cacheOnly.snapshots()
	if len(cacheOnlyAttempts) != 1 || cacheOnlyAttempts[0].downloadUncached {
		t.Fatalf("cache-only provider was permitted to start an uncached download: %+v", cacheOnlyAttempts)
	}
	// The probe is submitted and then removed — it occupies a slot on the
	// cache-only provider only transiently.
	if deleted := cacheOnly.deleted(); len(deleted) != 1 || deleted[0] != "cache-only-id" {
		t.Fatalf("cache-only probe was not cleaned up: %v", deleted)
	}

	permissiveAttempts := permissive.snapshots()
	if len(permissiveAttempts) != 1 || !permissiveAttempts[0].downloadUncached {
		t.Fatalf("permissive provider was not allowed to start the uncached download: %+v", permissiveAttempts)
	}
	if deleted := permissive.deleted(); len(deleted) != 0 {
		t.Fatalf("accepted uncached download was deleted: %v", deleted)
	}
}

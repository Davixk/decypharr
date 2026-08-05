package manager

import (
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// ONE TEST: the failsafe, then the ETA.
//
// The stall detector that used to live here is gone. It asked "has this moved
// zero bytes since it was added", which nobody ever specified — it was invented
// in the first version of this feature and then reasoned about for days as
// though it were a requirement. A stopped transfer is caught by an infinite
// ETA and needs no separate rule.
//
// The sampling window is NOT a grace period against stalls. It is the point
// before which no verdict exists, because torrent speeds float constantly and a
// reading taken over a few seconds is noise.

func stallSettings(mods func(*stallPruneSettings)) stallPruneSettings {
	s := stallPruneSettings{
		sampleWindow:   30 * time.Minute,
		maxETA:         24 * time.Hour,
		maxDownloading: 48 * time.Hour,
		maxPerSweep:    25,
	}
	if mods != nil {
		mods(&s)
	}
	return s
}

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

// TestNoVerdictInsideTheSamplingWindow. Zero progress is the NORMAL state of a
// transfer that has just started, and the window exists precisely because a
// measurement taken this early means nothing.
func TestNoVerdictInsideTheSamplingWindow(t *testing.T) {
	fresh := stallEntry(func(e *storage.Entry) { e.AddedOn = time.Now().Add(-5 * time.Minute) })
	if got := prunableReason(fresh, stallSettings(nil), time.Now()); got != "" {
		t.Fatalf("reason = %q; inside the sampling window there is no verdict to reach", got)
	}
}

// TestNoMeasurableRateIsAnInfiniteETA: past the window, a transfer that has
// moved nothing has an infinite ETA, and infinite exceeds any ceiling. This is
// the case the deleted stall detector used to claim as its own.
func TestNoMeasurableRateIsAnInfiniteETA(t *testing.T) {
	dead := stallEntry(nil) // 2h old, zero progress
	if got := prunableReason(dead, stallSettings(nil), time.Now()); got == "" {
		t.Fatal("a transfer with no measurable rate after 2h must be pruned by the ETA test")
	}
}

// TestSlowTransferPrunesOnProjectedETA — the ordinary case the feature exists
// for, and the reason the projection uses an average rather than the
// instantaneous rate.
func TestSlowTransferPrunesOnProjectedETA(t *testing.T) {
	// 1 GB, 1% done over 2h => ~1.4 KB/s => ~200h remaining, over a 24h ceiling.
	crawling := stallEntry(func(e *storage.Entry) {
		e.Size = 1_000_000_000
		e.Progress = 0.01
		e.Speed = 50_000_000 // an instantaneous burst that would project ~20s
	})
	if prunableReason(crawling, stallSettings(nil), time.Now()) == "" {
		t.Fatal("a transfer projecting ~200h must exceed a 24h ceiling — reading the instantaneous " +
			"50 MB/s burst instead would have called it 20 seconds from done")
	}
}

// TestHealthyTransferSurvives is the mirror: a suite that only proves it prunes
// would pass on an implementation that prunes everything.
func TestHealthyTransferSurvives(t *testing.T) {
	healthy := stallEntry(func(e *storage.Entry) {
		e.Size = 1_000_000_000
		e.Progress = 0.75 // 750 MB in 2h => ~104 KB/s => ~40min remaining
	})
	if got := prunableReason(healthy, stallSettings(nil), time.Now()); got != "" {
		t.Fatalf("reason = %q; this transfer finishes comfortably inside the ceiling", got)
	}
}

// TestFailsafePrunesRegardlessOfETA. The backstop needs no measurement at all,
// which is why it still works when nothing else can be computed.
func TestFailsafePrunesRegardlessOfETA(t *testing.T) {
	ancient := stallEntry(func(e *storage.Entry) {
		e.Size = 1_000_000_000
		e.Progress = 0.99 // healthy by every other measure
		e.AddedOn = time.Now().Add(-72 * time.Hour)
	})
	if got := prunableReason(ancient, stallSettings(nil), time.Now()); got == "" {
		t.Fatal("72h exceeds the 48h hard limit and must prune whatever the ETA says")
	}
}

// TestFailsafeMustNotContradictTheTestItBacksUp: a max_downloading_time below
// sample_window + max_eta would delete transfers still inside the ETA they were
// explicitly allowed — the backstop firing before the rule it exists to catch
// failures of. The feature refuses to arm rather than doing that, and refuses
// rather than clamping to a number we invented.
func TestFailsafeMustNotContradictTheTestItBacksUp(t *testing.T) {
	bad := resolveStallPruneSettings(config.StallPruneConfig{
		ETASampleWindow:    "38m",
		MaxETA:             "16h",
		MaxDownloadingTime: "2h", // below 38m + 16h
	})
	if bad.misconfigured == "" {
		t.Fatal("a failsafe below sample_window + max_eta must be refused, not silently clamped")
	}
	if bad.enabled() {
		t.Fatal("a misconfigured stall prune must not arm")
	}
	if got := prunableReason(stallEntry(nil), bad, time.Now()); got != "" {
		t.Fatalf("reason = %q from a refused configuration", got)
	}

	ok := resolveStallPruneSettings(config.StallPruneConfig{
		ETASampleWindow:    "38m",
		MaxETA:             "16h",
		MaxDownloadingTime: "24h",
	})
	if ok.misconfigured != "" || !ok.enabled() {
		t.Fatalf("a valid configuration was refused: %q", ok.misconfigured)
	}
}

func TestNeverPrunesWhatItCannotJudge(t *testing.T) {
	cases := []struct {
		name  string
		entry *storage.Entry
	}{
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
			if got := prunableReason(tc.entry, stallSettings(nil), time.Now()); got != "" {
				t.Fatalf("reason = %q, want empty — this entry must not be deleted", got)
			}
		})
	}
}

// TestBothKnobsRequired: the test is meaningless without either. No window
// means no trustworthy speed; no ceiling means nothing to judge against.
func TestBothKnobsRequired(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  config.StallPruneConfig
	}{
		{"nothing set", config.StallPruneConfig{}},
		{"window only", config.StallPruneConfig{ETASampleWindow: "38m"}},
		{"ceiling only", config.StallPruneConfig{MaxETA: "16h"}},
		{"unparseable", config.StallPruneConfig{ETASampleWindow: "nonsense", MaxETA: "also-nonsense"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := resolveStallPruneSettings(tc.cfg)
			if s.enabled() {
				t.Fatal("stall pruning armed without both a sampling window and a ceiling")
			}
			if got := prunableReason(stallEntry(nil), s, time.Now()); got != "" {
				t.Fatalf("reason = %q while disabled", got)
			}
		})
	}
}

// TestSeedersAreNotConsulted. A seeder count is a proxy; the ETA measures the
// outcome directly. This pins that no seeder logic crept back into the sweep —
// the grab-time gate is a separate feature with its own knob.
func TestSeedersAreNotConsulted(t *testing.T) {
	withSeeders := stallEntry(func(e *storage.Entry) { e.Seeders = 12 })
	if prunableReason(withSeeders, stallSettings(nil), time.Now()) == "" {
		t.Fatal("seeders must not rescue a transfer with no measurable rate")
	}

	healthy := stallEntry(func(e *storage.Entry) {
		e.Size = 1_000_000_000
		e.Progress = 0.75
		e.Seeders = 0
	})
	if got := prunableReason(healthy, stallSettings(nil), time.Now()); got != "" {
		t.Fatalf("reason = %q; a transfer that is moving must not be pruned for reporting no seeders", got)
	}
}

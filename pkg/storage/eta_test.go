package storage

import (
	"testing"
	"time"
)

// eta: 0 was hardcoded, and 0 is not "unknown" in the qBittorrent contract — it
// is "arriving now". Every consumer therefore read a confident wrong value
// rather than an absent one, which is the same defect as a stalled entry
// reporting "Downloading, 0 B, 0%".
//
// The tests below exist mainly to stop 0 coming back: the single most important
// property is that NO input produces an ETA of 0.

func TestUnknownETAIsNeverZero(t *testing.T) {
	cases := []struct {
		name  string
		entry Entry
	}{
		{"stalled: bytes left, no speed", Entry{Size: 1000, Progress: 0.5, Speed: 0}},
		{"fresh add: nothing moved yet", Entry{Size: 1000, Progress: 0, Speed: 0}},
		{"complete", Entry{Size: 1000, Progress: 1, Speed: 0}},
		{"complete but still reporting speed", Entry{Size: 1000, Progress: 1, Speed: 500}},
		{"unknown size", Entry{Size: 0, Progress: 0, Speed: 100}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.entry.ETASeconds(); got != EtaUnknown {
				t.Fatalf("ETASeconds() = %d, want EtaUnknown (%d)", got, EtaUnknown)
			}
			if got := tc.entry.ETASeconds(); got == 0 {
				t.Fatal("ETA of 0 means 'arriving now' to every qBittorrent client — it must never be produced")
			}
		})
	}
}

func TestETAFromCurrentSpeed(t *testing.T) {
	// 1000 bytes, half done, 100 B/s -> 500 remaining -> 5s
	e := Entry{Size: 1000, Progress: 0.5, Speed: 100}
	if got := e.ETASeconds(); got != 5 {
		t.Fatalf("ETASeconds() = %d, want 5", got)
	}
	if got := e.RemainingBytes(); got != 500 {
		t.Fatalf("RemainingBytes() = %d, want 500", got)
	}
	if got := e.DownloadedBytes(); got != 500 {
		t.Fatalf("DownloadedBytes() = %d, want 500", got)
	}
}

// A sub-second remainder must round to 1, not 0. Truncating integer division
// would otherwise reintroduce the sentinel-adjacent value this whole change is
// about.
func TestSubSecondRemainderReportsOneNotZero(t *testing.T) {
	e := Entry{Size: 1000, Progress: 0.999, Speed: 1000}
	if got := e.ETASeconds(); got != 1 {
		t.Fatalf("ETASeconds() = %d, want 1 — integer truncation must not produce 0", got)
	}
}

// The average is what a stall predicate must use. A torrent that sat dead for an
// hour and then briefly touched a high speed has a flattering instantaneous ETA
// and an honest average one; reporting the first would defeat the purpose.
func TestAverageSpeedReflectsLifetimeNotTheCurrentBurst(t *testing.T) {
	e := Entry{
		Size:     1_000_000,
		Progress: 0.01,      // 10 KB transferred
		Speed:    1_000_000, // currently claiming 1 MB/s
		AddedOn:  time.Now().Add(-100 * time.Second),
	}

	// 10,000 bytes over 100s = 100 B/s average, despite the 1 MB/s burst.
	if got := e.AverageSpeed(); got < 90 || got > 110 {
		t.Fatalf("AverageSpeed() = %d, want ~100 B/s", got)
	}

	instant := e.ETASeconds()        // 990,000 / 1,000,000 -> 1s
	average := e.ETAAtAverageSpeed() // 990,000 / 100 -> 9900s
	if average <= instant {
		t.Fatalf("ETA at average (%d) must exceed ETA at the current burst (%d), or stall detection "+
			"can be defeated by a momentary spike", average, instant)
	}
}

func TestAverageSpeedIsZeroWhenUncomputable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry Entry
	}{
		{"no added time", Entry{Size: 1000, Progress: 0.5}},
		{"nothing transferred", Entry{Size: 1000, Progress: 0, AddedOn: time.Now().Add(-time.Hour)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.entry.AverageSpeed(); got != 0 {
				t.Fatalf("AverageSpeed() = %d, want 0", got)
			}
			// And an uncomputable average must yield unknown, not a fake ETA.
			if got := tc.entry.ETAAtAverageSpeed(); got != EtaUnknown {
				t.Fatalf("ETAAtAverageSpeed() = %d, want EtaUnknown", got)
			}
		})
	}
}

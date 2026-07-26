package manager

import (
	"slices"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// sweepOrderBase is the fixture "now". Every fixture timestamp is strictly
// before it, so a simulated probe stamping LastCheckedAt = sweepOrderBase always
// produces the newest timestamp in the set.
var sweepOrderBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// sweepOrderFixtureEntry describes one health record to seed. wantTier is
// declared by hand rather than computed, so these tests pin the intended
// classification instead of restating sweepTierFor.
type sweepOrderFixtureEntry struct {
	name          string
	status        storage.HealthStatus
	lastCheckedAt time.Time
	noRecord      bool // seed no health row at all
	wantTier      sweepTier
}

// sweepOrderMixedFixture is the canonical mixed set. Read it top to bottom: it
// is also the expected probe order.
//
// The load-bearing line is healthy-ancient. It is by 290 days the
// least-recently-checked entry in the whole set, and under the old global
// oldest-first sort it led the queue outright — ahead of every unknown and every
// broken entry. Here it is 9th, behind repairing-recent, which was checked an
// hour ago. That inversion IS the fix.
func sweepOrderMixedFixture() []sweepOrderFixtureEntry {
	return []sweepOrderFixtureEntry{
		// --- tier 1: no usable verdict, oldest-first within the tier ---
		// Two never-checked entries share the zero timestamp and break on name.
		{name: "never-no-record", noRecord: true, wantTier: tierNoVerdict},
		// A `healthy` status that no probe ever stamped. It must NOT reach
		// tierRoutine on the strength of that status.
		{name: "never-zero-stamp", status: storage.HealthHealthy, wantTier: tierNoVerdict},
		{name: "unknown-oldest", status: storage.HealthUnknown, lastCheckedAt: sweepOrderBase.AddDate(0, 0, -10), wantTier: tierNoVerdict},
		{name: "blank-status", status: "", lastCheckedAt: sweepOrderBase.AddDate(0, 0, -8), wantTier: tierNoVerdict},
		{name: "stale-mid", status: storage.HealthStale, lastCheckedAt: sweepOrderBase.AddDate(0, 0, -6), wantTier: tierNoVerdict},
		{name: "repairing-recent", status: storage.HealthRepairing, lastCheckedAt: sweepOrderBase.Add(-time.Hour), wantTier: tierNoVerdict},

		// --- tier 2: broken, actionable now ---
		{name: "broken-oldest", status: storage.HealthBroken, lastCheckedAt: sweepOrderBase.AddDate(0, 0, -9), wantTier: tierBroken},
		{name: "broken-recent", status: storage.HealthBroken, lastCheckedAt: sweepOrderBase.Add(-time.Minute), wantTier: tierBroken},

		// --- tier 3: routine re-verification ---
		{name: "healthy-ancient", status: storage.HealthHealthy, lastCheckedAt: sweepOrderBase.AddDate(0, 0, -400), wantTier: tierRoutine},
		{name: "unsupported-old", status: storage.HealthUnsupported, lastCheckedAt: sweepOrderBase.AddDate(0, 0, -100), wantTier: tierRoutine},
		{name: "healthy-recent", status: storage.HealthHealthy, lastCheckedAt: sweepOrderBase.AddDate(0, 0, -30), wantTier: tierRoutine},
	}
}

// newSweepOrderFixture seeds the health store and returns the Repair plus the
// `due` map to order. It exercises the real storage-backed path, not a
// hand-built slice.
func newSweepOrderFixture(t *testing.T, entries []sweepOrderFixtureEntry) (*Repair, map[string]*candidate) {
	t.Helper()
	_, r := newProbeFixture(t, nil)
	due := make(map[string]*candidate, len(entries))
	for _, e := range entries {
		due[e.name] = &candidate{name: e.name}
		if e.noRecord {
			continue
		}
		sweepOrderStampHealth(t, r, e.name, e.status, e.lastCheckedAt)
	}
	return r, due
}

func sweepOrderStampHealth(t *testing.T, r *Repair, name string, status storage.HealthStatus, at time.Time) {
	t.Helper()
	h := &storage.EntryHealth{EntryName: name, Status: status, LastCheckedAt: at}
	if err := r.manager.storage.SaveEntryHealth(h); err != nil {
		t.Fatalf("SaveEntryHealth(%s): %v", name, err)
	}
}

func sweepOrderNames(entries []sweepOrderFixtureEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.name
	}
	return out
}

// TestSweepTierForClassifiesEveryStatus pins the mapping from the stored
// HealthStatus values to the three tiers, including the never-checked rule that
// overrides the status entirely.
func TestSweepTierForClassifiesEveryStatus(t *testing.T) {
	checked := sweepOrderBase.AddDate(0, 0, -1)
	cases := []struct {
		label string
		h     *storage.EntryHealth
		want  sweepTier
	}{
		{"nil record", nil, tierNoVerdict},
		{"never checked, no status", &storage.EntryHealth{}, tierNoVerdict},
		{"never checked but stamped healthy", &storage.EntryHealth{Status: storage.HealthHealthy}, tierNoVerdict},
		{"never checked but stamped broken", &storage.EntryHealth{Status: storage.HealthBroken}, tierNoVerdict},
		{"unknown", &storage.EntryHealth{Status: storage.HealthUnknown, LastCheckedAt: checked}, tierNoVerdict},
		{"repairing", &storage.EntryHealth{Status: storage.HealthRepairing, LastCheckedAt: checked}, tierNoVerdict},
		{"stale", &storage.EntryHealth{Status: storage.HealthStale, LastCheckedAt: checked}, tierNoVerdict},
		{"empty status", &storage.EntryHealth{Status: "", LastCheckedAt: checked}, tierNoVerdict},
		{"unrecognised status", &storage.EntryHealth{Status: storage.HealthStatus("invented-later"), LastCheckedAt: checked}, tierNoVerdict},
		{"broken", &storage.EntryHealth{Status: storage.HealthBroken, LastCheckedAt: checked}, tierBroken},
		{"healthy", &storage.EntryHealth{Status: storage.HealthHealthy, LastCheckedAt: checked}, tierRoutine},
		{"unsupported", &storage.EntryHealth{Status: storage.HealthUnsupported, LastCheckedAt: checked}, tierRoutine},
	}
	for _, tc := range cases {
		if got := sweepTierFor(tc.h); got != tc.want {
			t.Errorf("sweepTierFor(%s) = %d, want %d", tc.label, got, tc.want)
		}
	}
}

// TestSweepOrderTiersThenOldestFirst is the whole contract in one assertion:
// every tier-1 entry ahead of every tier-2 entry ahead of every tier-3 entry,
// and oldest-LastCheckedAt-first (name-tiebroken) within each tier.
func TestSweepOrderTiersThenOldestFirst(t *testing.T) {
	entries := sweepOrderMixedFixture()
	r, due := newSweepOrderFixture(t, entries)

	want := sweepOrderNames(entries)
	got := r.orderCandidatesBySweepPriority(due)
	if !slices.Equal(got, want) {
		t.Fatalf("order mismatch\n got: %v\nwant: %v", got, want)
	}

	// Independently of the exact permutation, assert the tier blocks never
	// interleave: position of each name must have a non-decreasing tier.
	tierOf := make(map[string]sweepTier, len(entries))
	for _, e := range entries {
		tierOf[e.name] = e.wantTier
	}
	last := tierNoVerdict
	for i, name := range got {
		tier, ok := tierOf[name]
		if !ok {
			t.Fatalf("position %d: unexpected name %q", i, name)
		}
		if tier < last {
			t.Fatalf("position %d (%s): tier %d follows tier %d — tiers interleaved", i, name, tier, last)
		}
		last = tier
	}
}

// TestSweepOrderNeverCheckedBeatsAncientHealthy is the narrow case stated on its
// own: an entry with no verdict at all outranks a healthy entry that has not
// been looked at in over a year, even though the old global oldest-first sort
// ranked them the other way round.
func TestSweepOrderNeverCheckedBeatsAncientHealthy(t *testing.T) {
	entries := []sweepOrderFixtureEntry{
		{name: "zzz-never-checked", noRecord: true, wantTier: tierNoVerdict},
		{name: "aaa-healthy-ancient", status: storage.HealthHealthy, lastCheckedAt: sweepOrderBase.AddDate(-3, 0, 0), wantTier: tierRoutine},
	}
	r, due := newSweepOrderFixture(t, entries)

	got := r.orderCandidatesBySweepPriority(due)
	want := []string{"zzz-never-checked", "aaa-healthy-ancient"}
	if !slices.Equal(got, want) {
		// The name ordering is deliberately adversarial: a plain name sort or a
		// plain oldest-first sort would both produce the reverse.
		t.Fatalf("order = %v, want %v", got, want)
	}
}

// TestSweepOrderTiebreakOnEqualTimestamps pins the stable tiebreak: same tier,
// byte-identical LastCheckedAt, so only the name can decide.
func TestSweepOrderTiebreakOnEqualTimestamps(t *testing.T) {
	at := sweepOrderBase.AddDate(0, 0, -5)
	entries := []sweepOrderFixtureEntry{
		{name: "unknown-a", status: storage.HealthUnknown, lastCheckedAt: at, wantTier: tierNoVerdict},
		{name: "unknown-b", status: storage.HealthUnknown, lastCheckedAt: at, wantTier: tierNoVerdict},
		{name: "unknown-c", status: storage.HealthUnknown, lastCheckedAt: at, wantTier: tierNoVerdict},
		{name: "broken-a", status: storage.HealthBroken, lastCheckedAt: at, wantTier: tierBroken},
		{name: "broken-b", status: storage.HealthBroken, lastCheckedAt: at, wantTier: tierBroken},
		{name: "healthy-a", status: storage.HealthHealthy, lastCheckedAt: at, wantTier: tierRoutine},
		{name: "healthy-b", status: storage.HealthHealthy, lastCheckedAt: at, wantTier: tierRoutine},
	}
	r, due := newSweepOrderFixture(t, entries)

	got := r.orderCandidatesBySweepPriority(due)
	want := []string{"unknown-a", "unknown-b", "unknown-c", "broken-a", "broken-b", "healthy-a", "healthy-b"}
	if !slices.Equal(got, want) {
		t.Fatalf("tiebreak order = %v, want %v", got, want)
	}
}

// TestSweepOrderIsAPermutation proves the ordering neither drops nor duplicates
// a candidate: it is a permutation of the input SET, not merely a slice of the
// same length.
func TestSweepOrderIsAPermutation(t *testing.T) {
	entries := sweepOrderMixedFixture()
	r, due := newSweepOrderFixture(t, entries)

	got := r.orderCandidatesBySweepPriority(due)

	gotSet := make(map[string]int, len(got))
	for _, name := range got {
		gotSet[name]++
	}
	wantSet := make(map[string]int, len(due))
	for name := range due {
		wantSet[name]++
	}
	for name, n := range gotSet {
		switch {
		case wantSet[name] == 0:
			t.Errorf("ordering invented candidate %q", name)
		case n != 1:
			t.Errorf("ordering emitted %q %d times, want 1", name, n)
		}
	}
	for name := range wantSet {
		if gotSet[name] == 0 {
			t.Errorf("ordering dropped candidate %q", name)
		}
	}
	if len(got) != len(due) {
		t.Fatalf("len(order) = %d, want %d", len(got), len(due))
	}
}

// TestSweepOrderTruncatedRunMakesForwardProgress simulates the production
// failure: a sweep cut short after a handful of entries. Stamping the processed
// prefix and re-ordering must move the head forward — the second run must not
// re-probe what the first one already did.
func TestSweepOrderTruncatedRunMakesForwardProgress(t *testing.T) {
	const processed = 3

	// A healthy verdict retires the prefix to tier 3 with the newest timestamp,
	// so run two resumes exactly at the first unprobed candidate.
	t.Run("healthy verdict retires the prefix", func(t *testing.T) {
		entries := sweepOrderMixedFixture()
		r, due := newSweepOrderFixture(t, entries)

		first := r.orderCandidatesBySweepPriority(due)
		for _, name := range first[:processed] {
			sweepOrderStampHealth(t, r, name, storage.HealthHealthy, sweepOrderBase)
		}

		second := r.orderCandidatesBySweepPriority(due)
		if second[0] == first[0] {
			t.Fatalf("second run re-treads the same head %q", second[0])
		}
		if second[0] != first[processed] {
			t.Fatalf("second head = %q, want %q (first unprobed candidate)", second[0], first[processed])
		}
		if !slices.Equal(second[:len(second)-processed], first[processed:]) {
			t.Fatalf("second run did not resume where the first stopped\n got: %v\nwant prefix: %v",
				second, first[processed:])
		}
		// The already-probed prefix must have moved to the tail, not vanished.
		for _, name := range first[:processed] {
			if slices.Index(second[:len(second)-processed], name) >= 0 {
				t.Fatalf("already-probed %q reappears before the unprobed candidates", name)
			}
		}
	})

	// Even when the probe reaches NO verdict again, the prefix must move to the
	// back of its own tier so the head still advances.
	t.Run("unknown verdict still advances the head", func(t *testing.T) {
		entries := sweepOrderMixedFixture()
		r, due := newSweepOrderFixture(t, entries)

		first := r.orderCandidatesBySweepPriority(due)
		for _, name := range first[:processed] {
			sweepOrderStampHealth(t, r, name, storage.HealthUnknown, sweepOrderBase)
		}

		second := r.orderCandidatesBySweepPriority(due)
		if second[0] == first[0] {
			t.Fatalf("second run re-treads the same head %q", second[0])
		}
		if second[0] != first[processed] {
			t.Fatalf("second head = %q, want %q (first unprobed candidate)", second[0], first[processed])
		}
		// Still tier 1 (unknown), so they stay ahead of broken/healthy — but
		// behind every unprobed tier-1 entry.
		for _, name := range first[:processed] {
			if idx := slices.Index(second, name); idx < 3 {
				t.Fatalf("re-probed %q sits at position %d, ahead of unprobed no-verdict entries", name, idx)
			}
		}
	})
}

// BenchmarkSortSweepCandidates measures the sort alone at the production
// library size (~47,600 candidates) with a realistic status mix, so "is the
// tiered comparator affordable" is a number rather than an opinion. It excludes
// the per-candidate GetEntryHealth read, which the previous ordering already
// performed unchanged.
func BenchmarkSortSweepCandidates(b *testing.B) {
	const n = 47_600
	base := make([]sweepCandidate, n)
	for i := range base {
		c := sweepCandidate{
			name:          "entry-" + string(rune('a'+i%26)) + "-" + time.Duration(i).String(),
			lastCheckedAt: sweepOrderBase.Add(-time.Duration(i%9973) * time.Minute),
			tier:          tierRoutine,
		}
		switch {
		case i%400 == 0:
			c.tier = tierNoVerdict
		case i%4000 == 0:
			c.tier = tierBroken
		}
		base[i] = c
	}
	items := make([]sweepCandidate, n)
	b.ReportAllocs()
	for b.Loop() {
		copy(items, base)
		sortSweepCandidates(items)
	}
}

// TestSweepOrderIsDeterministic guards against map-iteration nondeterminism:
// orderCandidatesBySweepPriority walks a Go map, whose iteration order is
// randomised per range, so repeated calls over the same store must still agree
// exactly.
func TestSweepOrderIsDeterministic(t *testing.T) {
	entries := sweepOrderMixedFixture()
	r, due := newSweepOrderFixture(t, entries)

	want := r.orderCandidatesBySweepPriority(due)
	for i := 0; i < 50; i++ {
		got := r.orderCandidatesBySweepPriority(due)
		if !slices.Equal(got, want) {
			t.Fatalf("iteration %d differs\n got: %v\nwant: %v", i, got, want)
		}
	}
}

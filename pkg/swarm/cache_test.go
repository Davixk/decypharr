package swarm

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingSource records how often it was actually consulted.
type countingSource struct {
	calls atomic.Int32
	md    Metadata
	known bool
}

func (c *countingSource) Name() string { return "counting" }

func (c *countingSource) Lookup(context.Context, string, []string) (Metadata, bool) {
	c.calls.Add(1)
	return c.md, c.known
}

// TestCacheServesRepeatedGrabsFromOneLookup is the point of the cache.
//
// Measured against a real tracker: 8 back-to-back scrapes answered 2, and 5
// spaced four seconds apart answered ZERO. Against an operator who runs
// deliberate grab floods, the fix is not to pace requests but to stop making
// most of them — the same release is re-grabbed and re-probed constantly.
func TestCacheServesRepeatedGrabsFromOneLookup(t *testing.T) {
	inner := &countingSource{md: Metadata{Seeders: 9, Source: "counting"}, known: true}
	c := &Cache{Inner: inner, TTL: time.Minute, NegativeTTL: time.Minute}

	for i := 0; i < 20; i++ {
		md, ok := c.Lookup(context.Background(), scrapeTestHash, nil)
		if !ok || md.Seeders != 9 {
			t.Fatalf("lookup %d: md=%+v ok=%v", i, md, ok)
		}
	}
	if got := inner.calls.Load(); got != 1 {
		t.Fatalf("20 grabs of one hash cost %d lookups, want 1", got)
	}
}

// TestCacheRemembersAnUnansweredLookup: re-asking a tracker that just refused
// is what deepens a cumulative penalty. A remembered unknown still ALLOWS, so
// this can never turn a lookup failure into a refusal.
func TestCacheRemembersAnUnansweredLookup(t *testing.T) {
	inner := &countingSource{known: false}
	c := &Cache{Inner: inner, TTL: time.Minute, NegativeTTL: time.Minute}

	for i := 0; i < 10; i++ {
		if _, ok := c.Lookup(context.Background(), scrapeTestHash, nil); ok {
			t.Fatal("a cached unknown must stay unknown, which allows")
		}
	}
	if got := inner.calls.Load(); got != 1 {
		t.Fatalf("10 grabs after an unanswered lookup cost %d, want 1", got)
	}
}

// TestNegativeEntriesExpireSoonerThanPositiveOnes. A transient block must not
// blind the gate for as long as a good reading stays valid.
func TestNegativeEntriesExpireSoonerThanPositiveOnes(t *testing.T) {
	inner := &countingSource{known: false}
	c := &Cache{Inner: inner, TTL: time.Hour, NegativeTTL: 30 * time.Millisecond}

	if _, ok := c.Lookup(context.Background(), scrapeTestHash, nil); ok {
		t.Fatal("expected unknown")
	}
	time.Sleep(60 * time.Millisecond)
	if _, ok := c.Lookup(context.Background(), scrapeTestHash, nil); ok {
		t.Fatal("expected unknown")
	}
	if got := inner.calls.Load(); got != 2 {
		t.Fatalf("lookups = %d, want 2: an expired negative entry must be retried", got)
	}
}

func TestCacheExpiresPositiveEntries(t *testing.T) {
	inner := &countingSource{md: Metadata{Seeders: 3}, known: true}
	c := &Cache{Inner: inner, TTL: 30 * time.Millisecond, NegativeTTL: time.Hour}

	_, _ = c.Lookup(context.Background(), scrapeTestHash, nil)
	time.Sleep(60 * time.Millisecond)
	_, _ = c.Lookup(context.Background(), scrapeTestHash, nil)

	if got := inner.calls.Load(); got != 2 {
		t.Fatalf("lookups = %d, want 2: a stale reading must be refreshed", got)
	}
}

// TestCacheKeysByHash: one release's reading must never answer for another's.
func TestCacheKeysByHash(t *testing.T) {
	inner := &countingSource{md: Metadata{Seeders: 5}, known: true}
	c := &Cache{Inner: inner, TTL: time.Minute, NegativeTTL: time.Minute}

	_, _ = c.Lookup(context.Background(), scrapeTestHash, nil)
	_, _ = c.Lookup(context.Background(), "ffffffffffffffffffffffffffffffffffffffff", nil)
	if got := inner.calls.Load(); got != 2 {
		t.Fatalf("lookups = %d, want 2: distinct hashes must not share a cache entry", got)
	}

	// Case must not create a second entry for the same torrent.
	_, _ = c.Lookup(context.Background(), "0123456789ABCDEF0123456789ABCDEF01234567", nil)
	if got := inner.calls.Load(); got != 2 {
		t.Fatalf("lookups = %d; hash lookup must be case-insensitive", got)
	}
}

// TestCacheIsConcurrencySafe — grabs arrive in parallel bursts by design.
func TestCacheIsConcurrencySafe(t *testing.T) {
	inner := &countingSource{md: Metadata{Seeders: 2}, known: true}
	c := &Cache{Inner: inner, TTL: time.Minute, NegativeTTL: time.Minute}

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := c.Lookup(context.Background(), scrapeTestHash, nil); !ok {
				t.Error("concurrent lookup lost the reading")
			}
		}()
	}
	wg.Wait()

	// Not single-flighted on purpose — a few concurrent misses are cheaper than
	// holding a lock across a network call inside a blocking add. The assertion
	// is that it collapses, not that it collapses perfectly.
	if got := inner.calls.Load(); got > 64 {
		t.Fatalf("lookups = %d, want well under 64", got)
	}
}

func TestCacheWithNoInnerIsUnknown(t *testing.T) {
	c := &Cache{TTL: time.Minute}
	if _, ok := c.Lookup(context.Background(), scrapeTestHash, nil); ok {
		t.Fatal("a cache with no source must report unknown")
	}
}

package hybrid

import (
	"fmt"
	"sync"
	"testing"
)

// TestIndexExistsImpliesIterable pins the invariant the queue layer depends on:
// if Get(key) resolves, KeysSortedByOffset must include that key.
//
// Storage.QueueExists is a bare index lookup, while every listing endpoint
// (/api/torrents, the qbit /torrents/info shim) walks Store.ForEach, which
// iterates KeysSortedByOffset. If the two disagree, an entry becomes
// simultaneously "already exists" for the reservation check in Queue.Add and
// invisible to every arr that polls — the entry can neither be surfaced nor
// re-added.
//
// The invariant is not obviously guaranteed by construction: Put and Delete
// mutate idx.entries (lock-free) outside the idx.mu section that maintains
// sortedDirty/sortedKeys, and the cache-validity test is a length comparison
// standing in for a membership comparison. This test exercises the
// delete-then-re-add-under-the-same-Size() shape that would be needed to
// satisfy "!sortedDirty && len(sortedKeys) == Size()" against stale membership.
// It currently passes — the window is narrower than the code shape suggests —
// so this is a guard against that changing, not a reproduction of a known bug.
func TestIndexExistsImpliesIterable(t *testing.T) {
	const (
		steadyKeys = 64
		rounds     = 300
	)

	idx := newIndex()
	entryFor := func(i int) *IndexEntry {
		return &IndexEntry{Offset: int64(i * 128), Size: 64}
	}

	// Steady-state population, then prime the sorted cache so it starts clean.
	for i := 0; i < steadyKeys; i++ {
		idx.Put(fmt.Sprintf("steady-%03d", i), entryFor(i))
	}
	_ = idx.KeysSortedByOffset()

	for round := 0; round < rounds; round++ {
		victim := fmt.Sprintf("steady-%03d", round%steadyKeys)
		arrival := fmt.Sprintf("arrival-%04d", round)

		var wg sync.WaitGroup
		wg.Add(3)

		// A delete and an add that net out to the same Size(), plus a
		// concurrent scan — the shape produced when an entry is rolled back
		// and immediately re-added under the same infohash.
		go func() { defer wg.Done(); idx.Delete(victim) }()
		go func() { defer wg.Done(); idx.Put(arrival, entryFor(steadyKeys+round)) }()
		go func() { defer wg.Done(); _ = idx.KeysSortedByOffset() }()
		wg.Wait()

		iterable := make(map[string]struct{})
		for _, k := range idx.KeysSortedByOffset() {
			iterable[k] = struct{}{}
		}

		for _, key := range []string{arrival, victim} {
			_, exists := idx.entries.Load(key)
			_, listed := iterable[key]
			if exists && !listed {
				t.Fatalf("round %d: key %q resolves via Get (Exists=true) but is absent from "+
					"KeysSortedByOffset — it would be rejected as a duplicate on re-add while "+
					"never appearing in any listing", round, key)
			}
			if !exists && listed {
				t.Fatalf("round %d: key %q was deleted but is still iterable", round, key)
			}
		}

		// Restore the steady key for the next round.
		idx.Put(victim, entryFor(round%steadyKeys))
	}
}

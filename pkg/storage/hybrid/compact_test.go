package hybrid

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	config.SetConfigPath(dir)
	s, err := New(Config{DataPath: filepath.Join(dir, "store.log"), SyncInterval: -1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// COMPACTION MUST NOT STALL READERS.
//
// The old implementation held the exclusive lock for the entire rewrite — every
// live record read off disk and appended to a new log — so a large store meant
// minutes during which every Get blocked. Measured in production as a ~2 minute
// API blackout and 420 *arr client errors, triggered by a repair sweep spiking
// the dead ratio.
//
// This asserts the property that fixes it: reads keep completing WHILE a
// compaction runs. It is deliberately a liveness assertion rather than a timing
// one — no wall-clock threshold to go flaky on CI — so it fails by deadlocking
// against the old code rather than by being slow.
func TestCompactionDoesNotBlockReaders(t *testing.T) {
	s := testStore(t)

	const keys = 400
	for i := range keys {
		if err := s.Put(fmt.Sprintf("key-%04d", i), []byte(fmt.Sprintf("value-%04d", i)), nil); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	// Overwrite everything so there is real dead weight to compact away.
	for i := range keys {
		if err := s.Put(fmt.Sprintf("key-%04d", i), []byte(fmt.Sprintf("v2-%04d", i)), nil); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Deterministic, not timing-based: the seam fires at the point where the old
	// implementation held the exclusive lock. A read issued from another
	// goroutine there must complete. Under the old code it blocks until the
	// whole rewrite finishes, so this fails by timeout rather than by luck.
	var reads atomic.Int64
	compactionYield = func() {
		read := make(chan struct{})
		go func() {
			defer close(read)
			if _, err := s.Get("key-0000"); err != nil {
				t.Errorf("Get during compaction: %v", err)
				return
			}
			reads.Add(1)
		}()
		select {
		case <-read:
		case <-time.After(10 * time.Second):
			t.Error("a read could not complete while the compaction was rewriting; the store lock is " +
				"held across the rewrite, which is what blacked out the API for two minutes")
		}
	}
	t.Cleanup(func() { compactionYield = nil })

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if reads.Load() == 0 {
		t.Fatal("no read completed during the compaction")
	}

	// And the data survived.
	for i := range keys {
		got, err := s.Get(fmt.Sprintf("key-%04d", i))
		if err != nil {
			t.Fatalf("Get after compaction: %v", err)
		}
		if want := fmt.Sprintf("v2-%04d", i); string(got) != want {
			t.Fatalf("key-%04d = %q, want %q", i, got, want)
		}
	}
}

// WRITES THAT LAND DURING A COMPACTION MUST SURVIVE IT.
//
// This is the risk the lock used to buy: with the rewrite no longer holding the
// lock, a concurrent Put goes into the OLD log after the snapshot was taken, and
// the swap would drop it unless phase 3 catches it up. Same for a Delete, which
// must not come back from the dead.
func TestCompactionPreservesConcurrentWrites(t *testing.T) {
	s := testStore(t)

	const keys = 300
	for i := range keys {
		if err := s.Put(fmt.Sprintf("key-%04d", i), []byte("original"), nil); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	for i := range keys {
		if err := s.Put(fmt.Sprintf("key-%04d", i), []byte("second"), nil); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	writesDone := make(chan struct{})
	go func() {
		defer wg.Done()
		defer close(writesDone)
		// Written after the compaction has begun; these are the records phase 3
		// exists to rescue.
		for i := range 50 {
			if err := s.Put(fmt.Sprintf("late-%04d", i), []byte("late"), nil); err != nil {
				t.Errorf("late Put: %v", err)
				return
			}
		}
		for i := range 20 {
			if err := s.Delete(fmt.Sprintf("key-%04d", i)); err != nil {
				t.Errorf("late Delete: %v", err)
				return
			}
		}
	}()

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	wg.Wait()
	<-writesDone

	for i := range 50 {
		got, err := s.Get(fmt.Sprintf("late-%04d", i))
		if err != nil {
			t.Fatalf("a write that landed during compaction was lost: late-%04d: %v", i, err)
		}
		if string(got) != "late" {
			t.Fatalf("late-%04d = %q, want %q", i, got, "late")
		}
	}
	for i := 20; i < keys; i++ {
		if _, err := s.Get(fmt.Sprintf("key-%04d", i)); err != nil {
			t.Fatalf("surviving key lost: key-%04d: %v", i, err)
		}
	}
}

// A key deleted during the compaction must stay deleted — the snapshot copied it
// into the new log, so only phase 3's live-index reconciliation removes it.
func TestCompactionHonoursDeletesTakenAfterTheSnapshot(t *testing.T) {
	s := testStore(t)

	for i := range 100 {
		if err := s.Put(fmt.Sprintf("k-%03d", i), []byte("v"), nil); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	for i := range 100 {
		if err := s.Put(fmt.Sprintf("k-%03d", i), []byte("v2"), nil); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := s.Delete("k-000"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if _, err := s.Get("k-000"); err == nil {
		t.Fatal("a deleted key came back after compaction")
	}
}

// 🔴 A DELETE TAKEN DURING THE REWRITE MUST SURVIVE A RESTART.
//
// This is the case the test above does NOT cover, and the difference is the
// whole bug: it deletes BEFORE compaction and asserts against the LIVE store.
// Both of those made it pass while the store was silently broken.
//
// The key is live at phase 1, so phase 2 copies its record into the new log with
// Deleted=false. If phase 3 only drops it from the in-memory index, the store
// looks perfectly correct for the rest of the process's life — and then
// recover() rebuilds the index by iterating the log on the next Open, replays
// that copied record as a live Put, and the entry comes back from the dead. For
// the queue store that is a row an *arr already deleted reappearing after a
// restart.
//
// So the assertion that matters is on the REOPENED store, not the live one. A
// test that never reopens exercises the right code and asserts the wrong scope.
func TestCompactionTombstonesADeleteTakenDuringTheRewrite(t *testing.T) {
	dir := t.TempDir()
	config.SetConfigPath(dir)
	path := filepath.Join(dir, "store.log")

	s, err := New(Config{DataPath: path, SyncInterval: -1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for i := range 50 {
		if err := s.Put(fmt.Sprintf("k-%03d", i), []byte("v"), nil); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	// Rewrite every key so the log carries dead bytes worth compacting.
	for i := range 50 {
		if err := s.Put(fmt.Sprintf("k-%03d", i), []byte("v2"), nil); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Delete INSIDE phase 2 — after the snapshot was taken, while no lock is
	// held. The seam fires before the copy loop, so the record is copied anyway,
	// which is exactly the window being tested.
	var once sync.Once
	var deleteErr error
	compactionYield = func() {
		once.Do(func() { deleteErr = s.Delete("k-000") })
	}
	t.Cleanup(func() { compactionYield = nil })

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if deleteErr != nil {
		t.Fatalf("Delete during compaction: %v", deleteErr)
	}

	// Passes with or without the fix — kept to show the live store is not where
	// the defect is visible.
	if _, err := s.Get("k-000"); err == nil {
		t.Fatal("deleted key is still readable from the live store after compaction")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := New(Config{DataPath: path, SyncInterval: -1})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	if _, err := reopened.Get("k-000"); err == nil {
		t.Fatal("A KEY DELETED DURING COMPACTION RESURRECTED ON REOPEN. Phase 2 copied its " +
			"record into the new log, so dropping it from the index alone is not a delete — " +
			"recover() replays the copied record as a live Put. Phase 3 must append a tombstone.")
	}
}

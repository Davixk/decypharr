package link

import (
	"sync"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// takedownLedger counts CONFIRMED legal-takedown refusals, per file, per entry
// lifecycle.
//
// WHY A LEDGER AND NOT A FLAG. A takedown arrives one FILE at a time, on the
// read path, but the verdict that matters — "this entry is dead, stop serving
// it" — is about the WHOLE entry. Condemning an entry on the first refused file
// would delete a thirteen-episode pack because one episode was taken down, which
// is the exact over-reach pruneIneligibleReason already refuses in the repair
// sweep ("refusing to delete a 13-file entry because 5 of its files are dead is
// the right call"). The ledger is what lets the read path apply that same
// whole-entry rule from a stream of per-file evidence.
//
// It is deliberately in memory only. A restart forgets partial evidence and the
// entry goes back to being served and re-refused — i.e. it degrades to the
// behaviour that shipped before this existed, never to a destructive one.
// Persisting it would mean a durable takedown record that no probe ever
// re-verifies, and a wrong one could never be argued with.
//
// Only files that have actually been refused ever appear here, so the map is
// bounded by the size of the dead set, not by the library.
type takedownLedger struct {
	mu    sync.Mutex
	files map[string]map[string]int // entry key -> filename -> confirmed refusals
}

// record counts one confirmed takedown refusal for filename under key.
//
// It returns whether THAT FILE has now reached the threshold, and how many
// distinct files under the same key have. The caller compares the second value
// against the entry's live file count to decide whether the whole entry is dead.
func (l *takedownLedger) record(key, filename string, threshold int) (fileDead bool, deadFiles int) {
	if key == "" || filename == "" {
		return false, 0
	}
	if threshold < 1 {
		threshold = 1
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	// Lazily built rather than requiring a constructor: a Service assembled
	// without New (tests, future wiring) must not silently no-op its way past
	// the whole takedown path because one field happened to be nil.
	if l.files == nil {
		l.files = make(map[string]map[string]int)
	}
	perFile := l.files[key]
	if perFile == nil {
		perFile = make(map[string]int)
		l.files[key] = perFile
	}
	perFile[filename]++

	for _, count := range perFile {
		if count >= threshold {
			deadFiles++
		}
	}
	return perFile[filename] >= threshold, deadFiles
}

// forget drops everything recorded for an entry lifecycle. Called once the
// evidence has been acted on (the entry was condemned) or invalidated (the
// entry was re-acquired somewhere else and is being served again), so the ledger
// never outlives its usefulness.
func (l *takedownLedger) forget(key string) {
	if key == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.files, key)
}

// takedownLedgerKey scopes the evidence by INFOHASH, deliberately, and not by
// the store lifecycle identity used for singleflight deduplication: that one
// embeds the store revision and therefore changes on every mutation of the
// entry, which would scatter one entry's refusals across a new key each time
// anything touched it and guarantee the whole-entry rule never fired.
//
// The infohash IS the content. A delete and re-add of the same hash is the same
// release, so evidence that it was legally removed carries over correctly.
func takedownLedgerKey(entry *storage.Entry) string {
	if entry == nil {
		return ""
	}
	return entry.InfoHash
}

// liveFileCount counts the files an entry is still expected to serve.
//
// Soft-deleted files are excluded because they are already not served: counting
// them would set a bar the remaining files can never clear, and an entry whose
// every surviving file had been taken down would sit "partially dead" forever.
func liveFileCount(entry *storage.Entry) int {
	if entry == nil {
		return 0
	}
	count := 0
	for _, file := range entry.Files {
		if file == nil || file.Deleted {
			continue
		}
		count++
	}
	return count
}

// takedownThreshold resolves Config.DebridTakedownThreshold live, per refusal,
// so the knob is hot (see clearHotFields). A negative value is preserved as-is
// and means the verdict is disabled entirely.
func takedownThreshold() int {
	cfg := config.Get()
	if cfg == nil || cfg.DebridTakedownThreshold == 0 {
		return config.DefaultDebridTakedownThreshold
	}
	return cfg.DebridTakedownThreshold
}

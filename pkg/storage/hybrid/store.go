// Package hybrid provides a high-performance append-only log storage engine
// with in-memory indexing, LRU caching, and secondary indexes.
//
// Architecture:
//   - Append-only log file for durability
//   - In-memory index with hot fields for O(1) lookups
//   - LRU cache for frequently accessed entries
//   - Secondary indexes for category/provider filtering
//   - Background compaction to reclaim deleted space
//
// Thread Safety:
//   - All operations are thread-safe via RWMutex
//   - Reads can proceed concurrently
//   - Writes are serialized
//
// Durability:
//   - All writes are appended to the log immediately
//   - Index is rebuilt from log on startup (crash recovery)
//   - Optional periodic sync for fsync guarantees
package hybrid

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
)

// Common errors
var (
	ErrStoreClosed          = errors.New("store is closed")
	ErrCorruptedData        = errors.New("corrupted data detected")
	ErrCompactionInProgress = errors.New("compaction already in progress")
	// ErrKeyNotFound is exported so callers can distinguish "absent" from a
	// real failure with errors.Is instead of matching on message text.
	ErrKeyNotFound = errors.New("key not found")
)

// errKeyNotFound is retained as an internal alias so existing call sites read
// unchanged; it is the same sentinel value as ErrKeyNotFound.
var errKeyNotFound = ErrKeyNotFound

// Config holds store configuration
type Config struct {
	// DataPath is the directory for data files
	DataPath string

	// CacheSize is the maximum number of entries to cache (default: 1000)
	CacheSize int

	// SyncInterval is how often to fsync (0 = every write, -1 = never)
	SyncInterval time.Duration

	// CompactionThreshold is the deleted entry ratio that triggers compaction (default: 0.2)
	CompactionThreshold float64

	// AutoCompact enables automatic background compaction
	AutoCompact bool
}

// Store is the main hybrid storage engine
type Store struct {
	mu sync.RWMutex

	// Core components
	log   *appendLog
	index *Index
	cache *lruCache

	// Configuration
	config Config
	logger zerolog.Logger

	// State
	closed     atomic.Bool
	compacting atomic.Bool

	// Background tasks
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Metrics
	stats Stats
}

// Stats holds operational statistics
type Stats struct {
	Writes      atomic.Int64
	Reads       atomic.Int64
	CacheHits   atomic.Int64
	CacheMisses atomic.Int64
	Deletes     atomic.Int64
	Compactions atomic.Int64
}

// StatsMeta holds operational statistics
type StatsMeta struct {
	Writes      int64
	Reads       int64
	CacheHits   int64
	CacheMisses int64
	Deletes     int64
	Compactions int64
}

// New creates a new Store with the given configuration
func New(config Config) (*Store, error) {
	if config.DataPath == "" {
		return nil, fmt.Errorf("DataPath is required")
	}

	// Apply defaults
	if config.CacheSize <= 0 {
		config.CacheSize = 1000
	}
	if config.CompactionThreshold <= 0 {
		config.CompactionThreshold = 0.2
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &Store{
		config: config,
		logger: logger.New("store"),
		ctx:    ctx,
		cancel: cancel,
	}

	// Initialize components
	logPath := config.DataPath
	var err error

	s.log, err = openAppendLog(logPath)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to open append log: %w", err)
	}

	s.index = newIndex()
	s.cache = newLRUCache(config.CacheSize)

	// Recover index from log
	if err := s.recover(); err != nil {
		_ = s.log.Close()
		cancel()
		return nil, fmt.Errorf("failed to recover from log: %w", err)
	}

	// Auto-compact if log version is outdated (migrates to new format)
	if s.log.version < logVersion && s.index.Len() > 0 {
		if err := s.Compact(); err != nil {
			s.logger.Warn().Err(err).Msg("Failed to auto-compact for version upgrade")
		}
	}

	// Start background tasks
	if config.SyncInterval > 0 {
		s.startSyncTask()
	}
	if config.AutoCompact {
		s.startCompactionTask()
	}
	return s, nil
}

// Close shuts down the store gracefully
func (s *Store) Close() error {
	if s.closed.Swap(true) {
		return nil // Already closed
	}

	// Stop background tasks
	s.cancel()
	s.wg.Wait()

	// Final sync
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.log.Sync(); err != nil {
		s.logger.Warn().Err(err).Msg("Failed to sync on close")
	}

	return s.log.Close()
}

// recover rebuilds the index from the append log
func (s *Store) recover() error {
	err := s.log.Iterate(func(record *LogRecord) error {
		if record.Deleted {
			s.index.Delete(record.Key)
			s.cache.Remove(record.Key)
		} else {
			s.index.Put(record.Key, &IndexEntry{
				Offset:    record.Offset,
				Size:      record.Size,
				Category:  record.Category,
				Provider:  record.Provider,
				Status:    record.Status,
				Name:      record.Name,
				TotalSize: record.TotalSize,
				Protocol:  record.Protocol,
				Bad:       record.Bad,
				AddedOn:   record.AddedOn,
			})
		}
		return nil
	})

	if err != nil {
		return err
	}
	return nil
}

// startSyncTask periodically syncs the log to disk
func (s *Store) startSyncTask() {
	s.wg.Go(func() {
		ticker := time.NewTicker(s.config.SyncInterval)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				s.mu.Lock()
				_ = s.log.Sync()
				s.mu.Unlock()
			}
		}
	})
}

// startCompactionTask periodically checks if compaction is needed
func (s *Store) startCompactionTask() {
	s.wg.Go(func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				if s.NeedsCompaction() {
					if err := s.Compact(); err != nil {
						s.logger.Warn().Err(err).Msg("Auto-compaction failed")
					}
				}
			}
		}
	})
}

// Put stores a key-value pair with optional metadata
func (s *Store) Put(key string, value []byte, meta *EntryMeta) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Prepare metadata
	var category, provider, name, status, protocol string
	var totalSize, addedOn int64
	var bad bool
	if meta != nil {
		category = meta.Category
		provider = meta.Provider
		status = meta.Status
		name = meta.Name
		totalSize = meta.TotalSize
		protocol = meta.Protocol
		bad = meta.Bad
		addedOn = meta.AddedOn
	}

	// Write to log
	offset, size, err := s.log.Append(key, value, false, category, provider, status, name, totalSize, protocol, bad, addedOn)
	if err != nil {
		return fmt.Errorf("failed to append to log: %w", err)
	}

	// Update index
	s.index.Put(key, &IndexEntry{
		Offset:    offset,
		Size:      size,
		Category:  category,
		Provider:  provider,
		Status:    status,
		Name:      name,
		TotalSize: totalSize,
		Protocol:  protocol,
		Bad:       bad,
		AddedOn:   addedOn,
	})

	// Invalidate cache (will be populated on next read)
	s.cache.Remove(key)

	s.stats.Writes.Add(1)
	return nil
}

// Get retrieves a value by key
func (s *Store) Get(key string) ([]byte, error) {
	if s.closed.Load() {
		return nil, ErrStoreClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check cache first
	if value, ok := s.cache.Get(key); ok {
		s.stats.CacheHits.Add(1)
		return value, nil
	}

	// Look up in index
	entry := s.index.Get(key)
	if entry == nil {
		return nil, fmt.Errorf("key %s: %w", key, errKeyNotFound)
	}

	// Read from log
	value, err := s.log.ReadAt(entry.Offset, entry.Size)
	if err != nil {
		return nil, fmt.Errorf("failed to read from log: %w", err)
	}

	// Populate cache (upgrade to write lock)
	s.mu.RUnlock()
	s.mu.Lock()
	s.cache.Put(key, value)
	s.mu.Unlock()
	s.mu.RLock()

	s.stats.CacheMisses.Add(1)
	s.stats.Reads.Add(1)
	return value, nil
}

// GetMeta retrieves just the metadata without reading the full value
// This is O(1) from the in-memory index - no disk access
func (s *Store) GetMeta(key string) (*IndexEntry, error) {
	if s.closed.Load() {
		return nil, ErrStoreClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entry := s.index.Get(key)
	if entry == nil {
		return nil, fmt.Errorf("key %s: %w", key, errKeyNotFound)
	}

	return entry, nil
}

// Delete removes a key
func (s *Store) Delete(key string) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if key exists
	if s.index.Get(key) == nil {
		return fmt.Errorf("key %s: %w", key, errKeyNotFound)
	}

	// Write tombstone to log
	if _, _, err := s.log.Append(key, nil, true, "", "", "", "", 0, "", false, 0); err != nil {
		return fmt.Errorf("failed to write tombstone: %w", err)
	}

	// Remove from index and cache
	s.index.Delete(key)
	s.cache.Remove(key)

	s.stats.Deletes.Add(1)
	return nil
}

// Exists checks if a key exists (O(1) from index)
func (s *Store) Exists(key string) bool {
	if s.closed.Load() {
		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.index.Get(key) != nil
}

// Len returns the number of entries
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.Len()
}

// Keys returns all keys (snapshot)
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.Keys()
}

// ForEach iterates over all entries in disk order (optimized for sequential
// I/O). The value passed to fn is owned by ForEach and only valid for the
// duration of that call — callers that retain it must copy.
//
// Scans deliberately bypass the LRU cache: a full pass touches every entry once
// and never reuses them, so caching would evict the genuinely-hot working set
// (active streams) and replace it with single-use scan data. Each value is read
// under a short RLock so writers interleave between keys and reads observe the
// current log even across a compaction swap. A single reusable buffer keeps the
// scan allocation-free per record.
func (s *Store) ForEach(fn func(key string, value []byte) error) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.mu.RLock()
	keys := s.index.KeysSortedByOffset()
	s.mu.RUnlock()

	var scratch []byte
	for _, key := range keys {
		s.mu.RLock()
		entry := s.index.Get(key)
		if entry == nil {
			s.mu.RUnlock()
			continue // deleted between the snapshot and now
		}
		value, err := s.log.ReadAtInto(entry.Offset, entry.Size, scratch)
		s.mu.RUnlock()
		if err != nil {
			return fmt.Errorf("failed to read from log: %w", err)
		}
		scratch = value // keep the (possibly grown) buffer for reuse
		s.stats.Reads.Add(1)

		if err := fn(key, value); err != nil {
			return err
		}
	}

	return nil
}

// ForEachMeta iterates over metadata only (no disk reads)
func (s *Store) ForEachMeta(fn func(key string, meta *IndexEntry) error) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.index.ForEach(fn)
}

// FilterByCategory returns all keys matching a category (O(1) lookup)
func (s *Store) FilterByCategory(category string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.GetByCategory(category)
}

// FilterByProvider returns all keys matching a provider (O(1) lookup)
func (s *Store) FilterByProvider(provider string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.GetByProvider(provider)
}

// CountByCategory returns entry count for a category (O(1))
func (s *Store) CountByCategory(category string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.index.GetByCategory(category))
}

// Categories returns all known categories
func (s *Store) Categories() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.Categories()
}

// NeedsCompaction returns true if compaction would reclaim significant space
func (s *Store) NeedsCompaction() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	liveSize := s.index.TotalSize()
	logSize := s.log.Size()

	if logSize == 0 {
		return false
	}

	deadRatio := 1.0 - (float64(liveSize) / float64(logSize))
	return deadRatio > s.config.CompactionThreshold
}

// compactionYield is a test-only seam, nil in production. It is called at the
// start of the lock-free rewrite — the exact point where the old implementation
// held the exclusive lock — so a test can assert that a reader still makes
// progress there rather than inferring it from timing.
var compactionYield func()

// Compact removes deleted entries and rewrites the log.
//
// ⚠️ THE EXPENSIVE PART RUNS WITHOUT THE STORE LOCK, AND THAT IS THE WHOLE
// POINT OF THE THREE PHASES BELOW.
//
// This used to hold s.mu (the EXCLUSIVE lock) for the entire rewrite: read
// every live entry off disk, append every one to a new file, then swap. On a
// large store that is minutes of held write lock, and because it is the write
// lock, every Get/ForEach/Put waits behind it — the HTTP API included.
//
// Measured: a repair sweep (797 dead findings, 13 deletions) spiked the dead
// ratio, the compaction ticker fired, and decypharr's API went unresponsive for
// ~2 minutes, producing 420 *arr client errors before recovering on its own. The
// *arrs depend on that API continuously, so a background housekeeping task must
// never be able to stall it.
//
// The rewrite is safe to do lock-free because THE LOG IS APPEND-ONLY: bytes at
// an already-written offset never change, and appendLog.ReadAt is a positioned
// read that takes no lock. So a snapshot of (key -> offset/size) stays readable
// no matter what concurrent writers append after it.
//
//	phase 1  brief RLock  — snapshot the index
//	phase 2  NO LOCK      — read every snapshotted record, write the new log
//	phase 3  brief Lock   — copy whatever changed during phase 2, then swap
//
// Phase 3 is bounded by the writes that happened DURING the compaction, not by
// the size of the store, which is what turns an O(store) stall into an O(delta)
// one.
func (s *Store) Compact() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	if !s.compacting.CompareAndSwap(false, true) {
		return ErrCompactionInProgress
	}
	defer s.compacting.Store(false)

	// ── phase 1: snapshot ────────────────────────────────────────────────────
	// Index entries are replaced by Put, never mutated in place, so these
	// pointers stay valid and describe the record as it was at snapshot time.
	s.mu.RLock()
	oldLog := s.log
	keys := s.index.KeysSortedByOffset()
	snapshot := make(map[string]*IndexEntry, len(keys))
	for _, key := range keys {
		if entry := s.index.Get(key); entry != nil {
			snapshot[key] = entry
		}
	}
	s.mu.RUnlock()

	newLogPath := oldLog.path + ".compact"
	newLog, err := createAppendLog(newLogPath)
	if err != nil {
		return fmt.Errorf("failed to create compaction log: %w", err)
	}
	abort := func(format string, err error) error {
		_ = newLog.Close()
		_ = os.Remove(newLogPath)
		return fmt.Errorf(format, err)
	}

	copyRecord := func(target *appendLog, dst *Index, src *appendLog, key string, entry *IndexEntry) error {
		value, err := src.ReadAt(entry.Offset, entry.Size)
		if err != nil {
			return fmt.Errorf("failed to read during compaction: %w", err)
		}
		offset, size, err := target.Append(key, value, false, entry.Category, entry.Provider,
			entry.Status, entry.Name, entry.TotalSize, entry.Protocol, entry.Bad, entry.AddedOn)
		if err != nil {
			return fmt.Errorf("failed to write during compaction: %w", err)
		}
		dst.Put(key, &IndexEntry{
			Offset:    offset,
			Size:      size,
			Category:  entry.Category,
			Provider:  entry.Provider,
			Status:    entry.Status,
			Name:      entry.Name,
			TotalSize: entry.TotalSize,
			Protocol:  entry.Protocol,
			Bad:       entry.Bad,
			AddedOn:   entry.AddedOn,
		})
		return nil
	}

	// ── phase 2: the bulk copy, holding NOTHING ──────────────────────────────
	if compactionYield != nil {
		compactionYield()
	}
	newIndex := newIndex()
	for _, key := range keys {
		entry := snapshot[key]
		if entry == nil {
			continue
		}
		if err := copyRecord(newLog, newIndex, oldLog, key, entry); err != nil {
			return abort("%w", err)
		}
	}

	// ── phase 3: catch up and swap, under the exclusive lock ──────────────────
	s.mu.Lock()
	defer s.mu.Unlock()

	// Anything written while phase 2 ran has a different offset than the
	// snapshot (or no snapshot entry at all) and must be copied now. Anything
	// deleted while phase 2 ran needs a TOMBSTONE, not just an index removal —
	// see below.
	live := make(map[string]struct{}, len(snapshot))
	var catchUpErr error
	caughtUp := 0
	_ = s.index.ForEach(func(key string, entry *IndexEntry) error {
		if catchUpErr != nil || entry == nil {
			return nil
		}
		live[key] = struct{}{}
		if prev, ok := snapshot[key]; ok && prev.Offset == entry.Offset && prev.Size == entry.Size {
			return nil // unchanged since the snapshot; already copied
		}
		if err := copyRecord(newLog, newIndex, s.log, key, entry); err != nil {
			catchUpErr = err
			return nil
		}
		caughtUp++
		return nil
	})
	if catchUpErr != nil {
		return abort("%w", catchUpErr)
	}
	// 🔴 A TOMBSTONE, NOT JUST AN INDEX REMOVAL — deleting from newIndex alone
	// RESURRECTS THE KEY ON THE NEXT RESTART.
	//
	// The key was live at phase 1, so phase 2 already copied its record into the
	// new log with Deleted=false. Dropping it from the in-memory index hides it
	// for the rest of this process's life and looks completely correct — but
	// recover() rebuilds the index by ITERATING THE LOG, so on the next Open the
	// copied record is replayed as a live Put and the entry comes back from the
	// dead. For the queue store that is a row an *arr already deleted reappearing
	// after a restart.
	//
	// This hole is specific to the unlocked phase 2. The original single-lock
	// compaction could not hit it: no delete could interleave with the rewrite,
	// so "absent from the live index" and "absent from the new log" were the same
	// statement. Splitting the lock separated them.
	//
	// It survived compact_test.go because every assertion there ran against the
	// live in-memory store and none reopened it — the test exercised the right
	// code and asserted the wrong scope.
	for key := range snapshot {
		if _, ok := live[key]; ok {
			continue
		}
		if _, _, err := newLog.Append(key, nil, true, "", "", "", "", 0, "", false, 0); err != nil {
			return abort("failed to write compaction tombstone: %w", err)
		}
		newIndex.Delete(key)
	}

	if err := newLog.Sync(); err != nil {
		return abort("failed to sync compaction log: %w", err)
	}

	oldPath := oldLog.path
	s.log = newLog
	s.index = newIndex
	s.cache.Clear()

	// Close and remove old log
	_ = oldLog.Close()
	_ = os.Remove(oldPath)
	_ = os.Rename(newLogPath, oldPath)
	s.log.path = oldPath

	s.stats.Compactions.Add(1)
	if caughtUp > 0 {
		s.logger.Debug().Int("caught_up", caughtUp).Int("live", len(live)).
			Msg("Compaction copied records written while it ran")
	}

	return nil
}

func (s *Store) GetStats() StatsMeta {
	return StatsMeta{
		Writes:      s.stats.Writes.Load(),
		Reads:       s.stats.Reads.Load(),
		CacheHits:   s.stats.CacheHits.Load(),
		CacheMisses: s.stats.CacheMisses.Load(),
		Deletes:     s.stats.Deletes.Load(),
		Compactions: s.stats.Compactions.Load(),
	}
}

// DiskSize returns the current log file size
func (s *Store) DiskSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.log.Size()
}

// MemoryUsage returns approximate in-memory usage
func (s *Store) MemoryUsage() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.MemoryUsage() + s.cache.MemoryUsage()
}

// EntryMeta holds metadata for a stored entry
type EntryMeta struct {
	Category  string
	Provider  string
	Status    string
	Name      string
	TotalSize int64
	Protocol  string // "torrent" or "nzb"
	Bad       bool
	AddedOn   int64 // Unix timestamp
}

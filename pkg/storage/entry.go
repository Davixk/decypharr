package storage

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/decypharr/pkg/storage/hybrid"
	"google.golang.org/protobuf/proto"
)

var (
	ErrEntryNotFound        = errors.New("entry not found")
	ErrStaleEntryGeneration = errors.New("stale entry generation")
)

func advanceStoreVersion(expectedGeneration string, expectedRevision uint64, currentGeneration string, currentRevision uint64) (string, uint64, error) {
	if expectedGeneration != currentGeneration || expectedRevision != currentRevision {
		return "", 0, ErrStaleEntryGeneration
	}
	// Legacy rows did not carry a generation. Adopt one on their first
	// versioned write. On the practically unreachable revision overflow,
	// rotate the generation so no old snapshot can become valid again.
	if currentGeneration == "" || currentRevision == ^uint64(0) {
		return uuid.NewString(), 1, nil
	}
	return currentGeneration, currentRevision + 1, nil
}

// AddOrUpdate adds or updates an entry
func (s *Storage) AddOrUpdate(entry *Entry) error {
	unlock := s.lockEntryMutation(entry.InfoHash)
	defer unlock()
	return s.addOrUpdateLocked(entry)
}

// addOrUpdateLocked writes an entry while its mutation stripe is held.
func (s *Storage) addOrUpdateLocked(entry *Entry) error {
	previousGeneration := entry.mainStoreGeneration
	previousRevision := entry.mainStoreRevision
	previousCreatedAt := entry.CreatedAt
	previousUpdatedAt := entry.UpdatedAt
	var previousEntry *Entry
	committed := false
	defer func() {
		if !committed {
			entry.mainStoreGeneration = previousGeneration
			entry.mainStoreRevision = previousRevision
			entry.CreatedAt = previousCreatedAt
			entry.UpdatedAt = previousUpdatedAt
		}
	}()

	now := time.Now()
	if s.entries.Exists(entry.InfoHash) {
		current, err := s.Get(entry.InfoHash)
		if err != nil {
			return err
		}
		nextGeneration, nextRevision, versionErr := advanceStoreVersion(
			entry.mainStoreGeneration,
			entry.mainStoreRevision,
			current.mainStoreGeneration,
			current.mainStoreRevision,
		)
		if versionErr != nil {
			return fmt.Errorf("%w for main entry %s", ErrStaleEntryGeneration, entry.InfoHash)
		}
		previousEntry = current
		entry.mainStoreGeneration = nextGeneration
		entry.mainStoreRevision = nextRevision
	} else {
		// A versioned value is an update snapshot whose row was deleted. Never
		// let it resurrect the old generation; explicit creation starts empty.
		if entry.mainStoreGeneration != "" || entry.mainStoreRevision != 0 {
			return fmt.Errorf("%w for deleted main entry %s", ErrStaleEntryGeneration, entry.InfoHash)
		}
		entry.mainStoreGeneration = uuid.NewString()
		entry.mainStoreRevision = 1
		if entry.CreatedAt.IsZero() {
			entry.CreatedAt = now
		}
	}
	entry.UpdatedAt = now

	// Serialize
	pb := EntryToProto(entry)
	data, err := proto.Marshal(pb)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	if err := s.entries.Put(entry.InfoHash, data, entryMetadata(entry)); err != nil {
		return err
	}
	committed = true
	// entryItems and repair state are rebuildable derivatives. Update them only
	// after the authoritative row commits, so a failed primary Put cannot create
	// phantom folder listings. A later repair pass can heal a derivative failure.
	if previousEntry != nil && previousEntry.GetFolder() != entry.GetFolder() {
		s.removeFromEntryItem(previousEntry)
	}
	if entryItemUpdateNeedsRebuild(previousEntry, entry) {
		if _, rebuildErr := s.rebuildEntryItem(entry.GetFolder(), entry.Protocol); rebuildErr != nil {
			s.markEntryItemFailure(entry, "rebuild", rebuildErr)
		}
	} else {
		s.updateEntryItem(entry)
	}
	return nil
}

// entryItemUpdateNeedsRebuild detects updates that can expose a file shadowed
// by this row. Additive and same-owner content changes are safe to merge, but
// removing a name or changing its ordering identity requires re-projecting all
// authoritative rows in the folder.
func entryItemUpdateNeedsRebuild(previous, current *Entry) bool {
	if previous == nil || current == nil || previous.GetFolder() != current.GetFolder() {
		return false
	}
	for name, previousFile := range previous.Files {
		currentFile, exists := current.Files[name]
		if !exists || currentFile == nil {
			return true
		}
		if previousFile == nil || previousFile.InfoHash != currentFile.InfoHash || !previousFile.AddedOn.Equal(currentFile.AddedOn) {
			return true
		}
	}
	return false
}

// MutateEntryIfPresent atomically reloads and updates an existing main entry.
// It never creates a record that was concurrently deleted.
func (s *Storage) MutateEntryIfPresent(infohash string, mutate func(*Entry) (bool, error)) (*Entry, bool, error) {
	unlock := s.lockEntryMutation(infohash)
	defer unlock()

	if !s.entries.Exists(infohash) {
		return nil, false, nil
	}
	entry, err := s.Get(infohash)
	if err != nil {
		return nil, true, err
	}
	if mutate == nil {
		return entry, true, nil
	}
	storeGeneration := entry.mainStoreGeneration
	storeRevision := entry.mainStoreRevision
	storeInfoHash := entry.InfoHash
	changed, err := mutate(entry)
	if err != nil {
		return nil, true, err
	}
	entry.InfoHash = storeInfoHash
	if changed {
		// A callback can replace the entire exported Entry value. Internal store
		// identity always belongs to the row we loaded, never to a snapshot it
		// merged in.
		entry.mainStoreGeneration = storeGeneration
		entry.mainStoreRevision = storeRevision
		if err := s.addOrUpdateLocked(entry); err != nil {
			return nil, true, err
		}
	}
	return entry, true, nil
}

// MutateEntrySnapshot atomically patches a main row only while it still
// belongs to the lifecycle captured by snapshot. Revision drift is allowed so
// provider refreshes can merge remote state with independent metadata edits;
// delete/re-add always creates a new generation and permanently fences the old
// remote response.
func (s *Storage) MutateEntrySnapshot(snapshot *Entry, mutate func(*Entry) (bool, error)) (*Entry, bool, error) {
	if snapshot == nil {
		return nil, false, fmt.Errorf("main snapshot is nil")
	}
	unlock := s.lockEntryMutation(snapshot.InfoHash)
	defer unlock()
	if !s.entries.Exists(snapshot.InfoHash) {
		return nil, false, nil
	}
	entry, err := s.Get(snapshot.InfoHash)
	if err != nil {
		return nil, true, err
	}
	if snapshot.mainStoreGeneration == "" || snapshot.mainStoreGeneration != entry.mainStoreGeneration {
		return entry, true, fmt.Errorf("%w for main entry %s", ErrStaleEntryGeneration, snapshot.InfoHash)
	}
	if mutate == nil {
		return entry, true, nil
	}
	storeGeneration := entry.mainStoreGeneration
	storeRevision := entry.mainStoreRevision
	storeInfoHash := entry.InfoHash
	changed, err := mutate(entry)
	if err != nil {
		return nil, true, err
	}
	entry.InfoHash = storeInfoHash
	if !changed {
		return entry, true, nil
	}
	entry.mainStoreGeneration = storeGeneration
	entry.mainStoreRevision = storeRevision
	if err := s.addOrUpdateLocked(entry); err != nil {
		return nil, true, err
	}
	return entry, true, nil
}

// SameMainGeneration compares opaque main-row lifecycle identities without
// exposing their persisted tokens to manager workflows.
func SameMainGeneration(first, second *Entry) bool {
	return first != nil && second != nil &&
		first.mainStoreGeneration != "" &&
		first.mainStoreGeneration == second.mainStoreGeneration
}

// EntryLifecycleIdentity returns an opaque identity suitable for in-process
// deduplication keys. Both main and queue versions are included because an
// Entry can simultaneously be a completed-row snapshot and an active job.
func EntryLifecycleIdentity(entry *Entry) string {
	if entry == nil {
		return ""
	}
	if entry.mainStoreGeneration == "" && entry.queueStoreGeneration == "" {
		return ""
	}
	return fmt.Sprintf("m:%s:%d:q:%s:%d", entry.mainStoreGeneration, entry.mainStoreRevision, entry.queueStoreGeneration, entry.queueStoreRevision)
}

// BatchAddOrUpdate adds or updates multiple entries
func (s *Storage) BatchAddOrUpdate(entries []*Entry) error {
	var errs []error
	for _, entry := range entries {
		if entry == nil {
			errs = append(errs, fmt.Errorf("entry is nil"))
			continue
		}
		if err := s.AddOrUpdate(entry); err != nil {
			errs = append(errs, fmt.Errorf("update main entry %s: %w", entry.InfoHash, err))
		}
	}
	return errors.Join(errs...)
}

// Exists checks if an entry exists
func (s *Storage) Exists(infohash string) (bool, error) {
	return s.entries.Exists(infohash), nil
}

// Get retrieves an entry by InfoHash
func (s *Storage) Get(infohash string) (*Entry, error) {
	data, err := s.entries.Get(infohash)
	if err != nil {
		return nil, err
	}

	var pb EntryProto
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, err
	}

	return ProtoToEntry(&pb), nil
}

// logCorruptRow records an undecodable row that a scan skipped. The row stays
// on disk (and keeps failing per-key reads) but must not black-hole whole
// listings or startup: one corrupt row in a large store should cost exactly
// that one row, never the rest.
func (s *Storage) logCorruptRow(store, key string, err error) {
	s.logger.Error().Err(err).Str("store", store).Str("key", key).Msg("Skipping corrupt storage row")
}

// List retrieves all cached entries with optional filtering.
// Rows that fail to decode are logged and skipped so a single corrupt record
// cannot hide every other entry from callers.
func (s *Storage) List(filter func(*Entry) bool) ([]*Entry, error) {
	var entries []*Entry

	err := s.entries.ForEach(func(key string, value []byte) error {
		if strings.HasPrefix(key, "__") {
			return nil
		}
		var pb EntryProto
		if err := proto.Unmarshal(value, &pb); err != nil {
			s.logCorruptRow("entries", key, err)
			return nil
		}
		entry := ProtoToEntry(&pb)
		if filter == nil || filter(entry) {
			entries = append(entries, entry)
		}
		return nil
	})

	return entries, err
}

// ForEach iterates over entries. Undecodable rows are logged and skipped.
func (s *Storage) ForEach(fn func(*Entry) error) error {
	return s.entries.ForEach(func(key string, value []byte) error {
		if strings.HasPrefix(key, "__") {
			return nil
		}
		var pb EntryProto
		if err := proto.Unmarshal(value, &pb); err != nil {
			s.logCorruptRow("entries", key, err)
			return nil
		}
		return fn(ProtoToEntry(&pb))
	})
}

// ForEachBatch iterates over entries in batches
func (s *Storage) ForEachBatch(batchSize int, fn func([]*Entry) error) error {
	batch := make([]*Entry, 0, batchSize)

	// Reuse a single proto message across the scan. proto.Reset zeroes it
	// between records, so Unmarshal reuses the message's backing storage
	// instead of allocating a fresh EntryProto (and its nested message/slice
	// fields) per entry. Safe because ProtoToEntry copies values out into a
	// fresh Entry (the one aliased field, Tags, is replaced by Reset->nil
	// before the next Unmarshal, leaving the prior entry's slice untouched).
	var pb EntryProto
	err := s.entries.ForEach(func(key string, value []byte) error {
		if strings.HasPrefix(key, "__") {
			return nil
		}
		proto.Reset(&pb)
		if err := proto.Unmarshal(value, &pb); err != nil {
			s.logCorruptRow("entries", key, err)
			return nil
		}
		batch = append(batch, ProtoToEntry(&pb))

		if len(batch) >= batchSize {
			if err := fn(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
		return nil
	})

	if err == nil && len(batch) > 0 {
		err = fn(batch)
	}
	return err
}

// EntryMetaInfo is a lightweight struct for folder listings (no disk reads)
type EntryMetaInfo struct {
	InfoHash string
	Name     string
	Size     int64
	AddedOn  time.Time
	Provider string
	Protocol string
	Bad      bool
}

// ForEachMeta iterates over entry metadata without reading full entries from disk.
// This is O(n) in-memory only - no disk reads, no protobuf deserialization.
func (s *Storage) ForEachMeta(fn func(*EntryMetaInfo) error) error {
	return s.entries.ForEachMeta(func(key string, meta *hybrid.IndexEntry) error {
		return fn(&EntryMetaInfo{
			InfoHash: key,
			Name:     meta.Name,
			Size:     meta.TotalSize,
			AddedOn:  time.Unix(meta.AddedOn, 0),
			Provider: meta.Provider,
			Protocol: meta.Protocol,
			Bad:      meta.Bad,
		})
	})
}

// MigrateMetadata re-saves all entries to populate the new metadata fields
// (Protocol, Bad, AddedOn, computed folder Name) in the index.
// This is a one-time migration for existing data.
// Returns the number of entries migrated, the number of corrupt rows skipped,
// and any store-level error. A row that fails to decode is logged and skipped
// so one bad record in real-world legacy state cannot abort startup; I/O
// failures against the underlying store still abort.
func (s *Storage) MigrateMetadata() (int, int, error) {
	var mainKeys []string
	if err := s.entries.ForEachMeta(func(key string, meta *hybrid.IndexEntry) error {
		// Skip special keys
		if strings.HasPrefix(key, "__") {
			return nil
		}
		// Check if metadata needs migration (Protocol empty = old format)
		if meta.Protocol == "" {
			mainKeys = append(mainKeys, key)
		}
		return nil
	}); err != nil {
		return 0, 0, fmt.Errorf("scan main entry metadata: %w", err)
	}
	var queueKeys []string
	if err := s.queue.ForEachMeta(func(key string, meta *hybrid.IndexEntry) error {
		if meta.Protocol == "" {
			queueKeys = append(queueKeys, key)
		}
		return nil
	}); err != nil {
		return 0, 0, fmt.Errorf("scan queue entry metadata: %w", err)
	}

	migrated := 0
	skipped := 0
	for _, key := range mainKeys {
		unlock := s.lockEntryMutation(key)
		entry, decodeErr, err := s.loadEntryClassified(s.entries, key)
		if err == nil && decodeErr == nil && entry != nil {
			pb := EntryToProto(entry)
			var data []byte
			data, err = proto.Marshal(pb)
			if err == nil {
				err = s.entries.Put(key, data, entryMetadata(entry))
			}
			if err == nil {
				s.updateEntryItem(entry)
			}
		}
		unlock()
		if err != nil {
			return migrated, skipped, fmt.Errorf("migrate main metadata %s: %w", key, err)
		}
		if decodeErr != nil {
			s.logCorruptRow("entries", key, decodeErr)
			skipped++
			continue
		}
		if entry == nil {
			continue // deleted concurrently
		}
		migrated++
	}
	for _, key := range queueKeys {
		unlock := s.lockQueueMutation(key)
		entry, decodeErr, err := s.loadEntryClassified(s.queue, strings.ToLower(key))
		if err == nil && decodeErr == nil && entry != nil {
			pb := EntryToProto(entry)
			var data []byte
			data, err = proto.Marshal(pb)
			if err == nil {
				err = s.queue.Put(strings.ToLower(key), data, entryMetadata(entry))
			}
		}
		unlock()
		if err != nil {
			return migrated, skipped, fmt.Errorf("migrate queue metadata %s: %w", key, err)
		}
		if decodeErr != nil {
			s.logCorruptRow("queue", key, decodeErr)
			skipped++
			continue
		}
		if entry == nil {
			continue
		}
		migrated++
	}

	return migrated, skipped, nil
}

// loadEntryClassified separates row-level corruption from store-level I/O
// failures. It returns (nil, nil, nil) when the key no longer exists.
func (s *Storage) loadEntryClassified(store *hybrid.Store, key string) (*Entry, error, error) {
	data, err := store.Get(key)
	if err != nil {
		if !store.Exists(key) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	var pb EntryProto
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, err, nil
	}
	return ProtoToEntry(&pb), nil, nil
}

// Delete removes an entry
func (s *Storage) Delete(infohash string) error {
	_, err := s.deleteEntryIfCurrent(infohash, nil, nil)
	return err
}

// DeleteIfCurrent removes a row only if the supplied snapshot is still the
// exact main-store revision. It fences scan-to-delete ABA races without
// exposing internal version tokens outside the storage package.
func (s *Storage) DeleteIfCurrent(expected *Entry) (bool, error) {
	if expected == nil {
		return false, fmt.Errorf("expected entry is nil")
	}
	return s.deleteEntryIfCurrent(expected.InfoHash, expected, nil)
}

// DeleteIfCurrentWithCleanup keeps the main-row lifecycle lock through
// synchronous external cleanup so a same-hash replacement cannot adopt a
// provider/NZB resource that the old generation is still deleting. Cleanup
// runs before the row is removed: if it fails, the row is retained and the
// deletion is reported as (false, err), so the operation stays retryable and
// external metadata is never orphaned by a half-finished delete.
func (s *Storage) DeleteIfCurrentWithCleanup(expected *Entry, cleanup func(*Entry) error) (bool, error) {
	if expected == nil {
		return false, fmt.Errorf("expected entry is nil")
	}
	return s.deleteEntryIfCurrent(expected.InfoHash, expected, cleanup)
}

// CleanupUnownedProviderPlacement runs external cleanup only if no durable
// main row owns the same provider/id pair. Callers serialize placement
// adoption and cleanup at the manager layer; the full-store reference check
// makes provider-backed folder aliases last-reference-wins.
func (s *Storage) CleanupUnownedProviderPlacement(infohash, provider, id string, cleanup func() error) (bool, error) {
	if infohash == "" || provider == "" || id == "" {
		return false, fmt.Errorf("provider cleanup identity is incomplete")
	}
	if cleanup == nil {
		return false, fmt.Errorf("provider cleanup callback is nil")
	}
	unlock := s.lockEntryMutation(infohash)
	defer unlock()
	owned := false
	if err := s.ForEach(func(entry *Entry) error {
		for _, placement := range entry.Providers {
			if placement != nil && placement.Provider == provider && placement.ID == id {
				owned = true
				return nil
			}
		}
		return nil
	}); err != nil {
		return false, fmt.Errorf("scan references for %s placement %s: %w", provider, id, err)
	}
	if owned {
		return false, nil
	}
	if err := cleanup(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Storage) deleteEntryIfCurrent(infohash string, expected *Entry, cleanup func(*Entry) error) (bool, error) {
	unlock := s.lockEntryMutation(infohash)
	defer unlock()

	entry, err := s.Get(infohash)
	if err != nil {
		if !s.entries.Exists(infohash) {
			return false, nil
		}
		return false, err
	}
	if expected != nil && (expected.mainStoreGeneration != entry.mainStoreGeneration || expected.mainStoreRevision != entry.mainStoreRevision) {
		return false, nil
	}
	// Run failure-prone external cleanup while the authoritative row still
	// exists. If cleanup fails, the row is retained and the delete can simply
	// be retried; the old order (delete row, then cleanup) left the entry gone
	// with its external metadata orphaned and unrecoverable. The per-key
	// mutation lock still spans cleanup and delete, so a same-hash replacement
	// cannot adopt the resource mid-delete.
	if cleanup != nil {
		if err := cleanup(entry); err != nil {
			return false, fmt.Errorf("cleanup before deleting main entry %s failed: %w", infohash, err)
		}
	}
	if err := s.entries.Delete(infohash); err != nil {
		return false, err
	}
	s.removeFromEntryItem(entry)
	return true, nil
}

// Count returns the number of entries
func (s *Storage) Count() (int, error) {
	return s.entries.Len(), nil
}

// updateEntryItem updates the name index
func (s *Storage) updateEntryItem(entry *Entry) {
	name := entry.GetFolder()
	if name == "" {
		return
	}
	unlock := s.lockItemMutation(name)
	defer unlock()

	var item *EntryItem
	if data, err := s.entryItems.Get(name); err == nil {
		var pb EntryItemProto
		if err := proto.Unmarshal(data, &pb); err != nil {
			if _, rebuildErr := s.rebuildEntryItemLocked(name, entry.Protocol); rebuildErr != nil {
				s.markEntryItemFailure(entry, "rebuild", rebuildErr)
			}
			return
		}
		item = ProtoToEntryItem(&pb)
	} else if s.entryItems.Exists(name) {
		if _, rebuildErr := s.rebuildEntryItemLocked(name, entry.Protocol); rebuildErr != nil {
			s.markEntryItemFailure(entry, "rebuild", rebuildErr)
		}
		return
	}
	oldFingerprint := EntryItemRepairFingerprint(item)

	created := item == nil
	if item == nil {
		item = &EntryItem{Name: name, Files: make(map[string]*File)}
	}

	mergeEntryIntoItem(item, entry)
	newFingerprint := EntryItemRepairFingerprint(item)
	pb := EntryItemToProto(item)
	data, err := proto.Marshal(pb)
	if err != nil {
		s.markEntryItemFailure(entry, "marshal", err)
		return
	}
	if err := s.entryItems.Put(name, data, nil); err != nil {
		s.markEntryItemFailure(entry, "write", err)
		return
	}
	if created {
		// Only a NEW key can change an alias. The far more common in-place
		// rewrite of an existing folder row leaves the reverse index valid, and
		// invalidating on those would rebuild it on every progress update.
		s.invalidateEntryItemAliases()
	}
	if oldFingerprint != newFingerprint {
		s.MarkEntryDirty(name, entry.Protocol, "entry_item_changed")
	}
}

func (s *Storage) markEntryItemFailure(entry *Entry, operation string, err error) {
	name := entry.GetFolder()
	s.MarkEntryDirty(name, entry.Protocol, "entry_item_"+operation+"_failed")
	s.logger.Error().Err(err).
		Str("entry", name).
		Str("infohash", entry.InfoHash).
		Str("operation", operation).
		Msg("Failed to update derived entry item; scheduled repair")
}

// removeFromEntryItem removes an entry from the name index
func (s *Storage) removeFromEntryItem(entry *Entry) {
	name := entry.GetFolder()
	if name == "" {
		return
	}
	unlock := s.lockItemMutation(name)
	defer unlock()
	if _, err := s.rebuildEntryItemLocked(name, entry.Protocol); err != nil {
		s.markEntryItemFailure(entry, "rebuild", err)
	}
}

// Queue operations

// AddQueue adds an entry to the queue
func (s *Storage) AddQueue(entry *Entry) error {
	key := strings.ToLower(entry.InfoHash)
	unlock := s.lockQueueMutation(key)
	defer unlock()
	previousGeneration := entry.queueStoreGeneration
	previousRevision := entry.queueStoreRevision
	previousCreatedAt := entry.CreatedAt
	previousUpdatedAt := entry.UpdatedAt
	entry.queueStoreGeneration, entry.queueStoreRevision = uuid.NewString(), 1
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	if err := s.putQueueLocked(entry); err != nil {
		entry.queueStoreGeneration = previousGeneration
		entry.queueStoreRevision = previousRevision
		entry.CreatedAt = previousCreatedAt
		entry.UpdatedAt = previousUpdatedAt
		return err
	}
	return nil
}

// UpdateQueue updates a queued entry
func (s *Storage) UpdateQueue(entry *Entry) error {
	key := strings.ToLower(entry.InfoHash)
	unlock := s.lockQueueMutation(key)
	defer unlock()
	if !s.queue.Exists(key) {
		return fmt.Errorf("%w for queue entry %s", ErrEntryNotFound, key)
	}
	current, err := s.GetQueued(key)
	if err != nil {
		return err
	}
	previousGeneration := entry.queueStoreGeneration
	previousRevision := entry.queueStoreRevision
	previousUpdatedAt := entry.UpdatedAt
	nextGeneration, nextRevision, versionErr := advanceStoreVersion(
		entry.queueStoreGeneration,
		entry.queueStoreRevision,
		current.queueStoreGeneration,
		current.queueStoreRevision,
	)
	if versionErr != nil {
		return fmt.Errorf("%w for queue entry %s", ErrStaleEntryGeneration, key)
	}
	entry.queueStoreGeneration = nextGeneration
	entry.queueStoreRevision = nextRevision
	if err := s.putQueueLocked(entry); err != nil {
		entry.queueStoreGeneration = previousGeneration
		entry.queueStoreRevision = previousRevision
		entry.UpdatedAt = previousUpdatedAt
		return err
	}
	return nil
}

func (s *Storage) putQueueLocked(entry *Entry) error {
	entry.UpdatedAt = time.Now()

	pb := EntryToProto(entry)
	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}

	return s.queue.Put(strings.ToLower(entry.InfoHash), data, entryMetadata(entry))
}

func entryMetadata(entry *Entry) *hybrid.EntryMeta {
	return &hybrid.EntryMeta{
		Category:  entry.Category,
		Provider:  entry.ActiveProvider,
		Status:    string(entry.Status),
		Name:      entry.GetFolder(),
		TotalSize: entry.Size,
		Protocol:  string(entry.Protocol),
		Bad:       entry.Bad,
		AddedOn:   entry.AddedOn.Unix(),
	}
}

func indexedEntryMetadata(indexed *hybrid.IndexEntry) *hybrid.EntryMeta {
	if indexed == nil {
		return nil
	}
	return &hybrid.EntryMeta{
		Category:  indexed.Category,
		Provider:  indexed.Provider,
		Status:    indexed.Status,
		Name:      indexed.Name,
		TotalSize: indexed.TotalSize,
		Protocol:  indexed.Protocol,
		Bad:       indexed.Bad,
		AddedOn:   indexed.AddedOn,
	}
}

// GetQueued retrieves a queued entry
func (s *Storage) GetQueued(infohash string) (*Entry, error) {
	data, err := s.queue.Get(strings.ToLower(infohash))
	if err != nil {
		return nil, err
	}

	var pb EntryProto
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, err
	}
	return ProtoToEntry(&pb), nil
}

// QueueExists reports whether a queue mirror exists for an entry.
func (s *Storage) QueueExists(infohash string) bool {
	return s.queue.Exists(strings.ToLower(infohash))
}

// MutateQueuedIfPresent atomically reloads and updates an existing queue row.
// It never recreates a row that queue cleanup deleted concurrently.
func (s *Storage) MutateQueuedIfPresent(infohash string, mutate func(*Entry) (bool, error)) (*Entry, bool, error) {
	key := strings.ToLower(infohash)
	unlock := s.lockQueueMutation(key)
	defer unlock()

	if !s.queue.Exists(key) {
		return nil, false, nil
	}
	entry, err := s.GetQueued(key)
	if err != nil {
		return nil, true, err
	}
	if mutate == nil {
		return entry, true, nil
	}
	storeGeneration := entry.queueStoreGeneration
	storeRevision := entry.queueStoreRevision
	storeInfoHash := entry.InfoHash
	changed, err := mutate(entry)
	if err != nil {
		return nil, true, err
	}
	entry.InfoHash = storeInfoHash
	if changed {
		// Preserve the loaded row identity even if the callback copied a whole
		// Entry snapshot from the main store or a long-lived workflow pointer.
		entry.queueStoreGeneration = storeGeneration
		entry.queueStoreRevision = storeRevision
		previousGeneration := storeGeneration
		previousRevision := storeRevision
		nextGeneration, nextRevision, versionErr := advanceStoreVersion(
			entry.queueStoreGeneration,
			entry.queueStoreRevision,
			entry.queueStoreGeneration,
			entry.queueStoreRevision,
		)
		if versionErr != nil {
			return nil, true, versionErr
		}
		entry.queueStoreGeneration = nextGeneration
		entry.queueStoreRevision = nextRevision
		if err := s.putQueueLocked(entry); err != nil {
			entry.queueStoreGeneration = previousGeneration
			entry.queueStoreRevision = previousRevision
			return nil, true, err
		}
	}
	return entry, true, nil
}

// MutateQueuedSnapshot atomically patches the current revision only when the
// caller's snapshot still belongs to the same queue generation. Revision
// drift is allowed so long-lived jobs can merge with independent mirror/user
// updates, while delete/re-add makes the old job permanently stale.
func (s *Storage) MutateQueuedSnapshot(snapshot *Entry, mutate func(*Entry) (bool, error)) (*Entry, bool, error) {
	if snapshot == nil {
		return nil, false, fmt.Errorf("queue snapshot is nil")
	}
	key := strings.ToLower(snapshot.InfoHash)
	unlock := s.lockQueueMutation(key)
	defer unlock()
	if !s.queue.Exists(key) {
		return nil, false, nil
	}
	entry, err := s.GetQueued(key)
	if err != nil {
		return nil, true, err
	}
	if snapshot.queueStoreGeneration == "" || snapshot.queueStoreGeneration != entry.queueStoreGeneration {
		return entry, true, fmt.Errorf("%w for queue entry %s", ErrStaleEntryGeneration, key)
	}
	if mutate == nil {
		return entry, true, nil
	}
	storeGeneration := entry.queueStoreGeneration
	storeRevision := entry.queueStoreRevision
	storeInfoHash := entry.InfoHash
	changed, err := mutate(entry)
	if err != nil {
		return nil, true, err
	}
	entry.InfoHash = storeInfoHash
	if !changed {
		return entry, true, nil
	}
	entry.queueStoreGeneration = storeGeneration
	entry.queueStoreRevision = storeRevision
	nextGeneration, nextRevision, versionErr := advanceStoreVersion(storeGeneration, storeRevision, storeGeneration, storeRevision)
	if versionErr != nil {
		return nil, true, versionErr
	}
	entry.queueStoreGeneration = nextGeneration
	entry.queueStoreRevision = nextRevision
	if err := s.putQueueLocked(entry); err != nil {
		return nil, true, err
	}
	return entry, true, nil
}

// IsCurrentQueuedSnapshot reports whether work retained by a queue job still
// belongs to the live generation. It intentionally ignores revision drift.
func (s *Storage) IsCurrentQueuedSnapshot(snapshot *Entry) (bool, error) {
	entry, present, err := s.MutateQueuedSnapshot(snapshot, nil)
	if err != nil {
		if errors.Is(err, ErrStaleEntryGeneration) {
			return false, nil
		}
		return false, err
	}
	return present && entry != nil, nil
}

// SameQueueGeneration compares the opaque lifecycle identities carried by two
// queue snapshots. Callers can coordinate in-memory work without exposing or
// persisting the token outside the storage model.
func SameQueueGeneration(first, second *Entry) bool {
	return first != nil && second != nil &&
		first.queueStoreGeneration != "" &&
		first.queueStoreGeneration == second.queueStoreGeneration
}

// MigrateStoreVersions eagerly assigns exact, persisted versions to legacy
// rows before NewStorage exposes any snapshots. Lazy adoption alone is not
// sufficient: an unversioned snapshot taken before delete/re-add could
// otherwise be mistaken for a brand-new row and resurrect stale data.
// Returns the number of rows migrated and the number of corrupt rows skipped.
// Per-row decode failures are logged and skipped so legacy state with one bad
// record still boots; store-level I/O failures abort.
func (s *Storage) MigrateStoreVersions() (int, int, error) {
	skipped := 0
	var mainKeys []string
	if err := s.entries.ForEach(func(key string, value []byte) error {
		if strings.HasPrefix(key, "__") {
			return nil
		}
		var pb EntryProto
		if err := proto.Unmarshal(value, &pb); err != nil {
			s.logCorruptRow("entries", key, err)
			skipped++
			return nil
		}
		if pb.MainStoreGeneration == "" || pb.MainStoreRevision == 0 {
			mainKeys = append(mainKeys, key)
		}
		return nil
	}); err != nil {
		return 0, skipped, fmt.Errorf("scan main entry versions: %w", err)
	}

	var queueKeys []string
	if err := s.queue.ForEach(func(key string, value []byte) error {
		var pb EntryProto
		if err := proto.Unmarshal(value, &pb); err != nil {
			s.logCorruptRow("queue", key, err)
			skipped++
			return nil
		}
		if pb.QueueStoreGeneration == "" || pb.QueueStoreRevision == 0 {
			queueKeys = append(queueKeys, key)
		}
		return nil
	}); err != nil {
		return 0, skipped, fmt.Errorf("scan queue entry versions: %w", err)
	}

	migrated := 0
	for _, key := range mainKeys {
		changed, err := s.migrateMainStoreVersion(key)
		if err != nil {
			return migrated, skipped, err
		}
		if changed {
			migrated++
		}
	}
	for _, key := range queueKeys {
		changed, err := s.migrateQueueStoreVersion(key)
		if err != nil {
			return migrated, skipped, err
		}
		if changed {
			migrated++
		}
	}
	return migrated, skipped, nil
}

func (s *Storage) migrateMainStoreVersion(key string) (bool, error) {
	unlock := s.lockEntryMutation(key)
	defer unlock()
	entry, err := s.Get(key)
	if err != nil {
		return false, fmt.Errorf("load legacy main entry %s: %w", key, err)
	}
	if entry.mainStoreGeneration != "" && entry.mainStoreRevision != 0 {
		return false, nil
	}
	entry.mainStoreGeneration = uuid.NewString()
	entry.mainStoreRevision = 1
	pb := EntryToProto(entry)
	data, err := proto.Marshal(pb)
	if err != nil {
		return false, fmt.Errorf("marshal versioned main entry %s: %w", key, err)
	}
	indexed, metaErr := s.entries.GetMeta(key)
	if metaErr != nil {
		return false, fmt.Errorf("load legacy main metadata %s: %w", key, metaErr)
	}
	if err := s.entries.Put(key, data, indexedEntryMetadata(indexed)); err != nil {
		return false, fmt.Errorf("persist versioned main entry %s: %w", key, err)
	}
	return true, nil
}

func (s *Storage) migrateQueueStoreVersion(key string) (bool, error) {
	key = strings.ToLower(key)
	unlock := s.lockQueueMutation(key)
	defer unlock()
	entry, err := s.GetQueued(key)
	if err != nil {
		return false, fmt.Errorf("load legacy queue entry %s: %w", key, err)
	}
	if entry.queueStoreGeneration != "" && entry.queueStoreRevision != 0 {
		return false, nil
	}
	entry.queueStoreGeneration = uuid.NewString()
	entry.queueStoreRevision = 1
	pb := EntryToProto(entry)
	data, err := proto.Marshal(pb)
	if err != nil {
		return false, fmt.Errorf("marshal versioned queue entry %s: %w", key, err)
	}
	indexed, metaErr := s.queue.GetMeta(key)
	if metaErr != nil {
		return false, fmt.Errorf("load legacy queue metadata %s: %w", key, metaErr)
	}
	if err := s.queue.Put(key, data, indexedEntryMetadata(indexed)); err != nil {
		return false, fmt.Errorf("persist versioned queue entry %s: %w", key, err)
	}
	return true, nil
}

// DeleteQueued removes a queued entry
func (s *Storage) DeleteQueued(infohash string, cleanup func(*Entry) error) error {
	deleted, err := s.deleteQueuedIfPresent(infohash, nil, nil, cleanup)
	if err != nil {
		return err
	}
	if !deleted {
		return fmt.Errorf("%w for queue entry %s", ErrEntryNotFound, strings.ToLower(infohash))
	}
	return nil
}

// DeleteQueuedSnapshot removes a queue row only while it still belongs to the
// supplied workflow generation. Revision drift is intentionally ignored: user
// edits and mirror updates do not transfer ownership to a different job, while
// delete/re-add always creates a generation that an old job cannot remove.
func (s *Storage) DeleteQueuedSnapshot(snapshot *Entry, cleanup func(*Entry) error) (bool, error) {
	return s.DeleteQueuedSnapshotWhere(snapshot, nil, cleanup)
}

// DeleteQueuedSnapshotWhere also rechecks a scan predicate while the queue
// mutation lock is held. It prevents a same-generation row that recovered or
// changed category after a bulk scan from being deleted by the stale result.
func (s *Storage) DeleteQueuedSnapshotWhere(snapshot *Entry, predicate func(*Entry) bool, cleanup func(*Entry) error) (bool, error) {
	if snapshot == nil {
		return false, fmt.Errorf("queue snapshot is nil")
	}
	if snapshot.queueStoreGeneration == "" {
		return false, fmt.Errorf("%w for unversioned queue entry %s", ErrStaleEntryGeneration, strings.ToLower(snapshot.InfoHash))
	}
	expectedGeneration := snapshot.queueStoreGeneration
	return s.deleteQueuedIfPresent(snapshot.InfoHash, &expectedGeneration, predicate, cleanup)
}

// TakeQueued atomically removes and returns the current queue row without
// running cleanup. A higher-level lifecycle owner can then cancel workers and
// clean external resources without holding the storage mutation lock.
func (s *Storage) TakeQueued(infohash string) (*Entry, bool, error) {
	return s.takeQueuedIfPresent(infohash, nil, nil)
}

// TakeQueuedSnapshotWhere atomically takes only the matching generation and
// predicate result. This is the scan-to-delete commit point.
func (s *Storage) TakeQueuedSnapshotWhere(snapshot *Entry, predicate func(*Entry) bool) (*Entry, bool, error) {
	if snapshot == nil {
		return nil, false, fmt.Errorf("queue snapshot is nil")
	}
	if snapshot.queueStoreGeneration == "" {
		return nil, false, fmt.Errorf("%w for unversioned queue entry %s", ErrStaleEntryGeneration, strings.ToLower(snapshot.InfoHash))
	}
	expectedGeneration := snapshot.queueStoreGeneration
	return s.takeQueuedIfPresent(snapshot.InfoHash, &expectedGeneration, predicate)
}

func (s *Storage) takeQueuedIfPresent(infohash string, expectedGeneration *string, predicate func(*Entry) bool) (*Entry, bool, error) {
	key := strings.ToLower(infohash)
	unlock := s.lockQueueMutation(key)
	defer unlock()
	if !s.queue.Exists(key) {
		return nil, false, nil
	}
	entry, err := s.GetQueued(key)
	if err != nil {
		return nil, false, err
	}
	if expectedGeneration != nil && entry.queueStoreGeneration != *expectedGeneration {
		return entry, false, nil
	}
	if predicate != nil && !predicate(entry) {
		return entry, false, nil
	}
	if err := s.queue.Delete(key); err != nil {
		return nil, false, err
	}
	return entry, true, nil
}

func (s *Storage) deleteQueuedIfPresent(
	infohash string,
	expectedGeneration *string,
	predicate func(*Entry) bool,
	cleanup func(*Entry) error,
) (bool, error) {
	key := strings.ToLower(infohash)
	unlock := s.lockQueueMutation(key)
	defer unlock()
	if !s.queue.Exists(key) {
		return false, nil
	}
	entry, err := s.GetQueued(key)
	if err != nil {
		return false, err
	}
	if expectedGeneration != nil && entry.queueStoreGeneration != *expectedGeneration {
		return false, nil
	}
	if predicate != nil && !predicate(entry) {
		return false, nil
	}
	if err := s.queue.Delete(key); err != nil {
		return false, err
	}
	// Keep the per-key lifecycle lock through cleanup. A same-hash AddQueue
	// cannot start a new generation while old-generation files/providers are
	// still being removed.
	if cleanup != nil {
		if err := cleanup(entry); err != nil {
			return true, fmt.Errorf("queue entry %s deleted but cleanup failed: %w", key, err)
		}
	}
	return true, nil
}

// FilterQueued returns entries matching a filter
func (s *Storage) FilterQueued(filter func(*Entry) bool) ([]*Entry, error) {
	var entries []*Entry
	if err := s.queue.ForEach(func(key string, value []byte) error {
		var pb EntryProto
		if proto.Unmarshal(value, &pb) == nil {
			entry := ProtoToEntry(&pb)
			if filter == nil || filter(entry) {
				entries = append(entries, entry)
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan queued entries: %w", err)
	}
	return entries, nil
}

// DeleteWhereQueued deletes matching queued entries
func (s *Storage) DeleteWhereQueued(predicate func(*Entry) bool, cleanup func(*Entry) error) error {
	type candidate struct {
		key        string
		generation string
	}
	var candidates []candidate
	if err := s.queue.ForEach(func(key string, value []byte) error {
		var pb EntryProto
		if proto.Unmarshal(value, &pb) == nil {
			entry := ProtoToEntry(&pb)
			if predicate == nil || predicate(entry) {
				candidates = append(candidates, candidate{key: key, generation: entry.queueStoreGeneration})
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scan queued entries for deletion: %w", err)
	}

	var deleteErrors []error
	for _, candidate := range candidates {
		if _, err := s.deleteQueuedIfPresent(candidate.key, &candidate.generation, predicate, cleanup); err != nil {
			deleteErrors = append(deleteErrors, err)
		}
	}
	return errors.Join(deleteErrors...)
}

// UpdateWhereQueued updates matching queued entries
func (s *Storage) UpdateWhereQueued(filter func(*Entry) bool, updateFunc func(*Entry) bool) error {
	var keys []string

	if err := s.queue.ForEach(func(key string, value []byte) error {
		var pb EntryProto
		if proto.Unmarshal(value, &pb) == nil {
			entry := ProtoToEntry(&pb)
			if filter == nil || filter(entry) {
				keys = append(keys, key)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scan queued entries for update: %w", err)
	}

	var updateErrors []error
	for _, key := range keys {
		_, _, err := s.MutateQueuedIfPresent(key, func(entry *Entry) (bool, error) {
			if filter != nil && !filter(entry) {
				return false, nil
			}
			if updateFunc == nil {
				return false, nil
			}
			return updateFunc(entry), nil
		})
		if err != nil {
			updateErrors = append(updateErrors, fmt.Errorf("update queued entry %s: %w", key, err))
		}
	}
	return errors.Join(updateErrors...)
}

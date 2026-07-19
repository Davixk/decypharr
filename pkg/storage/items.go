package storage

import (
	"fmt"
	"sort"
	"strings"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage/hybrid"
	"google.golang.org/protobuf/proto"
)

type entryItemProjection struct {
	item     *EntryItem
	protocol config.Protocol
}

// preferEntryItemFile makes duplicate folder/file projections independent of
// insertion and scan order. The newest file wins; an exact timestamp tie is
// broken by the durable infohash identity.
func preferEntryItemFile(candidate, existing *File) bool {
	if candidate == nil {
		return false
	}
	if existing == nil || candidate.InfoHash == existing.InfoHash {
		return true
	}
	if !candidate.AddedOn.Equal(existing.AddedOn) {
		return candidate.AddedOn.After(existing.AddedOn)
	}
	if candidate.InfoHash != existing.InfoHash {
		return candidate.InfoHash > existing.InfoHash
	}
	if candidate.Size != existing.Size {
		return candidate.Size > existing.Size
	}
	return candidate.Name > existing.Name
}

func mergeEntryIntoItem(item *EntryItem, entry *Entry) {
	if item == nil || entry == nil {
		return
	}
	if item.Files == nil {
		item.Files = make(map[string]*File)
	}
	for fileName, file := range entry.Files {
		if preferEntryItemFile(file, item.Files[fileName]) {
			item.Files[fileName] = file
		}
	}
	item.Size = item.GetSize()
}

func addEntryToProjection(projections map[string]*entryItemProjection, entry *Entry) {
	if entry == nil {
		return
	}
	name := entry.GetFolder()
	if name == "" {
		return
	}
	projection := projections[name]
	if projection == nil {
		projection = &entryItemProjection{item: &EntryItem{Name: name, Files: make(map[string]*File)}}
		projections[name] = projection
	}
	if entry.Protocol != "" && (projection.protocol == "" || entry.Protocol < projection.protocol) {
		projection.protocol = entry.Protocol
	}
	mergeEntryIntoItem(projection.item, entry)
}

func (s *Storage) projectEntryItem(name string) (*entryItemProjection, error) {
	projections := make(map[string]*entryItemProjection, 1)
	if err := s.entries.ForEach(func(key string, value []byte) error {
		if strings.HasPrefix(key, "__") {
			return nil
		}
		var pb EntryProto
		if err := proto.Unmarshal(value, &pb); err != nil {
			// A corrupt sibling row must not permanently break folder rebuilds
			// for every healthy entry; it is skipped here just like in listings.
			s.logCorruptRow("entries", key, err)
			return nil
		}
		entry := ProtoToEntry(&pb)
		if entry.GetFolder() == name {
			addEntryToProjection(projections, entry)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return projections[name], nil
}

func (s *Storage) writeEntryItemProjectionLocked(name string, projection *entryItemProjection, oldPresent, oldReadable bool, oldItem *EntryItem) (bool, error) {
	if projection == nil || projection.item == nil || len(projection.item.Files) == 0 {
		if oldPresent {
			if err := s.entryItems.Delete(name); err != nil {
				return false, fmt.Errorf("delete stale entry item %s: %w", name, err)
			}
		}
		if err := s.DeleteEntryHealth(name); err != nil {
			return oldPresent, fmt.Errorf("delete stale entry health %s: %w", name, err)
		}
		return oldPresent, nil
	}

	projection.item.Name = name
	projection.item.Size = projection.item.GetSize()
	data, err := proto.Marshal(EntryItemToProto(projection.item))
	if err != nil {
		return false, fmt.Errorf("marshal rebuilt entry item %s: %w", name, err)
	}
	if err := s.entryItems.Put(name, data, nil); err != nil {
		return false, fmt.Errorf("write rebuilt entry item %s: %w", name, err)
	}

	changed := !oldPresent || !oldReadable || oldItem == nil ||
		oldItem.Name != name || oldItem.Size != projection.item.Size ||
		EntryItemRepairFingerprint(oldItem) != EntryItemRepairFingerprint(projection.item)
	if changed {
		s.MarkEntryDirty(name, projection.protocol, "entry_item_rebuilt")
	}
	return changed, nil
}

// rebuildEntryItemLocked replaces one derivative folder row from authoritative
// main-store entries. The caller must hold the folder's item mutation lock.
func (s *Storage) rebuildEntryItemLocked(name string, fallbackProtocol config.Protocol) (bool, error) {
	if name == "" {
		return false, nil
	}
	oldPresent := s.entryItems.Exists(name)
	oldReadable := false
	var oldItem *EntryItem
	if oldPresent {
		if data, err := s.entryItems.Get(name); err == nil {
			var pb EntryItemProto
			if err := proto.Unmarshal(data, &pb); err == nil {
				oldItem = ProtoToEntryItem(&pb)
				oldReadable = true
			}
		}
	}

	projection, err := s.projectEntryItem(name)
	if err != nil {
		return false, err
	}
	if projection != nil && projection.protocol == "" {
		projection.protocol = fallbackProtocol
	}
	return s.writeEntryItemProjectionLocked(name, projection, oldPresent, oldReadable, oldItem)
}

func (s *Storage) rebuildEntryItem(name string, fallbackProtocol config.Protocol) (bool, error) {
	if name == "" {
		return false, nil
	}
	unlock := s.lockItemMutation(name)
	defer unlock()
	return s.rebuildEntryItemLocked(name, fallbackProtocol)
}

// reconcileEntryItemsAtStartup reconstructs the complete WebDAV folder index
// from authoritative main rows. It runs before Storage is published to callers,
// so its one-pass snapshot cannot overwrite a concurrent mutation.
// It returns the number of items changed and the number of corrupt main rows
// skipped; corrupt rows are logged and excluded rather than aborting startup.
func (s *Storage) reconcileEntryItemsAtStartup() (int, int, error) {
	skipped := 0
	projections := make(map[string]*entryItemProjection)
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
		addEntryToProjection(projections, ProtoToEntry(&pb))
		return nil
	}); err != nil {
		return 0, skipped, err
	}

	existing := make(map[string]existingEntryItem)
	if err := s.entryItems.ForEach(func(key string, value []byte) error {
		state := existingEntryItem{present: true}
		var pb EntryItemProto
		if err := proto.Unmarshal(value, &pb); err == nil {
			state.readable = true
			state.item = ProtoToEntryItem(&pb)
		}
		existing[key] = state
		return nil
	}); err != nil {
		return 0, skipped, fmt.Errorf("scan existing entry items: %w", err)
	}

	if err := s.adoptLegacyItemFileDeletions(projections, existing); err != nil {
		return 0, skipped, err
	}

	names := make(map[string]struct{}, len(projections)+len(existing))
	for name := range projections {
		names[name] = struct{}{}
	}
	for name := range existing {
		names[name] = struct{}{}
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)

	changed := 0
	for _, name := range ordered {
		state := existing[name]
		unlock := s.lockItemMutation(name)
		itemChanged, err := s.writeEntryItemProjectionLocked(name, projections[name], state.present, state.readable, state.item)
		unlock()
		if err != nil {
			return changed, skipped, err
		}
		if itemChanged {
			changed++
		}
	}
	return changed, skipped, nil
}

type existingEntryItem struct {
	present  bool
	readable bool
	item     *EntryItem
}

// adoptLegacyItemFileDeletions migrates file soft-deletes that only exist on
// derived folder items into the authoritative main rows. Before the projection
// rework, RemoveTorrentFile recorded Deleted only on the EntryItem; rebuilding
// items purely from main rows would therefore resurrect those files in WebDAV
// after every restart. The flag is adopted only for the exact same durable file
// instance (same owning infohash and AddedOn), so a genuinely re-added file
// still reappears as expected. After adoption the main store owns the flag and
// every later projection or merge preserves it naturally.
func (s *Storage) adoptLegacyItemFileDeletions(projections map[string]*entryItemProjection, existing map[string]existingEntryItem) error {
	adoptions := make(map[string]map[string]struct{})
	for name, projection := range projections {
		if projection == nil || projection.item == nil {
			continue
		}
		state := existing[name]
		if !state.readable || state.item == nil {
			continue
		}
		for fileName, file := range projection.item.Files {
			if file == nil || file.Deleted || file.InfoHash == "" {
				continue
			}
			old := state.item.Files[fileName]
			if old == nil || !old.Deleted {
				continue
			}
			if old.InfoHash != file.InfoHash || !old.AddedOn.Equal(file.AddedOn) {
				continue
			}
			file.Deleted = true
			if adoptions[file.InfoHash] == nil {
				adoptions[file.InfoHash] = make(map[string]struct{})
			}
			adoptions[file.InfoHash][fileName] = struct{}{}
		}
	}
	for infohash, fileNames := range adoptions {
		if err := s.persistAdoptedFileDeletions(infohash, fileNames); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) persistAdoptedFileDeletions(infohash string, fileNames map[string]struct{}) error {
	unlock := s.lockEntryMutation(infohash)
	defer unlock()
	entry, decodeErr, err := s.loadEntryClassified(s.entries, infohash)
	if err != nil {
		return fmt.Errorf("load entry %s for legacy deletion adoption: %w", infohash, err)
	}
	if decodeErr != nil || entry == nil {
		// Corrupt or vanished rows have nothing durable to adopt into.
		return nil
	}
	changed := false
	for fileName := range fileNames {
		if file := entry.Files[fileName]; file != nil && !file.Deleted {
			file.Deleted = true
			changed = true
		}
	}
	if !changed {
		return nil
	}
	data, err := proto.Marshal(EntryToProto(entry))
	if err != nil {
		return fmt.Errorf("marshal entry %s after legacy deletion adoption: %w", infohash, err)
	}
	if err := s.entries.Put(infohash, data, entryMetadata(entry)); err != nil {
		return fmt.Errorf("persist entry %s after legacy deletion adoption: %w", infohash, err)
	}
	s.logger.Info().Str("entry", infohash).Int("files", len(fileNames)).Msg("Adopted legacy file soft-deletes into authoritative entry")
	return nil
}

// GetEntryItems returns all entry item names
func (s *Storage) GetEntryItems() map[string]struct{} {
	items := make(map[string]struct{})
	_ = s.entryItems.ForEachMeta(func(key string, meta *hybrid.IndexEntry) error {
		items[key] = struct{}{}
		return nil
	})
	return items
}

// UpdateEntryItem updates an entry item from an entry
func (s *Storage) UpdateEntryItem(entry *Entry) error {
	if entry == nil {
		return fmt.Errorf("entry is nil")
	}
	_, err := s.rebuildEntryItem(entry.GetFolder(), entry.Protocol)
	return err
}

func (s *Storage) UpdateItem(item *EntryItem) error {
	unlock := s.lockItemMutation(item.Name)
	defer unlock()

	var oldFingerprint string
	if existing, err := s.GetEntryItem(item.Name); err == nil {
		oldFingerprint = EntryItemRepairFingerprint(existing)
	}

	pb := EntryItemToProto(item)
	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	if err := s.entryItems.Put(item.Name, data, nil); err != nil {
		return err
	}
	if oldFingerprint != EntryItemRepairFingerprint(item) {
		s.MarkEntryDirty(item.Name, "", "entry_item_changed")
	}
	return nil
}

// GetEntryItem retrieves an entry item by name
func (s *Storage) GetEntryItem(name string) (*EntryItem, error) {
	data, err := s.entryItems.Get(name)
	if err != nil {
		return nil, err
	}

	var pb EntryItemProto
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, err
	}
	return ProtoToEntryItem(&pb), nil
}

// ForEachEntryItem iterates over entry items. Undecodable derived rows are
// logged and skipped (they are rebuilt from authoritative entries by the
// startup reconcile and repair passes).
func (s *Storage) ForEachEntryItem(fn func(*EntryItem) error) error {
	return s.entryItems.ForEach(func(key string, value []byte) error {
		var pb EntryItemProto
		if err := proto.Unmarshal(value, &pb); err != nil {
			s.logCorruptRow("items", key, err)
			return nil
		}
		return fn(ProtoToEntryItem(&pb))
	})
}

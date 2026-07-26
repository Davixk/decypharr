package manager

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/version"
)

const (
	EntryAllFolder     string = "__all__"
	EntryBadFolder     string = "__bad__"
	EntryTorrentFolder string = "torrents"
	EntryNZBFolder     string = "nzbs"
	// EntryVersionFile is the only non-directory node at the mount root.
	EntryVersionFile string = "version.txt"
)

var (
	ErrCopyDestinationExists        = errors.New("copy destination exists")
	ErrCopyDestinationParentMissing = errors.New("copy destination parent does not exist")
	ErrCopyUnsupported              = errors.New("copy operation is not supported")
	ErrCopySourceActive             = errors.New("copy source has active queue work")
)

// FileInfo implements os.FileInfo
type FileInfo struct {
	name         string
	size         int64
	mode         os.FileMode
	modTime      time.Time
	isDir        bool
	content      []byte
	parent       string
	activeDebrid string
	canDelete    bool
	byteRange    *[2]int64
	infohash     string
	sys          any // For caching fuse nodes
}

func (f *FileInfo) Name() string         { return f.name }
func (f *FileInfo) Size() int64          { return f.size }
func (f *FileInfo) Mode() os.FileMode    { return f.mode }
func (f *FileInfo) ModTime() time.Time   { return f.modTime }
func (f *FileInfo) IsDir() bool          { return f.isDir }
func (f *FileInfo) Sys() any             { return f.sys }
func (f *FileInfo) SetSys(v any)         { f.sys = v }
func (f *FileInfo) Content() []byte      { return f.content }
func (f *FileInfo) Parent() string       { return f.parent }
func (f *FileInfo) ActiveDebrid() string { return f.activeDebrid }
func (f *FileInfo) CanDelete() bool      { return f.canDelete }
func (f *FileInfo) IsRemote() bool       { return len(f.content) == 0 }
func (f *FileInfo) ByteRange() *[2]int64 { return f.byteRange }
func (f *FileInfo) InfoHash() string     { return f.infohash }

// GetTorrentMountPath returns the full mount path for a torrent
// Returns the path based on the new unified mount structure
func (m *Manager) GetTorrentMountPath(torrent *storage.Entry) string {
	return filepath.Join(m.config.Mount.MountPath, EntryAllFolder, torrent.GetFolder())
}

func (m *Manager) setMountPaths() {
	m.rootInfo = &FileInfo{
		name:    "",
		size:    0,
		modTime: utils.Now(),
		isDir:   true,
	}
}

func (m *Manager) RootInfo() *FileInfo {
	if m.rootInfo == nil {
		m.rootInfo = &FileInfo{
			name:    "",
			size:    0,
			modTime: utils.Now(),
			isDir:   true,
		}
	}
	return m.rootInfo
}

// GetEntries returns the subdirectories under a given mount name
// it would show __all__, __bad__, torrents, nzbs, per-provider folders and any custom folders
func (m *Manager) GetEntries() []FileInfo {
	now := utils.Now()
	var subDirs []FileInfo
	extras := []string{EntryAllFolder, EntryBadFolder, EntryTorrentFolder, EntryNZBFolder}
	for _, dir := range extras {
		subDirs = append(subDirs, FileInfo{
			name:    dir,
			isDir:   true,
			modTime: now,
			size:    0,
		})
	}

	// Per-provider folders (one per configured debrid client)
	m.clients.Range(func(name string, _ debrid.Client) bool {
		subDirs = append(subDirs, FileInfo{
			name:    name,
			isDir:   true,
			modTime: now,
			size:    0,
		})
		return true
	})

	// AddOrUpdate custom folders
	if m.customFolders != nil {
		for _, folderName := range m.customFolders.folders {
			subDirs = append(subDirs, FileInfo{
				name:    folderName,
				isDir:   true,
				modTime: now,
				size:    0,
			})
		}
	}

	// AddOrUpdate version.txt — size MUST equal len(content) or FUSE reads
	// will hang/short-read waiting for bytes the backend never produces.
	versionContent := []byte(version.GetInfo().String() + "\n")
	subDirs = append(subDirs, FileInfo{
		name:    EntryVersionFile,
		isDir:   false,
		modTime: now,
		size:    int64(len(versionContent)),
		content: versionContent,
	})
	return subDirs
}

func (m *Manager) GetEntryChildren(group string) (*FileInfo, []FileInfo) {
	return m.entry.Get(group)
}

// isEntryGroup reports whether `group` names a REAL top-level node, i.e. one of
// the names GetEntries() actually advertises at the mount root:
//
//   - the four built-in folders __all__, __bad__, torrents, nzbs
//   - one folder per configured debrid client
//   - every configured custom folder
//   - the version.txt metadata file
//
// The set is deliberately read from the same three sources GetEntries() emits
// from, so the root listing and the per-name lookup cannot disagree about what
// exists.
//
// WHY THIS EXISTS. GetEntryNode used to synthesise a directory node for ANY
// name, so a one-segment PROPFIND on a group that does not exist answered
// `207 Multi-Status` with a self-entry and no children — byte-for-byte
// indistinguishable from a real but empty group. The same nonexistent name one
// level deeper has always returned 404 correctly. That gap is not cosmetic: an
// operator probing a suspected-missing path read "the directory exists and is
// empty" off an authoritative-looking 207 and an investigation went the wrong
// way for a while. A wrong answer that looks valid is worse than an error.
//
// This is a MEMBERSHIP test and nothing else. A real group holding no entries
// still resolves and still lists zero children — telling "real and empty" apart
// from "does not exist" is the entire point, and collapsing them the other way
// would hide legitimately empty groups.
//
// Nothing here is evidence about content. Group membership never reaches a
// repair/health verdict; the sweep enumerates entryItems keys directly.
func (m *Manager) isEntryGroup(group string) bool {
	switch group {
	case EntryAllFolder, EntryBadFolder, EntryTorrentFolder, EntryNZBFolder, EntryVersionFile:
		return true
	}
	// Per-provider folders. m.clients is replaced wholesale on a config reload,
	// so a provider folder can legitimately appear or disappear while the process
	// runs — which is why the answer is recomputed per lookup and a negative one
	// is never written to the entry cache.
	if m.clients != nil {
		if _, ok := m.clients.Load(group); ok {
			return true
		}
	}
	if m.customFolders != nil && slices.Contains(m.customFolders.folders, group) {
		return true
	}
	return false
}

// GetEntryNode returns only a top-level virtual node. Unlike GetEntryChildren
// it never refreshes or allocates the node's child cache, which keeps WebDAV
// Depth:0 metadata requests O(1). Most nodes are directories; version.txt is
// the local metadata file exposed at the mount root.
//
// It returns nil for a name that is not a real top-level node, so the WebDAV
// handler answers 404 rather than rendering an empty collection for it.
func (m *Manager) GetEntryNode(group string) *FileInfo {
	if !m.isEntryGroup(group) {
		return nil
	}
	info := &FileInfo{
		name:    group,
		modTime: utils.Now(),
		isDir:   true,
	}
	if group == EntryVersionFile {
		info.content = []byte(version.GetInfo().String() + "\n")
		info.size = int64(len(info.content))
		info.isDir = false
	}
	return info
}

func (m *Manager) GetTorrentChildren(name string) (*FileInfo, []FileInfo) {
	return m.entry.Get(torrentEntryCachePrefix + name)
}

func (m *Manager) GetTorrentEntry(torrentName string) (*FileInfo, error) {
	current, _ := m.GetTorrentChildren(torrentName)
	if current == nil {
		return nil, fmt.Errorf("torrent %s not found", torrentName)
	}
	return current, nil
}

// resolveEntryItem loads the folder row that serves `name`, following a legacy
// folder-name alias when — and only when — `name` matches no live entryItems key.
//
// A FolderNaming change re-derives every entryItems key (`filename` keeps a
// media extension, `filename_no_ext` strips it) but moves nothing else: the
// *arr symlinks on disk and the frozen IndexEntry.Name snapshots the `__all__`
// listing is built from still carry the OLD name. Resolving the alias here makes
// the old name address the same entry, and therefore the same children, as the
// new one — additively, with no rename and no data movement.
//
// EVERY name-addressed read path in the manager goes through this one function,
// so enumeration (getTorrentChildren), directory metadata (GetEntryInfo) and
// file reads (GetTorrentFile) can never disagree about which names resolve.
//
// The exact-name lookup is attempted FIRST and unchanged, so a live name costs
// exactly what it did before: one store Get. The alias work is on the miss path
// only. storage.GetEntryItem itself is deliberately left alone — the repair
// sweep resolves through it and must keep seeing exactly the key set it
// enumerates.
func (m *Manager) resolveEntryItem(name string) (*storage.EntryItem, error) {
	item, err := m.storage.GetEntryItem(name)
	if err == nil {
		return item, nil
	}
	alias, ok := m.storage.LegacyEntryItemName(name)
	if !ok {
		return nil, err
	}
	aliased, aliasErr := m.storage.GetEntryItem(alias)
	if aliasErr != nil {
		// The alias raced a delete. Report the original miss; inventing a
		// different error here would only obscure which name was asked for.
		return nil, err
	}
	return aliased, nil
}

// GetEntryInfo returns a FileInfo for a torrent/entry by name - O(1) lookup
func (m *Manager) GetEntryInfo(name string) (*FileInfo, error) {
	entry, err := m.resolveEntryItem(name)
	if err != nil {
		return nil, fmt.Errorf("entry %s not found", name)
	}

	// get metadata from first file (all files in an entry share the same parent entry)
	var modTime time.Time
	var infohash string
	for _, f := range entry.Files {
		modTime = f.AddedOn
		infohash = f.InfoHash
		break
	}

	return &FileInfo{
		infohash: infohash,
		// The REQUESTED name, not entry.Name. For a live name the two are
		// identical (the entryItems key is always written as the item's Name);
		// for a legacy alias this keeps the node's displayname matching the href
		// the client asked for, so the directory does not appear to rename
		// itself mid-listing.
		name:      name,
		size:      entry.Size,
		modTime:   modTime,
		isDir:     true,
		canDelete: true,
	}, nil
}

func (m *Manager) GetTorrentFile(torrentName, fileName string) (*FileInfo, error) {
	entry, err := m.resolveEntryItem(torrentName)
	if err != nil {
		return nil, fmt.Errorf("torrent %s not found", torrentName)
	}
	file, err := entry.GetFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("file %s not found in torrent %s", fileName, torrentName)
	}
	return &FileInfo{
		infohash:  file.InfoHash,
		name:      file.Name,
		size:      file.Size,
		modTime:   file.AddedOn,
		isDir:     false,
		parent:    entry.Name,
		canDelete: true,
		byteRange: file.ByteRange,
	}, nil
}

// getEntryChildren
// Groups are __all__, __bad__, custom folders
// Uses metadata-only iteration (no disk reads, no protobuf deserialization)
func (m *Manager) getEntryChildren(group string) (*FileInfo, []FileInfo) {
	currentDir := m.GetEntryNode(group)
	if currentDir == nil {
		// Not a real top-level name. Resolving it to nothing is what makes
		// PROPFIND and /api/browse answer 404 here, exactly as they already do
		// for a nonexistent name one level deeper. A real-but-empty group still
		// gets a node from GetEntryNode and falls through to list zero children.
		return nil, nil
	}
	switch group {
	case EntryAllFolder:
		// This returns all entries - using metadata-only iteration (no disk reads)
		var infos []FileInfo
		seen := make(map[string]struct{})
		err := m.storage.ForEachMeta(func(meta *storage.EntryMetaInfo) error {
			if meta.Name == "" {
				// Internal bookkeeping rows (e.g. __migration_status__) are stored
				// in the same index with nil metadata, so they decode to a blank
				// Name. ForEachMeta does not filter them the way ForEach/List do,
				// and a blank-named child both claims the seen[""] slot and
				// advertises an unopenable directory on the mount.
				return nil
			}
			if _, ok := seen[meta.Name]; ok {
				return nil
			}
			seen[meta.Name] = struct{}{}
			infos = append(infos, FileInfo{
				infohash:     meta.InfoHash,
				name:         meta.Name,
				size:         meta.Size,
				modTime:      meta.AddedOn,
				isDir:        true,
				activeDebrid: meta.Provider,
				canDelete:    true,
			})
			return nil
		})
		if err != nil {
			return nil, nil
		}
		return currentDir, m.reconcileAllFolderChildren(infos, seen)
	case EntryTorrentFolder:
		// This returns all torrents - using metadata-only iteration
		var infos []FileInfo
		seen := make(map[string]struct{})
		err := m.storage.ForEachMeta(func(meta *storage.EntryMetaInfo) error {
			if meta.Protocol == "torrent" {
				if _, ok := seen[meta.Name]; ok {
					return nil
				}
				seen[meta.Name] = struct{}{}
				infos = append(infos, FileInfo{
					infohash:     meta.InfoHash,
					name:         meta.Name,
					size:         meta.Size,
					modTime:      meta.AddedOn,
					isDir:        true,
					activeDebrid: meta.Provider,
					canDelete:    true,
				})
			}
			return nil
		})
		if err != nil {
			return nil, nil
		}
		return currentDir, infos
	case EntryNZBFolder:
		// This returns all nzbs - using metadata-only iteration
		var infos []FileInfo
		seen := make(map[string]struct{})
		err := m.storage.ForEachMeta(func(meta *storage.EntryMetaInfo) error {
			if meta.Protocol == "nzb" {
				if _, ok := seen[meta.Name]; ok {
					return nil
				}
				seen[meta.Name] = struct{}{}
				infos = append(infos, FileInfo{
					infohash:     meta.InfoHash,
					name:         meta.Name,
					size:         meta.Size,
					modTime:      meta.AddedOn,
					isDir:        true,
					activeDebrid: meta.Provider,
					canDelete:    true,
				})
			}
			return nil
		})
		if err != nil {
			return nil, nil
		}
		return currentDir, infos
	case EntryBadFolder:
		// Filter for bad entries - using metadata-only iteration
		var infos []FileInfo
		seen := make(map[string]struct{})
		err := m.storage.ForEachMeta(func(meta *storage.EntryMetaInfo) error {
			if meta.Bad {
				if _, ok := seen[meta.Name]; ok {
					return nil
				}
				seen[meta.Name] = struct{}{}
				infos = append(infos, FileInfo{
					infohash:     meta.InfoHash,
					name:         meta.Name,
					size:         meta.Size,
					modTime:      meta.AddedOn,
					isDir:        true,
					activeDebrid: meta.Provider,
					canDelete:    true,
				})
			}
			return nil
		})
		if err != nil {
			return nil, nil
		}
		return currentDir, infos
	case EntryVersionFile:
		currentDir.content = []byte(version.GetInfo().String() + "\n")
		currentDir.size = int64(len(currentDir.content))
		currentDir.isDir = false
		return currentDir, nil
	default:
		// Per-provider folder if the name matches a configured client
		if m.clients != nil {
			if _, ok := m.clients.Load(group); ok {
				var infos []FileInfo
				seen := make(map[string]struct{})
				err := m.storage.ForEachMeta(func(meta *storage.EntryMetaInfo) error {
					if meta.Provider == group {
						if _, ok := seen[meta.Name]; ok {
							return nil
						}
						seen[meta.Name] = struct{}{}
						infos = append(infos, FileInfo{
							infohash:     meta.InfoHash,
							name:         meta.Name,
							size:         meta.Size,
							modTime:      meta.AddedOn,
							isDir:        true,
							activeDebrid: meta.Provider,
							canDelete:    true,
						})
					}
					return nil
				})
				if err != nil {
					return nil, nil
				}
				return currentDir, infos
			}
		}
		// Custom folder. isEntryGroup already established membership, so the
		// only way to reach here is a configured folder — one whose filters match
		// nothing simply lists zero children.
		return currentDir, m.getCustomFolderChildren(group)
	}
}

// reconcileAllFolderChildren makes the `__all__` listing advertise every entry
// that is actually NAVIGABLE, and reports every listed name that is not.
//
// THE BUG IT FIXES. The listing and the folder it links to derive their names
// from the same function, storage.Entry.GetFolder(), at two different times:
//
//   - the LISTING name is IndexEntry.Name — computed once, at the entry's last
//     Put, and then frozen on disk. Nothing recomputes it: the metadata
//     migration self-selects out for any row that already carries a Protocol.
//   - the NAVIGABLE name is the entryItems key — re-derived live, and rebuilt
//     for every entry on every boot by reconcileEntryItemsAtStartup.
//
// GetFolder() reads the process-global config.FolderNaming, so it is not a pure
// function of the stored entry. Change that setting (or an entry's Name) without
// re-Putting the entry and the two permanently disagree: `filename` keeps a
// media extension, `filename_no_ext` strips it. Observed in production on 9
// entries — the listing advertised `<name>.mkv`, which resolves to nothing
// (PROPFIND 207 with zero children, /api/browse "Torrent not found"), while the
// real, byte-serving `<name>` was never emitted at all. rclone mirrors the
// listing faithfully, so Plex and the *arrs saw an empty phantom directory where
// working content lived.
//
// THE FIX IS PURELY ADDITIVE. Every entryItems key missing from the listing is
// appended; nothing that is advertised today is removed. That matters twice
// over: it cannot rename an on-mount path (these names ALREADY resolve — they
// are the keys the mount, the API and the repair sweep have always used), and a
// freshly-added entry whose derived row lags briefly cannot be hidden by it.
//
// Phantoms are counted and logged rather than dropped, so the affected set is
// enumerable without guessing. Dropping them is a separate decision — and one
// that must not be taken lightly, because the legacy names among them are
// exactly the names existing *arr symlinks point at. Those are now navigable
// through resolveEntryItem's alias, so they are counted separately (aliased_names)
// from names that resolve to nothing at all (phantom_names).
//
// THIS IS A VISIBILITY FIX AND NOTHING ELSE. Listing membership is never
// consulted by any health verdict: the repair sweep enumerates entryItems keys
// directly (enumerateManagedCandidates), so no entry can be classified broken —
// and therefore pruned — for being absent here. These 9 entries are alive and
// serve full range reads; the listing was the only thing wrong with them.
func (m *Manager) reconcileAllFolderChildren(infos []FileInfo, listed map[string]struct{}) []FileInfo {
	navigable := m.storage.GetEntryItems()
	if len(navigable) == 0 {
		return infos
	}

	var restored []string
	for name := range navigable {
		if _, ok := listed[name]; ok {
			continue
		}
		item, err := m.storage.GetEntryItem(name)
		if err != nil || item == nil || len(item.Files) == 0 {
			continue
		}
		info := FileInfo{
			name:      name,
			size:      item.GetSize(),
			isDir:     true,
			canDelete: true,
		}
		for _, f := range item.Files {
			if f == nil || f.Deleted {
				continue
			}
			info.infohash = f.InfoHash
			info.modTime = f.AddedOn
			break
		}
		if info.infohash == "" {
			// Every file soft-deleted: nothing to navigate to.
			continue
		}
		infos = append(infos, info)
		restored = append(restored, name)
	}

	// Split the names that are advertised but not navigable into the two very
	// different things they actually are. A name that resolves through a legacy
	// folder-name alias is fully usable — it enumerates and serves the same
	// entry as the live name — and counting it as a phantom made the gauge read
	// as thousands of broken directories when none of them were broken. One
	// alias snapshot covers the whole loop, so this adds no index walk per name.
	aliases := m.storage.EntryItemAliases()
	phantoms := 0
	aliased := 0
	for name := range listed {
		if _, ok := navigable[name]; ok {
			continue
		}
		if _, ok := aliases.Resolve(name); ok {
			aliased++
			continue
		}
		phantoms++
	}

	if len(restored) > 0 || phantoms > 0 || aliased > 0 {
		// Counts at Warn: one greppable line per listing rebuild. `restored` is
		// the EXACT number of entries that would otherwise be invisible on the
		// mount. `aliased` is the number of advertised legacy names that resolve
		// onto a live entry through the alternate FolderNaming derivation — the
		// names *arr symlinks written under the old setting still point at.
		// Both stay non-zero until the frozen IndexEntry.Name snapshots are
		// rewritten, so read them as standing gauges, not one-shot events.
		// `phantom_names` is what is left: advertised, not navigable, and not
		// resolvable under any derivation.
		m.logger.Warn().
			Int("restored", len(restored)).
			Int("aliased_names", aliased).
			Int("phantom_names", phantoms).
			Int("listed", len(listed)).
			Int("navigable", len(navigable)).
			Msg("__all__ listing disagreed with the navigable entry set; servable entries restored to the listing")
		// Names at Debug: thousands of release names is several KB, which does
		// not belong on a line that repeats on every entry-cache refresh.
		m.logger.Debug().Strs("restored_entries", restored).Msg("__all__ entries restored to the listing")
	}
	return infos
}

func (m *Manager) getTorrentChildren(name string) (*FileInfo, []FileInfo) {
	// Find the torrent by folder name, following a legacy folder-name alias on
	// the miss path so an old name enumerates the SAME children as the live one.
	entry, err := m.resolveEntryItem(name)
	if err != nil || entry == nil {
		return nil, nil
	}

	// Convert files to FileInfo
	infos := make([]FileInfo, 0, len(entry.Files))
	size := int64(0)
	for _, file := range entry.Files {
		infos = append(infos, FileInfo{
			infohash: file.InfoHash,
			name:     file.Name,
			size:     file.Size,
			modTime:  file.AddedOn,
			isDir:    false,
			// The LIVE key, never the requested alias: parent is what every
			// mutation path (RemoveTorrentFile, COPY/MOVE) keys off, and those
			// must address the row that actually exists.
			parent:    entry.Name,
			canDelete: true,
			byteRange: file.ByteRange,
		})
		size += file.Size
	}
	if len(infos) == 0 {
		return nil, nil
	}

	currentDir := &FileInfo{
		// The REQUESTED name, so the self-entry of a PROPFIND keeps the href the
		// client asked for. Identical to entry.Name for every live name.
		name:    name,
		size:    size,
		modTime: infos[0].modTime,
		isDir:   true,
	}
	return currentDir, infos
}

func (m *Manager) RemoveEntry(entry *FileInfo) error {
	if entry == nil {
		return fmt.Errorf("entry is nil")
	}
	if !entry.CanDelete() {
		return fmt.Errorf("entry %s cannot be deleted", entry.name)
	}

	if entry.isDir {
		// This is a torrent folder
		m.logger.Debug().Str("entry", entry.name).Msg("Removing entry folder")
		infohash := entry.infohash
		if infohash == "" {
			// Fallback: look up from storage
			et, err := m.storage.GetEntryItem(entry.name)
			if err != nil {
				return fmt.Errorf("torrent %s not found", entry.name)
			}
			if len(et.Files) == 0 {
				return fmt.Errorf("torrent %s has no files", entry.name)
			}
			firstFile, err := et.GetFirstFile()
			if err != nil {
				return fmt.Errorf("failed to get first file of torrent %s: %w", entry.name, err)
			}
			infohash = firstFile.InfoHash
		}
		return m.DeleteEntry(infohash, true)
	}
	// This is a file within a torrent
	return m.RemoveTorrentFile(entry.Parent(), entry.Name())
}

// CopyEntry preserves the historical Manager API and applies the WebDAV
// default of overwriting an existing destination. Callers that need the
// creation/replacement result or Overwrite: F use CopyEntryWithOverwrite.
func (m *Manager) CopyEntry(entry *FileInfo, destPath string, delete bool) error {
	_, err := m.CopyEntryWithOverwrite(entry, destPath, delete, true)
	return err
}

// CopyEntryWithOverwrite copies a virtual torrent folder or a file within one
// torrent. It returns true when a new destination resource was created and
// false when an existing resource was replaced. A MOVE always commits the
// complete destination before attempting to remove the source.
func (m *Manager) CopyEntryWithOverwrite(entry *FileInfo, destPath string, delete, overwrite bool) (bool, error) {
	if entry == nil {
		return false, fmt.Errorf("entry is nil")
	}
	if !entry.CanDelete() {
		return false, fmt.Errorf("entry %s cannot be copied", entry.name)
	}

	destination, err := parseCopyDestination(destPath, entry.isDir)
	if err != nil {
		return false, err
	}

	m.copyEntryMu.Lock()
	defer m.copyEntryMu.Unlock()

	var changed bool
	var created bool
	if entry.isDir {
		created, changed, err = m.copyFolderEntry(entry, destination.name, delete, overwrite)
	} else {
		created, changed, err = m.copyFileEntry(entry, destination, delete, overwrite)
	}
	if changed {
		m.refreshCopiedEntryViews()
	}
	return created, err
}

type copyDestination struct {
	parent string
	name   string
}

func parseCopyDestination(destPath string, directory bool) (copyDestination, error) {
	clean := path.Clean(strings.ReplaceAll(strings.TrimSpace(destPath), "\\", "/"))
	if clean == "." || clean == "/" {
		return copyDestination{}, fmt.Errorf("%w: %q", ErrCopyDestinationParentMissing, destPath)
	}
	parts := strings.Split(strings.Trim(clean, "/"), "/")
	name := parts[len(parts)-1]
	if name == "" || name == "." || name == ".." {
		return copyDestination{}, fmt.Errorf("%w: %q", ErrCopyDestinationParentMissing, destPath)
	}
	if directory {
		return copyDestination{name: name}, nil
	}
	if len(parts) < 2 {
		return copyDestination{}, fmt.Errorf("%w: %q", ErrCopyDestinationParentMissing, destPath)
	}
	return copyDestination{parent: parts[len(parts)-2], name: name}, nil
}

func (m *Manager) copyFolderEntry(info *FileInfo, destinationName string, move, overwrite bool) (bool, bool, error) {
	source, err := m.singleFolderEntry(info.name, info.infohash)
	if err != nil {
		return false, false, err
	}
	if m.storage.QueueExists(source.InfoHash) {
		return false, false, fmt.Errorf("%w: %s", ErrCopySourceActive, source.InfoHash)
	}
	if !source.IsTorrent() {
		return false, false, fmt.Errorf("%w: folder COPY/MOVE currently supports torrents only", ErrCopyUnsupported)
	}
	if destinationName == source.GetFolder() {
		if !overwrite {
			return false, false, fmt.Errorf("%w: %s", ErrCopyDestinationExists, destinationName)
		}
		return false, false, nil
	}

	existing, destinationExists, err := m.lookupSingleFolderEntry(destinationName)
	if err != nil {
		return false, false, err
	}
	if destinationExists && !overwrite {
		return false, false, fmt.Errorf("%w: %s", ErrCopyDestinationExists, destinationName)
	}

	destination, err := m.cloneFolderEntry(source, destinationName)
	if err != nil {
		return false, false, err
	}
	if err := m.storage.AddOrUpdate(destination); err != nil {
		return false, false, fmt.Errorf("commit destination folder %s: %w", destinationName, err)
	}
	changed := true

	if destinationExists {
		deleted, deleteErr := m.storage.DeleteIfCurrentWithCleanup(existing, m.removeTorrentPlacementsLocked)
		if deleteErr != nil || !deleted {
			// Cleanup now runs before the old destination row is removed, so a
			// cleanup failure leaves that row in place (deleted=false) and the
			// copied destination is rolled back. The deleted==true guard stays
			// as defense in depth for a failure after the row was removed; in
			// that case the new destination is the sole durable owner and must
			// not be rolled back to a deleted snapshot.
			if deleted {
				return false, changed, fmt.Errorf("replace destination folder %s: old placement cleanup failed: %w", destinationName, deleteErr)
			}
			if deleteErr == nil {
				deleteErr = fmt.Errorf("destination folder changed before replacement")
			}
			rollbackErr := m.rollbackCopiedFolder(destination, existing)
			return false, changed, errors.Join(fmt.Errorf("replace destination folder %s: %w", destinationName, deleteErr), rollbackErr)
		}
	}

	if move {
		m.runCopyEntryTestHook("destination-committed")
		deleted, deleteErr := m.storage.DeleteIfCurrent(source)
		if deleteErr != nil {
			return !destinationExists, changed, fmt.Errorf("remove MOVE source folder %s: %w", source.GetFolder(), deleteErr)
		}
		if !deleted {
			return !destinationExists, changed, fmt.Errorf("remove MOVE source folder %s: source changed", source.GetFolder())
		}
	}
	return !destinationExists, changed, nil
}

func (m *Manager) copyFileEntry(info *FileInfo, destination copyDestination, move, overwrite bool) (bool, bool, error) {
	if destination.parent != info.parent {
		if _, exists := m.storage.GetEntryItems()[destination.parent]; !exists {
			return false, false, fmt.Errorf("%w: %s", ErrCopyDestinationParentMissing, destination.parent)
		}
		return false, false, fmt.Errorf("%w: copying files between backing torrents", ErrCopyUnsupported)
	}
	source, err := m.fileBackingEntry(info)
	if err != nil {
		return false, false, err
	}
	if m.storage.QueueExists(source.InfoHash) {
		return false, false, fmt.Errorf("%w: %s", ErrCopySourceActive, source.InfoHash)
	}
	if !source.IsTorrent() {
		return false, false, fmt.Errorf("%w: file COPY/MOVE currently supports torrents only", ErrCopyUnsupported)
	}
	if destination.name == info.name {
		if !overwrite {
			return false, false, fmt.Errorf("%w: %s", ErrCopyDestinationExists, destination.name)
		}
		return false, false, nil
	}

	created := false
	updated, present, err := m.storage.MutateEntrySnapshot(source, func(current *storage.Entry) (bool, error) {
		sourceFile, exists := current.Files[info.name]
		if !exists || sourceFile == nil || sourceFile.Deleted {
			return false, fmt.Errorf("source file %s is no longer available", info.name)
		}
		destinationFile, destinationExists := current.Files[destination.name]
		destinationExists = destinationExists && destinationFile != nil && !destinationFile.Deleted
		if destinationExists && !overwrite {
			return false, fmt.Errorf("%w: %s", ErrCopyDestinationExists, destination.name)
		}

		created = !destinationExists
		copiedFile := *sourceFile
		copiedFile.Name = destination.name
		copiedFile.InfoHash = current.InfoHash
		copiedFile.Deleted = false
		copiedFile.AddedOn = utils.Now()
		current.Files[destination.name] = &copiedFile
		copyProviderFileAliases(current, info.name, destination.name)
		return true, nil
	})
	if err != nil {
		return false, false, fmt.Errorf("commit destination file %s: %w", destination.name, err)
	}
	if !present || updated == nil {
		return false, false, fmt.Errorf("source entry %s disappeared before destination commit", source.InfoHash)
	}
	changed := true

	if move {
		m.runCopyEntryTestHook("destination-committed")
		_, present, err = m.storage.MutateEntrySnapshot(updated, func(current *storage.Entry) (bool, error) {
			sourceFile, exists := current.Files[info.name]
			if !exists || sourceFile == nil || sourceFile.Deleted {
				return false, fmt.Errorf("source file %s is no longer available", info.name)
			}
			sourceFile.Deleted = true
			for _, provider := range current.Providers {
				if provider != nil {
					delete(provider.Files, info.name)
				}
			}
			return true, nil
		})
		if err != nil {
			return created, changed, fmt.Errorf("remove MOVE source file %s: %w", info.name, err)
		}
		if !present {
			return created, changed, fmt.Errorf("remove MOVE source file %s: source entry disappeared", info.name)
		}
	}
	return created, changed, nil
}

func copyProviderFileAliases(entry *storage.Entry, sourceName, destinationName string) {
	for _, provider := range entry.Providers {
		if provider == nil {
			continue
		}
		sourceFile, exists := provider.Files[sourceName]
		if !exists || sourceFile == nil {
			delete(provider.Files, destinationName)
			continue
		}
		if provider.Files == nil {
			provider.Files = make(map[string]*storage.ProviderFile)
		}
		copiedFile := *sourceFile
		provider.Files[destinationName] = &copiedFile
	}
}

func (m *Manager) fileBackingEntry(info *FileInfo) (*storage.Entry, error) {
	infohash := info.infohash
	if infohash == "" {
		item, err := m.storage.GetEntryItem(info.parent)
		if err != nil {
			return nil, fmt.Errorf("load source folder %s: %w", info.parent, err)
		}
		file, err := item.GetFile(info.name)
		if err != nil {
			return nil, fmt.Errorf("load source file %s/%s: %w", info.parent, info.name, err)
		}
		infohash = file.InfoHash
	}
	entry, err := m.storage.Get(infohash)
	if err != nil {
		return nil, fmt.Errorf("load backing entry %s: %w", infohash, err)
	}
	return entry, nil
}

func (m *Manager) singleFolderEntry(folder, preferredInfohash string) (*storage.Entry, error) {
	entry, exists, err := m.lookupSingleFolderEntry(folder)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("source folder %s not found", folder)
	}
	if preferredInfohash != "" && entry.InfoHash != preferredInfohash {
		return nil, fmt.Errorf("%w: folder %s has a different backing entry", ErrCopyUnsupported, folder)
	}
	return entry, nil
}

func (m *Manager) lookupSingleFolderEntry(folder string) (*storage.Entry, bool, error) {
	if _, exists := m.storage.GetEntryItems()[folder]; !exists {
		return nil, false, nil
	}
	item, err := m.storage.GetEntryItem(folder)
	if err != nil {
		return nil, true, fmt.Errorf("load folder %s: %w", folder, err)
	}
	infohashes := make(map[string]struct{})
	for _, file := range item.Files {
		if file != nil && !file.Deleted && file.InfoHash != "" {
			infohashes[file.InfoHash] = struct{}{}
		}
	}
	if len(infohashes) != 1 {
		return nil, true, fmt.Errorf("%w: folder %s has %d backing entries", ErrCopyUnsupported, folder, len(infohashes))
	}
	var infohash string
	for candidate := range infohashes {
		infohash = candidate
	}
	entry, err := m.storage.Get(infohash)
	if err != nil {
		return nil, true, fmt.Errorf("load backing entry %s for folder %s: %w", infohash, folder, err)
	}
	return entry, true, nil
}

func (m *Manager) cloneFolderEntry(source *storage.Entry, destinationName string) (*storage.Entry, error) {
	pb := storage.EntryToProto(source)
	pb.MainStoreGeneration = ""
	pb.MainStoreRevision = 0
	pb.QueueStoreGeneration = ""
	pb.QueueStoreRevision = 0
	destination := storage.ProtoToEntry(pb)

	var destinationInfohash string
	for attempt := 0; attempt < 16; attempt++ {
		candidate := utils.GenerateInfoHash()
		if candidate == "" {
			continue
		}
		exists, err := m.storage.Exists(candidate)
		if err != nil {
			return nil, fmt.Errorf("check generated destination identity: %w", err)
		}
		if !exists {
			destinationInfohash = candidate
			break
		}
	}
	if destinationInfohash == "" {
		return nil, fmt.Errorf("generate unique destination identity")
	}

	now := utils.Now()
	destination.InfoHash = destinationInfohash
	destination.Name = destinationName
	destination.OriginalFilename = destinationName
	destination.AddedOn = now
	destination.CreatedAt = time.Time{}
	destination.UpdatedAt = time.Time{}
	for _, file := range destination.Files {
		if file != nil {
			file.InfoHash = destinationInfohash
			file.AddedOn = now
		}
	}
	if destination.GetFolder() != destinationName {
		return nil, fmt.Errorf("%w: folder naming mode maps %q to %q", ErrCopyUnsupported, destinationName, destination.GetFolder())
	}
	return destination, nil
}

func (m *Manager) rollbackCopiedFolder(destination, restore *storage.Entry) error {
	var rollbackErrors []error
	if deleted, err := m.storage.DeleteIfCurrent(destination); err != nil {
		rollbackErrors = append(rollbackErrors, fmt.Errorf("rollback destination: %w", err))
	} else if !deleted {
		rollbackErrors = append(rollbackErrors, fmt.Errorf("rollback destination: destination changed"))
	}
	if restore != nil {
		current, err := m.storage.Get(restore.InfoHash)
		if err != nil {
			rollbackErrors = append(rollbackErrors, fmt.Errorf("restore destination index: %w", err))
		} else if err := m.storage.UpdateEntryItem(current); err != nil {
			rollbackErrors = append(rollbackErrors, fmt.Errorf("restore destination index: %w", err))
		}
	}
	return errors.Join(rollbackErrors...)
}

func (m *Manager) runCopyEntryTestHook(stage string) {
	if m.copyEntryTestHook != nil {
		m.copyEntryTestHook(stage)
	}
}

func (m *Manager) refreshCopiedEntryViews() {
	if m.entry != nil {
		m.entry.Refresh()
	}
	if m.config != nil {
		if err := m.RefreshMount(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to refresh mount after COPY/MOVE")
		}
	}
}

func (m *Manager) RemoveTorrentFile(torrentName, filename string) error {
	item, err := m.storage.GetEntryItem(torrentName)
	if err != nil {
		return fmt.Errorf("entry %s not found", torrentName)
	}
	file, err := item.GetFile(filename)
	if err != nil {
		return fmt.Errorf("file %s not found in entry %s", filename, torrentName)
	}
	updated, present, err := m.storage.MutateEntryIfPresent(file.InfoHash, func(entry *storage.Entry) (bool, error) {
		current, exists := entry.Files[filename]
		if !exists || current == nil || current.Deleted {
			return false, fmt.Errorf("file %s not found in entry %s", filename, torrentName)
		}
		current.Deleted = true
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("failed to update entry %s: %w", torrentName, err)
	}
	if !present || updated == nil {
		return fmt.Errorf("entry %s disappeared before file removal", torrentName)
	}

	if len(updated.GetActiveFiles()) == 0 {
		m.logger.Debug().Str("entry", torrentName).Msg("Removing entry folder as it has no more files")
		return m.DeleteEntry(file.InfoHash, true)
	}
	m.refreshCopiedEntryViews()
	return nil
}

func (m *Manager) getCustomFolderChildren(folder string) []FileInfo {
	filters := m.customFolders.filters[folder]
	if len(filters) == 0 {
		return nil
	}

	// Use metadata-only iteration (no disk reads)
	var infos []FileInfo
	seen := make(map[string]struct{})
	err := m.storage.ForEachMeta(func(meta *storage.EntryMetaInfo) error {
		if meta.Bad {
			return nil
		}
		getFileNames := func() []string {
			item, err := m.storage.GetEntryItem(meta.Name)
			if err != nil || item == nil {
				return nil
			}
			names := make([]string, 0, len(item.Files))
			for fn := range item.Files {
				names = append(names, strings.ToLower(fn))
			}
			return names
		}
		if m.customFolders.matchesFilter(folder, &FileInfo{
			name: meta.Name,
			size: meta.Size,
		}, meta.AddedOn, getFileNames) {
			if _, ok := seen[meta.Name]; ok {
				return nil
			}
			seen[meta.Name] = struct{}{}
			infos = append(infos, FileInfo{
				infohash:     meta.InfoHash,
				name:         meta.Name,
				size:         meta.Size,
				modTime:      meta.AddedOn,
				isDir:        true,
				activeDebrid: meta.Provider,
				canDelete:    true,
			})
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return infos
}

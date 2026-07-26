package manager

import (
	"strings"

	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/sync/singleflight"
)

const (
	torrentEntryCachePrefix = "torrent::"
)

func (m *Manager) initEntryCache() {
	m.entry = NewEntryCache(m)
}

type EntryCacheItem struct {
	current  *FileInfo
	children []FileInfo
}

type EntryCache struct {
	manager    *Manager
	entries    *xsync.Map[string, EntryCacheItem]
	refreshing singleflight.Group
}

func NewEntryCache(manager *Manager) *EntryCache {
	return &EntryCache{
		manager: manager,
		entries: xsync.NewMap[string, EntryCacheItem](),
	}
}

func (e *EntryCache) Get(name string) (*FileInfo, []FileInfo) {
	item, ok := e.entries.Load(name)
	if !ok {
		item = e.refreshEntry(name)
	}
	return item.current, item.children
}

func (e *EntryCache) refreshEntry(name string) EntryCacheItem {
	result, _, _ := e.refreshing.Do(name, func() (any, error) {
		return e._refreshEntry(name), nil
	})
	return result.(EntryCacheItem)
}

// _refreshEntry computes an entry's listing and caches it — but ONLY when the
// computation actually produced one.
//
// A nil `current` is never a fact; it is the failure return of both producers.
// getTorrentChildren returns (nil, nil) when storage.GetEntryItem ERRORS, and
// getEntryChildren returns (nil, nil) when ForEachMeta ERRORS. Both are
// transient I/O outcomes. Caching one used to PIN the entry permanently: the
// map has no TTL and is cleared only by a global EntryCache.Refresh(), so a
// single unlucky read made every later PROPFIND answer "not found" from cache
// while /api/browse, the repair sweep and direct reads all still saw the entry
// perfectly well.
//
// Not storing negatives removes the pin outright, which is the smallest change
// that can. The cost is bounded: singleflight still collapses concurrent misses
// onto one computation, and the recomputation for the negative cases is a single
// GetEntryItem / ForEachMeta pass.
//
// SCOPE, stated plainly: this is defensive hardening. It is NOT the cause of
// entries missing from the `__all__` parent listing — those return a valid 207
// on their own PROPFIND, so nothing is pinned negative for them.
func (e *EntryCache) _refreshEntry(name string) EntryCacheItem {
	if after, ok := strings.CutPrefix(name, torrentEntryCachePrefix); ok {
		// This is a torrent folder
		torrentName := after
		current, children := e.manager.getTorrentChildren(torrentName)
		return e.store(name, EntryCacheItem{current: current, children: children})
	}

	// This is either a __all__, __bad__ or custom folder
	current, children := e.manager.getEntryChildren(name)
	return e.store(name, EntryCacheItem{current: current, children: children})
}

// store caches item unless it is a negative result. It always returns item, so
// the caller still serves this lookup's answer — the miss is not cached, not
// suppressed.
func (e *EntryCache) store(name string, item EntryCacheItem) EntryCacheItem {
	if item.current == nil {
		return item
	}
	e.entries.Store(name, item)
	return item
}

// Refresh triggers a cache refresh with debouncing.
// If called multiple times rapidly, only one refresh will occur.
func (e *EntryCache) Refresh() {
	e.entries.Delete(EntryAllFolder)
	e.entries.Delete(EntryBadFolder)
	e.entries.Delete(EntryTorrentFolder)
	e.entries.Delete(EntryNZBFolder)
	for k := range e.manager.config.CustomFolders {
		e.entries.Delete(k)
	}
	// Also clear torrent-level cache entries to prevent stale file listings
	e.entries.Range(func(key string, _ EntryCacheItem) bool {
		if strings.HasPrefix(key, torrentEntryCachePrefix) {
			e.entries.Delete(key)
		}
		return true
	})
}

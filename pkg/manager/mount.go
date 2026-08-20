package manager

import (
	"context"
	"io"
	"os"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sourcegraph/conc/pool"
)

const (
	MaxCacheWarmWorkers = 10
	MaxNZBPreCacheFiles = 5
	CacheWarmTimeout    = 60 * time.Second

	// Container metadata lives at the head (streamable MP4 moov, EBML header)
	// or the tail (non-streamable MP4 moov, MKV cues/seek index), so warming
	// head+tail covers what a downstream ffprobe/import scan will seek to.
	cacheWarmHeadSize = 2 * 1024 * 1024 // 2MB
	cacheWarmTailSize = 2 * 1024 * 1024 // 2MB
)

type MountManager interface {
	Start(ctx context.Context) error
	Stop() error
	Stats() map[string]any
	IsReady() bool
	Type() string
	Refresh(dirs []string) error
	// ForgetPath drops ONE cached node, by path, re-reading nothing else. See
	// RefreshDeletedEntry for what it is for and why a recursive refresh is the
	// wrong answer.
	ForgetPath(path string) error
}

func (m *Manager) RefreshEntries(refreshMount bool) {
	// Refresh entries
	m.entry.Refresh()

	// Refresh mount if needed
	if refreshMount {
		go func() {
			_ = m.RefreshMount()
		}()
	}
}

// refreshDirs resolves the configured group directories, defaulting to
// __all__. Kept in one place so the group refresh and the per-entry forget can
// never disagree about which groups exist.
func (m *Manager) refreshDirs() []string {
	dirs := strings.FieldsFunc(m.config.RefreshDirs, func(r rune) bool {
		return r == ',' || r == '&'
	})
	if len(dirs) == 0 {
		dirs = []string{"__all__"}
	}
	return dirs
}

func (m *Manager) RefreshMount() error {
	// Call event handler if set
	if m.mountManager != nil {
		return m.mountManager.Refresh(m.refreshDirs())
	}
	return nil
}

// RefreshDeletedEntry invalidates the mount for an entry that has just been
// deleted — its OWN path, not merely the group it lived in.
//
// 🔴 THE GHOST THIS CLOSES, measured on a live 60,000-symlink library. Deleting
// an entry refreshes the configured group directories, and that refresh is
// NON-RECURSIVE: the group listing is re-read and nothing beneath it is. The
// per-entry child node survives, still holding attributes decypharr has already
// stopped backing.
//
//	stat on the full path -> OK, 3,577,947,542 bytes   (stale child node)
//	readdir of the parent -> empty                     (the group refresh worked)
//	read                  -> 0 bytes, no error         (the GET behind it 404s)
//
// Plex renders that as "Error opening input file" and the transcoder exits. And
// while it lasts NOTHING can catch it: decypharr has no entry left to reason
// about, and the arr's dangling-symlink reaper sees a perfectly healthy stat.
// The node does expire on its own — observed hours later, well past the mount's
// nominal 5m dir_cache_time — but "eventually" is not a mechanism, and the
// window is user-visible when it lands on something being played.
//
// ⚠️ THE OBVIOUS FIX IS THE WRONG ONE. rclone's refresh takes recursive=true,
// which would walk every entry in the group on EVERY delete — thousands of them,
// exactly when prune waves make deletes arrive in batches. That trades a rare
// ghost for a predictable stampede. Forgetting the one path that actually
// changed costs a single call and cannot scale with the library at all.
//
// BEST EFFORT BY CONSTRUCTION. This runs after the entry is already gone from
// the store and after the group refresh that always happened. A failure here
// leaves exactly the behaviour that shipped before it, so it is logged and never
// returned: it must not be able to fail a deletion that has already succeeded.
func (m *Manager) RefreshDeletedEntry(folder string) {
	if m.mountManager == nil || folder == "" {
		return
	}
	for _, dir := range m.refreshDirs() {
		path := folder
		if dir != "" {
			path = dir + "/" + folder
		}
		if err := m.mountManager.ForgetPath(path); err != nil {
			m.logger.Debug().Err(err).
				Str("path", path).
				Msg("Could not drop the mount's cached node for a deleted entry; it may serve stale metadata " +
					"for that path until the client expires it on its own")
		}
	}
}

// WarmFileCache reads the head and tail of each media file through the mount
// to warm the VFS disk cache, so a subsequent media probe or import scan over
// the mount is fast. This replaces spawning ffprobe: the read pattern is
// deterministic, needs no external binary, and warms the exact bytes a
// downstream probe seeks to (see cacheWarmHeadSize/cacheWarmTailSize).
func (m *Manager) WarmFileCache(filePaths []string) error {
	if len(filePaths) == 0 {
		return nil
	}

	// Use a worker pool to limit concurrency and avoid overwhelming the system
	p := pool.New().WithMaxGoroutines(min(len(filePaths), MaxCacheWarmWorkers))

	for _, fp := range filePaths {
		if !utils.IsMediaFile(fp) {
			continue
		}
		p.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), CacheWarmTimeout)
			defer cancel()
			if err := m.warmOneFile(ctx, fp); err != nil {
				// Log error but continue
				m.logger.Warn().
					Err(err).
					Str("file", fp).
					Msg("cache warm failed")
			}
		})
	}

	p.Wait()
	return nil
}

// warmOneFile reads the head and (for large enough files) the tail of path,
// going through the mount so the FUSE/VFS cache is populated.
func (m *Manager) warmOneFile(ctx context.Context, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()
	if size == 0 {
		return nil
	}

	head := min(int64(cacheWarmHeadSize), size)
	if err := drainRange(ctx, f, 0, head); err != nil {
		return err
	}

	// Only warm the tail when it doesn't overlap the head we just read.
	if size > int64(cacheWarmHeadSize)+int64(cacheWarmTailSize) {
		if err := drainRange(ctx, f, size-int64(cacheWarmTailSize), int64(cacheWarmTailSize)); err != nil {
			return err
		}
	}
	return nil
}

// drainRange reads length bytes starting at off, in chunks, discarding the
// data and checking ctx between chunks so a stalled mount can't pin a worker
// past CacheWarmTimeout.
func drainRange(ctx context.Context, r io.ReaderAt, off, length int64) error {
	const chunk = 1 << 20 // 1MB
	buf := make([]byte, chunk)
	for read := int64(0); read < length; {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(length-read, chunk)
		got, err := r.ReadAt(buf[:n], off+read)
		read += int64(got)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return nil
}

type stubMountManager struct{}

func (s *stubMountManager) Refresh(dirs []string) error {
	return nil
}

// No mount is configured, so there is no cached node anywhere to drop. Nil is
// the honest answer here rather than a silent no-op standing in for one.
func (s *stubMountManager) ForgetPath(path string) error {
	return nil
}

func NewStubMountManager() MountManager {
	return &stubMountManager{}
}

func (s *stubMountManager) Start(ctx context.Context) error {
	return nil
}
func (s *stubMountManager) Stop() error {
	return nil
}
func (s *stubMountManager) Stats() map[string]any {
	return map[string]any{
		"message": "no mount configured",
	}
}
func (s *stubMountManager) IsReady() bool {
	return false
}
func (s *stubMountManager) Type() string {
	return "none"
}

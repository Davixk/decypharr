package manager

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// staleNodeMount models rclone's VFS holding a STALE child node for one entry
// folder: until that exact path is forgotten, a scan of the folder sees only
// `stale` (possibly nothing). A group Refresh is recorded but, like rclone's
// non-recursive vfs/refresh, does not touch the child node.
type staleNodeMount struct {
	countingMountManager
	mu        sync.Mutex
	stalePath string
	cleared   bool
}

func (s *staleNodeMount) ForgetPath(path string) error {
	_ = s.countingMountManager.ForgetPath(path)
	s.mu.Lock()
	defer s.mu.Unlock()
	if path == s.stalePath {
		s.cleared = true
	}
	return nil
}

func (s *staleNodeMount) isCleared() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cleared
}

func (s *staleNodeMount) readDir(stale, live []string) func(string) ([]os.DirEntry, error) {
	return func(string) ([]os.DirEntry, error) {
		names := stale
		if s.isCleared() {
			names = live
		}
		out := make([]os.DirEntry, 0, len(names))
		for _, n := range names {
			out = append(out, fakeDirEntry{name: n})
		}
		return out, nil
	}
}

// 🔴 A SAME-NAME RE-ADD MUST NOT WAIT OUT A STALE NODE.
//
// Production, during the AD migration: radarr deleted the only file of a usenet
// entry THROUGH the rclone mount, which removed the entry and left rclone's
// `__all__/<folder>/` node cached as EMPTY. The same NZB re-added under the same
// name re-created the same folder; the non-recursive group refresh never
// re-read that node, and the symlink wait sat at 100% toward its 30-minute
// timeout while decypharr was already serving the file.
func TestSymlinkWaitForgetsTheEntrysOwnStaleNode(t *testing.T) {
	m := newActionLifecycleFixture(t, 0)
	entry := addActionLifecycleEntry(t, m, "stale-node-entry")
	mount := &staleNodeMount{stalePath: "__all__/" + entry.GetFolder()}
	m.mountManager = mount
	d := m.downloader
	d.readDir = mount.readDir(nil, []string{"lifecycle.mkv"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	filePaths, err := d.createSymlinksWhenMountFilesAppear(ctx, entry, entry.GetActiveFiles(), "/mnt/remote/__all__/x", t.TempDir())
	if err != nil {
		t.Fatalf("wait did not complete (%v): the entry's own stale node at %q was never forgotten, so the "+
			"group refresh alone left its files invisible", err, mount.stalePath)
	}
	if len(filePaths) != 1 {
		t.Fatalf("filePaths = %v, want one symlink", filePaths)
	}
}

// A stale node can hold SOME of an entry's files. A first scan that finds part
// of the set must still drop the node, or the rest waits out the cache.
func TestSymlinkWaitForgetsAPartiallyStaleNode(t *testing.T) {
	m := newActionLifecycleFixture(t, 0)
	entry := addActionLifecycleEntry(t, m, "partial-stale-entry")
	mount := &staleNodeMount{stalePath: "__all__/" + entry.GetFolder()}
	m.mountManager = mount
	d := m.downloader
	d.readDir = mount.readDir([]string{"a.mkv"}, []string{"a.mkv", "b.mkv"})

	files := []*storage.File{
		{Name: "a.mkv", InfoHash: entry.InfoHash, Size: 5},
		{Name: "b.mkv", InfoHash: entry.InfoHash, Size: 5},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	filePaths, err := d.createSymlinksWhenMountFilesAppear(ctx, entry, files, "/mnt/remote/__all__/x", t.TempDir())
	if err != nil {
		t.Fatalf("wait did not complete (%v): a first scan that found only part of the files never dropped "+
			"the stale node at %q", err, mount.stalePath)
	}
	if len(filePaths) != 2 {
		t.Fatalf("filePaths = %v, want two symlinks", filePaths)
	}
}

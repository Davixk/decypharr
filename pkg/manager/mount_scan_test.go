package manager

import (
	"context"
	"io/fs"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

type fakeDirEntry struct {
	name string
	dir  bool
}

func (f fakeDirEntry) Name() string               { return f.name }
func (f fakeDirEntry) IsDir() bool                { return f.dir }
func (f fakeDirEntry) Type() fs.FileMode          { return 0 }
func (f fakeDirEntry) Info() (fs.FileInfo, error) { return nil, fs.ErrInvalid }

type countingMountManager struct {
	refreshes     atomic.Int32
	forgotten     atomic.Int32
	lastForgotten atomic.Value
}

func (c *countingMountManager) Start(context.Context) error { return nil }
func (c *countingMountManager) Stop() error                 { return nil }
func (c *countingMountManager) Stats() map[string]any       { return nil }
func (c *countingMountManager) IsReady() bool               { return true }
func (c *countingMountManager) Type() string                { return "counting" }
func (c *countingMountManager) Refresh([]string) error {
	c.refreshes.Add(1)
	return nil
}

func (c *countingMountManager) ForgetPath(path string) error {
	c.forgotten.Add(1)
	c.lastForgotten.Store(path)
	return nil
}

// TestMountScanTimeoutDoesNotWedgeWait pins fix 4b: a ReadDir call that hangs
// (simulated hung FUSE mount) must not pin the mount wait past its per-scan
// timeout - the poll loop treats the timed-out scan as "not visible yet" and
// the next scan can still complete the wait. It also pins fix 4a: an empty
// first scan triggers exactly one mount refresh through the manager.
func TestMountScanTimeoutDoesNotWedgeWait(t *testing.T) {
	m := newActionLifecycleFixture(t, 0)
	mount := &countingMountManager{}
	m.mountManager = mount
	entry := addActionLifecycleEntry(t, m, "mount-scan-entry")

	blockFirstScan := make(chan struct{})
	t.Cleanup(func() { close(blockFirstScan) })
	var scans atomic.Int32
	d := m.downloader
	d.scanTimeout = 100 * time.Millisecond
	d.readDir = func(string) ([]os.DirEntry, error) {
		if scans.Add(1) == 1 {
			// Simulate a hung mount: never return until the test tears down.
			<-blockFirstScan
			return nil, os.ErrNotExist
		}
		return []os.DirEntry{fakeDirEntry{name: "lifecycle.mkv"}}, nil
	}

	symlinkDir := t.TempDir()
	start := time.Now()
	filePaths, err := d.createSymlinksWhenMountFilesAppear(
		context.Background(), entry, entry.GetActiveFiles(), "/hung/mount", symlinkDir)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("createSymlinksWhenMountFilesAppear: %v", err)
	}
	if len(filePaths) != 1 {
		t.Fatalf("filePaths = %v, want one symlink", filePaths)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("wait took %s; the hung first scan pinned the loop past its per-scan timeout", elapsed)
	}
	info, err := os.Lstat(filePaths[0])
	if err != nil {
		t.Fatalf("Lstat(%s): %v", filePaths[0], err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("%s is not a symlink", filePaths[0])
	}
	if got := scans.Load(); got < 2 {
		t.Fatalf("scan count = %d, want at least 2 (timed-out first scan plus a retry)", got)
	}

	// The empty (timed-out) first scan must have nudged the mount exactly once.
	deadline := time.Now().Add(2 * time.Second)
	for mount.refreshes.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("empty first scan did not trigger a mount refresh")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if got := mount.refreshes.Load(); got != 1 {
		t.Fatalf("mount refreshes = %d, want exactly 1 for a single wait", got)
	}
}

// TestMountScanNoRefreshWithoutMountManager pins the no-op path of fix 4a: a
// manager without mount-refresh capability must not panic or block when the
// first scan comes up empty.
func TestMountScanNoRefreshWithoutMountManager(t *testing.T) {
	m := newActionLifecycleFixture(t, 0)
	entry := addActionLifecycleEntry(t, m, "mount-scan-no-refresh")

	var scans atomic.Int32
	d := m.downloader
	d.readDir = func(string) ([]os.DirEntry, error) {
		if scans.Add(1) == 1 {
			return nil, os.ErrNotExist
		}
		return []os.DirEntry{fakeDirEntry{name: "lifecycle.mkv"}}, nil
	}

	filePaths, err := d.createSymlinksWhenMountFilesAppear(
		context.Background(), entry, entry.GetActiveFiles(), "/absent/mount", t.TempDir())
	if err != nil {
		t.Fatalf("createSymlinksWhenMountFilesAppear: %v", err)
	}
	if len(filePaths) != 1 {
		t.Fatalf("filePaths = %v, want one symlink", filePaths)
	}
}

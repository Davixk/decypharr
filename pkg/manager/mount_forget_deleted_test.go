package manager

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// 🔴 DELETING AN ENTRY MUST INVALIDATE ITS OWN PATH, NOT JUST ITS GROUP.
//
// Measured on a live 60,000-symlink library. The group refresh that always ran
// is NON-RECURSIVE, so it re-read `__all__` and stopped. The per-entry child
// node beneath it survived, holding attributes decypharr had already stopped
// backing:
//
//	stat on the full path -> OK, 3,577,947,542 bytes   (stale child node)
//	readdir of the parent -> empty                     (the group refresh worked)
//	read                  -> 0 bytes, no error         (the GET behind it 404s)
//
// Plex showed "Error opening input file". Nothing could catch it: decypharr had
// no entry left to reason about, and the arr's dangling-symlink reaper saw a
// healthy stat. Confirmed by direct measurement that decypharr itself answered
// 404 for that path throughout — the lie was entirely the client's cached node,
// and it took hours to expire against a nominal 5m dir_cache_time.
type forgetRecorder struct {
	mu        sync.Mutex
	forgotten []string
	refreshed [][]string
	err       error
}

func (f *forgetRecorder) Start(context.Context) error { return nil }
func (f *forgetRecorder) Stop() error                 { return nil }
func (f *forgetRecorder) Stats() map[string]any       { return nil }
func (f *forgetRecorder) IsReady() bool               { return true }
func (f *forgetRecorder) Type() string                { return "recorder" }

func (f *forgetRecorder) Refresh(dirs []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refreshed = append(f.refreshed, slices.Clone(dirs))
	return nil
}

func (f *forgetRecorder) ForgetPath(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forgotten = append(f.forgotten, path)
	return f.err
}

func (f *forgetRecorder) paths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.forgotten)
}

func TestDeletedEntryForgetsItsOwnPath(t *testing.T) {
	m := newActionLifecycleFixture(t, 1)
	rec := &forgetRecorder{}
	m.mountManager = rec

	m.RefreshDeletedEntry("Silo.S01E01.Freedom.Day.1080p.AMZN.WEB-DL.DDP5.1.H.264-PMI")

	got := rec.paths()
	want := "__all__/Silo.S01E01.Freedom.Day.1080p.AMZN.WEB-DL.DDP5.1.H.264-PMI"
	if !slices.Contains(got, want) {
		t.Fatalf("forgot %v, want it to include %q. Without the entry's OWN path the client keeps a child node "+
			"whose stat answers with a live size for a file decypharr already 404s", got, want)
	}
}

// 🛑 AND IT MUST ACTUALLY BE WIRED INTO DeleteEntry.
//
// A negative control earned this one: removing the call from DeleteEntry broke
// nothing, because every test above exercises RefreshDeletedEntry directly. A
// correct function nobody calls is the same ghost with extra steps.
func TestDeleteEntryForgetsTheEntrysPath(t *testing.T) {
	m := newActionLifecycleFixture(t, 1)
	rec := &forgetRecorder{}
	m.mountManager = rec

	entry := &storage.Entry{
		Protocol: config.ProtocolTorrent,
		InfoHash: "ghosthash",
		Name:     "Silo.S01E01.Freedom.Day.1080p.AMZN.WEB-DL.DDP5.1.H.264-PMI",
		AddedOn:  time.Unix(1_700_000_000, 0).UTC(),
		Files:    map[string]*storage.File{},
	}
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	folder := entry.GetFolder()

	if err := m.DeleteEntry("ghosthash", false); err != nil {
		t.Fatalf("DeleteEntry: %v", err)
	}

	want := "__all__/" + folder
	if got := rec.paths(); !slices.Contains(got, want) {
		t.Fatalf("DeleteEntry forgot %v, want it to include %q. The mount keeps a child node whose stat answers "+
			"with a live size for content decypharr already 404s, and nothing can catch that: there is no entry "+
			"left to reason about and the arr's reaper sees a healthy stat", got, want)
	}
}

// EVERY CONFIGURED GROUP, not just the first. An entry can be listed under more
// than one group, and a node left cached under any of them is the same ghost.
func TestDeletedEntryForgetsUnderEveryConfiguredGroup(t *testing.T) {
	m := newActionLifecycleFixture(t, 1)
	rec := &forgetRecorder{}
	m.mountManager = rec
	m.config.RefreshDirs = "__all__,__bad__"
	t.Cleanup(func() { m.config.RefreshDirs = "" })

	m.RefreshDeletedEntry("Some.Release")

	got := rec.paths()
	for _, want := range []string{"__all__/Some.Release", "__bad__/Some.Release"} {
		if !slices.Contains(got, want) {
			t.Fatalf("forgot %v, want %q — a node cached under any configured group is the same ghost", got, want)
		}
	}
}

// 🛑 IT MUST NEVER ASK FOR A RECURSIVE WALK, and this is the assertion that
// keeps the fix cheap. rclone's refresh takes recursive=true, which would walk
// every entry in the group on EVERY delete — thousands, precisely when prune
// waves batch deletes. Forgetting one path costs one call; that is the whole
// reason this is a forget and not a refresh.
func TestForgettingADeletedEntryIsOneCallPerGroup(t *testing.T) {
	m := newActionLifecycleFixture(t, 1)
	rec := &forgetRecorder{}
	m.mountManager = rec

	m.RefreshDeletedEntry("One.Release")

	if got := len(rec.paths()); got != 1 {
		t.Fatalf("a single delete made %d forget calls against one configured group, want 1", got)
	}
	rec.mu.Lock()
	refreshes := len(rec.refreshed)
	rec.mu.Unlock()
	if refreshes != 0 {
		t.Fatalf("RefreshDeletedEntry issued %d group refreshes; the group refresh is RefreshEntries' job and "+
			"doing it twice per delete is the cost this design avoids", refreshes)
	}
}

// A FAILURE HERE MUST NOT BREAK ANYTHING. It runs after the entry is already
// gone from the store and after the group refresh that always happened, so the
// worst case is exactly the behaviour that shipped before it.
func TestForgetFailureIsSurvivable(t *testing.T) {
	m := newActionLifecycleFixture(t, 1)
	rec := &forgetRecorder{err: context.DeadlineExceeded}
	m.mountManager = rec

	m.RefreshDeletedEntry("Fails.To.Forget") // must not panic

	if got := len(rec.paths()); got != 1 {
		t.Fatalf("the forget was not even attempted (%d calls)", got)
	}
}

// No mount configured, nothing cached anywhere, nothing to do — and it must not
// dereference a nil manager on the way to finding that out.
func TestForgetWithoutAMountIsANoOp(t *testing.T) {
	m := newActionLifecycleFixture(t, 1)
	m.mountManager = nil
	m.RefreshDeletedEntry("No.Mount")
}

// An empty folder name would forget the GROUP ROOT — dropping the whole listing
// on every delete, which is both wrong and expensive.
func TestForgetIgnoresAnEmptyFolder(t *testing.T) {
	m := newActionLifecycleFixture(t, 1)
	rec := &forgetRecorder{}
	m.mountManager = rec

	m.RefreshDeletedEntry("")

	if got := rec.paths(); len(got) != 0 {
		t.Fatalf("an empty folder name forgot %v — that is the group root, and dropping it on every delete "+
			"discards the entire listing", got)
	}
}

// The group resolution must stay identical to the one RefreshMount uses, or the
// forget and the refresh end up disagreeing about which groups exist and the
// forget silently targets a path nobody serves.
func TestRefreshDirsResolutionIsShared(t *testing.T) {
	m := newActionLifecycleFixture(t, 1)

	if got := m.refreshDirs(); !slices.Equal(got, []string{"__all__"}) {
		t.Fatalf("default refreshDirs = %v, want [__all__]", got)
	}
	m.config.RefreshDirs = "__all__&custom"
	t.Cleanup(func() { m.config.RefreshDirs = "" })
	if got := m.refreshDirs(); !slices.Equal(got, []string{"__all__", "custom"}) {
		t.Fatalf("refreshDirs = %v, want both separators honoured", got)
	}
	_ = config.Get()
}

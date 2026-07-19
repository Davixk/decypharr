package manager

import (
	"errors"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestCopyFolderCreatesIndependentDestination(t *testing.T) {
	m := newCopyEntryTestManager(t)
	source := addCopyTestEntry(t, m, strings.Repeat("a", 40), "Source Folder", map[string]int64{"video.mkv": 123})
	info, err := m.GetEntryInfo(source.GetFolder())
	if err != nil {
		t.Fatalf("GetEntryInfo: %v", err)
	}

	created, err := m.CopyEntryWithOverwrite(info, "/__all__/Copied Folder", false, true)
	if err != nil {
		t.Fatalf("CopyEntryWithOverwrite: %v", err)
	}
	if !created {
		t.Fatal("copy reported replacement, want creation")
	}

	if exists, err := m.storage.Exists(source.InfoHash); err != nil || !exists {
		t.Fatalf("source exists = %v, err=%v, want true", exists, err)
	}
	item, err := m.storage.GetEntryItem("Copied Folder")
	if err != nil {
		t.Fatalf("GetEntryItem(destination): %v", err)
	}
	copiedFile, err := item.GetFile("video.mkv")
	if err != nil {
		t.Fatalf("destination file: %v", err)
	}
	if copiedFile.InfoHash == source.InfoHash || copiedFile.InfoHash == "" {
		t.Fatalf("destination infohash = %q, want independent identity", copiedFile.InfoHash)
	}
	destination, err := m.storage.Get(copiedFile.InfoHash)
	if err != nil {
		t.Fatalf("Get(destination): %v", err)
	}
	if destination.Name != "Copied Folder" || destination.OriginalFilename != "Copied Folder" {
		t.Fatalf("destination names = %q/%q", destination.Name, destination.OriginalFilename)
	}
	if destination.Files["video.mkv"].InfoHash != destination.InfoHash {
		t.Fatalf("copied file points to %q, want %q", destination.Files["video.mkv"].InfoHash, destination.InfoHash)
	}
}

func TestCopyFolderOverwriteReplacesDestinationSynchronously(t *testing.T) {
	m := newCopyEntryTestManager(t)
	source := addCopyTestEntry(t, m, strings.Repeat("b", 40), "Source Folder", map[string]int64{"new.mkv": 200})
	existing := addCopyTestEntry(t, m, strings.Repeat("c", 40), "Destination Folder", map[string]int64{"old.mkv": 100})
	info, err := m.GetEntryInfo(source.GetFolder())
	if err != nil {
		t.Fatalf("GetEntryInfo: %v", err)
	}

	created, err := m.CopyEntryWithOverwrite(info, "/__all__/Destination Folder", false, true)
	if err != nil {
		t.Fatalf("overwrite destination: %v", err)
	}
	if created {
		t.Fatal("overwrite reported creation")
	}
	if exists, err := m.storage.Exists(existing.InfoHash); err != nil || exists {
		t.Fatalf("old destination exists = %v, err=%v, want false", exists, err)
	}
	item, err := m.storage.GetEntryItem("Destination Folder")
	if err != nil {
		t.Fatalf("GetEntryItem(destination): %v", err)
	}
	if _, err := item.GetFile("new.mkv"); err != nil {
		t.Fatalf("replacement file missing: %v", err)
	}
	if _, err := item.GetFile("old.mkv"); err == nil {
		t.Fatal("old destination file survived replacement")
	}
	if exists, err := m.storage.Exists(source.InfoHash); err != nil || !exists {
		t.Fatalf("source exists = %v, err=%v, want true", exists, err)
	}
	client, _ := m.clients.Load("test")
	deleted := client.(*lifecycleDebridClient).deleted()
	if len(deleted) != 1 || deleted[0] != "remote-"+existing.InfoHash {
		t.Fatalf("synchronous destination cleanup = %v", deleted)
	}
}

func TestCopiedFolderPlacementIsDeletedOnlyAfterLastReference(t *testing.T) {
	m := newCopyEntryTestManager(t)
	source := addCopyTestEntry(t, m, strings.Repeat("3", 40), "Source Folder", map[string]int64{"video.mkv": 123})
	info, err := m.GetEntryInfo(source.GetFolder())
	if err != nil {
		t.Fatalf("GetEntryInfo: %v", err)
	}
	if _, err := m.CopyEntryWithOverwrite(info, "/__all__/Copied Folder", false, true); err != nil {
		t.Fatalf("COPY folder: %v", err)
	}
	item, err := m.storage.GetEntryItem("Copied Folder")
	if err != nil {
		t.Fatalf("GetEntryItem(copy): %v", err)
	}
	copyFile, err := item.GetFile("video.mkv")
	if err != nil {
		t.Fatalf("copied file: %v", err)
	}

	client, _ := m.clients.Load("test")
	lifecycleClient := client.(*lifecycleDebridClient)
	if err := m.DeleteEntry(source.InfoHash, true); err != nil {
		t.Fatalf("DeleteEntry(source): %v", err)
	}
	if deleted := lifecycleClient.deleted(); len(deleted) != 0 {
		t.Fatalf("source deletion removed shared placement: %v", deleted)
	}
	if err := m.DeleteEntry(copyFile.InfoHash, true); err != nil {
		t.Fatalf("DeleteEntry(copy): %v", err)
	}
	deleted := lifecycleClient.deleted()
	if len(deleted) != 1 || deleted[0] != "remote-"+source.InfoHash {
		t.Fatalf("last-reference cleanup = %v", deleted)
	}
}

func TestMoveFolderDestinationCommitsBeforeSourceDeletion(t *testing.T) {
	m := newCopyEntryTestManager(t)
	source := addCopyTestEntry(t, m, strings.Repeat("d", 40), "Source Folder", map[string]int64{"video.mkv": 123})
	info, err := m.GetEntryInfo(source.GetFolder())
	if err != nil {
		t.Fatalf("GetEntryInfo: %v", err)
	}

	destinationCommitted := make(chan struct{})
	releaseMove := make(chan struct{})
	m.copyEntryTestHook = func(stage string) {
		if stage == "destination-committed" {
			close(destinationCommitted)
			<-releaseMove
		}
	}
	type result struct {
		created bool
		err     error
	}
	resultCh := make(chan result, 1)
	go func() {
		created, err := m.CopyEntryWithOverwrite(info, "/webdav/__all__/Moved Folder", true, true)
		resultCh <- result{created: created, err: err}
	}()

	select {
	case <-destinationCommitted:
	case <-time.After(2 * time.Second):
		t.Fatal("MOVE did not reach destination commit")
	}
	if exists, err := m.storage.Exists(source.InfoHash); err != nil || !exists {
		t.Fatalf("source before deletion exists = %v, err=%v, want true", exists, err)
	}
	if _, err := m.storage.GetEntryItem("Moved Folder"); err != nil {
		t.Fatalf("destination was not visible before source deletion: %v", err)
	}
	close(releaseMove)

	select {
	case got := <-resultCh:
		if got.err != nil || !got.created {
			t.Fatalf("MOVE result = created:%v err:%v", got.created, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("MOVE did not finish")
	}
	if exists, err := m.storage.Exists(source.InfoHash); err != nil || exists {
		t.Fatalf("source after MOVE exists = %v, err=%v, want false", exists, err)
	}
}

func TestConcurrentMoveAndDeleteUseCopyThenStorageLockOrder(t *testing.T) {
	m := newCopyEntryTestManager(t)
	source := addCopyTestEntry(t, m, strings.Repeat("8", 40), "Source Folder", map[string]int64{"video.mkv": 123})
	info, err := m.GetEntryInfo(source.GetFolder())
	if err != nil {
		t.Fatalf("GetEntryInfo: %v", err)
	}

	destinationCommitted := make(chan struct{})
	releaseMove := make(chan struct{})
	deleteAtCopyLock := make(chan struct{})
	m.copyEntryTestHook = func(stage string) {
		if stage == "destination-committed" {
			close(destinationCommitted)
			<-releaseMove
		}
	}
	m.deleteEntryTestHook = func(stage string) {
		if stage == "before-copy-lock" {
			close(deleteAtCopyLock)
		}
	}

	type moveResult struct {
		created bool
		err     error
	}
	moveDone := make(chan moveResult, 1)
	go func() {
		created, moveErr := m.CopyEntryWithOverwrite(info, "/__all__/Moved Folder", true, true)
		moveDone <- moveResult{created: created, err: moveErr}
	}()
	select {
	case <-destinationCommitted:
	case <-time.After(2 * time.Second):
		t.Fatal("MOVE did not reach destination commit")
	}

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- m.DeleteEntry(source.InfoHash, true) }()
	select {
	case <-deleteAtCopyLock:
	case <-time.After(2 * time.Second):
		t.Fatal("DeleteEntry did not reach the copy lifecycle lock")
	}
	close(releaseMove)

	select {
	case got := <-moveDone:
		if got.err != nil || !got.created {
			t.Fatalf("MOVE result = created:%v err:%v", got.created, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("MOVE deadlocked with DeleteEntry")
	}
	if err := awaitResult(t, deleteDone, "DeleteEntry after MOVE"); err == nil {
		t.Fatal("DeleteEntry unexpectedly deleted the source after MOVE won ownership")
	}
	if exists, existsErr := m.storage.Exists(source.InfoHash); existsErr != nil || exists {
		t.Fatalf("source after MOVE exists = %v, err=%v, want false", exists, existsErr)
	}
	if _, loadErr := m.storage.GetEntryItem("Moved Folder"); loadErr != nil {
		t.Fatalf("MOVE destination missing: %v", loadErr)
	}
	client, _ := m.clients.Load("test")
	if deleted := client.(*lifecycleDebridClient).deleted(); len(deleted) != 0 {
		t.Fatalf("stale DeleteEntry removed the MOVE destination placement: %v", deleted)
	}
}

func TestMoveFolderDestinationFailureLeavesSource(t *testing.T) {
	m := newCopyEntryTestManager(t)
	source := addCopyTestEntry(t, m, strings.Repeat("e", 40), "Source Folder", map[string]int64{"source.mkv": 123})
	existing := addCopyTestEntry(t, m, strings.Repeat("f", 40), "Destination Folder", map[string]int64{"destination.mkv": 321})
	info, err := m.GetEntryInfo(source.GetFolder())
	if err != nil {
		t.Fatalf("GetEntryInfo: %v", err)
	}

	_, err = m.CopyEntryWithOverwrite(info, "/__all__/Destination Folder", true, false)
	if !errors.Is(err, ErrCopyDestinationExists) {
		t.Fatalf("MOVE error = %v, want ErrCopyDestinationExists", err)
	}
	for name, infohash := range map[string]string{"source": source.InfoHash, "destination": existing.InfoHash} {
		if exists, existsErr := m.storage.Exists(infohash); existsErr != nil || !exists {
			t.Fatalf("%s exists = %v, err=%v, want true", name, exists, existsErr)
		}
	}
}

func TestMoveFolderCleanupFailureLeavesSource(t *testing.T) {
	m := newCopyEntryTestManager(t)
	source := addCopyTestEntry(t, m, strings.Repeat("4", 40), "Source Folder", map[string]int64{"source.mkv": 123})
	existing := addCopyTestEntry(t, m, strings.Repeat("5", 40), "Destination Folder", map[string]int64{"destination.mkv": 321})
	client, _ := m.clients.Load("test")
	lifecycleClient := client.(*lifecycleDebridClient)
	lifecycleClient.onDelete = func(id string) error {
		if id != "remote-"+existing.InfoHash {
			t.Fatalf("cleanup id = %q", id)
		}
		return errors.New("injected cleanup failure")
	}
	info, err := m.GetEntryInfo(source.GetFolder())
	if err != nil {
		t.Fatalf("GetEntryInfo: %v", err)
	}

	_, err = m.CopyEntryWithOverwrite(info, "/__all__/Destination Folder", true, true)
	if err == nil || !strings.Contains(err.Error(), "injected cleanup failure") {
		t.Fatalf("MOVE cleanup error = %v", err)
	}
	if exists, existsErr := m.storage.Exists(source.InfoHash); existsErr != nil || !exists {
		t.Fatalf("source exists = %v, err=%v, want true", exists, existsErr)
	}
	// Placement cleanup now runs before the old destination row is removed, so
	// a cleanup failure keeps the old destination and rolls the copy back: the
	// MOVE fails as a clean no-op instead of leaving half-replaced state.
	if exists, existsErr := m.storage.Exists(existing.InfoHash); existsErr != nil || !exists {
		t.Fatalf("old destination exists = %v, err=%v, want true after failed cleanup", exists, existsErr)
	}
	item, loadErr := m.storage.GetEntryItem("Destination Folder")
	if loadErr != nil {
		t.Fatalf("destination folder missing after rolled-back MOVE: %v", loadErr)
	}
	if _, fileErr := item.GetFile("destination.mkv"); fileErr != nil {
		t.Fatalf("old destination content missing after rollback: %v", fileErr)
	}
	if _, fileErr := item.GetFile("source.mkv"); fileErr == nil {
		t.Fatal("rolled-back copy left its file in the destination folder")
	}
}

func TestCopyAndMoveFilePersistAuthoritativeState(t *testing.T) {
	m := newCopyEntryTestManager(t)
	source := addCopyTestEntry(t, m, strings.Repeat("1", 40), "Source Folder", map[string]int64{"video.mkv": 123})
	info, err := m.GetTorrentFile(source.GetFolder(), "video.mkv")
	if err != nil {
		t.Fatalf("GetTorrentFile: %v", err)
	}

	created, err := m.CopyEntryWithOverwrite(info, "/__all__/Source Folder/copied.mkv", false, true)
	if err != nil || !created {
		t.Fatalf("file COPY = created:%v err:%v", created, err)
	}
	persisted, err := m.storage.Get(source.InfoHash)
	if err != nil {
		t.Fatalf("Get(source): %v", err)
	}
	if persisted.Files["copied.mkv"] == nil || persisted.Files["copied.mkv"].Deleted {
		t.Fatal("copied file was not persisted in authoritative entry")
	}
	if got := persisted.Providers["test"].Files["copied.mkv"].Link; got != "https://example.invalid/video.mkv" {
		t.Fatalf("copied provider alias link = %q", got)
	}

	_, err = m.CopyEntryWithOverwrite(info, "/__all__/Source Folder/copied.mkv", false, false)
	if !errors.Is(err, ErrCopyDestinationExists) {
		t.Fatalf("Overwrite:F error = %v, want ErrCopyDestinationExists", err)
	}

	created, err = m.CopyEntryWithOverwrite(info, "/__all__/Source Folder/moved.mkv", true, true)
	if err != nil || !created {
		t.Fatalf("file MOVE = created:%v err:%v", created, err)
	}
	persisted, err = m.storage.Get(source.InfoHash)
	if err != nil {
		t.Fatalf("Get(source after MOVE): %v", err)
	}
	if !persisted.Files["video.mkv"].Deleted {
		t.Fatal("MOVE source is still active in authoritative entry")
	}
	if persisted.Files["moved.mkv"] == nil || persisted.Files["moved.mkv"].Deleted {
		t.Fatal("MOVE destination is not active in authoritative entry")
	}
	item, err := m.storage.GetEntryItem("Source Folder")
	if err != nil {
		t.Fatalf("GetEntryItem: %v", err)
	}
	if _, err := item.GetFile("video.mkv"); err == nil {
		t.Fatal("MOVE source survived in derivative folder index")
	}
	if _, err := item.GetFile("moved.mkv"); err != nil {
		t.Fatalf("MOVE destination missing from derivative folder index: %v", err)
	}
}

func TestCopyFileRejectsCrossFolderGraftWithoutChangingSource(t *testing.T) {
	m := newCopyEntryTestManager(t)
	source := addCopyTestEntry(t, m, strings.Repeat("2", 40), "Source Folder", map[string]int64{"video.mkv": 123})
	info, err := m.GetTorrentFile(source.GetFolder(), "video.mkv")
	if err != nil {
		t.Fatalf("GetTorrentFile: %v", err)
	}

	_, err = m.CopyEntryWithOverwrite(info, "/__all__/Missing Folder/video.mkv", true, true)
	if !errors.Is(err, ErrCopyDestinationParentMissing) {
		t.Fatalf("cross-folder MOVE error = %v, want ErrCopyDestinationParentMissing", err)
	}
	persisted, err := m.storage.Get(source.InfoHash)
	if err != nil {
		t.Fatalf("Get(source): %v", err)
	}
	if persisted.Files["video.mkv"].Deleted {
		t.Fatal("destination failure deleted the source file")
	}
}

func TestCopyMoveRejectsQueuedSourceBeforeMutation(t *testing.T) {
	tests := []struct {
		name        string
		file        bool
		move        bool
		destination string
	}{
		{name: "folder copy", destination: "/__all__/Destination Folder"},
		{name: "folder move", move: true, destination: "/__all__/Destination Folder"},
		{name: "file copy", file: true, destination: "/__all__/Source Folder/copied.mkv"},
		{name: "file move", file: true, move: true, destination: "/__all__/Source Folder/moved.mkv"},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := newCopyEntryTestManager(t)
			infohash := strings.Repeat(string(rune('6'+i)), 40)
			source := addCopyTestEntry(t, m, infohash, "Source Folder", map[string]int64{"video.mkv": 123})
			if err := m.storage.AddQueue(source); err != nil {
				t.Fatalf("AddQueue: %v", err)
			}

			var info *FileInfo
			var err error
			if test.file {
				info, err = m.GetTorrentFile(source.GetFolder(), "video.mkv")
			} else {
				info, err = m.GetEntryInfo(source.GetFolder())
			}
			if err != nil {
				t.Fatalf("source info: %v", err)
			}
			_, err = m.CopyEntryWithOverwrite(info, test.destination, test.move, true)
			if !errors.Is(err, ErrCopySourceActive) {
				t.Fatalf("COPY/MOVE error = %v, want ErrCopySourceActive", err)
			}
			if !m.storage.QueueExists(source.InfoHash) {
				t.Fatal("COPY/MOVE removed the active queue row")
			}
			persisted, loadErr := m.storage.Get(source.InfoHash)
			if loadErr != nil {
				t.Fatalf("Get(source): %v", loadErr)
			}
			if persisted.Files["video.mkv"].Deleted {
				t.Fatal("COPY/MOVE deleted the queued source file")
			}
			if test.file {
				destinationName := path.Base(test.destination)
				if _, exists := persisted.Files[destinationName]; exists {
					t.Fatalf("queued source gained destination file %q", destinationName)
				}
			} else if _, exists := m.storage.GetEntryItems()["Destination Folder"]; exists {
				t.Fatal("queued source created a destination folder")
			}
		})
	}
}

func newCopyEntryTestManager(t *testing.T) *Manager {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	cfg := config.Get()
	oldFolderNaming := cfg.FolderNaming
	cfg.FolderNaming = config.WebDavUseFileName
	t.Cleanup(func() { cfg.FolderNaming = oldFolderNaming })

	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	})
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("test", &lifecycleDebridClient{name: "test"})
	return &Manager{storage: store, config: cfg, logger: zerolog.Nop(), clients: clients}
}

func addCopyTestEntry(t *testing.T, m *Manager, infohash, name string, files map[string]int64) *storage.Entry {
	t.Helper()
	added := time.Unix(1_700_000_000, 0).UTC()
	entry := &storage.Entry{
		Protocol:         config.ProtocolTorrent,
		InfoHash:         infohash,
		Name:             name,
		OriginalFilename: name,
		ActiveProvider:   "test",
		Providers: map[string]*storage.ProviderEntry{
			"test": {
				Provider: "test",
				ID:       "remote-" + infohash,
				Status:   debridTypes.TorrentStatusDownloaded,
				Files:    make(map[string]*storage.ProviderFile),
			},
		},
		Files:      make(map[string]*storage.File),
		AddedOn:    added,
		IsComplete: true,
	}
	for filename, size := range files {
		entry.Files[filename] = &storage.File{Name: filename, Size: size, InfoHash: infohash, AddedOn: added}
		entry.Providers["test"].Files[filename] = &storage.ProviderFile{Link: "https://example.invalid/" + filename}
		entry.Size += size
		entry.Bytes += size
	}
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate(%s): %v", name, err)
	}
	return entry
}

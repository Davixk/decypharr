package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type lifecycleDebridClient struct {
	name       string
	submit     func(*debridTypes.Torrent) (*debridTypes.Torrent, error)
	check      func(*debridTypes.Torrent) (*debridTypes.Torrent, error)
	update     func(*debridTypes.Torrent) error
	get        func(string) (*debridTypes.Torrent, error)
	getAll     func() ([]*debridTypes.Torrent, error)
	onDelete   func(string) error
	deleteMu   sync.Mutex
	deletedIDs []string
}

func (c *lifecycleDebridClient) SubmitMagnet(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
	if c.submit != nil {
		return c.submit(torrent)
	}
	return nil, fmt.Errorf("unexpected SubmitMagnet call")
}

func (c *lifecycleDebridClient) CheckStatus(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
	if c.check != nil {
		return c.check(torrent)
	}
	return torrent, nil
}

func (c *lifecycleDebridClient) DeleteTorrent(id string) error {
	if c.onDelete != nil {
		if err := c.onDelete(id); err != nil {
			return err
		}
	}
	c.deleteMu.Lock()
	c.deletedIDs = append(c.deletedIDs, id)
	c.deleteMu.Unlock()
	return nil
}

func (c *lifecycleDebridClient) deleted() []string {
	c.deleteMu.Lock()
	defer c.deleteMu.Unlock()
	return append([]string(nil), c.deletedIDs...)
}

func (c *lifecycleDebridClient) UpdateTorrent(torrent *debridTypes.Torrent) error {
	if c.update != nil {
		return c.update(torrent)
	}
	return nil
}

func (c *lifecycleDebridClient) GetTorrent(id string) (*debridTypes.Torrent, error) {
	if c.get != nil {
		return c.get(id)
	}
	return nil, fmt.Errorf("unexpected GetTorrent call")
}

func (c *lifecycleDebridClient) GetAllTorrents() ([]*debridTypes.Torrent, error) {
	return c.GetTorrents()
}

func (c *lifecycleDebridClient) GetTorrents() ([]*debridTypes.Torrent, error) {
	if c.getAll != nil {
		return c.getAll()
	}
	return nil, nil
}

func (c *lifecycleDebridClient) GetDownloadLink(string, *debridTypes.File) (debridTypes.DownloadLink, error) {
	return debridTypes.DownloadLink{}, nil
}
func (c *lifecycleDebridClient) IsAvailable([]string) map[string]bool { return nil }
func (c *lifecycleDebridClient) Config() config.Debrid                { return config.Debrid{Name: c.name} }
func (c *lifecycleDebridClient) Logger() zerolog.Logger               { return zerolog.Nop() }
func (c *lifecycleDebridClient) RefreshDownloadLinks() error          { return nil }
func (c *lifecycleDebridClient) CheckFile(context.Context, string, string) error {
	return nil
}
func (c *lifecycleDebridClient) AccountManager() *account.Manager { return nil }
func (c *lifecycleDebridClient) GetProfile() (*debridTypes.Profile, error) {
	return nil, nil
}
func (c *lifecycleDebridClient) GetAvailableSlots() (int, error) { return 0, nil }
func (c *lifecycleDebridClient) SyncAccounts()                   {}
func (c *lifecycleDebridClient) DeleteLink(debridTypes.DownloadLink) error {
	return nil
}
func (c *lifecycleDebridClient) SpeedTest(context.Context) debridTypes.SpeedTestResult {
	return debridTypes.SpeedTestResult{}
}
func (c *lifecycleDebridClient) SupportsCheck() bool { return false }

func newProviderLifecycleManager(t *testing.T) *Manager {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	m := &Manager{
		storage:       store,
		clients:       xsync.NewMap[string, debrid.Client](),
		migrationJobs: xsync.NewMap[string, *storage.SwitcherJob](),
		logger:        zerolog.Nop(),
		config:        config.Get(),
	}
	m.fixer = NewFixer(m)
	return m
}

func lifecycleEntry(hash, provider, id string) *storage.Entry {
	added := time.Unix(1_700_000_000, 0).UTC()
	return &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       hash,
		Name:           "Movie.mkv",
		Size:           100,
		Bytes:          100,
		Magnet:         "magnet:?xt=urn:btih:" + hash,
		ActiveProvider: provider,
		Providers: map[string]*storage.ProviderEntry{
			provider: {
				Provider: provider,
				ID:       id,
				Status:   debridTypes.TorrentStatusDownloaded,
				Files: map[string]*storage.ProviderFile{
					"Movie.mkv": {Id: "file-" + id, Link: "https://example.invalid/" + id},
				},
			},
		},
		Files: map[string]*storage.File{
			"Movie.mkv": {Name: "Movie.mkv", Size: 100, InfoHash: hash, AddedOn: added},
		},
		Status:     debridTypes.TorrentStatusDownloaded,
		IsComplete: true,
		AddedOn:    added,
		CreatedAt:  added,
		UpdatedAt:  added,
	}
}

func completedRemote(hash, provider, id string) *debridTypes.Torrent {
	return &debridTypes.Torrent{
		Id:       id,
		InfoHash: hash,
		Name:     "Movie.mkv",
		Size:     100,
		Debrid:   provider,
		Status:   debridTypes.TorrentStatusDownloaded,
		Progress: 1,
		Added:    time.Unix(1_700_000_100, 0).UTC(),
		Files: map[string]debridTypes.File{
			"Movie.mkv": {Id: "file-" + id, Name: "Movie.mkv", Size: 100, Link: "https://example.invalid/" + id},
		},
	}
}

func persistLifecycleEntry(t *testing.T, m *Manager, entry *storage.Entry) *storage.Entry {
	t.Helper()
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	snapshot, err := m.storage.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return snapshot
}

func TestMoveTorrentCrossProviderKeepsSourcePlacement(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("a", 40)
	snapshot := persistLifecycleEntry(t, m, lifecycleEntry(hash, "source", "source-old"))
	source := &lifecycleDebridClient{name: "source"}
	target := &lifecycleDebridClient{name: "target"}
	target.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		return completedRemote(hash, "target", "target-new"), nil
	}
	m.clients.Store("source", source)
	m.clients.Store("target", target)

	success, err := m.fixer.MoveTorrent(snapshot, "target", false)
	if err != nil || !success {
		t.Fatalf("MoveTorrent = (%v, %v), want success", success, err)
	}
	current, err := m.storage.Get(hash)
	if err != nil {
		t.Fatalf("Get current: %v", err)
	}
	if current.ActiveProvider != "target" || current.Providers["source"] == nil || current.Providers["target"] == nil {
		t.Fatalf("cross-provider move lost ownership: active=%q providers=%v", current.ActiveProvider, current.Providers)
	}
	if got := source.deleted(); len(got) != 0 {
		t.Fatalf("Fixer deleted source placement: %v", got)
	}
}

func TestSwitcherKeepOldControlsSourceCleanupAndUsesSourceClient(t *testing.T) {
	for _, keepOld := range []bool{true, false} {
		t.Run(fmt.Sprintf("keep_old_%v", keepOld), func(t *testing.T) {
			m := newProviderLifecycleManager(t)
			hash := strings.Repeat(map[bool]string{true: "b", false: "c"}[keepOld], 40)
			snapshot := persistLifecycleEntry(t, m, lifecycleEntry(hash, "source", "source-old"))
			source := &lifecycleDebridClient{name: "source"}
			target := &lifecycleDebridClient{name: "target"}
			target.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
				return completedRemote(hash, "target", "target-new"), nil
			}
			source.onDelete = func(id string) error {
				current, err := m.storage.Get(hash)
				if err != nil {
					return err
				}
				if id != "source-old" || current.ActiveProvider != "target" || current.Providers["target"].ID != "target-new" {
					return fmt.Errorf("source cleanup ran before target commit: id=%s active=%s", id, current.ActiveProvider)
				}
				return nil
			}
			m.clients.Store("source", source)
			m.clients.Store("target", target)
			job := &storage.SwitcherJob{
				ID:             "job",
				InfoHash:       hash,
				SourceProvider: "source",
				TargetProvider: "target",
				KeepOld:        keepOld,
			}

			m.executeMigration(job, snapshot)
			if job.Status != storage.SwitcherStatusCompleted {
				t.Fatalf("job status=%s error=%q", job.Status, job.Error)
			}
			current, err := m.storage.Get(hash)
			if err != nil {
				t.Fatalf("Get current: %v", err)
			}
			_, sourcePresent := current.Providers["source"]
			if sourcePresent != keepOld {
				t.Fatalf("source present=%v, want %v", sourcePresent, keepOld)
			}
			if got := source.deleted(); (len(got) == 0) != keepOld {
				t.Fatalf("source deletions=%v keepOld=%v", got, keepOld)
			}
			if got := target.deleted(); len(got) != 0 {
				t.Fatalf("target client deleted a source id: %v", got)
			}
		})
	}
}

func TestMoveTorrentCompensatesTargetWhenGenerationWasReplaced(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("d", 40)
	snapshot := persistLifecycleEntry(t, m, lifecycleEntry(hash, "source", "source-old"))
	target := &lifecycleDebridClient{name: "target"}
	target.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		return completedRemote(hash, "target", "target-new"), nil
	}
	target.check = func(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
		if err := m.storage.Delete(hash); err != nil {
			return nil, err
		}
		replacement := lifecycleEntry(hash, "source", "replacement-source")
		replacement.Category = "replacement"
		if err := m.storage.AddOrUpdate(replacement); err != nil {
			return nil, err
		}
		return remote, nil
	}
	m.clients.Store("source", &lifecycleDebridClient{name: "source"})
	m.clients.Store("target", target)

	success, err := m.fixer.MoveTorrent(snapshot, "target", false)
	if success || !errors.Is(err, storage.ErrStaleEntryGeneration) {
		t.Fatalf("MoveTorrent = (%v, %v), want stale failure", success, err)
	}
	if got := target.deleted(); len(got) != 1 || got[0] != "target-new" {
		t.Fatalf("target compensation=%v, want [target-new]", got)
	}
	current, getErr := m.storage.Get(hash)
	if getErr != nil {
		t.Fatalf("Get replacement: %v", getErr)
	}
	if current.Category != "replacement" || current.ActiveProvider != "source" || current.Providers["target"] != nil {
		t.Fatalf("stale target mutated replacement: %+v", current)
	}
}

func TestStaleCompensationDoesNotDeletePlacementOwnedByReplacement(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("4", 40)
	snapshot := persistLifecycleEntry(t, m, lifecycleEntry(hash, "source", "source-old"))
	target := &lifecycleDebridClient{name: "target"}
	target.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		return completedRemote(hash, "target", "shared-target-id"), nil
	}
	target.check = func(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
		if err := m.storage.Delete(hash); err != nil {
			return nil, err
		}
		replacement := lifecycleEntry(hash, "target", "shared-target-id")
		replacement.Category = "replacement-owner"
		if err := m.storage.AddOrUpdate(replacement); err != nil {
			return nil, err
		}
		return remote, nil
	}
	m.clients.Store("source", &lifecycleDebridClient{name: "source"})
	m.clients.Store("target", target)

	success, err := m.fixer.MoveTorrent(snapshot, "target", false)
	if success || !errors.Is(err, storage.ErrStaleEntryGeneration) {
		t.Fatalf("MoveTorrent = (%v, %v), want stale failure", success, err)
	}
	if got := target.deleted(); len(got) != 0 {
		t.Fatalf("compensation deleted replacement-owned placement: %v", got)
	}
	current, _ := m.storage.Get(hash)
	if current.Category != "replacement-owner" || current.Providers["target"].ID != "shared-target-id" {
		t.Fatalf("replacement ownership changed: %+v", current)
	}
}

func TestReinsertDeletesOldTargetSynchronouslyAfterCommit(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("e", 40)
	snapshot := persistLifecycleEntry(t, m, lifecycleEntry(hash, "target", "target-old"))
	target := &lifecycleDebridClient{name: "target"}
	target.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		return completedRemote(hash, "target", "target-new"), nil
	}
	target.onDelete = func(id string) error {
		current, err := m.storage.Get(hash)
		if err != nil {
			return err
		}
		if id != "target-old" || current.Providers["target"].ID != "target-new" {
			return fmt.Errorf("old target cleanup preceded ownership commit")
		}
		return nil
	}
	m.clients.Store("target", target)

	success, err := m.fixer.MoveTorrent(snapshot, "target", true)
	if err != nil || !success {
		t.Fatalf("MoveTorrent = (%v, %v), want success", success, err)
	}
	if got := target.deleted(); len(got) != 1 || got[0] != "target-old" {
		t.Fatalf("synchronous target cleanup=%v", got)
	}
}

func TestProcessNewTorrentsContinuesAfterStaleResponse(t *testing.T) {
	m := newProviderLifecycleManager(t)
	staleHash := strings.Repeat("f", 40)
	goodHash := strings.Repeat("1", 40)
	persistLifecycleEntry(t, m, lifecycleEntry(staleHash, "provider", "stale-old"))
	persistLifecycleEntry(t, m, lifecycleEntry(goodHash, "provider", "good-old"))
	client := &lifecycleDebridClient{name: "provider"}
	client.update = func(remote *debridTypes.Torrent) error {
		if remote.InfoHash == staleHash {
			if err := m.storage.Delete(staleHash); err != nil {
				return err
			}
			replacement := lifecycleEntry(staleHash, "provider", "replacement-id")
			replacement.Category = "replacement"
			if err := m.storage.AddOrUpdate(replacement); err != nil {
				return err
			}
		}
		filled := completedRemote(remote.InfoHash, "provider", "refreshed-"+remote.InfoHash[:1])
		remote.Id = filled.Id
		remote.Name = filled.Name
		remote.Size = filled.Size
		remote.Debrid = filled.Debrid
		remote.Status = filled.Status
		remote.Progress = filled.Progress
		remote.Added = filled.Added
		remote.Files = filled.Files
		return nil
	}
	m.clients.Store("provider", client)

	staleSnapshot, _ := m.storage.Get(staleHash)
	goodSnapshot, _ := m.storage.Get(goodHash)
	err := m.processNewTorrents("provider", []providerRefreshCandidate{
		{remote: &debridTypes.Torrent{Id: "stale-old", InfoHash: staleHash, Name: "Movie.mkv", Debrid: "provider"}, snapshot: staleSnapshot},
		{remote: &debridTypes.Torrent{Id: "good-old", InfoHash: goodHash, Name: "Movie.mkv", Debrid: "provider"}, snapshot: goodSnapshot},
	})
	if !errors.Is(err, storage.ErrStaleEntryGeneration) {
		t.Fatalf("processNewTorrents error=%v, want stale member error", err)
	}
	staleCurrent, _ := m.storage.Get(staleHash)
	if staleCurrent.Category != "replacement" || staleCurrent.Providers["provider"].ID != "replacement-id" {
		t.Fatalf("stale response changed replacement: %+v", staleCurrent)
	}
	goodCurrent, _ := m.storage.Get(goodHash)
	if got := goodCurrent.Providers["provider"].ID; got != "refreshed-1" {
		t.Fatalf("independent good refresh was skipped: id=%q", got)
	}
}

func TestProviderRemovalSkipsDeleteReaddReplacement(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("2", 40)
	persistLifecycleEntry(t, m, lifecycleEntry(hash, "provider", "same-id"))
	// Zero-value presence: the provider holds nothing at all, which is the only
	// condition under which a placement may be removed.
	_, removals, err := m.detectTorrentChanges("provider", map[string]*debridTypes.Torrent{}, map[string]*debridTypes.Torrent{}, providerPresence{})
	if err != nil || len(removals) != 1 {
		t.Fatalf("detect removals=(%d, %v)", len(removals), err)
	}
	if err := m.storage.Delete(hash); err != nil {
		t.Fatalf("Delete old: %v", err)
	}
	replacement := lifecycleEntry(hash, "provider", "same-id")
	replacement.Category = "replacement"
	if err := m.storage.AddOrUpdate(replacement); err != nil {
		t.Fatalf("Add replacement: %v", err)
	}
	if err := m.handleProviderRemovals(removals); err != nil {
		t.Fatalf("handle removals: %v", err)
	}
	current, err := m.storage.Get(hash)
	if err != nil || current.Category != "replacement" {
		t.Fatalf("replacement deleted by stale remote scan: current=%+v err=%v", current, err)
	}
}

func TestPersistLinkEntryBadMergesWithTerminalWorkflowState(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("3", 40)
	entry := lifecycleEntry(hash, "provider", "provider-id")
	entry.Action = config.DownloadActionDownload
	entry.CallbackURL = "https://callback.invalid/original"
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("Add main: %v", err)
	}
	if err := m.storage.AddQueue(entry); err != nil {
		t.Fatalf("Add queue: %v", err)
	}
	staleQueueSnapshot, err := m.storage.GetQueued(hash)
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	currentMain, _ := m.storage.Get(hash)
	currentMain.LastError = "terminal failure"
	currentMain.State = storage.EntryStateError
	if err := m.storage.AddOrUpdate(currentMain); err != nil {
		t.Fatalf("advance main: %v", err)
	}

	if err := m.persistLinkEntryBad(staleQueueSnapshot); err != nil {
		t.Fatalf("persistLinkEntryBad: %v", err)
	}
	mainAfter, _ := m.storage.Get(hash)
	queueAfter, _ := m.storage.GetQueued(hash)
	if !mainAfter.Bad || mainAfter.LastError != "terminal failure" || mainAfter.State != storage.EntryStateError {
		t.Fatalf("Bad merge overwrote main terminal state: %+v", mainAfter)
	}
	if !queueAfter.Bad || queueAfter.Action != config.DownloadActionDownload || queueAfter.CallbackURL != entry.CallbackURL {
		t.Fatalf("Bad merge overwrote queue workflow state: %+v", queueAfter)
	}
}

func TestRemoveFromProviderRejectsUnknownOrGenerationBlindCleanup(t *testing.T) {
	m := newProviderLifecycleManager(t)
	if err := m.RemoveFromProvider(&storage.ProviderEntry{Provider: "missing", ID: "id"}); err == nil {
		t.Fatal("missing provider client was silently accepted")
	}
	if err := m.RemoveFromProvider(&storage.ProviderEntry{Provider: "usenet", ID: "id"}); err == nil {
		t.Fatal("generation-blind Usenet cleanup was silently accepted")
	}
}

func TestDeleteEntryCleansProviderOnlyAfterLastFolderAlias(t *testing.T) {
	m := newProviderLifecycleManager(t)
	sourceHash := strings.Repeat("5", 40)
	aliasHash := strings.Repeat("6", 40)
	persistLifecycleEntry(t, m, lifecycleEntry(sourceHash, "provider", "shared-id"))
	alias := lifecycleEntry(aliasHash, "provider", "shared-id")
	alias.Name = "Alias.mkv"
	persistLifecycleEntry(t, m, alias)
	client := &lifecycleDebridClient{name: "provider"}
	m.clients.Store("provider", client)

	if err := m.DeleteEntry(sourceHash, true); err != nil {
		t.Fatalf("Delete source alias: %v", err)
	}
	if got := client.deleted(); len(got) != 0 {
		t.Fatalf("non-final alias deletion removed shared placement: %v", got)
	}
	if _, err := m.storage.Get(aliasHash); err != nil {
		t.Fatalf("remaining alias disappeared: %v", err)
	}

	if err := m.DeleteEntry(aliasHash, true); err != nil {
		t.Fatalf("Delete final alias: %v", err)
	}
	if got := client.deleted(); len(got) != 1 || got[0] != "shared-id" {
		t.Fatalf("final alias cleanup=%v, want [shared-id]", got)
	}
}

func TestSwitcherDoesNotDeleteSourcePlacementSharedByAlias(t *testing.T) {
	m := newProviderLifecycleManager(t)
	sourceHash := strings.Repeat("7", 40)
	aliasHash := strings.Repeat("8", 40)
	snapshot := persistLifecycleEntry(t, m, lifecycleEntry(sourceHash, "source", "shared-source"))
	alias := lifecycleEntry(aliasHash, "source", "shared-source")
	alias.Name = "Alias.mkv"
	persistLifecycleEntry(t, m, alias)
	source := &lifecycleDebridClient{name: "source"}
	target := &lifecycleDebridClient{name: "target"}
	target.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		return completedRemote(sourceHash, "target", "target-new"), nil
	}
	m.clients.Store("source", source)
	m.clients.Store("target", target)
	job := &storage.SwitcherJob{
		ID:             "shared-source-job",
		InfoHash:       sourceHash,
		SourceProvider: "source",
		TargetProvider: "target",
		KeepOld:        false,
	}

	m.executeMigration(job, snapshot)
	if job.Status != storage.SwitcherStatusCompleted {
		t.Fatalf("job status=%s error=%q", job.Status, job.Error)
	}
	if got := source.deleted(); len(got) != 0 {
		t.Fatalf("switcher deleted alias-shared source placement: %v", got)
	}
	current, _ := m.storage.Get(sourceHash)
	if current.Providers["source"] != nil || current.ActiveProvider != "target" {
		t.Fatalf("switched row retained source ownership: %+v", current)
	}
	aliasCurrent, _ := m.storage.Get(aliasHash)
	if aliasCurrent.Providers["source"] == nil {
		t.Fatal("other alias lost source placement")
	}
}

func TestReinsertDoesNotDeleteOldTargetSharedByAlias(t *testing.T) {
	m := newProviderLifecycleManager(t)
	sourceHash := strings.Repeat("9", 40)
	aliasHash := strings.Repeat("0", 40)
	snapshot := persistLifecycleEntry(t, m, lifecycleEntry(sourceHash, "target", "shared-old"))
	alias := lifecycleEntry(aliasHash, "target", "shared-old")
	alias.Name = "Alias.mkv"
	persistLifecycleEntry(t, m, alias)
	target := &lifecycleDebridClient{name: "target"}
	target.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		return completedRemote(sourceHash, "target", "target-new"), nil
	}
	m.clients.Store("target", target)

	success, err := m.fixer.MoveTorrent(snapshot, "target", true)
	if err != nil || !success {
		t.Fatalf("MoveTorrent = (%v, %v), want success", success, err)
	}
	if got := target.deleted(); len(got) != 0 {
		t.Fatalf("reinsert deleted alias-shared old target: %v", got)
	}
	aliasCurrent, _ := m.storage.Get(aliasHash)
	if aliasCurrent.Providers["target"].ID != "shared-old" {
		t.Fatalf("other alias ownership changed: %+v", aliasCurrent.Providers["target"])
	}
}

func TestProviderRefreshMatchesSyntheticAliasByPlacementID(t *testing.T) {
	m := newProviderLifecycleManager(t)
	aliasHash := strings.Repeat("a", 39) + "b"
	remoteHash := strings.Repeat("b", 39) + "a"
	alias := lifecycleEntry(aliasHash, "provider", "old-remote-id")
	alias.Name = "Renamed Alias.mkv"
	alias.Magnet = "magnet:?xt=urn:btih:" + remoteHash
	alias.Providers["provider"].Status = debridTypes.TorrentStatusDownloading
	alias.Providers["provider"].Files = map[string]*storage.ProviderFile{}
	persistLifecycleEntry(t, m, alias)
	remote := completedRemote(remoteHash, "provider", "new-remote-id")
	client := &lifecycleDebridClient{name: "provider"}
	m.clients.Store("provider", client)

	refreshes, removals, err := m.detectTorrentChanges(
		"provider",
		map[string]*debridTypes.Torrent{remoteHash: remote},
		map[string]*debridTypes.Torrent{"new-remote-id": remote},
		presenceOf(remote),
	)
	if err != nil || len(removals) != 0 || len(refreshes) != 1 || refreshes[0].snapshot == nil || refreshes[0].snapshot.InfoHash != aliasHash {
		t.Fatalf("alias detection refreshes=%+v removals=%+v err=%v", refreshes, removals, err)
	}
	if err := m.processNewTorrents("provider", refreshes); err != nil {
		t.Fatalf("process alias refresh: %v", err)
	}
	current, err := m.storage.Get(aliasHash)
	if err != nil {
		t.Fatalf("Get alias after refresh: %v", err)
	}
	if current.InfoHash != aliasHash || current.Name != "Renamed Alias.mkv" || current.Providers["provider"].ID != "new-remote-id" || current.Status != debridTypes.TorrentStatusDownloaded {
		t.Fatalf("alias refresh changed logical identity or missed provider state: %+v", current)
	}
	if exists, _ := m.storage.Exists(remoteHash); exists {
		t.Fatal("provider refresh recreated a canonical row beside the synthetic alias")
	}
}

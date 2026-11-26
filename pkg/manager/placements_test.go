package manager

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/appendstore"
	"github.com/sirrobot01/decypharr/internal/config"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"google.golang.org/protobuf/proto"
)

// dualPlacementEntry is an entry reading from "rd" that still carries an "ad"
// copy — the shape a library migrating off AllDebrid is full of.
func dualPlacementEntry(hash string) *storage.Entry {
	entry := lifecycleEntry(hash, "rd", "rd-1")
	entry.Providers["ad"] = &storage.ProviderEntry{
		Provider: "ad",
		ID:       "ad-1",
		Status:   debridTypes.TorrentStatusDownloaded,
		Files: map[string]*storage.ProviderFile{
			"Movie.mkv": {Id: "file-ad-1", Link: "https://example.invalid/ad-1"},
		},
	}
	return entry
}

func placementManager(t *testing.T) (*Manager, *lifecycleDebridClient, *lifecycleDebridClient) {
	t.Helper()
	m := newProviderLifecycleManager(t)
	ad := &lifecycleDebridClient{name: "ad"}
	rd := &lifecycleDebridClient{name: "rd"}
	m.clients.Store("ad", ad)
	m.clients.Store("rd", rd)
	return m, ad, rd
}

// THE MIGRATION'S CORE MOVE: drop the stale copy, keep the one the library reads.
func TestDropPlacementRemovesOnlyTheNamedNonActiveCopy(t *testing.T) {
	m, ad, rd := placementManager(t)
	hash := strings.Repeat("a", 40)
	persistLifecycleEntry(t, m, dualPlacementEntry(hash))

	if err := m.DropPlacement(hash, "ad"); err != nil {
		t.Fatalf("DropPlacement: %v", err)
	}

	current, err := m.storage.Get(hash)
	if err != nil {
		t.Fatalf("the entry is gone after dropping one of its two copies: %v", err)
	}
	if _, still := current.Providers["ad"]; still {
		t.Fatal("the ad placement is still recorded after it was dropped")
	}
	if current.Providers["rd"] == nil || current.ActiveProvider != "rd" {
		t.Fatalf("the copy the library reads was disturbed: active=%q providers=%v", current.ActiveProvider, current.Providers)
	}
	if got := ad.deleted(); len(got) != 1 || got[0] != "ad-1" {
		t.Fatalf("ad provider deletions = %v, want exactly [ad-1]; the slot is not freed otherwise", got)
	}
	if got := rd.deleted(); len(got) != 0 {
		t.Fatalf("the ACTIVE provider was told to delete %v — that destroys the library's copy", got)
	}
}

// 🛑 Dropping the active copy would leave the entry reading from nothing.
func TestDropPlacementRefusesTheActiveProvider(t *testing.T) {
	m, ad, rd := placementManager(t)
	hash := strings.Repeat("b", 40)
	persistLifecycleEntry(t, m, dualPlacementEntry(hash))

	err := m.DropPlacement(hash, "rd")
	if !errors.Is(err, ErrPlacementIsActive) {
		t.Fatalf("dropping the active provider returned %v, want ErrPlacementIsActive", err)
	}
	assertNothingDeleted(t, ad, rd)
	assertPlacements(t, m, hash, "ad", "rd")
}

// 🛑 An entry's only copy is not a "placement to drop", it is the entry.
func TestDropPlacementRefusesTheOnlyPlacement(t *testing.T) {
	m, ad, rd := placementManager(t)
	hash := strings.Repeat("c", 40)
	// Active names a provider the map does not hold — exactly the inconsistent
	// row the migration feared: "active_debrid is RealDebrid" while only an AD
	// copy exists. Dropping AD here would strand the entry with no copy at all.
	entry := lifecycleEntry(hash, "ad", "ad-1")
	entry.ActiveProvider = "rd"
	persistLifecycleEntry(t, m, entry)

	err := m.DropPlacement(hash, "ad")
	if !errors.Is(err, ErrLastPlacement) {
		t.Fatalf("dropping the only placement returned %v, want ErrLastPlacement", err)
	}
	assertNothingDeleted(t, ad, rd)
	assertPlacements(t, m, hash, "ad")
}

// 🛑 A copy still downloading is not yet a copy, so the one being dropped may be
// the only complete one.
func TestDropPlacementRefusesWhenTheActiveCopyIsIncomplete(t *testing.T) {
	m, ad, rd := placementManager(t)
	hash := strings.Repeat("d", 40)
	entry := dualPlacementEntry(hash)
	entry.Providers["rd"].Status = debridTypes.TorrentStatusDownloading
	persistLifecycleEntry(t, m, entry)

	err := m.DropPlacement(hash, "ad")
	if !errors.Is(err, storage.ErrPlacementNotCompleted) {
		t.Fatalf("dropping while the active copy is incomplete returned %v, want ErrPlacementNotCompleted", err)
	}
	assertNothingDeleted(t, ad, rd)
	assertPlacements(t, m, hash, "ad", "rd")
}

// CASE TWINS share provider copies. The main store keys rows by the hash string
// as it arrived, so "abc…" and "ABC…" are two rows that can hold the SAME AD
// torrent. Releasing it on behalf of one row would delete it out from under the
// other.
func TestDropPlacementKeepsACopyACaseTwinStillReferences(t *testing.T) {
	m, ad, rd := placementManager(t)
	lower := strings.Repeat("e", 40)
	upper := strings.ToUpper(lower)
	twin := lifecycleEntry(upper, "ad", "ad-1") // same AD torrent id
	twin.Name = "Movie (old name).mkv"
	withLegacyMainRows(t, m, dualPlacementEntry(lower), twin)

	if err := m.DropPlacement(lower, "ad"); err != nil {
		t.Fatalf("DropPlacement: %v", err)
	}
	if got := ad.deleted(); len(got) != 0 {
		t.Fatalf("released AD torrent %v while the case twin %s still references it", got, upper)
	}
	assertNothingDeleted(t, ad, rd)
	assertPlacements(t, m, lower, "rd")
	assertPlacements(t, m, upper, "ad")
}

func TestEntryPlacementsReportsCaseTwinsAndAnExactMiss(t *testing.T) {
	m, _, _ := placementManager(t)
	lower := strings.Repeat("f", 40)
	upper := strings.ToUpper(lower)
	withLegacyMainRows(t, m, dualPlacementEntry(lower), lifecycleEntry(upper, "ad", "ad-9"))

	report, _, err := m.EntryPlacements(lower)
	if err != nil {
		t.Fatalf("EntryPlacements: %v", err)
	}
	if report.ActiveProvider != "rd" || len(report.Placements) != 2 {
		t.Fatalf("report = active %q, %d placements; want rd and 2", report.ActiveProvider, len(report.Placements))
	}
	if len(report.CaseTwins) != 1 || report.CaseTwins[0].InfoHash != upper {
		t.Fatalf("case twins = %+v, want exactly %s", report.CaseTwins, upper)
	}

	// The uppercase twin, addressed by its own key, reports itself and names
	// the lowercase row — never itself.
	report, _, err = m.EntryPlacements(upper)
	if err != nil {
		t.Fatalf("EntryPlacements(upper): %v", err)
	}
	if report.InfoHash != upper || len(report.CaseTwins) != 1 || report.CaseTwins[0].InfoHash != lower {
		t.Fatalf("upper report = %s twins %+v, want %s with twin %s", report.InfoHash, report.CaseTwins, upper, lower)
	}

	// A caller holding a THIRD casing (say, a normalised id) resolves to the
	// lowercase row and is told about the other one, instead of a bare miss.
	mixed := "F" + lower[1:]
	report, _, err = m.EntryPlacements(mixed)
	if err != nil {
		t.Fatalf("lookup of %s returned %v, want the lowercase row", mixed, err)
	}
	if report.InfoHash != lower || len(report.CaseTwins) != 1 || report.CaseTwins[0].InfoHash != upper {
		t.Fatalf("mixed-case report = %s twins %+v, want %s with twin %s", report.InfoHash, report.CaseTwins, lower, upper)
	}

	if _, _, err := m.EntryPlacements(strings.Repeat("5", 40)); !errors.Is(err, storage.ErrEntryNotFound) {
		t.Fatalf("lookup of an absent hash returned %v, want ErrEntryNotFound", err)
	}
}

// withLegacyMainRows swaps m's store for one reopened from a database whose
// main rows were written under their raw keys — exactly how builds before
// case-canonical keys stored them. It is the only way to create a case twin
// now that every write resolves to an existing row.
func withLegacyMainRows(t *testing.T, m *Manager, rows ...*storage.Entry) {
	t.Helper()
	dir := t.TempDir()
	raw, err := appendstore.Open(filepath.Join(dir, "entries.db"), appendstore.Options{})
	if err != nil {
		t.Fatalf("open raw entries store: %v", err)
	}
	for _, row := range rows {
		data, err := proto.Marshal(storage.EntryToProto(row))
		if err != nil {
			t.Fatalf("marshal legacy row %s: %v", row.InfoHash, err)
		}
		if err := raw.Put(row.InfoHash, data, nil); err != nil {
			t.Fatalf("put legacy row %s: %v", row.InfoHash, err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw entries store: %v", err)
	}
	reopened, err := storage.NewStorage(dir)
	if err != nil {
		t.Fatalf("reopen legacy store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	m.storage = reopened
}

// After a switch, the target must hold every file the library links to. A
// provider that names files differently leaves them unreadable even though the
// switch "succeeded".
func TestEntryPlacementsFlagsFilesTheActiveCopyLacks(t *testing.T) {
	m, _, _ := placementManager(t)
	hash := strings.Repeat("9", 40)
	entry := dualPlacementEntry(hash)
	entry.Files["Extra.mkv"] = &storage.File{Name: "Extra.mkv", Size: 10, InfoHash: hash}
	persistLifecycleEntry(t, m, entry)

	report, _, err := m.EntryPlacements(hash)
	if err != nil {
		t.Fatalf("EntryPlacements: %v", err)
	}
	if len(report.MissingFromActive) != 1 || report.MissingFromActive[0] != "Extra.mkv" {
		t.Fatalf("missing_from_active = %v, want [Extra.mkv]", report.MissingFromActive)
	}
}

func TestSwitchTorrentValidatesBeforeCreatingAJob(t *testing.T) {
	m, _, _ := placementManager(t)
	hash := strings.Repeat("8", 40)
	persistLifecycleEntry(t, m, dualPlacementEntry(hash))

	if _, err := m.SwitchTorrent(context.Background(), hash, "rd", false, false); !errors.Is(err, storage.ErrAlreadyOnDebrid) {
		t.Fatalf("switching to the active provider returned %v, want ErrAlreadyOnDebrid", err)
	}
	if _, err := m.SwitchTorrent(context.Background(), hash, "nope", false, false); !errors.Is(err, ErrSwitchTargetUnknown) {
		t.Fatalf("switching to an unconfigured provider returned %v, want ErrSwitchTargetUnknown", err)
	}
	if _, err := m.SwitchTorrent(context.Background(), strings.Repeat("7", 40), "ad", false, false); !errors.Is(err, storage.ErrEntryNotFound) {
		t.Fatalf("switching a missing entry returned %v, want ErrEntryNotFound", err)
	}
	jobs := 0
	m.migrationJobs.Range(func(string, *storage.SwitcherJob) bool { jobs++; return true })
	if jobs != 0 {
		t.Fatalf("%d jobs were created by refused switches; a refusal must leave no job behind", jobs)
	}
}

// 🔴 Job state is written on the migration goroutine and read on request
// goroutines. Run with -race: storing the live pointer instead of a snapshot
// makes this fail.
func TestSwitchJobReadsAreRaceFree(t *testing.T) {
	m, _, _ := placementManager(t)
	hash := strings.Repeat("6", 40)
	persistLifecycleEntry(t, m, lifecycleEntry(hash, "ad", "ad-1"))
	target := &lifecycleDebridClient{name: "rd"}
	target.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		time.Sleep(20 * time.Millisecond)
		return completedRemote(hash, "rd", "rd-1"), nil
	}
	m.clients.Store("rd", target)

	job, err := m.SwitchTorrent(context.Background(), hash, "rd", false, false)
	if err != nil {
		t.Fatalf("SwitchTorrent: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if snap, err := m.GetMigrationJob(job.ID); err == nil {
					_ = snap.Status
					_ = snap.Progress
					_ = snap.Error
					_ = snap.CompletedAt
				}
				m.migrationJobs.Range(func(_ string, j *storage.SwitcherJob) bool {
					_ = j.Status
					return true
				})
			}
		})
	}

	deadline := time.Now().Add(10 * time.Second)
	var final *storage.SwitcherJob
	for time.Now().Before(deadline) {
		snap, err := m.GetMigrationJob(job.ID)
		if err == nil && (snap.Status == storage.SwitcherStatusCompleted || snap.Status == storage.SwitcherStatusFailed) {
			final = snap
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if final == nil {
		t.Fatal("the switch never reached a terminal state")
	}
	if final.Status != storage.SwitcherStatusCompleted {
		t.Fatalf("switch ended %s: %s", final.Status, final.Error)
	}
	assertPlacements(t, m, hash, "rd")
}

func assertNothingDeleted(t *testing.T, clients ...*lifecycleDebridClient) {
	t.Helper()
	for _, c := range clients {
		if got := c.deleted(); len(got) != 0 {
			t.Fatalf("a refused drop still told %s to delete %v", c.name, got)
		}
	}
}

func assertPlacements(t *testing.T, m *Manager, hash string, want ...string) {
	t.Helper()
	current, err := m.storage.Get(hash)
	if err != nil {
		t.Fatalf("Get %s: %v", hash, err)
	}
	got := make(map[string]bool, len(current.Providers))
	for _, p := range current.Providers {
		if p != nil {
			got[p.Provider] = true
		}
	}
	if len(got) != len(want) {
		t.Fatalf("%s placements = %v, want %v", hash, got, want)
	}
	for _, w := range want {
		if !got[w] {
			t.Fatalf("%s placements = %v, want %v", hash, got, want)
		}
	}
}

// forbiddenProviderClient fails the test on ANY call: every interface method is
// either overridden to record and fail, or reaches the nil embedded Client and
// panics. The merge must never touch a provider.
type forbiddenProviderClient struct {
	debrid.Client
	t     *testing.T
	name  string
	calls atomic.Int32
}

func (c *forbiddenProviderClient) forbid(method string) {
	c.calls.Add(1)
	c.t.Errorf("the case-twin merge called %s on provider %s; it must never contact a provider", method, c.name)
}

func (c *forbiddenProviderClient) SubmitMagnet(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
	c.forbid("SubmitMagnet")
	return nil, errors.New("forbidden")
}
func (c *forbiddenProviderClient) CheckStatus(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
	c.forbid("CheckStatus")
	return nil, errors.New("forbidden")
}
func (c *forbiddenProviderClient) DeleteTorrent(string) error {
	c.forbid("DeleteTorrent")
	return errors.New("forbidden")
}
func (c *forbiddenProviderClient) UpdateTorrent(*debridTypes.Torrent) error {
	c.forbid("UpdateTorrent")
	return errors.New("forbidden")
}
func (c *forbiddenProviderClient) GetTorrent(string) (*debridTypes.Torrent, error) {
	c.forbid("GetTorrent")
	return nil, errors.New("forbidden")
}
func (c *forbiddenProviderClient) GetTorrents() ([]*debridTypes.Torrent, error) {
	c.forbid("GetTorrents")
	return nil, errors.New("forbidden")
}
func (c *forbiddenProviderClient) GetAllTorrents() ([]*debridTypes.Torrent, error) {
	c.forbid("GetAllTorrents")
	return nil, errors.New("forbidden")
}
func (c *forbiddenProviderClient) GetDownloadLink(string, *debridTypes.File) (debridTypes.DownloadLink, error) {
	c.forbid("GetDownloadLink")
	return debridTypes.DownloadLink{}, errors.New("forbidden")
}
func (c *forbiddenProviderClient) Config() config.Debrid {
	c.forbid("Config")
	return config.Debrid{Name: c.name}
}

// 🛑 THE MERGE MOVES RECORDS ONLY. Even with a shared AD copy, an extra RD copy
// and a removed twin, no provider is contacted, dry run or real.
func TestMergeCaseTwinsMakesNoProviderCalls(t *testing.T) {
	m := newProviderLifecycleManager(t)
	ad := &forbiddenProviderClient{t: t, name: "ad"}
	rd := &forbiddenProviderClient{t: t, name: "rd"}
	m.clients.Store("ad", ad)
	m.clients.Store("rd", rd)

	lower := strings.Repeat("3", 39) + "a"
	upper := strings.ToUpper(lower)
	stale := dualPlacementEntry(lower) // rd active + the shared ad copy
	stale.ActiveProvider = ""
	stale.Name = "Movie (old name).mkv"
	withLegacyMainRows(t, m, lifecycleEntry(upper, "ad", "ad-1"), stale)

	for _, dryRun := range []bool{true, false} {
		report, err := m.MergeCaseTwins(dryRun)
		if err != nil {
			t.Fatalf("MergeCaseTwins(dry_run=%v): %v", dryRun, err)
		}
		if report.Merged != 1 {
			t.Fatalf("MergeCaseTwins(dry_run=%v) merged %d groups, want 1: %+v", dryRun, report.Merged, report.Results)
		}
	}
	if calls := ad.calls.Load() + rd.calls.Load(); calls != 0 {
		t.Fatalf("the merge made %d provider call(s)", calls)
	}
	assertPlacements(t, m, lower, "ad", "rd")
}

// After the merge there is one row, every casing reaches it, and it reports no
// twins.
func TestEntryPlacementsAfterMergeReportsNoTwins(t *testing.T) {
	m, _, _ := placementManager(t)
	lower := strings.Repeat("4", 39) + "f"
	upper := strings.ToUpper(lower)
	stale := lifecycleEntry(upper, "ad", "ad-1")
	stale.ActiveProvider = ""
	withLegacyMainRows(t, m, dualPlacementEntry(lower), stale)

	if report, _, err := m.EntryPlacements(upper); err != nil || len(report.CaseTwins) != 1 {
		t.Fatalf("before the merge, %s = %+v, %v; want one twin reported", upper, report, err)
	}
	if _, err := m.MergeCaseTwins(false); err != nil {
		t.Fatalf("MergeCaseTwins: %v", err)
	}
	for _, key := range []string{lower, upper} {
		report, twins, err := m.EntryPlacements(key)
		if err != nil {
			t.Fatalf("EntryPlacements(%s): %v", key, err)
		}
		if report.InfoHash != lower || len(report.CaseTwins) != 0 || len(twins) != 0 {
			t.Fatalf("after merge, %s reported %s with twins %+v; want %s and none", key, report.InfoHash, report.CaseTwins, lower)
		}
	}
}

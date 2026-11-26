package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirrobot01/appendstore"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// liveUpperTwin / staleLowerTwin are the production shape: the uppercase row
// reads from AllDebrid and owns the library's folder; the lowercase row has no
// active provider, a different name, an extra placement, an extra file and
// another tag.
func liveUpperTwin(upper string) *Entry {
	entry := caseTestEntry(upper, "Upper Name", "ad", "ad-1")
	entry.Tags = []string{"a"}
	return entry
}

func staleLowerTwin(lower string) *Entry {
	entry := caseTestEntry(lower, "Lower Name", "ad", "ad-1") // the SAME AD copy
	entry.ActiveProvider = ""
	entry.Providers["rd"] = &ProviderEntry{
		Provider: "rd",
		ID:       "rd-7",
		Status:   debridTypes.TorrentStatusDownloaded,
		Files:    map[string]*ProviderFile{"extra.mkv": {Id: "f-rd-7", Link: "https://example.invalid/rd-7"}},
	}
	entry.Files["extra.mkv"] = &File{Name: "extra.mkv", Size: 5, InfoHash: lower, AddedOn: entry.AddedOn}
	entry.Tags = []string{"a", "b"}
	return entry
}

// storeImage captures every byte the merge could touch: every row's value and
// index attributes in every bucket, plus the database directory listing (a
// journal would appear there).
func storeImage(t *testing.T, s *Storage, dir string) map[string]string {
	t.Helper()
	image := make(map[string]string)
	buckets := map[string]*appendstore.Store{
		"entries": s.entries, "queue": s.queue, "items": s.entryItems,
		"repair_state": s.repairState, "repair_runs": s.repairRuns,
	}
	for name, bucket := range buckets {
		if err := bucket.ForEach(func(key string, value []byte) error {
			image[name+"/"+key] = string(value)
			return nil
		}); err != nil {
			t.Fatalf("scan %s: %v", name, err)
		}
		if err := bucket.ForEachMetadata(func(key string, meta *appendstore.Metadata) error {
			image[name+"/"+key+"#attrs"] = fmt.Sprint(meta.Attributes)
			return nil
		}); err != nil {
			t.Fatalf("scan %s metadata: %v", name, err)
		}
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("list db dir: %v", err)
	}
	names := make([]string, 0, len(files))
	for _, file := range files {
		names = append(names, file.Name())
	}
	sort.Strings(names)
	image["#dir"] = strings.Join(names, ",")
	return image
}

func assertImageUnchanged(t *testing.T, before, after map[string]string) {
	t.Helper()
	if reflect.DeepEqual(before, after) {
		return
	}
	for key, value := range before {
		if got, ok := after[key]; !ok {
			t.Errorf("%s disappeared", key)
		} else if got != value {
			t.Errorf("%s changed", key)
		}
	}
	for key := range after {
		if _, ok := before[key]; !ok {
			t.Errorf("%s appeared", key)
		}
	}
	t.Fatal("the store changed")
}

func onlyGroup(t *testing.T, report *CaseTwinMergeReport) CaseTwinGroup {
	t.Helper()
	if report == nil || len(report.Results) != 1 {
		t.Fatalf("report = %+v, want exactly one group", report)
	}
	return report.Results[0]
}

// A dry run reports the full plan and writes NOTHING: no row, no derived row,
// no health record, no journal.
func TestCaseTwinDryRunWritesNothing(t *testing.T) {
	lower := strings.Repeat("1f", 20)
	upper := strings.ToUpper(lower)
	store, dir := openLegacyStore(t, liveUpperTwin(upper), staleLowerTwin(lower))
	before := storeImage(t, store, dir)
	beforeSize := store.DiskSize()

	report, err := store.MergeCaseTwins(true)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	group := onlyGroup(t, report)
	if !report.DryRun || group.Action != CaseTwinActionWouldMerge || group.Survivor != upper {
		t.Fatalf("dry run = action %q survivor %q, want would_merge keeping %s", group.Action, group.Survivor, upper)
	}
	if len(group.AddedPlacements) != 1 || group.AddedPlacements[0].ID != "rd-7" {
		t.Fatalf("planned placements = %+v, want the rd copy unioned", group.AddedPlacements)
	}
	if len(group.AddedFiles) != 1 || group.AddedFiles[0].Name != "extra.mkv" || len(group.AddedTags) != 1 || group.AddedTags[0] != "b" {
		t.Fatalf("planned files %+v tags %+v, want extra.mkv and b", group.AddedFiles, group.AddedTags)
	}
	if report.RowsRemoved != 1 || report.JournalPath != "" || report.JournalRows != 0 {
		t.Fatalf("dry run report = removed %d journal %q/%d; want 1 and no journal", report.RowsRemoved, report.JournalPath, report.JournalRows)
	}

	assertImageUnchanged(t, before, storeImage(t, store, dir))
	if after := store.DiskSize(); after != beforeSize {
		t.Fatalf("disk size %d -> %d across a dry run", beforeSize, after)
	}
}

// The merge unions placements, files and tags onto the survivor, keeps the
// survivor's folder (the one the library symlinks into), and cleans up the
// removed twin's derived folder row and its health record.
func TestCaseTwinMergeUnionsAndKeepsSurvivorFolder(t *testing.T) {
	lower := strings.Repeat("2e", 20)
	upper := strings.ToUpper(lower)
	survivor, stale := liveUpperTwin(upper), staleLowerTwin(lower)
	store, _ := openLegacyStore(t, survivor, stale)
	survivorFolder, staleFolder := survivor.GetFolder(), stale.GetFolder()
	if _, err := store.GetEntryHealth(staleFolder); err != nil {
		t.Fatalf("precondition: the stale twin's folder should carry health state: %v", err)
	}
	if _, err := store.GetEntryItem(staleFolder); err != nil {
		t.Fatalf("precondition: the stale twin's folder row should exist: %v", err)
	}

	report, err := store.MergeCaseTwins(false)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	group := onlyGroup(t, report)
	if group.Action != CaseTwinActionMerged || group.Survivor != upper {
		t.Fatalf("merge = action %q survivor %q (conflicts %v), want merged keeping %s", group.Action, group.Survivor, group.Conflicts, upper)
	}

	keys := mainKeys(t, store)
	if len(keys) != 1 || keys[0] != lower {
		t.Fatalf("main keys = %v, want exactly [%s]", keys, lower)
	}
	merged, err := store.Get(upper)
	if err != nil {
		t.Fatalf("Get(upper) after merge: %v", err)
	}
	if merged.InfoHash != lower || merged.Name != "Upper Name" || merged.ActiveProvider != "ad" || merged.GetFolder() != survivorFolder {
		t.Fatalf("merged row = hash %q name %q active %q folder %q; want %s, the survivor's name, ad, %q",
			merged.InfoHash, merged.Name, merged.ActiveProvider, merged.GetFolder(), lower, survivorFolder)
	}
	if merged.Providers["ad"] == nil || merged.Providers["ad"].ID != "ad-1" || merged.Providers["rd"] == nil || merged.Providers["rd"].ID != "rd-7" {
		t.Fatalf("merged placements = %+v, want ad-1 and rd-7", merged.Providers)
	}
	for _, name := range []string{"movie.mkv", "extra.mkv"} {
		if file := merged.Files[name]; file == nil || file.InfoHash != lower {
			t.Fatalf("merged file %s = %+v, want it present and owned by %s", name, file, lower)
		}
	}
	if !reflect.DeepEqual(merged.Tags, []string{"a", "b"}) {
		t.Fatalf("merged tags = %v, want [a b]", merged.Tags)
	}

	item, err := store.GetEntryItem(survivorFolder)
	if err != nil {
		t.Fatalf("survivor folder row: %v", err)
	}
	for _, name := range []string{"movie.mkv", "extra.mkv"} {
		if file := item.Files[name]; file == nil || file.InfoHash != lower {
			t.Fatalf("survivor folder file %s = %+v, want it projected from %s", name, file, lower)
		}
	}
	if _, err := store.GetEntryItem(staleFolder); err == nil {
		t.Fatalf("the removed twin's folder row %q survived the merge", staleFolder)
	}
	if _, err := store.GetEntryHealth(staleFolder); err == nil {
		t.Fatalf("the removed twin's health record %q survived the merge", staleFolder)
	}
	if store.legacyMainKeyCount() != 0 {
		t.Fatalf("legacy index still holds %d key(s) after the merge", store.legacyMainKeyCount())
	}
}

// Every conflict class is reported and the store is left byte-for-byte as it
// was — no row, no derived row, no journal.
func TestCaseTwinConflictsAreReportedAndLeftUntouched(t *testing.T) {
	cases := map[string]func(lower, upper string) []*Entry{
		"both live, different folders": func(lower, upper string) []*Entry {
			return []*Entry{liveUpperTwin(upper), caseTestEntry(lower, "Lower Name", "rd", "rd-1")}
		},
		"neither live, different folders": func(lower, upper string) []*Entry {
			first := liveUpperTwin(upper)
			first.ActiveProvider = ""
			return []*Entry{first, staleLowerTwin(lower)}
		},
		"same placement key, different copy": func(lower, upper string) []*Entry {
			other := staleLowerTwin(lower)
			other.Providers["ad"].ID = "ad-OTHER"
			return []*Entry{liveUpperTwin(upper), other}
		},
		"different protocols": func(lower, upper string) []*Entry {
			other := staleLowerTwin(lower)
			other.Name = "Upper Name"
			other.OriginalFilename = "Upper Name"
			other.Protocol = "nzb"
			return []*Entry{liveUpperTwin(upper), other}
		},
		"lowercasing would move the folder": func(_, upper string) []*Entry {
			// An unusable name makes the folder the infohash itself.
			nameless := liveUpperTwin(upper)
			nameless.Name, nameless.OriginalFilename = "", ""
			return []*Entry{nameless}
		},
	}
	for name, rows := range cases {
		t.Run(name, func(t *testing.T) {
			lower := strings.Repeat("3d", 20)
			upper := strings.ToUpper(lower)
			store, dir := openLegacyStore(t, rows(lower, upper)...)
			before := storeImage(t, store, dir)

			report, err := store.MergeCaseTwins(false)
			if err != nil {
				t.Fatalf("merge: %v", err)
			}
			group := onlyGroup(t, report)
			if group.Action != CaseTwinActionConflict || len(group.Conflicts) == 0 || report.Conflicts != 1 {
				t.Fatalf("group = action %q conflicts %v, want a reported conflict", group.Action, group.Conflicts)
			}
			if report.RowsRemoved != 0 || report.JournalPath != "" {
				t.Fatalf("a conflict-only run removed %d rows / journaled to %q", report.RowsRemoved, report.JournalPath)
			}
			assertImageUnchanged(t, before, storeImage(t, store, dir))
		})
	}
}

// Two live rows sharing one folder are not a conflict: the lowercase key wins
// the tie and keeps the shared folder.
func TestCaseTwinBothLiveSameFolderMergesIntoLowercase(t *testing.T) {
	lower := strings.Repeat("4c", 20)
	upper := strings.ToUpper(lower)
	other := caseTestEntry(lower, "Upper Name", "rd", "rd-1")
	store, _ := openLegacyStore(t, liveUpperTwin(upper), other)

	report, err := store.MergeCaseTwins(false)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	group := onlyGroup(t, report)
	if group.Action != CaseTwinActionMerged || group.Survivor != lower {
		t.Fatalf("group = action %q survivor %q conflicts %v, want merged keeping %s", group.Action, group.Survivor, group.Conflicts, lower)
	}
	merged, err := store.Get(lower)
	if err != nil || merged.ActiveProvider != "rd" || merged.Providers["ad"] == nil {
		t.Fatalf("merged = %+v, %v; want rd active with the ad copy unioned", merged, err)
	}
}

// A legacy row with no twin is moved to the lowercase key with its folder.
func TestCaseTwinMergeRekeysALegacySingleton(t *testing.T) {
	lower := strings.Repeat("5b", 20)
	upper := strings.ToUpper(lower)
	legacy := liveUpperTwin(upper)
	store, _ := openLegacyStore(t, legacy)

	report, err := store.MergeCaseTwins(false)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	group := onlyGroup(t, report)
	if group.Kind != CaseTwinKindRekey || group.Action != CaseTwinActionRekeyed || report.Rekeyed != 1 {
		t.Fatalf("group = kind %q action %q, want a rekey", group.Kind, group.Action)
	}
	keys := mainKeys(t, store)
	if len(keys) != 1 || keys[0] != lower {
		t.Fatalf("main keys = %v, want [%s]", keys, lower)
	}
	if entry, err := store.Get(upper); err != nil || entry.InfoHash != lower || entry.GetFolder() != legacy.GetFolder() {
		t.Fatalf("rekeyed row = %+v, %v; want %s in the same folder", entry, err, lower)
	}
}

// Running the merge again changes nothing and writes no new journal.
func TestCaseTwinMergeSecondRunIsANoOp(t *testing.T) {
	lower := strings.Repeat("6a", 20)
	upper := strings.ToUpper(lower)
	store, dir := openLegacyStore(t, liveUpperTwin(upper), staleLowerTwin(lower))
	if report, err := store.MergeCaseTwins(false); err != nil || report.Merged != 1 {
		t.Fatalf("first merge = %+v, %v", report, err)
	}
	before := storeImage(t, store, dir)

	report, err := store.MergeCaseTwins(false)
	if err != nil {
		t.Fatalf("second merge: %v", err)
	}
	if report.Groups != 0 || report.Merged != 0 || report.RowsRemoved != 0 || report.JournalPath != "" {
		t.Fatalf("second merge = %+v, want a no-op", report)
	}
	assertImageUnchanged(t, before, storeImage(t, store, dir))
}

// The journal puts a merged group back exactly: same keys, same stored bytes,
// same index attributes, and both rows reachable again.
func TestCaseTwinMergeJournalRestoresAGroupByteForByte(t *testing.T) {
	lower := strings.Repeat("7f", 20)
	upper := strings.ToUpper(lower)
	otherLower := strings.Repeat("8e", 20)
	store, _ := openLegacyStore(t,
		liveUpperTwin(upper), staleLowerTwin(lower),
		liveUpperTwin(strings.ToUpper(otherLower)), // a second group that must stay merged
	)
	original := make(map[string][]byte)
	originalAttrs := make(map[string]map[string]string)
	for _, key := range []string{upper, lower} {
		value, err := store.entries.Get(key)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		meta, err := store.entries.GetMetadata(key)
		if err != nil {
			t.Fatalf("read %s attrs: %v", key, err)
		}
		original[key] = append([]byte(nil), value...)
		originalAttrs[key] = meta.Attributes
	}

	report, err := store.MergeCaseTwins(false)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if report.JournalPath == "" || report.JournalRows != 3 {
		t.Fatalf("journal = %q with %d rows, want a path and 3 rows (2 twins + 1 singleton)", report.JournalPath, report.JournalRows)
	}

	restore, err := store.RestoreCaseTwinMergeJournal(report.JournalName, CaseTwinRestoreOptions{InfoHash: upper})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restore.RowsRestored != 2 || restore.Restored != 1 || restore.Results[0].Action != CaseTwinRestoreRestored || !restore.Results[0].Fenced {
		t.Fatalf("restore = %+v, want one fenced group with 2 rows restored", restore)
	}
	for key, want := range original {
		got, err := store.entries.Get(key)
		if err != nil {
			t.Fatalf("restored row %s missing: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("restored row %s differs from its pre-merge bytes", key)
		}
		meta, err := store.entries.GetMetadata(key)
		if err != nil || !reflect.DeepEqual(meta.Attributes, originalAttrs[key]) {
			t.Fatalf("restored row %s attributes = %v, want %v", key, meta.Attributes, originalAttrs[key])
		}
	}
	if entry, err := store.Get(upper); err != nil || entry.Name != "Upper Name" {
		t.Fatalf("Get(upper) after restore = %+v, %v; want the original uppercase twin", entry, err)
	}
	if entry, err := store.Get(lower); err != nil || entry.Name != "Lower Name" {
		t.Fatalf("Get(lower) after restore = %+v, %v; want the original lowercase twin", entry, err)
	}
	if store.entries.Exists(strings.ToUpper(otherLower)) || !store.entries.Exists(otherLower) {
		t.Fatal("restoring one group touched another group")
	}
	if _, err := store.GetEntryItem(staleLowerTwin(lower).GetFolder()); err != nil {
		t.Fatalf("the restored twin's folder row was not rebuilt: %v", err)
	}
}

// A refresh-style writer hammering the hash in BOTH cases while the merge runs
// loses nothing: every write that succeeded is in the final, single row.
func TestCaseTwinMergeIsAtomicAgainstAConcurrentWriter(t *testing.T) {
	for round := range 5 {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			lower := strings.Repeat("9d", 20)
			upper := strings.ToUpper(lower)
			store, _ := openLegacyStore(t, liveUpperTwin(upper), staleLowerTwin(lower))

			var (
				wg      sync.WaitGroup
				stop    atomic.Bool
				mu      sync.Mutex
				written []string
				failure error
			)
			wg.Go(func() {
				for i := 0; !stop.Load() || i < 20; i++ {
					key := lower
					if i%2 == 0 {
						key = upper
					}
					tag := fmt.Sprintf("w%d", i)
					_, present, err := store.MutateEntryIfPresent(key, func(entry *Entry) (bool, error) {
						entry.Tags = append(entry.Tags, tag)
						return true, nil
					})
					mu.Lock()
					if err != nil && failure == nil {
						failure = err
					}
					if present && err == nil {
						written = append(written, tag)
					}
					mu.Unlock()
				}
			})
			report, err := store.MergeCaseTwins(false)
			stop.Store(true)
			wg.Wait()
			if err != nil {
				t.Fatalf("merge: %v", err)
			}
			if failure != nil {
				t.Fatalf("a concurrent writer failed: %v", failure)
			}
			if group := onlyGroup(t, report); group.Action != CaseTwinActionMerged {
				t.Fatalf("group = %+v, want merged", group)
			}
			keys := mainKeys(t, store)
			if len(keys) != 1 || keys[0] != lower {
				t.Fatalf("main keys = %v, want [%s]", keys, lower)
			}
			final, err := store.Get(lower)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			have := make(map[string]bool, len(final.Tags))
			for _, tag := range final.Tags {
				have[tag] = true
			}
			for _, tag := range written {
				if !have[tag] {
					t.Fatalf("write %s succeeded but is missing from the merged row (tags %v)", tag, final.Tags)
				}
			}
		})
	}
}

// Only one merge or restore runs at a time.
func TestCaseTwinMergeRefusesAConcurrentRun(t *testing.T) {
	store, _ := openLegacyStore(t)
	store.caseTwinMergeMu.Lock()
	defer store.caseTwinMergeMu.Unlock()
	if _, err := store.MergeCaseTwins(true); err != ErrCaseTwinMergeRunning {
		t.Fatalf("a second merge returned %v, want ErrCaseTwinMergeRunning", err)
	}
}

// danglingActive names "rd" as its active provider but holds no rd placement,
// so it cannot serve a single read.
func danglingActive(hash, name string) *Entry {
	entry := caseTestEntry(hash, name, "tb", "tb-3")
	entry.ActiveProvider = "rd"
	return entry
}

// 🛑 LIVE MEANS SERVABLE. A lowercase row whose ActiveProvider dangles must not
// beat a really-live uppercase row on the lowercase tie-break: the merged entry
// would keep the dangling name and become unservable, although one input row
// could serve it.
func TestCaseTwinDanglingActiveRowNeverWinsSurvivor(t *testing.T) {
	lower := strings.Repeat("ac", 20)
	upper := strings.ToUpper(lower)
	store, _ := openLegacyStore(t, liveUpperTwin(upper), danglingActive(lower, "Upper Name"))

	report, err := store.MergeCaseTwins(false)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	group := onlyGroup(t, report)
	if group.Action != CaseTwinActionMerged || group.Survivor != upper {
		t.Fatalf("group = action %q survivor %q conflicts %v; want merged keeping the servable %s", group.Action, group.Survivor, group.Conflicts, upper)
	}
	for _, row := range group.Rows {
		if want := row.Key == lower; row.DanglingActive != want || row.Live == want {
			t.Fatalf("row %s reported live=%v dangling_active=%v", row.Key, row.Live, row.DanglingActive)
		}
	}
	merged, err := store.Get(lower)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if merged.ActiveProvider != "ad" || merged.GetActiveProvider() == nil || merged.GetActiveProvider().ID != "ad-1" {
		t.Fatalf("merged active = %q (placement %+v); want the servable ad-1", merged.ActiveProvider, merged.GetActiveProvider())
	}
	if merged.Providers["tb"] == nil {
		t.Fatalf("the dangling row's real placement was not unioned: %+v", merged.Providers)
	}
}

// A dangling row in a DIFFERENT folder claimed to be live, so its folder may be
// the one the library reads: conflict, untouched.
func TestCaseTwinDanglingActiveRowInAnotherFolderIsAConflict(t *testing.T) {
	lower := strings.Repeat("bd", 20)
	upper := strings.ToUpper(lower)
	store, dir := openLegacyStore(t, liveUpperTwin(upper), danglingActive(lower, "Lower Name"))
	before := storeImage(t, store, dir)

	report, err := store.MergeCaseTwins(false)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	group := onlyGroup(t, report)
	if group.Action != CaseTwinActionConflict || len(group.Conflicts) != 1 || !strings.Contains(group.Conflicts[0], "holds no placement") {
		t.Fatalf("group = action %q conflicts %v; want one dangling-active conflict", group.Action, group.Conflicts)
	}
	assertImageUnchanged(t, before, storeImage(t, store, dir))
}

// mergedTwinStore merges one twin pair and returns the store, its directory,
// the journal name, and each row's exact pre-merge bytes.
func mergedTwinStore(t *testing.T, lower string) (*Storage, string, string, map[string][]byte) {
	t.Helper()
	upper := strings.ToUpper(lower)
	store, dir := openLegacyStore(t, liveUpperTwin(upper), staleLowerTwin(lower))
	original := make(map[string][]byte)
	for _, key := range []string{upper, lower} {
		value, err := store.entries.Get(key)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		original[key] = append([]byte(nil), value...)
	}
	report, err := store.MergeCaseTwins(false)
	if err != nil || report.Merged != 1 || report.JournalName == "" {
		t.Fatalf("merge = %+v, %v; want one merged group and a journal", report, err)
	}
	return store, dir, report.JournalName, original
}

func assertRowsAreOriginal(t *testing.T, store *Storage, original map[string][]byte) {
	t.Helper()
	for key, want := range original {
		got, err := store.entries.Get(key)
		if err != nil {
			t.Fatalf("row %s missing after restore: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("row %s differs from its pre-merge bytes", key)
		}
	}
}

// 🛑 THE FENCE. A merged row that gained a placement after the merge (a
// switch, a new grab) must not be silently rolled back: that would drop the
// record and orphan a live provider copy. Only force overrides.
func TestCaseTwinRestoreRefusesARowChangedSinceMergeUnlessForced(t *testing.T) {
	lower := strings.Repeat("c1", 20)
	store, dir, journal, original := mergedTwinStore(t, lower)
	if _, _, err := store.MutateEntryIfPresent(lower, func(entry *Entry) (bool, error) {
		entry.Providers["tb"] = &ProviderEntry{Provider: "tb", ID: "tb-new", Status: debridTypes.TorrentStatusDownloaded}
		return true, nil
	}); err != nil {
		t.Fatalf("post-merge write: %v", err)
	}
	before := storeImage(t, store, dir)

	report, err := store.RestoreCaseTwinMergeJournal(journal, CaseTwinRestoreOptions{})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if report.Refused != 1 || report.RowsRestored != 0 || report.Results[0].Action != CaseTwinRestoreRefused ||
		!strings.Contains(report.Results[0].Reason, "changed since merge") {
		t.Fatalf("restore over a changed row = %+v; want refused: changed since merge", report.Results)
	}
	assertImageUnchanged(t, before, storeImage(t, store, dir))

	forced, err := store.RestoreCaseTwinMergeJournal(journal, CaseTwinRestoreOptions{Force: true})
	if err != nil {
		t.Fatalf("forced restore: %v", err)
	}
	if forced.Restored != 1 || !forced.Results[0].Forced || forced.Results[0].Action != CaseTwinRestoreRestored {
		t.Fatalf("forced restore = %+v; want restored and marked forced", forced.Results)
	}
	assertRowsAreOriginal(t, store, original)
}

// A merged row deleted since the merge is also "changed since merge".
func TestCaseTwinRestoreRefusesADeletedMergedRow(t *testing.T) {
	lower := strings.Repeat("c2", 20)
	store, _, journal, _ := mergedTwinStore(t, lower)
	if err := store.Delete(lower); err != nil {
		t.Fatalf("delete merged row: %v", err)
	}
	report, err := store.RestoreCaseTwinMergeJournal(journal, CaseTwinRestoreOptions{})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if report.Refused != 1 || !strings.Contains(report.Results[0].Reason, "deleted") {
		t.Fatalf("restore after delete = %+v; want refused", report.Results)
	}
	if len(mainKeys(t, store)) != 0 {
		t.Fatalf("a refused restore wrote rows: %v", mainKeys(t, store))
	}
}

// A dry restore reports and writes nothing, whether it would restore or refuse.
func TestCaseTwinRestoreDryRunWritesNothing(t *testing.T) {
	lower := strings.Repeat("c3", 20)
	store, dir, journal, _ := mergedTwinStore(t, lower)
	before := storeImage(t, store, dir)

	report, err := store.RestoreCaseTwinMergeJournal(journal, CaseTwinRestoreOptions{DryRun: true})
	if err != nil {
		t.Fatalf("dry restore: %v", err)
	}
	if !report.DryRun || report.Results[0].Action != CaseTwinRestoreWouldRestore || report.RowsRestored != 0 {
		t.Fatalf("dry restore = %+v; want would_restore and nothing written", report)
	}
	assertImageUnchanged(t, before, storeImage(t, store, dir))

	if _, _, err := store.MutateEntryIfPresent(lower, func(entry *Entry) (bool, error) {
		entry.Category = "changed"
		return true, nil
	}); err != nil {
		t.Fatalf("post-merge write: %v", err)
	}
	before = storeImage(t, store, dir)
	report, err = store.RestoreCaseTwinMergeJournal(journal, CaseTwinRestoreOptions{DryRun: true})
	if err != nil {
		t.Fatalf("dry restore: %v", err)
	}
	if report.Results[0].Action != CaseTwinRestoreWouldRefuse || !strings.Contains(report.Results[0].Reason, "changed since merge") {
		t.Fatalf("dry restore over a changed row = %+v; want would_refuse", report.Results)
	}
	assertImageUnchanged(t, before, storeImage(t, store, dir))
}

// A journal written before merges recorded their version cannot prove the row
// is unchanged: refused, with that reason, unless forced.
func TestCaseTwinRestoreRefusesAnUnfencedJournalUnlessForced(t *testing.T) {
	lower := strings.Repeat("c4", 20)
	store, dir, journal, original := mergedTwinStore(t, lower)
	data, err := os.ReadFile(filepath.Join(dir, journal))
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	var legacy bytes.Buffer
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode journal line: %v", err)
		}
		delete(record, "merged_generation")
		delete(record, "merged_revision")
		out, _ := json.Marshal(record)
		legacy.Write(append(out, '\n'))
	}
	const legacyName = "case-twin-merge-legacy.jsonl"
	if err := os.WriteFile(filepath.Join(dir, legacyName), legacy.Bytes(), 0o600); err != nil {
		t.Fatalf("write legacy journal: %v", err)
	}

	report, err := store.RestoreCaseTwinMergeJournal(legacyName, CaseTwinRestoreOptions{})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if report.Refused != 1 || report.Results[0].Fenced || !strings.Contains(report.Results[0].Reason, "unfenced journal") {
		t.Fatalf("unfenced restore = %+v; want refused as unfenced", report.Results)
	}
	forced, err := store.RestoreCaseTwinMergeJournal(legacyName, CaseTwinRestoreOptions{Force: true})
	if err != nil || forced.Restored != 1 {
		t.Fatalf("forced unfenced restore = %+v, %v", forced, err)
	}
	assertRowsAreOriginal(t, store, original)
}

// The journal argument is a bare file name in the database directory and
// nothing else.
func TestCaseTwinRestoreRejectsJournalNamesOutsideTheDatabase(t *testing.T) {
	lower := strings.Repeat("c5", 20)
	store, dir, _, _ := mergedTwinStore(t, lower)

	outside := filepath.Join(t.TempDir(), "case-twin-merge-outside.jsonl")
	if err := os.WriteFile(outside, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "case-twin-merge-link.jsonl")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	for _, name := range []string{
		"",
		"..",
		"../case-twin-merge-x.jsonl",
		"sub/case-twin-merge-x.jsonl",
		`..\case-twin-merge-x.jsonl`,
		outside,
		"/etc/passwd",
		"case-twin-merge-..jsonl",
		"entries.db",
		"case-twin-merge-x.json",
		"case-twin-merge-link.jsonl",
	} {
		if _, err := store.RestoreCaseTwinMergeJournal(name, CaseTwinRestoreOptions{Force: true}); !errors.Is(err, ErrInvalidCaseTwinJournal) {
			t.Errorf("journal name %q returned %v, want ErrInvalidCaseTwinJournal", name, err)
		}
	}
	if _, err := store.RestoreCaseTwinMergeJournal("case-twin-merge-missing.jsonl", CaseTwinRestoreOptions{}); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a well-formed missing journal returned %v, want not-exist", err)
	}
	if keys := mainKeys(t, store); len(keys) != 1 || keys[0] != lower {
		t.Fatalf("a refused journal name changed the store: %v", keys)
	}
}

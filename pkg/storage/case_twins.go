package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/sirrobot01/appendstore"
	"github.com/sirrobot01/decypharr/internal/config"
	"google.golang.org/protobuf/proto"
)

// The one-time case-twin merge.
//
// Rows written before main-store keys were case-canonical (see case_keys.go)
// can still exist under a non-lowercase key: as a TWIN of a lowercase row for
// the same hash, or ALONE. MergeCaseTwins folds every such group into one row
// under the lowercase key.
//
// 🛑 IT IS AN OPERATOR ACTION. Nothing runs it at startup or on a schedule; it
// runs only when called, and it defaults to a dry run at the API.
//
// 🛑 IT NEVER RELEASES A PROVIDER COPY. It moves records only. Nothing in this
// file can reach a provider. Placements are unioned onto the survivor BEFORE a
// twin's row is deleted, so at no instant does a provider copy have fewer
// referencing rows than before. A concurrent reference-checked release on
// another hash can therefore never see the copy as unreferenced.
//
// SURVIVOR RULE. The survivor keeps its Name and OriginalFilename, so its
// folder (the one the library symlinks point into) is unchanged. Candidates are
// the LIVE rows — servable ones, whose ActiveProvider names a placement the row
// actually holds — or every row if none is live. Among candidates the
// lowercase key wins, then the lexicographically smallest key. A row whose
// ActiveProvider dangles is reported (dangling_active) and is never live.
// Placements, files and tags of the other rows are unioned onto the survivor.
// On a filename both rows hold, the survivor's record is kept and the
// difference is reported.
//
// CONFLICTS are reported and left completely untouched, never guessed:
//   - the rows have different protocols;
//   - two or more rows are live with different folders;
//   - a row's ActiveProvider dangles and its folder differs from the
//     survivor's: it claimed to be live, so its folder may be the library's;
//   - no row is live and their folders differ, since nothing says which one
//     the library uses;
//   - two rows hold the same placement key for a different provider/id, since
//     unioning would drop a record and orphan a provider copy;
//   - lowercasing the hash would move the survivor's folder (hash folder
//     naming, or the unusable-name fallback to the infohash);
//   - a row does not decode, or stores a hash that does not match its key.
//
// ATOMICITY. Each group runs under its lowercase mutation stripe, the same
// lock every hash-keyed write takes, from read through decide to write. A
// concurrent refresh therefore either completes first and is merged, or runs
// after and sees the merged row. The merged row keeps the survivor's
// generation with an advanced revision: exact-revision deletes taken earlier
// are fenced, a snapshot of the survivor still mutates the merged row, and a
// snapshot of a removed twin is stale.
//
// REVERSIBILITY. Before a group's first write, the exact stored bytes and index
// attributes of every row it will overwrite or delete are appended to a
// JSON-lines journal beside the database and synced, together with the exact
// version the merge then writes to the canonical row. RestoreCaseTwinMergeJournal
// puts them back byte for byte, but only while that row still carries that
// version (the fence), unless forced.
//
// CRASH SAFETY. The merged row is written before any twin is deleted. A crash
// in between leaves a group the next run converges: the merged row is live and
// already holds every placement, so the leftover twin unions to nothing and is
// removed.

// ErrCaseTwinMergeRunning refuses a second concurrent merge or restore.
var ErrCaseTwinMergeRunning = errors.New("a case-twin merge or restore is already running")

// Case-twin group kinds and actions.
const (
	CaseTwinKindMerge = "merge" // several rows for one hash
	CaseTwinKindRekey = "rekey" // one legacy row, moved to the lowercase key

	CaseTwinActionWouldMerge = "would_merge"
	CaseTwinActionMerged     = "merged"
	CaseTwinActionWouldRekey = "would_rekey"
	CaseTwinActionRekeyed    = "rekeyed"
	CaseTwinActionConflict   = "conflict"
	CaseTwinActionSkipped    = "skipped"
	CaseTwinActionError      = "error"
)

// CaseTwinMergeReport is the outcome of one merge run, dry or real.
type CaseTwinMergeReport struct {
	DryRun     bool      `json:"dry_run"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	// Groups is every hash with a non-lowercase key that the run examined.
	Groups int `json:"groups"`
	// Merged / Rekeyed count groups merged or rekeyed, or that WOULD be in a
	// dry run.
	Merged    int `json:"merged"`
	Rekeyed   int `json:"rekeyed"`
	Conflicts int `json:"conflicts"`
	Skipped   int `json:"skipped"`
	Errors    int `json:"errors"`
	// RowsRemoved is the number of non-lowercase keys deleted, or that would be.
	RowsRemoved int `json:"rows_removed"`
	// JournalPath / JournalName / JournalRows locate the pre-image of every row
	// a real run changed. JournalName is what the restore route takes. Empty
	// when nothing was changed.
	JournalPath string          `json:"journal_path,omitempty"`
	JournalName string          `json:"journal_name,omitempty"`
	JournalRows int             `json:"journal_rows"`
	Results     []CaseTwinGroup `json:"results"`
}

// CaseTwinGroup is one hash's result.
type CaseTwinGroup struct {
	CanonicalKey      string              `json:"canonical_key"`
	Kind              string              `json:"kind"`
	Action            string              `json:"action"`
	Rows              []CaseTwinRow       `json:"rows"`
	Survivor          string              `json:"survivor,omitempty"`
	SurvivorFolder    string              `json:"survivor_folder,omitempty"`
	RemovedKeys       []string            `json:"removed_keys,omitempty"`
	RemovedFolders    []string            `json:"removed_folders,omitempty"`
	AddedPlacements   []CaseTwinPlacement `json:"added_placements,omitempty"`
	AddedFiles        []CaseTwinFile      `json:"added_files,omitempty"`
	AddedTags         []string            `json:"added_tags,omitempty"`
	KeptSurvivorFiles []CaseTwinFile      `json:"kept_survivor_files,omitempty"`
	Conflicts         []string            `json:"conflicts,omitempty"`
	Reason            string              `json:"reason,omitempty"`
	Error             string              `json:"error,omitempty"`
}

// CaseTwinRow describes one stored row of a group, as found.
type CaseTwinRow struct {
	Key            string `json:"key"`
	Name           string `json:"name"`
	Folder         string `json:"folder"`
	Protocol       string `json:"protocol"`
	ActiveProvider string `json:"active_provider"`
	Placements     int    `json:"placements"`
	Files          int    `json:"files"`
	// Live: the row is servable (it holds the placement ActiveProvider names).
	Live bool `json:"live"`
	// DanglingActive: ActiveProvider names a provider the row holds no
	// placement for. Such a row is never live.
	DanglingActive bool `json:"dangling_active,omitempty"`
	Survivor       bool `json:"survivor"`
}

// CaseTwinPlacement is a placement unioned onto the survivor.
type CaseTwinPlacement struct {
	Key      string `json:"key"`
	Provider string `json:"provider"`
	ID       string `json:"id"`
	From     string `json:"from"`
}

// CaseTwinFile is a file unioned onto, or kept by, the survivor.
type CaseTwinFile struct {
	Name   string `json:"name"`
	From   string `json:"from"`
	Detail string `json:"detail,omitempty"`
}

// caseTwinStoredRow is one row as loaded under the group's lock.
type caseTwinStoredRow struct {
	key        string
	value      []byte
	attributes map[string]string
	entry      *Entry
	folder     string
}

// MergeCaseTwins canonicalises every main-store hash that still has a
// non-lowercase key. With dryRun it writes NOTHING (no row, no derived row, no
// journal) and reports what a real run would do.
func (s *Storage) MergeCaseTwins(dryRun bool) (*CaseTwinMergeReport, error) {
	if !s.caseTwinMergeMu.TryLock() {
		return nil, ErrCaseTwinMergeRunning
	}
	defer s.caseTwinMergeMu.Unlock()

	report := &CaseTwinMergeReport{DryRun: dryRun, StartedAt: time.Now().UTC(), Results: []CaseTwinGroup{}}
	groups, err := s.caseTwinCandidates()
	if err != nil {
		return nil, err
	}
	canonicals := make([]string, 0, len(groups))
	for canonical := range groups {
		canonicals = append(canonicals, canonical)
	}
	sort.Strings(canonicals)

	var journal *caseTwinJournal
	defer func() {
		if journal != nil {
			_ = journal.close()
		}
	}()
	affected := make(map[string]config.Protocol)
	var runErr error
	for _, canonical := range canonicals {
		result, applyErr := s.mergeCaseTwinGroup(canonical, groups[canonical], dryRun, &journal, report.StartedAt, affected)
		report.tally(result)
		if applyErr != nil {
			// A journal that cannot be written means the next change would have
			// no way back: stop the whole run, not just this group.
			runErr = applyErr
			break
		}
	}
	if journal != nil {
		report.JournalPath = journal.path
		report.JournalName = filepath.Base(journal.path)
		report.JournalRows = journal.rows
	}
	if !dryRun && len(affected) > 0 {
		if err := s.rebuildEntryItemsBatch(affected); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("rebuild derived folder rows after case-twin merge: %w", err))
		}
	}
	report.FinishedAt = time.Now().UTC()
	s.logger.Info().
		Bool("dry_run", dryRun).
		Int("groups", report.Groups).
		Int("merged", report.Merged).
		Int("rekeyed", report.Rekeyed).
		Int("conflicts", report.Conflicts).
		Int("errors", report.Errors).
		Int("rows_removed", report.RowsRemoved).
		Str("journal", report.JournalPath).
		Msg("Case-twin merge finished")
	return report, runErr
}

func (r *CaseTwinMergeReport) tally(result CaseTwinGroup) {
	r.Groups++
	switch result.Action {
	case CaseTwinActionMerged, CaseTwinActionWouldMerge:
		r.Merged++
		r.RowsRemoved += len(result.RemovedKeys)
	case CaseTwinActionRekeyed, CaseTwinActionWouldRekey:
		r.Rekeyed++
		r.RowsRemoved += len(result.RemovedKeys)
	case CaseTwinActionConflict:
		r.Conflicts++
	case CaseTwinActionSkipped:
		r.Skipped++
	case CaseTwinActionError:
		r.Errors++
	}
	r.Results = append(r.Results, result)
}

// caseTwinCandidates maps each lowercase hash that has a non-lowercase key to
// the keys found for it. It reads the in-memory key index only.
func (s *Storage) caseTwinCandidates() (map[string][]string, error) {
	groups := make(map[string][]string)
	if err := s.entries.ForEachMetadata(func(key string, _ *appendstore.Metadata) error {
		if isInternalMainKey(key) {
			return nil
		}
		if canonical := canonicalMainKey(key); canonical != key {
			groups[canonical] = append(groups[canonical], key)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan main keys for case twins: %w", err)
	}
	return groups, nil
}

// mergeCaseTwinGroup re-reads, decides and (unless dryRun) applies one group,
// all under the group's mutation stripe. Only a journal failure is returned as
// an error; every other failure is recorded on the result.
func (s *Storage) mergeCaseTwinGroup(
	canonical string,
	scanned []string,
	dryRun bool,
	journal **caseTwinJournal,
	runStarted time.Time,
	affected map[string]config.Protocol,
) (CaseTwinGroup, error) {
	result := CaseTwinGroup{CanonicalKey: canonical}
	unlock := s.lockEntryMutation(canonical)
	defer unlock()

	keySet := map[string]struct{}{canonical: {}}
	for _, key := range scanned {
		keySet[key] = struct{}{}
	}
	keys := make([]string, 0, len(keySet))
	for key := range keySet {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var rows []*caseTwinStoredRow
	for _, key := range keys {
		row, present, err := s.loadCaseTwinRow(key)
		if err != nil {
			result.Action, result.Error = CaseTwinActionError, err.Error()
			return result, nil
		}
		if !present {
			continue
		}
		rows = append(rows, row)
		result.Rows = append(result.Rows, CaseTwinRow{
			Key:            row.key,
			Name:           row.entry.Name,
			Folder:         row.folder,
			Protocol:       string(row.entry.Protocol),
			ActiveProvider: row.entry.ActiveProvider,
			Placements:     len(row.entry.Providers),
			Files:          len(row.entry.Files),
			Live:           caseTwinRowIsLive(row.entry),
			DanglingActive: row.entry.ActiveProvider != "" && !caseTwinRowIsLive(row.entry),
		})
	}
	switch {
	case len(rows) == 0:
		result.Action, result.Reason = CaseTwinActionSkipped, "every row of this hash was deleted before the merge reached it"
		return result, nil
	case len(rows) == 1 && rows[0].key == canonical:
		result.Action, result.Reason = CaseTwinActionSkipped, "already canonical"
		return result, nil
	case len(rows) == 1:
		result.Kind = CaseTwinKindRekey
	default:
		result.Kind = CaseTwinKindMerge
	}

	merged := planCaseTwinGroup(canonical, rows, &result)
	for i := range result.Rows {
		result.Rows[i].Survivor = result.Rows[i].Key == result.Survivor
	}
	if merged == nil {
		result.Action = CaseTwinActionConflict
		return result, nil
	}
	if dryRun {
		result.Action = CaseTwinActionWouldMerge
		if result.Kind == CaseTwinKindRekey {
			result.Action = CaseTwinActionWouldRekey
		}
		return result, nil
	}

	if *journal == nil {
		opened, err := s.openCaseTwinJournal(runStarted)
		if err != nil {
			result.Action, result.Error = CaseTwinActionError, err.Error()
			return result, err
		}
		*journal = opened
	}
	// The version is fixed BEFORE journaling so the record can carry it: a
	// restore later proves the canonical row is still exactly what this merge
	// wrote, and refuses otherwise.
	nextGeneration, nextRevision, _ := advanceStoreVersion(
		merged.mainStoreGeneration, merged.mainStoreRevision,
		merged.mainStoreGeneration, merged.mainStoreRevision,
	)
	if err := (*journal).append(canonical, rows, nextGeneration, nextRevision); err != nil {
		result.Action, result.Error = CaseTwinActionError, err.Error()
		return result, err
	}

	for _, row := range rows {
		affected[row.folder] = row.entry.Protocol
	}
	affected[merged.GetFolder()] = merged.Protocol

	merged.mainStoreGeneration, merged.mainStoreRevision = nextGeneration, nextRevision
	merged.UpdatedAt = time.Now()
	data, err := proto.Marshal(EntryToProto(merged))
	if err != nil {
		result.Action, result.Error = CaseTwinActionError, fmt.Sprintf("marshal merged row: %v", err)
		return result, nil
	}
	if err := s.entries.Put(canonical, data, entryPutOptions(merged)); err != nil {
		result.Action, result.Error = CaseTwinActionError, fmt.Sprintf("write merged row: %v", err)
		return result, nil
	}
	for _, key := range result.RemovedKeys {
		if err := s.entries.Delete(key); err != nil {
			// The merged row already holds everything; a rerun removes the rest.
			result.Action, result.Error = CaseTwinActionError, fmt.Sprintf("delete merged twin %s: %v", key, err)
			return result, nil
		}
		s.forgetLegacyMainKey(key)
	}
	result.Action = CaseTwinActionMerged
	if result.Kind == CaseTwinKindRekey {
		result.Action = CaseTwinActionRekeyed
	}
	s.logger.Info().
		Str("canonical", canonical).
		Str("kind", result.Kind).
		Str("survivor", result.Survivor).
		Strs("removed", result.RemovedKeys).
		Int("added_placements", len(result.AddedPlacements)).
		Int("added_files", len(result.AddedFiles)).
		Msg("Merged case-twin rows")
	return result, nil
}

func (s *Storage) loadCaseTwinRow(key string) (*caseTwinStoredRow, bool, error) {
	if !s.entries.Exists(key) {
		return nil, false, nil
	}
	value, err := s.entries.Get(key)
	if err != nil {
		if !s.entries.Exists(key) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read row %s: %w", key, err)
	}
	meta, err := s.entries.GetMetadata(key)
	if err != nil {
		return nil, false, fmt.Errorf("read index attributes of row %s: %w", key, err)
	}
	var pb EntryProto
	if err := proto.Unmarshal(value, &pb); err != nil {
		return nil, false, fmt.Errorf("row %s does not decode: %w", key, err)
	}
	entry := ProtoToEntry(&pb)
	var attributes map[string]string
	if meta != nil {
		attributes = meta.Attributes
	}
	return &caseTwinStoredRow{
		key:        key,
		value:      append([]byte(nil), value...),
		attributes: attributes,
		entry:      entry,
		folder:     entry.GetFolder(),
	}, true, nil
}

// planCaseTwinGroup applies the survivor and conflict rules. It returns the
// merged row, or nil with result.Conflicts filled. It only mutates the freshly
// decoded rows it was given.
func planCaseTwinGroup(canonical string, rows []*caseTwinStoredRow, result *CaseTwinGroup) *Entry {
	conflict := func(format string, args ...any) {
		result.Conflicts = append(result.Conflicts, fmt.Sprintf(format, args...))
	}

	protocols := make(map[config.Protocol]struct{})
	for _, row := range rows {
		protocols[row.entry.Protocol] = struct{}{}
		if row.entry.InfoHash != row.key {
			conflict("row %s stores infohash %q, not its own key", row.key, row.entry.InfoHash)
		}
	}
	if len(protocols) > 1 {
		conflict("rows have different protocols")
	}

	var live, dangling []*caseTwinStoredRow
	for _, row := range rows {
		switch {
		case caseTwinRowIsLive(row.entry):
			live = append(live, row)
		case row.entry.ActiveProvider != "":
			dangling = append(dangling, row)
		}
	}
	if folders := distinctCaseTwinFolders(live); len(folders) > 1 {
		conflict("%d rows are live with different folders %q; the library may read any of them", len(live), folders)
	}
	if len(live) == 0 && len(rows) > 1 {
		if folders := distinctCaseTwinFolders(rows); len(folders) > 1 {
			conflict("no row is live and their folders differ %q; nothing says which one the library uses", folders)
		}
	}

	candidates := live
	if len(candidates) == 0 {
		candidates = rows
	}
	candidates = append([]*caseTwinStoredRow(nil), candidates...)
	sort.Slice(candidates, func(i, j int) bool {
		if (candidates[i].key == canonical) != (candidates[j].key == canonical) {
			return candidates[i].key == canonical
		}
		return candidates[i].key < candidates[j].key
	})
	survivorRow := candidates[0]
	survivor := survivorRow.entry
	result.Survivor = survivorRow.key
	result.SurvivorFolder = survivorRow.folder

	// A dangling row CLAIMED to be live, so its folder may be the one the
	// library reads. Merging it away into a different folder would be a guess.
	for _, row := range dangling {
		if row != survivorRow && row.folder != survivorRow.folder {
			conflict("row %s names active provider %q but holds no placement for it, and its folder %q differs from the survivor's %q; it may be what the library reads",
				row.key, row.entry.ActiveProvider, row.folder, survivorRow.folder)
		}
	}

	canonicalizeEntryIdentity(survivor, canonical)
	if folder := survivor.GetFolder(); folder != survivorRow.folder {
		conflict("lowercasing the hash would move the survivor's folder from %q to %q", survivorRow.folder, folder)
	}
	if survivor.Providers == nil {
		survivor.Providers = make(map[string]*ProviderEntry)
	}
	if survivor.Files == nil {
		survivor.Files = make(map[string]*File)
	}

	tags := make(map[string]struct{}, len(survivor.Tags))
	for _, tag := range survivor.Tags {
		tags[tag] = struct{}{}
	}
	removedFolders := make(map[string]struct{})
	for _, row := range rows {
		if row.key != canonical {
			result.RemovedKeys = append(result.RemovedKeys, row.key)
		}
		if row == survivorRow {
			continue
		}
		if row.folder != survivorRow.folder {
			removedFolders[row.folder] = struct{}{}
		}
		other := row.entry

		for _, key := range sortedKeys(other.Providers) {
			placement := other.Providers[key]
			if placement == nil {
				continue
			}
			existing := survivor.Providers[key]
			switch {
			case existing == nil:
				survivor.Providers[key] = placement
				result.AddedPlacements = append(result.AddedPlacements, CaseTwinPlacement{
					Key: key, Provider: placement.Provider, ID: placement.ID, From: row.key,
				})
			case existing.Provider != placement.Provider || existing.ID != placement.ID:
				conflict("placement %q is %s/%s on the survivor but %s/%s on %s; a union would drop one record",
					key, existing.Provider, existing.ID, placement.Provider, placement.ID, row.key)
			}
		}

		for _, name := range sortedKeys(other.Files) {
			file := other.Files[name]
			if file == nil {
				continue
			}
			existing := survivor.Files[name]
			if existing == nil {
				if strings.EqualFold(file.InfoHash, canonical) {
					file.InfoHash = canonical
				}
				survivor.Files[name] = file
				result.AddedFiles = append(result.AddedFiles, CaseTwinFile{Name: name, From: row.key})
				continue
			}
			if detail := caseTwinFileDifference(existing, file); detail != "" {
				result.KeptSurvivorFiles = append(result.KeptSurvivorFiles, CaseTwinFile{Name: name, From: row.key, Detail: detail})
			}
		}

		for _, tag := range other.Tags {
			if _, ok := tags[tag]; ok {
				continue
			}
			tags[tag] = struct{}{}
			survivor.Tags = append(survivor.Tags, tag)
			result.AddedTags = append(result.AddedTags, tag)
		}
	}
	for folder := range removedFolders {
		result.RemovedFolders = append(result.RemovedFolders, folder)
	}
	sort.Strings(result.RemovedFolders)

	if len(result.Conflicts) > 0 {
		return nil
	}
	return survivor
}

// caseTwinRowIsLive reports whether a row is SERVABLE: it names an active
// provider AND holds that placement (resolved exactly as GetActiveProvider
// does). A row whose ActiveProvider dangles is not live; letting it survive
// would carry the dangling name onto the merged row and make an entry that one
// input could serve unservable.
func caseTwinRowIsLive(entry *Entry) bool {
	return entry != nil && entry.ActiveProvider != "" && entry.GetActiveProvider() != nil
}

func distinctCaseTwinFolders(rows []*caseTwinStoredRow) []string {
	set := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		set[row.folder] = struct{}{}
	}
	return sortedKeys(set)
}

func caseTwinFileDifference(survivor, other *File) string {
	var diffs []string
	if survivor.Size != other.Size {
		diffs = append(diffs, fmt.Sprintf("size %d vs %d", survivor.Size, other.Size))
	}
	if survivor.Deleted != other.Deleted {
		diffs = append(diffs, fmt.Sprintf("deleted %t vs %t", survivor.Deleted, other.Deleted))
	}
	if !reflect.DeepEqual(survivor.ByteRange, other.ByteRange) {
		diffs = append(diffs, "byte range differs")
	}
	if survivor.Path != other.Path {
		diffs = append(diffs, fmt.Sprintf("path %q vs %q", survivor.Path, other.Path))
	}
	if !strings.EqualFold(survivor.InfoHash, other.InfoHash) {
		diffs = append(diffs, fmt.Sprintf("owner %q vs %q", survivor.InfoHash, other.InfoHash))
	}
	return strings.Join(diffs, "; ")
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// rebuildEntryItemsBatch re-projects several derived folder rows from ONE scan
// of the main store, instead of one full scan per folder. All of the folders'
// item locks are held across scan and write, so a concurrent writer to one of
// them either commits before the scan or waits and applies on top of the
// rebuilt row. Item locks are only ever taken after entry locks, never
// before, so holding several cannot deadlock against a writer.
func (s *Storage) rebuildEntryItemsBatch(folders map[string]config.Protocol) error {
	names := make([]string, 0, len(folders))
	for name := range folders {
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	unlocks := make([]func(), 0, len(names))
	for _, name := range names {
		unlocks = append(unlocks, s.lockItemMutation(name))
	}
	defer func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}()

	wanted := make(map[string]struct{}, len(names))
	for _, name := range names {
		wanted[name] = struct{}{}
	}
	projections := make(map[string]*entryItemProjection, len(names))
	if err := s.entries.ForEach(func(key string, value []byte) error {
		if isInternalMainKey(key) {
			return nil
		}
		var pb EntryProto
		if err := proto.Unmarshal(value, &pb); err != nil {
			s.logCorruptRow("entries", key, err)
			return nil
		}
		entry := ProtoToEntry(&pb)
		if _, ok := wanted[entry.GetFolder()]; ok {
			addEntryToProjection(projections, entry)
		}
		return nil
	}); err != nil {
		return err
	}

	var errs []error
	for _, name := range names {
		oldPresent := s.entryItems.Exists(name)
		oldReadable := false
		var oldItem *EntryItem
		if oldPresent {
			if data, err := s.entryItems.Get(name); err == nil {
				var pb EntryItemProto
				if proto.Unmarshal(data, &pb) == nil {
					oldItem = ProtoToEntryItem(&pb)
					oldReadable = true
				}
			}
		}
		projection := projections[name]
		if projection != nil && projection.protocol == "" {
			projection.protocol = folders[name]
		}
		if _, err := s.writeEntryItemProjectionLocked(name, projection, oldPresent, oldReadable, oldItem); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ---------------------------------------------------------------------------
// Journal
// ---------------------------------------------------------------------------

// caseTwinJournalPattern is the only shape of name a journal may have; a
// restore accepts nothing else.
const caseTwinJournalPattern = "case-twin-merge-*.jsonl"

// ErrInvalidCaseTwinJournal refuses a journal name that is not a bare
// case-twin-merge-*.jsonl file name resolving to a regular file inside the
// database directory.
var ErrInvalidCaseTwinJournal = errors.New("invalid case-twin journal name")

// caseTwinJournalRecord is one group's pre-image: one JSON line, written and
// synced before the group's first write.
//
// MergedGeneration/MergedRevision are the exact version the merge then wrote
// to the canonical row. They FENCE a restore: a canonical row that no longer
// carries that version has been written since the merge (a switch, a new
// grab), and restoring over it would drop that write. Journals from builds
// before the fence lack them and are treated as unfenced.
type caseTwinJournalRecord struct {
	Run              string               `json:"run"`
	At               time.Time            `json:"at"`
	CanonicalKey     string               `json:"canonical_key"`
	CanonicalExisted bool                 `json:"canonical_existed"`
	MergedGeneration string               `json:"merged_generation,omitempty"`
	MergedRevision   uint64               `json:"merged_revision,omitempty"`
	Rows             []caseTwinJournalRow `json:"rows"`
}

// caseTwinJournalRow is a row exactly as stored. Value is the raw record bytes
// (base64 in JSON), and Attributes are its index attributes.
type caseTwinJournalRow struct {
	Key        string            `json:"key"`
	Value      []byte            `json:"value"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type caseTwinJournal struct {
	path string
	run  string
	file *os.File
	rows int
}

const caseTwinJournalTimeLayout = "20060102T150405.000000000Z"

func (s *Storage) openCaseTwinJournal(started time.Time) (*caseTwinJournal, error) {
	run := started.UTC().Format(caseTwinJournalTimeLayout)
	path := filepath.Join(s.dir, "case-twin-merge-"+run+".jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open case-twin merge journal: %w", err)
	}
	return &caseTwinJournal{path: path, run: run, file: file}, nil
}

func (j *caseTwinJournal) append(canonical string, rows []*caseTwinStoredRow, mergedGeneration string, mergedRevision uint64) error {
	record := caseTwinJournalRecord{
		Run:              j.run,
		At:               time.Now().UTC(),
		CanonicalKey:     canonical,
		MergedGeneration: mergedGeneration,
		MergedRevision:   mergedRevision,
	}
	for _, row := range rows {
		if row.key == canonical {
			record.CanonicalExisted = true
		}
		record.Rows = append(record.Rows, caseTwinJournalRow{Key: row.key, Value: row.value, Attributes: row.attributes})
	}
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode case-twin journal record for %s: %w", canonical, err)
	}
	if _, err := j.file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write case-twin journal record for %s: %w", canonical, err)
	}
	if err := j.file.Sync(); err != nil {
		return fmt.Errorf("sync case-twin journal record for %s: %w", canonical, err)
	}
	j.rows += len(rows)
	return nil
}

func (j *caseTwinJournal) close() error {
	return j.file.Close()
}

// caseTwinJournalPath resolves a journal NAME to its path in the database
// directory. It accepts only a bare file name matching case-twin-merge-*.jsonl
// that is a regular file directly inside the directory: no separator, no
// "..", no absolute path, and no symlink that could point elsewhere.
func (s *Storage) caseTwinJournalPath(name string) (string, error) {
	invalid := func(why string) (string, error) {
		return "", fmt.Errorf("%w %q: %s", ErrInvalidCaseTwinJournal, name, why)
	}
	switch {
	case name == "":
		return invalid("a journal file name is required")
	case strings.ContainsAny(name, `/\`), strings.Contains(name, ".."), filepath.IsAbs(name), filepath.Base(name) != name:
		return invalid("must be a bare file name inside the database directory")
	}
	if matched, _ := filepath.Match(caseTwinJournalPattern, name); !matched {
		return invalid("must match " + caseTwinJournalPattern)
	}
	path := filepath.Join(s.dir, name)
	if filepath.Dir(path) != filepath.Clean(s.dir) {
		return invalid("does not resolve inside the database directory")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("case-twin journal %q: %w", name, err)
	}
	if !info.Mode().IsRegular() {
		return invalid("is not a regular file")
	}
	return path, nil
}

// CaseTwinRestoreOptions selects what a journal restore does.
type CaseTwinRestoreOptions struct {
	// InfoHash restores only that hash's group (any casing); empty means all.
	InfoHash string
	// DryRun reports what would be restored or refused and writes nothing.
	DryRun bool
	// Force restores a group even when the fence refuses it.
	Force bool
}

// Restore actions.
const (
	CaseTwinRestoreWouldRestore = "would_restore"
	CaseTwinRestoreRestored     = "restored"
	CaseTwinRestoreWouldRefuse  = "would_refuse"
	CaseTwinRestoreRefused      = "refused"
	CaseTwinRestoreError        = "error"
)

// CaseTwinRestoreReport is the outcome of one journal restore, dry or real.
type CaseTwinRestoreReport struct {
	Journal      string                 `json:"journal"`
	DryRun       bool                   `json:"dry_run"`
	Force        bool                   `json:"force"`
	Groups       int                    `json:"groups"`
	Restored     int                    `json:"restored"`
	Refused      int                    `json:"refused"`
	Errors       int                    `json:"errors"`
	RowsRestored int                    `json:"rows_restored"`
	Results      []CaseTwinRestoreGroup `json:"results"`
}

// CaseTwinRestoreGroup is one journal group's restore result.
type CaseTwinRestoreGroup struct {
	CanonicalKey string   `json:"canonical_key"`
	Action       string   `json:"action"`
	Keys         []string `json:"keys"`
	// Fenced is false for a journal written before merges recorded their version.
	Fenced bool `json:"fenced"`
	// Forced: the fence refused this group and Force overrode it.
	Forced       bool   `json:"forced,omitempty"`
	Reason       string `json:"reason,omitempty"`
	RowsRestored int    `json:"rows_restored,omitempty"`
	Error        string `json:"error,omitempty"`
}

// RestoreCaseTwinMergeJournal undoes a merge from the journal named journal
// (a bare file name in the database directory; see caseTwinJournalPath). For
// each recorded group, newest first, it removes the merged lowercase row if the
// merge created it and puts every recorded row back under its original key
// with its exact original bytes and index attributes.
//
// 🛑 FENCED. A group is refused unless the canonical row still carries exactly
// the version the merge wrote and no recorded legacy key exists again: any
// write since the merge (a provider switch, a new grab) would otherwise be
// silently dropped, orphaning a provider copy nobody tracks. Unfenced journals
// are refused too. Only opts.Force overrides a refusal.
//
// Like the merge, it never contacts a provider.
func (s *Storage) RestoreCaseTwinMergeJournal(journal string, opts CaseTwinRestoreOptions) (*CaseTwinRestoreReport, error) {
	path, err := s.caseTwinJournalPath(journal)
	if err != nil {
		return nil, err
	}
	if !s.caseTwinMergeMu.TryLock() {
		return nil, ErrCaseTwinMergeRunning
	}
	defer s.caseTwinMergeMu.Unlock()

	records, err := readCaseTwinJournal(path)
	if err != nil {
		return nil, err
	}
	report := &CaseTwinRestoreReport{Journal: journal, DryRun: opts.DryRun, Force: opts.Force, Results: []CaseTwinRestoreGroup{}}
	affected := make(map[string]config.Protocol)
	var runErr error
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if opts.InfoHash != "" && record.CanonicalKey != canonicalMainKey(opts.InfoHash) {
			continue
		}
		result := s.restoreCaseTwinRecord(record, opts, affected)
		report.Groups++
		switch result.Action {
		case CaseTwinRestoreRestored, CaseTwinRestoreWouldRestore:
			report.Restored++
		case CaseTwinRestoreRefused, CaseTwinRestoreWouldRefuse:
			report.Refused++
		case CaseTwinRestoreError:
			report.Errors++
			runErr = errors.Join(runErr, fmt.Errorf("restore %s: %s", result.CanonicalKey, result.Error))
		}
		report.RowsRestored += result.RowsRestored
		report.Results = append(report.Results, result)
	}
	if opts.InfoHash != "" && report.Groups == 0 {
		return report, fmt.Errorf("%w: journal %s holds no group for %s", ErrEntryNotFound, journal, opts.InfoHash)
	}
	if !opts.DryRun && len(affected) > 0 {
		if err := s.rebuildEntryItemsBatch(affected); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("rebuild derived folder rows after restore: %w", err))
		}
	}
	s.logger.Warn().
		Str("journal", journal).
		Str("infohash", opts.InfoHash).
		Bool("dry_run", opts.DryRun).
		Bool("force", opts.Force).
		Int("restored", report.Restored).
		Int("refused", report.Refused).
		Int("rows", report.RowsRestored).
		Msg("Case-twin journal restore finished")
	return report, runErr
}

// restoreCaseTwinRecord checks one group's fence and, unless refused or dry,
// restores it. The group's mutation stripe is held across check and write, so
// no writer can slip in between the fence and the restore.
func (s *Storage) restoreCaseTwinRecord(record caseTwinJournalRecord, opts CaseTwinRestoreOptions, affected map[string]config.Protocol) CaseTwinRestoreGroup {
	result := CaseTwinRestoreGroup{CanonicalKey: record.CanonicalKey, Fenced: record.MergedGeneration != ""}
	for _, row := range record.Rows {
		result.Keys = append(result.Keys, row.Key)
	}
	unlock := s.lockEntryMutation(record.CanonicalKey)
	defer unlock()

	current, present, loadErr := s.loadCaseTwinRow(record.CanonicalKey)
	if refusal := s.caseTwinRestoreRefusal(record, current, present, loadErr); refusal != "" {
		if !opts.Force {
			result.Action = CaseTwinRestoreRefused
			if opts.DryRun {
				result.Action = CaseTwinRestoreWouldRefuse
			}
			result.Reason = refusal
			return result
		}
		result.Forced = true
		result.Reason = "forced past: " + refusal
	}
	if opts.DryRun {
		result.Action = CaseTwinRestoreWouldRestore
		return result
	}

	if present {
		affected[current.folder] = current.entry.Protocol
	}
	if !record.CanonicalExisted && s.entries.Exists(record.CanonicalKey) {
		if err := s.entries.Delete(record.CanonicalKey); err != nil {
			result.Action, result.Error = CaseTwinRestoreError, fmt.Sprintf("remove merged row %s: %v", record.CanonicalKey, err)
			return result
		}
	}
	for _, row := range record.Rows {
		if err := s.entries.Put(row.Key, row.Value, &appendstore.PutOptions{Attributes: row.Attributes}); err != nil {
			result.Action, result.Error = CaseTwinRestoreError, fmt.Sprintf("restore row %s: %v", row.Key, err)
			return result
		}
		s.rememberLegacyMainKey(row.Key)
		result.RowsRestored++
		var pb EntryProto
		if proto.Unmarshal(row.Value, &pb) == nil {
			entry := ProtoToEntry(&pb)
			affected[entry.GetFolder()] = entry.Protocol
		}
	}
	result.Action = CaseTwinRestoreRestored
	return result
}

// caseTwinRestoreRefusal returns why the fence refuses a group, or "".
func (s *Storage) caseTwinRestoreRefusal(record caseTwinJournalRecord, current *caseTwinStoredRow, present bool, loadErr error) string {
	if record.MergedGeneration == "" {
		return "unfenced journal: it was written before merges recorded the version they wrote, so the row cannot be proven unchanged since the merge"
	}
	if loadErr != nil {
		return fmt.Sprintf("the current row %s cannot be read to check the fence: %v", record.CanonicalKey, loadErr)
	}
	if !present {
		return fmt.Sprintf("changed since merge: the merged row %s has been deleted", record.CanonicalKey)
	}
	if got, gotRev := current.entry.mainStoreGeneration, current.entry.mainStoreRevision; got != record.MergedGeneration || gotRev != record.MergedRevision {
		return fmt.Sprintf("changed since merge: the merge wrote version %s/%d, the row now carries %s/%d",
			record.MergedGeneration, record.MergedRevision, got, gotRev)
	}
	for _, row := range record.Rows {
		if row.Key != record.CanonicalKey && s.entries.Exists(row.Key) {
			return fmt.Sprintf("changed since merge: key %s exists again", row.Key)
		}
	}
	return ""
}

func readCaseTwinJournal(path string) ([]caseTwinJournalRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open case-twin journal: %w", err)
	}
	defer func() { _ = file.Close() }()
	reader := bufio.NewReader(file)
	var records []caseTwinJournalRecord
	for {
		line, readErr := reader.ReadBytes('\n')
		if trimmed := strings.TrimSpace(string(line)); trimmed != "" {
			var record caseTwinJournalRecord
			if err := json.Unmarshal([]byte(trimmed), &record); err != nil {
				return nil, fmt.Errorf("decode case-twin journal %s record %d: %w", path, len(records)+1, err)
			}
			records = append(records, record)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read case-twin journal: %w", readErr)
		}
	}
	return records, nil
}

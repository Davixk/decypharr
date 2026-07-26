package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"time"

	json "github.com/bytedance/sonic"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage/hybrid"
)

// RepairStrategy controls how the probe groups files for a single entry.
type RepairStrategy string

const (
	RepairStrategyPerEntry RepairStrategy = "per_entry"
	RepairStrategyPerFile  RepairStrategy = "per_file"
)

type RepairRunStatus string
type RepairRunStage string
type RepairRunTrigger string

const (
	RepairRunRunning   RepairRunStatus = "running"
	RepairRunCompleted RepairRunStatus = "completed"
	RepairRunFailed    RepairRunStatus = "failed"
	RepairRunCancelled RepairRunStatus = "cancelled"
)

const (
	RepairStageSelecting RepairRunStage = "selecting"
	RepairStageProbing   RepairRunStage = "probing"
	RepairStageRepairing RepairRunStage = "repairing"
	RepairStageDone      RepairRunStage = "done"
)

const (
	RepairTriggerScheduled RepairRunTrigger = "scheduled"
	RepairTriggerManual    RepairRunTrigger = "manual"
)

// RepairRunStats carries the per-component outcome counters for one run. The
// four repair components each report their own outcome so the history UI can
// show what actually happened rather than a single blended "repaired" count:
//
//	CHECK   → Probed / Healthy / Broken / Unknown (detection only).
//	REPAIR  → Reacquired: dead items re-acquired across providers.
//	          RepairFailed: re-acquire attempts that were MADE and errored.
//	          RepairSkippedUnsupported: dead items REPAIR structurally cannot
//	          re-acquire (usenet has no re-insert analogue) or whose entry row
//	          has vanished — attempts that were never made.
//	PRUNE   → Pruned: entries deleted decypharr-side (no arr call).
//	          PruneSkippedNotEligible: dead entries PRUNE DECLINED to delete
//	          because they are not fully broken / carry no infohash.
//	RE-GRAB → Regrabbed: arr file records deleted + blocklisted + re-searched;
//	          RegrabFailed counts arr file deletes that errored;
//	          RegrabSkippedNoArrLink counts dead entries with no resolved arr
//	          link, which RE-GRAB cannot route.
//
// The three *Skipped* counters exist because a component that DECLINES to act
// is otherwise indistinguishable from a component that never ran: a run that
// reports `pruned=0` with no skip counter reads as "PRUNE is broken" when the
// truth may be "PRUNE correctly refused to delete 3 partially-broken entries".
// Every non-action on an action path is counted here and given a per-entry
// reason in EntryHealth.ActionSkips.
//
// Deletions is how many entries consumed a destructive-deletion slot this run
// (PRUNE and/or RE-GRAB combined); DeletionCapSkipped is how many broken
// entries were left un-deleted because the per-run cap was already exhausted.
//
// NOTE for UI/API consumers: RepairFailed (`repair_failed`) counts REPAIR
// failures, matching its name. Arr-side RE-GRAB delete failures — which it
// used to carry — now have their own RegrabFailed (`regrab_failed`), which is
// the key the run logs already used for them.
type RepairRunStats struct {
	Candidates   int `json:"candidates"`
	SkippedFresh int `json:"skipped_fresh"`
	Probed       int `json:"probed"`
	Healthy      int `json:"healthy"`
	Broken       int `json:"broken"`
	Unknown      int `json:"unknown"`

	Reacquired               int `json:"reacquired"`
	RepairFailed             int `json:"repair_failed"`
	RepairSkippedUnsupported int `json:"repair_skipped_unsupported"`

	Pruned                  int `json:"pruned"`
	PruneSkippedNotEligible int `json:"prune_skipped_not_eligible"`

	Regrabbed              int `json:"regrabbed"`
	RegrabFailed           int `json:"regrab_failed"`
	RegrabSkippedNoArrLink int `json:"regrab_skipped_no_arr_link"`

	Deletions          int `json:"deletions"`
	DeletionCapSkipped int `json:"deletion_cap_skipped"`
}

// RepairRun is the append-only history record produced by a single sweep.
// Counters and stage are mutated live during the run so the status endpoint
// can report progress without holding any in-memory state.
type RepairRun struct {
	ID           string           `json:"id"`
	Trigger      RepairRunTrigger `json:"trigger"`
	Status       RepairRunStatus  `json:"status"`
	Stage        RepairRunStage   `json:"stage,omitempty"`
	StartedAt    time.Time        `json:"started_at"`
	UpdatedAt    time.Time        `json:"updated_at"`
	CompletedAt  time.Time        `json:"completed_at"`
	Stats        RepairRunStats   `json:"stats"`
	Error        string           `json:"error,omitempty"`
	CancelReason string           `json:"cancel_reason,omitempty"`
	Source       string           `json:"source,omitempty"`
}

// NormalizeRepairStrategy maps user-supplied values to a known strategy.
// Unknown / empty input falls back to per_entry.
func NormalizeRepairStrategy(strategy RepairStrategy) RepairStrategy {
	switch strategy {
	case RepairStrategyPerFile:
		return RepairStrategyPerFile
	default:
		return RepairStrategyPerEntry
	}
}

func (s *Storage) SaveRepairRun(run *RepairRun) error {
	if run == nil || run.ID == "" {
		return fmt.Errorf("repair run is missing id")
	}
	run.UpdatedAt = time.Now()
	data, err := json.Marshal(run)
	if err != nil {
		return err
	}
	return s.repairRuns.Put(run.ID, data, nil)
}

func (s *Storage) GetRepairRun(id string) (*RepairRun, error) {
	if id == "" {
		return nil, fmt.Errorf("repair run id is empty")
	}
	data, err := s.repairRuns.Get(id)
	if err != nil {
		return nil, err
	}
	var run RepairRun
	if err := json.Unmarshal(data, &run); err != nil {
		return nil, err
	}
	if run.ID == "" {
		run.ID = id
	}
	return &run, nil
}

// ListRepairRuns returns runs sorted newest-first.
func (s *Storage) ListRepairRuns() ([]*RepairRun, error) {
	runs := make([]*RepairRun, 0)
	err := s.repairRuns.ForEach(func(key string, value []byte) error {
		var run RepairRun
		if err := json.Unmarshal(value, &run); err != nil {
			return nil
		}
		if run.ID == "" {
			run.ID = key
		}
		runs = append(runs, &run)
		return nil
	})
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].StartedAt.After(runs[j].StartedAt)
	})
	return runs, err
}

func (s *Storage) DeleteRepairRun(id string) error {
	if id == "" {
		return nil
	}
	return s.repairRuns.Delete(id)
}

// PruneRepairRuns keeps the newest `keep` runs and deletes the rest. Runs in
// status running are always retained.
func (s *Storage) PruneRepairRuns(keep int) error {
	if keep <= 0 {
		keep = 100
	}
	runs, err := s.ListRepairRuns()
	if err != nil {
		return err
	}
	if len(runs) <= keep {
		return nil
	}
	for _, run := range runs[keep:] {
		if run.Status == RepairRunRunning {
			continue
		}
		_ = s.repairRuns.Delete(run.ID)
	}
	return nil
}

// ClearRepairRuns deletes every non-running run.
func (s *Storage) ClearRepairRuns() error {
	runs, err := s.ListRepairRuns()
	if err != nil {
		return err
	}
	for _, run := range runs {
		if run.Status == RepairRunRunning {
			continue
		}
		_ = s.repairRuns.Delete(run.ID)
	}
	return nil
}

// HealthStatus is the rolled-up state of an entry as seen by the repair system.
type HealthStatus string

const (
	HealthUnknown     HealthStatus = "unknown"
	HealthHealthy     HealthStatus = "healthy"
	HealthBroken      HealthStatus = "broken"
	HealthRepairing   HealthStatus = "repairing"
	HealthUnsupported HealthStatus = "unsupported"
	HealthStale       HealthStatus = "stale"
)

// ArrKind narrows BrokenFile.ArrName to a typed Arr (sonarr / radarr / ...).
type ArrKind string

const (
	ArrKindSonarr  ArrKind = "sonarr"
	ArrKindRadarr  ArrKind = "radarr"
	ArrKindLidarr  ArrKind = "lidarr"
	ArrKindReadarr ArrKind = "readarr"
	ArrKindOther   ArrKind = "other"
)

// BrokenFile carries everything the repair pipeline needs to act on a single
// broken file: where it lives in storage, which infohash it belongs to, and —
// when an Arr knows about it — the Arr-side identifiers needed to delete and
// re-search without another lookup.
type BrokenFile struct {
	EntryName string          `json:"entry_name"`
	FileName  string          `json:"file_name"`
	InfoHash  string          `json:"info_hash,omitempty"`
	Protocol  config.Protocol `json:"protocol,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	Size      int64           `json:"size,omitempty"`

	// Arr re-acquire payload. Empty when source=managed or when no Arr owns
	// the file.
	ArrName    string  `json:"arr_name,omitempty"`
	ArrKind    ArrKind `json:"arr_kind,omitempty"`
	MediaID    int     `json:"media_id,omitempty"`
	EpisodeID  int     `json:"episode_id,omitempty"`
	ArrFileID  int     `json:"arr_file_id,omitempty"`
	TargetPath string  `json:"target_path,omitempty"`
	SourcePath string  `json:"source_path,omitempty"`
}

// EntryHealth is the source of truth for repair decisions. It is keyed by
// EntryName (the folder-name shared across files of the same release) and is
// updated live during a sweep — once when probing starts, once when it
// finishes.
type EntryHealth struct {
	EntryName     string          `json:"entry_name"`
	Protocol      config.Protocol `json:"protocol,omitempty"`
	Status        HealthStatus    `json:"status"`
	Fingerprint   string          `json:"fingerprint,omitempty"`
	FileCount     int             `json:"file_count"`
	BrokenCount   int             `json:"broken_count"`
	BrokenFiles   []BrokenFile    `json:"broken_files,omitempty"`
	FailureReason string          `json:"failure_reason,omitempty"`

	// Structural marks a verdict the probe reached WITHOUT being able to
	// change its mind on any future run: the entry-item lists no probeable
	// file at all (empty, or every file soft-deleted), so there is nothing a
	// probe could ever resolve. Such a verdict must not be re-probed every
	// single run — see IsDue. It is cleared on any probe that DID reach files.
	Structural bool `json:"structural,omitempty"`

	// ActionSkips records, per component ("repair" / "prune" / "regrab"), why
	// that component DECLINED to act on this entry on the most recent run that
	// considered it. A decline is not a failure — PRUNE refusing to delete a
	// partially-broken entry is correct — but it must be visible, otherwise a
	// correct refusal is indistinguishable from a broken action path.
	ActionSkips map[string]string `json:"action_skips,omitempty"`

	// LastRepairError is why the most recent REPAIR (re-acquire) ATTEMPT
	// failed. Paired with LastRepairAt, which is stamped whenever an attempt
	// was MADE, so a failed repair and a never-attempted repair are
	// distinguishable.
	LastRepairError string `json:"last_repair_error,omitempty"`

	Dirty       bool   `json:"dirty"`
	DirtyReason string `json:"dirty_reason,omitempty"`

	LastCheckedAt  time.Time    `json:"last_checked_at"`
	LastOKAt       time.Time    `json:"last_ok_at"`
	LastFailedAt   time.Time    `json:"last_failed_at"`
	LastRepairAt   time.Time    `json:"last_repair_at"`
	NextCheckDueAt time.Time    `json:"next_check_due_at"`
	ActiveRunID    string       `json:"active_run_id,omitempty"`
	PreviousStatus HealthStatus `json:"previous_status,omitempty"`

	UpdatedAt time.Time `json:"updated_at"`
}

// SetActionSkip records why a component declined to act on this entry, or
// clears the entry for that component when reason is empty.
func (h *EntryHealth) SetActionSkip(component, reason string) {
	if h == nil || component == "" {
		return
	}
	if reason == "" {
		delete(h.ActionSkips, component)
		if len(h.ActionSkips) == 0 {
			h.ActionSkips = nil
		}
		return
	}
	if h.ActionSkips == nil {
		h.ActionSkips = make(map[string]string, 3)
	}
	h.ActionSkips[component] = reason
}

// IsDue reports whether this entry should be visited by the next sweep, given a
// recheck interval. Entries that have never been checked, that are dirty, or
// whose last status was anything other than healthy/unsupported are always due.
//
// The one exception is a STRUCTURAL verdict (see EntryHealth.Structural): an
// entry that lists no probeable file cannot produce a different answer no
// matter how often it is re-probed. Treating it like any other non-healthy
// status put it on a permanent every-run treadmill — it bypassed the freshness
// skip, got stamped, returned the same verdict instantly, and no action could
// ever touch it. Its file set is the only thing that can change the answer, and
// every file-set mutation goes through MarkEntryDirty (handled above), so it is
// safe to fall through to the plain staleness check and revisit it once per
// recheck interval as a backstop rather than once per run.
func (h *EntryHealth) IsDue(now time.Time, recheck time.Duration) bool {
	if h == nil {
		return true
	}
	if h.Dirty {
		return true
	}
	if h.LastCheckedAt.IsZero() {
		return true
	}
	switch h.Status {
	case HealthHealthy, HealthUnsupported:
		// fall through to staleness check
	case HealthUnknown:
		if h.Structural {
			break // fall through to staleness check
		}
		// An INDETERMINATE verdict carries its own short retry deadline
		// (NextCheckDueAt, set from repairIndeterminateRetry) precisely so it
		// is re-examined soon WITHOUT being re-probed on every single run.
		// That deadline was written by the probe and then never read by
		// anything, so `unknown` in practice bypassed the freshness skip every
		// run: probe, stamp, return the same non-verdict, forever. Honor it —
		// but only when it is actually set, and only for `unknown`, so a blank
		// or cleared record still falls through to always-due.
		if h.NextCheckDueAt.IsZero() {
			return true
		}
		if now.Before(h.NextCheckDueAt) {
			return false
		}
		return true
	default:
		// broken / repairing / stale stay ALWAYS due unless the verdict is
		// structural. A broken entry must be re-picked every run: that is what
		// lets an entry skipped by the deletion cap get its action on the next
		// run, and what makes a recovered entry flip back to healthy promptly.
		if !h.Structural {
			return true
		}
		// fall through to staleness check
	}
	if recheck <= 0 {
		return false
	}
	return now.Sub(h.LastCheckedAt) >= recheck
}

func (s *Storage) SaveEntryHealth(state *EntryHealth) error {
	if state == nil || state.EntryName == "" {
		return fmt.Errorf("entry health is missing entry name")
	}
	state.UpdatedAt = time.Now()
	state.BrokenCount = len(state.BrokenFiles)
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	// Index the status so CountEntryHealthByStatus can build its histogram
	// straight from the in-memory index without decoding every record.
	if err := s.repairState.Put(state.EntryName, data, &hybrid.EntryMeta{Status: string(state.Status)}); err != nil {
		return err
	}
	s.invalidateHealthCounts()
	return nil
}

func (s *Storage) GetEntryHealth(entryName string) (*EntryHealth, error) {
	if entryName == "" {
		return nil, fmt.Errorf("entry name is empty")
	}
	data, err := s.repairState.Get(entryName)
	if err != nil {
		return nil, err
	}
	var state EntryHealth
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.EntryName == "" {
		state.EntryName = entryName
	}
	return &state, nil
}

func (s *Storage) ForEachEntryHealth(fn func(*EntryHealth) error) error {
	return s.repairState.ForEach(func(key string, value []byte) error {
		var state EntryHealth
		if err := json.Unmarshal(value, &state); err != nil {
			return nil
		}
		if state.EntryName == "" {
			state.EntryName = key
		}
		return fn(&state)
	})
}

func (s *Storage) DeleteEntryHealth(entryName string) error {
	if entryName == "" || !s.repairState.Exists(entryName) {
		return nil
	}
	if err := s.repairState.Delete(entryName); err != nil {
		return err
	}
	s.invalidateHealthCounts()
	return nil
}

// ClearEntryHealthByStatuses deletes persisted repair health records whose
// status matches one of the supplied statuses. It only clears repair state;
// it does not touch entries, files, Arrs, or debrid placements.
func (s *Storage) ClearEntryHealthByStatuses(statuses []HealthStatus) (int, error) {
	wanted := make(map[HealthStatus]struct{}, len(statuses))
	for _, status := range statuses {
		if status != "" {
			wanted[status] = struct{}{}
		}
	}
	if len(wanted) == 0 {
		return 0, nil
	}

	names := make([]string, 0)
	if err := s.ForEachEntryHealth(func(state *EntryHealth) error {
		if state == nil {
			return nil
		}
		if _, ok := wanted[state.Status]; ok {
			names = append(names, state.EntryName)
		}
		return nil
	}); err != nil {
		return 0, err
	}

	cleared := 0
	for _, name := range names {
		if err := s.DeleteEntryHealth(name); err != nil {
			return cleared, err
		}
		cleared++
	}
	return cleared, nil
}

// MarkEntryDirty flags an entry's health as out-of-date so the next sweep will
// re-probe it. Called from the storage layer whenever the underlying file set
// of an entry mutates.
func (s *Storage) MarkEntryDirty(entryName string, protocol config.Protocol, reason string) {
	if entryName == "" {
		return
	}
	state, err := s.GetEntryHealth(entryName)
	if err != nil || state == nil {
		state = &EntryHealth{EntryName: entryName, Status: HealthUnknown}
	}
	if protocol != "" {
		state.Protocol = protocol
	}
	state.Dirty = true
	state.DirtyReason = reason
	state.NextCheckDueAt = time.Time{}
	_ = s.SaveEntryHealth(state)
}

// healthCountsTTL is a backstop only: the cache is invalidated eagerly by
// invalidateHealthCounts on every repair-state mutation, so this bound just
// caps how long a histogram can survive if some future write path forgets to
// invalidate. Rebuilding is an in-memory index walk (no disk reads, no
// JSON-unmarshal) once every record carries an indexed status.
const healthCountsTTL = 30 * time.Second

// invalidateHealthCounts drops the cached histogram. It MUST be called from
// every path that mutates repair state (SaveEntryHealth / DeleteEntryHealth,
// and therefore ClearEntryHealthByStatuses and MarkEntryDirty, which go
// through them). Without it, GET /api/repair/status could report
// health_counts.broken == 0 from a stale histogram while
// GET /api/repair/health?status=broken — which reads live — still listed
// broken entries, disabling the "act on broken" control for no visible reason.
func (s *Storage) invalidateHealthCounts() {
	s.healthCountsMu.Lock()
	s.healthCounts = nil
	s.healthCountsBuiltAt = time.Time{}
	s.healthCountsMu.Unlock()
}

// CountEntryHealthByStatus returns a per-status histogram without loading full
// EntryHealth payloads. The result is cached until the next repair-state
// mutation (see invalidateHealthCounts), with healthCountsTTL as a backstop.
func (s *Storage) CountEntryHealthByStatus() map[HealthStatus]int {
	s.healthCountsMu.Lock()
	if s.healthCounts != nil && time.Since(s.healthCountsBuiltAt) < healthCountsTTL {
		out := make(map[HealthStatus]int, len(s.healthCounts))
		maps.Copy(out, s.healthCounts)
		s.healthCountsMu.Unlock()
		return out
	}
	s.healthCountsMu.Unlock()

	counts := make(map[HealthStatus]int)
	// Fast path: read the status straight from the index (no disk read, no
	// JSON decode). Records persisted before the status was indexed have an
	// empty meta.Status; collect those and decode them after the iteration so
	// we never call Get (which RLocks) while ForEachMeta holds the read lock.
	// This self-heals: the next SaveEntryHealth (every sweep) populates the
	// index, so the fallback set shrinks to zero.
	var needDecode []string
	_ = s.repairState.ForEachMeta(func(key string, meta *hybrid.IndexEntry) error {
		if meta.Status != "" {
			counts[HealthStatus(meta.Status)]++
		} else {
			needDecode = append(needDecode, key)
		}
		return nil
	})
	for _, key := range needDecode {
		data, err := s.repairState.Get(key)
		if err != nil {
			continue
		}
		var stub struct {
			Status HealthStatus `json:"status"`
		}
		if json.Unmarshal(data, &stub) == nil && stub.Status != "" {
			counts[stub.Status]++
		}
	}

	s.healthCountsMu.Lock()
	s.healthCounts = counts
	s.healthCountsBuiltAt = time.Now()
	s.healthCountsMu.Unlock()

	out := make(map[HealthStatus]int, len(counts))
	maps.Copy(out, counts)
	return out
}

// EntryItemRepairFingerprint produces a deterministic hash of the file set
// inside an EntryItem. When this hash changes between two snapshots, the
// repair system knows the underlying files changed and the entry needs to be
// re-probed even if its last status was healthy.
func EntryItemRepairFingerprint(item *EntryItem) string {
	if item == nil || len(item.Files) == 0 {
		return ""
	}

	names := make([]string, 0, len(item.Files))
	for name := range item.Files {
		names = append(names, name)
	}
	sort.Strings(names)

	h := sha256.New()
	for _, name := range names {
		file := item.Files[name]
		if file == nil {
			continue
		}
		h.Write([]byte(name))
		h.Write([]byte{0})
		h.Write([]byte(file.InfoHash))
		h.Write([]byte{0})
		h.Write([]byte(strconv.FormatInt(file.Size, 10)))
		h.Write([]byte{0})
		if file.Deleted {
			h.Write([]byte("deleted"))
		}
		if file.ByteRange != nil {
			h.Write([]byte(strconv.FormatInt(file.ByteRange[0], 10)))
			h.Write([]byte{':'})
			h.Write([]byte(strconv.FormatInt(file.ByteRange[1], 10)))
		}
		h.Write([]byte{0xff})
	}
	return hex.EncodeToString(h.Sum(nil))
}

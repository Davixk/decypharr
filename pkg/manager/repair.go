// The repair service is the manager's health-checker. When enabled in config
// it registers a recurring sweep that probes only the entries that need
// probing (unhealthy, dirty, or stale) and persists per-entry health live
// during the run.
package manager

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// RepairStatus is the snapshot returned by the /api/repair/status endpoint.
type RepairStatus struct {
	Enabled      bool                         `json:"enabled"`
	NextRunAt    *time.Time                   `json:"next_run_at,omitempty"`
	ActiveRun    *storage.RepairRun           `json:"active_run,omitempty"`
	LastRun      *storage.RepairRun           `json:"last_run,omitempty"`
	HealthCounts map[storage.HealthStatus]int `json:"health_counts"`
}

// RepairRunOptions are one-off options for a manually-started repair run.
// Nil fields inherit the persisted repair config.
type RepairRunOptions struct {
	IgnoreLastChecked bool
	// Actions, when non-nil, is an explicit per-component override for this
	// one-off run (from the Run modal). Nil means use the configured
	// REPAIR/PRUNE/ARR-DELETE knobs.
	Actions        *ManualActions
	UnrestrictLink bool
	ProtocolScope  string
}

type ClearRepairStateResult struct {
	Statuses []storage.HealthStatus `json:"statuses"`
	Cleared  int                    `json:"cleared"`
}

const (
	repairSchedulerTag     = "repair-sweep"
	repairStopSchedulerTag = "repair-sweep-stop"
	repairDefaultWorkers   = 5
	repairDefaultRecheck   = 7 * 24 * time.Hour

	// NO IMPORT FAST LANE, deliberately — the obvious idea here is wrong.
	//
	// A fresh import looks like the urgent case (it carries no health verdict,
	// and the nightly 03:00 sweep may not reach it for ~24h), so an hourly lane
	// over no-verdict entries is the natural fix. It was built and removed.
	//
	// A FRESHLY IMPORTED FILE IS THE MOST-VETTED FILE IN THE SYSTEM, not the
	// least: it has just been through admission, the cache/seeder gates and the
	// import path. Re-probing it an hour later re-asks a question that was
	// answered minutes ago. The defect the payload ladder exists to catch —
	// serves its head, dies at the tail — is content ROT, which develops with
	// age; it is the settled library, not the new arrival, that needs re-reading.
	// A lane biased toward the newest entries spends the probe budget where the
	// evidence is freshest and the risk is lowest.
	repairHistoryRetained = 100
	// repairDefaultMaxDeletionsPerRun bounds how many entries a single sweep/run
	// may destructively heal when config leaves it unset. Mirrors the
	// missing-download reconciler's missingDownloadSweepLimit so a provider-wide
	// false "unavailable" recovers progressively instead of mass-deleting the
	// whole due set in one run.
	repairDefaultMaxDeletionsPerRun = 100
	// At most this many files probed concurrently within a single entry. The
	// outer worker count comes from cfg.Repair.Workers.
	repairFilesPerEntry    = 2
	repairStopDrainTimeout = 30 * time.Second
	// repairStopFinalRepairTimeout bounds the Arr delete + re-search pass run
	// when StopSchedule fires and auto-repair is enabled.
	repairStopFinalRepairTimeout = 5 * time.Minute
)

// Repair is the health-check / auto-repair service. One instance per Manager.
type Repair struct {
	manager   *Manager
	scheduler gocron.Scheduler
	logger    zerolog.Logger

	mu             sync.Mutex
	parentCtx      context.Context
	activeRunID    string
	cancelRun      context.CancelFunc
	scheduled      bool
	stopScheduled  bool
	activeStopFunc func() // called by the stop job for the active run
	runWG          sync.WaitGroup
}

// NewRepair builds the repair service for the given manager. Call
// Repair.Start to register the recurring sweep with the scheduler.
func NewRepair(m *Manager) *Repair {
	return &Repair{
		manager:   m,
		scheduler: m.scheduler,
		logger:    logger.New("repair"),
		parentCtx: context.Background(),
	}
}

func (r *Repair) cfg() config.RepairConfig { return config.Get().Repair }

// repairActions is the resolved set of per-item components a run may apply to a
// dead item after CHECK has found it. The four components are independent and
// individually knob-gated:
//
//   - repair    (REPAIR)     — re-acquire the item across providers. Non-destructive.
//   - prune     (PRUNE)      — delete the item decypharr-side only (no arr). Destructive.
//   - arrDelete (ARR-DELETE) — delete the arr's file record. Destructive.
//
// CHECK itself (enumerate + probe + record) always runs and is not represented
// here — it is the detection that produces the dead items these actions target.
//
// arrDelete was called REGRAB until the acts were split apart. With search and
// blocklist now separately gated and both defaulting off, the component's
// default behaviour grabs nothing whatsoever, so the old name asserted the
// opposite of what it does.
//
// search and blocklist are SUB-actions of arrDelete, not peers of it. They are
// meaningless on their own — both describe something done to the arr alongside
// the delete — so both are gated on arrDelete and neither appears in any(),
// destructive() or label() as an independent component. Keeping them subordinate
// is what preserves the invariant that this is the only arr-coupled component:
// with arrDelete off, nothing here can reach the arr.
type repairActions struct {
	repair    bool
	prune     bool
	arrDelete bool
	search    bool
	blocklist bool
}

// wantSearch / wantBlocklist resolve the sub-actions with their gate applied, so
// no caller has to remember to && with arrDelete.
func (a repairActions) wantSearch() bool    { return a.arrDelete && a.search }
func (a repairActions) wantBlocklist() bool { return a.arrDelete && a.blocklist }

// destructive reports whether any component that consumes a per-run deletion
// slot is enabled (PRUNE and ARR-DELETE). REPAIR is non-destructive.
func (a repairActions) destructive() bool { return a.prune || a.arrDelete }

// any reports whether any action component is enabled. When false the run is
// CHECK-only: probe and record health, take no further action.
func (a repairActions) any() bool { return a.repair || a.prune || a.arrDelete }

// label renders the enabled components as a compact "+"-joined string for the
// run's Source field (traceability), e.g. "repair+prune". "check-only" when no
// component is set.
//
// The arr component renders its enabled sub-actions inline —
// "arr(delete+search)" — rather than as a bare component name. A run record that
// just names the component no longer says what was done to the arr, and the
// whole point of the split is that delete, search and blocklist are different
// acts with different blast radii. "delete" is always listed because the
// component always deletes.
func (a repairActions) label() string {
	parts := make([]string, 0, 3)
	if a.repair {
		parts = append(parts, "repair")
	}
	if a.prune {
		parts = append(parts, "prune")
	}
	if a.arrDelete {
		sub := []string{"delete"}
		if a.search {
			sub = append(sub, "search")
		}
		if a.blocklist {
			sub = append(sub, "blocklist")
		}
		parts = append(parts, "arr("+strings.Join(sub, "+")+")")
	}
	if len(parts) == 0 {
		return "check-only"
	}
	return strings.Join(parts, "+")
}

// resolveActions maps the configured component knobs directly to the
// per-component action set: REPAIR defaults on (RepairEnabled), PRUNE/ARR-DELETE
// default off. There is no master gate — the run is CHECK-only (no REPAIR/
// PRUNE/ARR-DELETE) exactly when all three knobs are off.
func resolveActions(cfg config.RepairConfig) repairActions {
	return repairActions{
		repair:    cfg.RepairEnabled(),
		prune:     cfg.Prune,
		arrDelete: cfg.ArrDeleteEnabled(),
		search:    cfg.ArrSearch,
		blocklist: cfg.ArrBlocklist,
	}
}

// ManualActions is an explicit per-component selection supplied by the manual
// fix endpoints (FixBroken, RecheckEntry, RecheckMedia). It lets a caller drive
// a SINGLE component (e.g. PRUNE-only) rather than the old force-all bundle. A
// nil *ManualActions means "the request specified no components" — the caller
// then falls back to the configured knobs (see resolveManualActions), never
// force-all.
type ManualActions struct {
	Repair bool `json:"repair"`
	Prune  bool `json:"prune"`

	// ArrDelete selects the arr-side component. Regrab is its deprecated alias,
	// accepted so API clients written before the rename keep working; resolve
	// with arrDeleteSelected(), never by reading either field.
	ArrDelete bool `json:"arr_delete"`
	Regrab    bool `json:"regrab"` // Deprecated: use ArrDelete.

	// Search / Blocklist override the arr component's sub-actions for this one
	// call. Both are *bool, and nil means "not specified" — the CONFIGURED knob
	// is used. They are not part of any(): naming only a sub-action selects no
	// component and must not make an otherwise-empty selection look non-empty, or
	// {"blocklist":true} alone would resurrect the all-false footgun that
	// resolveManualActions exists to prevent.
	Search    *bool `json:"search,omitempty"`
	Blocklist *bool `json:"blocklist,omitempty"`
}

// arrDeleteSelected accepts either the current key or the deprecated alias.
func (a *ManualActions) arrDeleteSelected() bool {
	return a != nil && (a.ArrDelete || a.Regrab)
}

func (a *ManualActions) any() bool {
	return a != nil && (a.Repair || a.Prune || a.arrDeleteSelected())
}

// toActions converts an explicit component selection into a full action set,
// resolving the arr sub-actions against cfg. Every caller that overrides the
// configured knobs with an opts.Actions selection goes through here, so none of
// them can forget to carry the sub-actions and silently fall back to the
// zero-value (search off, blocklist off) when the operator configured otherwise.
func (a *ManualActions) toActions(cfg config.RepairConfig) repairActions {
	if a == nil {
		return repairActions{}
	}
	search, blocklist := a.subActions(cfg)
	return repairActions{
		repair: a.Repair, prune: a.Prune, arrDelete: a.arrDeleteSelected(),
		search: search, blocklist: blocklist,
	}
}

// subActions resolves this selection's arr sub-actions against the configured
// defaults, honouring an explicit override when one was supplied.
func (a *ManualActions) subActions(cfg config.RepairConfig) (search, blocklist bool) {
	search, blocklist = cfg.ArrSearch, cfg.ArrBlocklist
	if a == nil {
		return search, blocklist
	}
	if a.Search != nil {
		search = *a.Search
	}
	if a.Blocklist != nil {
		blocklist = *a.Blocklist
	}
	return search, blocklist
}

// resolveManualActions maps an explicit component selection + the legacy fix
// flag to the per-component action set for a user-initiated fix. Precedence:
//
//   - sel is non-nil but names NO component → NOTHING runs. An all-false
//     selection is an EXPLICIT "no components", the opposite of "unspecified".
//   - sel names ≥1 component                → exactly those components (single-
//     component invocation, e.g. PRUNE-only, works here).
//   - sel is nil but fix == true            → fall back to the CONFIGURED
//     REPAIR/PRUNE/ARR-DELETE knobs (back-compat with the old fix:true clients) —
//     NOT force-all, fixing the previous force-all footgun.
//   - otherwise                             → CHECK-only (no action).
//
// The first rule is the root-cause fix for the all-false footgun. Gating on
// sel.any() alone made an explicit all-false selection indistinguishable from a
// nil one, so it fell through to the configured knobs and could run PRUNE /
// ARR-DELETE on a request that asked for nothing. The HTTP layer now rejects that
// shape with a 400 before it ever reaches here, but the guard belongs at the
// source: every non-HTTP caller of FixBroken / RecheckEntry / RecheckMedia gets
// it too, and the two layers agree rather than conflict (both resolve an
// explicit all-false selection to "do nothing").
func (r *Repair) resolveManualActions(sel *ManualActions, fix bool) repairActions {
	if sel != nil && !sel.any() {
		return repairActions{}
	}
	if sel.any() {
		return sel.toActions(r.cfg())
	}
	if fix {
		return resolveActions(r.cfg())
	}
	return repairActions{}
}

// recordBudgetStats copies the per-run destructive-deletion budget totals onto
// the run record so the history UI can surface them (previously logged only).
func recordBudgetStats(run *storage.RepairRun, budget *repairDeletionBudget) {
	run.Stats.Deletions = budget.deletions()
	run.Stats.DeletionCapSkipped = budget.skippedCount()
}

func normalizeRepairProtocolScope(scope string) string {
	switch strings.ToLower(strings.TrimSpace(scope)) {
	case "all", "both":
		return "all"
	case string(config.ProtocolTorrent):
		return string(config.ProtocolTorrent)
	case string(config.ProtocolNZB):
		return string(config.ProtocolNZB)
	default:
		return ""
	}
}

func (r *Repair) effectiveProtocolScope(opts RepairRunOptions) string {
	if scope := normalizeRepairProtocolScope(opts.ProtocolScope); scope != "" {
		return scope
	}
	if r.cfg().SkipNZBRepair {
		return string(config.ProtocolTorrent)
	}
	return "all"
}

func repairProtocolMatches(scope string, protocol config.Protocol) bool {
	switch normalizeRepairProtocolScope(scope) {
	case "", "all":
		return true
	case string(config.ProtocolTorrent):
		return protocol == config.ProtocolTorrent
	case string(config.ProtocolNZB):
		return protocol == config.ProtocolNZB
	default:
		return true
	}
}

func (r *Repair) workers() int {
	if w := r.cfg().Workers; w > 0 {
		return w
	}
	return repairDefaultWorkers
}

func (r *Repair) recheckInterval() time.Duration {
	raw := r.cfg().RecheckInterval
	if raw == "" {
		return repairDefaultRecheck
	}
	d, err := utils.ParseDuration(raw)
	if err != nil || d <= 0 {
		return repairDefaultRecheck
	}
	return d
}

// maxDeletionsPerRun resolves the per-run destructive-deletion cap used by
// newDeletionBudget. 0/unset -> repairDefaultMaxDeletionsPerRun (100); a
// negative value (e.g. -1) -> 0, which the budget treats as unlimited.
func (r *Repair) maxDeletionsPerRun() int {
	switch v := r.cfg().MaxDeletionsPerRun; {
	case v < 0:
		return 0 // unlimited
	case v == 0:
		return repairDefaultMaxDeletionsPerRun
	default:
		return v
	}
}

// repairDeletionBudget bounds how many entries a single repair run may
// destructively heal — an entry-folder whose Arr file records get deleted
// (a.DeleteFiles) and/or whose decypharr entry gets deleted (DeleteEntry).
// One slot is reserved per entry that is about to have a real destructive
// action performed; non-destructive probing and re-inserts that heal never
// reserve. A provider-wide false "unavailable" could otherwise mark the whole
// due set broken and mass-delete in one run; capping makes recovery
// progressive — entries past the cap stay broken in storage and are re-picked
// next run, so nothing is lost.
//
// A nil budget, or one whose limit is <= 0, is unlimited. EVERY path that can
// reach a destructive action now builds a real budget — the scheduled sweep,
// FixBroken, the stop-schedule pass, RecheckMedia AND RecheckEntry. The manual
// recheck paths used to pass nil, silently opting out of the only guard against
// a mass-delete; a single-entry recheck is bounded anyway, so carrying the
// budget costs it nothing while closing the bypass. Only an explicitly
// configured negative max_deletions_per_run disables the cap.
type repairDeletionBudget struct {
	limit    int
	used     atomic.Int64
	skipped  atomic.Int64
	warnOnce sync.Once
	logger   zerolog.Logger
	runID    string
}

// newDeletionBudget builds a budget for one run using the configured cap.
func (r *Repair) newDeletionBudget(runID string) *repairDeletionBudget {
	return &repairDeletionBudget{
		limit:  r.maxDeletionsPerRun(),
		logger: r.logger,
		runID:  runID,
	}
}

// reserve claims one destructive-deletion slot for the entry about to be
// healed-with-deletion. It returns true when the caller may proceed with the
// Arr file-record delete and/or the entry delete; false when the per-run cap
// is exhausted, in which case the caller must skip ALL destructive actions for
// that entry and leave it broken (it is re-picked next run). The first denial
// logs a single WARN naming the cap. Safe for concurrent callers (the sweep
// heals entries in parallel).
func (b *repairDeletionBudget) reserve() bool {
	if b == nil || b.limit <= 0 {
		return true
	}
	for {
		cur := b.used.Load()
		if cur >= int64(b.limit) {
			b.skipped.Add(1)
			b.warnOnce.Do(func() {
				b.logger.Warn().
					Str("run_id", b.runID).
					Int("cap", b.limit).
					Msg("Repair deletion cap reached; further broken entries left un-deleted this run — they stay broken and are retried next run; raise repair.max_deletions_per_run to delete more per run")
			})
			return false
		}
		if b.used.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// deletions reports how many destructive-heal slots were consumed this run.
func (b *repairDeletionBudget) deletions() int {
	if b == nil {
		return 0
	}
	return int(b.used.Load())
}

// skippedCount reports how many entries were left un-deleted because the cap
// was already exhausted.
func (b *repairDeletionBudget) skippedCount() int {
	if b == nil {
		return 0
	}
	return int(b.skipped.Load())
}

// Start registers the recurring sweep with the scheduler if repair is
// enabled. It also reconciles any orphaned state left by a previous process:
// runs marked running flip to cancelled; entries stuck on `repairing` revert
// to their previous status. Idempotent.
func (r *Repair) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.parentCtx = ctx

	r.reconcileOrphans()

	cfg := r.cfg()
	if !cfg.Enabled {
		r.logger.Info().Msg("Repair disabled in config")
		return nil
	}
	if strings.TrimSpace(cfg.Schedule) == "" {
		return fmt.Errorf("repair enabled but schedule is empty")
	}

	jd, err := utils.ConvertToJobDef(cfg.Schedule)
	if err != nil {
		return fmt.Errorf("invalid repair schedule %q: %w", cfg.Schedule, err)
	}

	r.scheduler.RemoveByTags(repairSchedulerTag)
	if _, err := r.scheduler.NewJob(jd,
		gocron.NewTask(func() {
			if _, err := r.runSweep(storage.RepairTriggerScheduled, RepairRunOptions{}); err != nil {
				r.logger.Warn().Err(err).Msg("Scheduled repair sweep skipped")
			}
		}),
		gocron.WithTags(repairSchedulerTag),
	); err != nil {
		return fmt.Errorf("failed to register repair sweep: %w", err)
	}
	r.scheduled = true
	r.logger.Info().Str("schedule", cfg.Schedule).Msg("Repair sweep scheduled")

	r.scheduler.RemoveByTags(repairStopSchedulerTag)
	r.stopScheduled = false
	if stopSchedule := strings.TrimSpace(cfg.StopSchedule); stopSchedule != "" {
		stopJD, err := utils.ConvertToJobDef(stopSchedule)
		if err != nil {
			return fmt.Errorf("invalid repair stop schedule %q: %w", stopSchedule, err)
		}
		if _, err := r.scheduler.NewJob(stopJD,
			gocron.NewTask(func() {
				r.stopActiveRepairSweep()
			}),
			gocron.WithTags(repairStopSchedulerTag),
		); err != nil {
			return fmt.Errorf("failed to register repair stop schedule: %w", err)
		}
		r.stopScheduled = true
		r.logger.Info().Str("stop_schedule", stopSchedule).Msg("Repair sweep stop schedule registered")
	}
	return nil
}

// Stop cancels any running sweep and unregisters the scheduled job. It blocks
// until the sweep goroutine exits (bounded by repairStopDrainTimeout) so
// in-flight saves don't race with storage.Close.
func (r *Repair) Stop() {
	r.mu.Lock()
	cancel := r.cancelRun
	r.cancelRun = nil
	r.activeRunID = ""
	r.activeStopFunc = nil
	if r.scheduled {
		r.scheduler.RemoveByTags(repairSchedulerTag)
		r.scheduled = false
	}
	if r.stopScheduled {
		r.scheduler.RemoveByTags(repairStopSchedulerTag)
		r.stopScheduled = false
	}
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}

	done := make(chan struct{})
	go func() {
		r.runWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(repairStopDrainTimeout):
		r.logger.Warn().Dur("timeout", repairStopDrainTimeout).Msg("Repair: drain timed out")
	}
}

// ApplyConfig reconciles the scheduler with the latest repair config. Called
// after /api/repair/config is updated.
func (r *Repair) ApplyConfig() error {
	r.Stop()
	return r.Start(r.parentCtx)
}

// RunNow triggers a manual sweep. Returns the new run ID.
func (r *Repair) RunNow(opts RepairRunOptions) (string, error) {
	return r.runSweep(storage.RepairTriggerManual, opts)
}

// ClearStates clears persisted repair-health state for selected statuses. It
// does not touch files, Arrs, debrid placements, or run history.
func (r *Repair) ClearStates(statuses []storage.HealthStatus) (ClearRepairStateResult, error) {
	result := ClearRepairStateResult{Statuses: statuses}
	if len(statuses) == 0 {
		return result, errors.New("at least one status is required")
	}

	r.mu.Lock()
	activeID := r.activeRunID
	r.mu.Unlock()
	if activeID != "" {
		return result, fmt.Errorf("repair already running (run %s)", activeID)
	}

	cleared, err := r.manager.storage.ClearEntryHealthByStatuses(statuses)
	if err != nil {
		return result, err
	}
	result.Cleared = cleared
	return result, nil
}

// StopRun cancels the currently-active sweep, if any. The run record is also
// flipped to cancelled in storage immediately so the UI sees the stop on the
// next poll, even before the goroutine unwinds.
func (r *Repair) StopRun() error {
	r.mu.Lock()
	cancel := r.cancelRun
	id := r.activeRunID
	r.mu.Unlock()
	if cancel == nil {
		return errors.New("no active repair run")
	}

	if id != "" {
		if run, err := r.manager.storage.GetRepairRun(id); err == nil && run != nil && run.Status == storage.RepairRunRunning {
			run.Status = storage.RepairRunCancelled
			run.Stage = storage.RepairStageDone
			run.CancelReason = "stopped by user"
			run.CompletedAt = time.Now()
			if err := r.manager.storage.SaveRepairRun(run); err != nil {
				r.logger.Warn().Err(err).Str("run_id", id).Msg("Stop: failed to persist optimistic cancel")
			}
		}
	}

	r.logger.Info().Str("run_id", id).Msg("Cancelling repair run")
	cancel()
	return nil
}

// stopActiveRepairSweep is invoked by the StopSchedule job. Unlike StopRun, this is
// not a user-initiated abort: the repair sweep is marked completed (not cancelled),
// and whether whatever was found broken up to this point gets acted on is
// decided by the enabled REPAIR / PRUNE / ARR-DELETE components. With no active
// repair sweep this is a no-op.
func (r *Repair) stopActiveRepairSweep() {
	r.mu.Lock()
	cancel := r.cancelRun
	id := r.activeRunID
	stopFunc := r.activeStopFunc
	r.mu.Unlock()
	if cancel == nil {
		return
	}

	r.logger.Info().Str("run_id", id).Msg("Repair sweep stop schedule fired; stopping repair sweep")
	if stopFunc != nil {
		stopFunc()
	}
	cancel()
}

// Status reports the current repair state for the API.
func (r *Repair) Status() RepairStatus {
	cfg := r.cfg()
	st := RepairStatus{
		Enabled:      cfg.Enabled,
		HealthCounts: r.manager.storage.CountEntryHealthByStatus(),
	}
	if next := r.nextScheduledRun(); next != nil {
		st.NextRunAt = next
	}

	r.mu.Lock()
	activeID := r.activeRunID
	r.mu.Unlock()
	if activeID != "" {
		if run, err := r.manager.storage.GetRepairRun(activeID); err == nil {
			st.ActiveRun = run
		}
	}

	if runs, err := r.manager.storage.ListRepairRuns(); err == nil {
		for _, run := range runs {
			if st.ActiveRun != nil && run.ID == st.ActiveRun.ID {
				continue
			}
			if run.Status == storage.RepairRunRunning {
				continue
			}
			st.LastRun = run
			break
		}
	}
	return st
}

func (r *Repair) nextScheduledRun() *time.Time {
	if !r.scheduled {
		return nil
	}
	for _, j := range r.scheduler.Jobs() {
		for _, tag := range j.Tags() {
			if tag != repairSchedulerTag {
				continue
			}
			if next, err := j.NextRun(); err == nil {
				return &next
			}
		}
	}
	return nil
}

// reconcileOrphans cleans up state left by a previous process that died
// mid-sweep. Called from Start under r.mu.
func (r *Repair) reconcileOrphans() {
	s := r.manager.storage
	if s == nil {
		return
	}

	if runs, err := s.ListRepairRuns(); err == nil {
		now := time.Now()
		n := 0
		for _, run := range runs {
			if run == nil || run.Status != storage.RepairRunRunning {
				continue
			}
			run.Status = storage.RepairRunCancelled
			run.Stage = storage.RepairStageDone
			run.CompletedAt = now
			run.CancelReason = "interrupted by restart"
			if err := s.SaveRepairRun(run); err != nil {
				r.logger.Warn().Err(err).Str("run_id", run.ID).Msg("Reconcile: failed to persist orphaned run")
				continue
			}
			n++
		}
		if n > 0 {
			r.logger.Info().Int("count", n).Msg("Reconciled orphaned repair runs")
		}
	}

	cleared := 0
	_ = s.ForEachEntryHealth(func(state *storage.EntryHealth) error {
		if state == nil || state.ActiveRunID == "" {
			return nil
		}
		if state.PreviousStatus != "" {
			state.Status = state.PreviousStatus
		} else {
			state.Status = storage.HealthUnknown
		}
		state.ActiveRunID = ""
		state.PreviousStatus = ""
		if err := s.SaveEntryHealth(state); err == nil {
			cleared++
		}
		return nil
	})
	if cleared > 0 {
		r.logger.Info().Int("count", cleared).Msg("Reverted entries stuck on 'repairing'")
	}
}

// runSweep is the entry-point shared by RunNow and the scheduled callback. It
// guards against concurrent runs, persists the run record, then dispatches.
func (r *Repair) runSweep(trigger storage.RepairRunTrigger, opts RepairRunOptions) (string, error) {
	cfg := r.cfg()
	if !cfg.Enabled && trigger == storage.RepairTriggerScheduled {
		return "", errors.New("repair disabled")
	}

	r.mu.Lock()
	if r.activeRunID != "" {
		id := r.activeRunID
		r.mu.Unlock()
		return id, errors.New("repair already running")
	}

	runCtx, cancel := context.WithCancel(r.parentCtx)
	stopState := &repairStopState{}
	sourceParts := []string{string(cfg.Source)}
	if opts.IgnoreLastChecked {
		sourceParts = append(sourceParts, "ignore-last-checked")
	}
	if opts.Actions != nil {
		sourceParts = append(sourceParts, opts.Actions.toActions(cfg).label())
	}
	if opts.UnrestrictLink {
		sourceParts = append(sourceParts, "unrestrict-link")
	}
	if scope := normalizeRepairProtocolScope(opts.ProtocolScope); scope != "" {
		sourceParts = append(sourceParts, "protocol-"+scope)
	}
	run := &storage.RepairRun{
		ID:        uuid.NewString(),
		Trigger:   trigger,
		Status:    storage.RepairRunRunning,
		Stage:     storage.RepairStageSelecting,
		StartedAt: time.Now(),
		Source:    strings.Join(sourceParts, ":"),
	}
	r.activeRunID = run.ID
	r.cancelRun = cancel
	r.activeStopFunc = stopState.set
	r.mu.Unlock()

	if err := r.manager.storage.SaveRepairRun(run); err != nil {
		r.mu.Lock()
		r.activeRunID = ""
		r.cancelRun = nil
		r.activeStopFunc = nil
		r.mu.Unlock()
		cancel()
		return "", fmt.Errorf("failed to persist repair run: %w", err)
	}

	r.runWG.Go(func() {
		defer func() {
			r.mu.Lock()
			if r.activeRunID == run.ID {
				r.activeRunID = ""
				r.cancelRun = nil
				r.activeStopFunc = nil
			}
			r.mu.Unlock()
			cancel()
		}()
		r.executeSweep(runCtx, run, opts, stopState)
	})

	r.logger.Info().Str("run_id", run.ID).Str("trigger", string(trigger)).Msg("Repair sweep started")
	return run.ID, nil
}

func (r *Repair) finalizeRun(run *storage.RepairRun, status storage.RepairRunStatus, errStr, cancelReason string) {
	// A user-initiated cancel that already landed in storage must not be
	// clobbered by a sweep that completed successfully after Stop was pressed.
	if existing, err := r.manager.storage.GetRepairRun(run.ID); err == nil && existing != nil && existing.Status == storage.RepairRunCancelled {
		status = storage.RepairRunCancelled
		if cancelReason == "" {
			cancelReason = existing.CancelReason
		}
	}

	run.Status = status
	run.Stage = storage.RepairStageDone
	run.CompletedAt = time.Now()
	if errStr != "" {
		run.Error = errStr
	}
	if cancelReason != "" {
		run.CancelReason = cancelReason
	}
	if err := r.manager.storage.SaveRepairRun(run); err != nil {
		r.logger.Warn().Err(err).Str("run_id", run.ID).Msg("Failed to persist final run state")
	}
	_ = r.manager.storage.PruneRepairRuns(repairHistoryRetained)

	if r.manager.Notifications != nil {
		if event := notificationEventFor(status); event != "" {
			r.manager.Notifications.Notify(notifications.Event{
				Type:    event,
				Status:  string(status),
				Message: discordContextFor(run),
			})
		}
	}

	// Repair scans the full entry set and allocates aggressively (sonic JSON
	// decode, appendLog.ReadAt buffers). Hand the freed heap back to the OS
	// so RSS doesn't sit at the post-repair peak.
	debug.FreeOSMemory()
}

func notificationEventFor(status storage.RepairRunStatus) config.NotificationEvent {
	switch status {
	case storage.RepairRunCompleted:
		return config.EventRepairComplete
	case storage.RepairRunFailed:
		return config.EventRepairFailed
	case storage.RepairRunCancelled:
		return config.EventRepairCancelled
	}
	return ""
}

func discordContextFor(run *storage.RepairRun) string {
	const dateFmt = "2006-01-02 15:04:05"
	return fmt.Sprintf(
		// "re-grabbed" outlived the action it named: ARR-DELETE deletes the arr's
		// file record and nothing else unless search/blocklist are opted into.
		// This line goes to the operator, so it uses the same word as the log
		// prefix and the run-history column.
		"\n**Run**: %s\n**Trigger**: %s\n**Source**: %s\n**Status**: %s\n**Started**: %s\n**Completed**: %s\n**Checked**: %d (broken: %d)\n**Actions**: re-acquired %d · pruned %d · arr-deleted %d\n",
		run.ID, run.Trigger, run.Source, run.Status,
		run.StartedAt.Format(dateFmt), run.CompletedAt.Format(dateFmt),
		run.Stats.Probed, run.Stats.Broken,
		run.Stats.Reacquired, run.Stats.Pruned, run.Stats.ArrDeleted,
	)
}

// repairStopState communicates a StopSchedule-triggered stop from
// stopActiveRepairSweep (called on the scheduler goroutine) into the running
// repair sweep. set is called at most once; get is read after the probing context
// is observed as cancelled.
type repairStopState struct {
	mu      sync.Mutex
	stopped bool
}

func (s *repairStopState) set() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
}

func (s *repairStopState) get() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}

func (r *Repair) saveRun(run *storage.RepairRun) {
	if err := r.manager.storage.SaveRepairRun(run); err != nil {
		r.logger.Trace().Err(err).Str("run_id", run.ID).Msg("Failed to persist run progress")
	}
}

func (r *Repair) saveHealth(state *storage.EntryHealth) {
	if err := r.manager.storage.SaveEntryHealth(state); err != nil {
		r.logger.Trace().Err(err).Str("entry", state.EntryName).Msg("Failed to persist entry health")
	}
}

// ReinsertEntry attempts to fix a torrent by re-inserting it across debrids.
// Used by the link service and by the repair auto-heal pass.
func (m *Manager) ReinsertEntry(ctx context.Context, entry *storage.Entry) error {
	if m.fixer == nil {
		return fmt.Errorf("fixer not initialized")
	}
	res, err := m.fixer.FixTorrent(ctx, entry, false)
	if err != nil {
		return err
	}
	if !res.Success {
		return fmt.Errorf("failed to re-insert torrent")
	}
	return nil
}

// linkOf returns the resolvable link/id for a torrent file in its active
// provider placement, or "" when no link is available.
func linkOf(entry *storage.Entry, name string) string {
	pe := entry.GetActiveProvider()
	if pe == nil || pe.Files == nil {
		return ""
	}
	f, ok := pe.Files[name]
	if !ok || f == nil {
		return ""
	}
	return cmp.Or(f.Link, f.Id)
}

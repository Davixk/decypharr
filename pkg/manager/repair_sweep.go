package manager

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

const (
	// repairProbeReadBytes is how much REAL payload a CHECK probe transfers at
	// each sampled offset to prove a file is servable. Metadata-level probes
	// (NNTP STAT, a hoster availability call, a HEAD served from cached
	// metadata) move zero bytes, which is precisely how a 100%-unreadable entry
	// recorded `healthy` with broken_count 0.
	repairProbeReadBytes = 256 * 1024

	// repairProbeSamplePoints is how many offsets the payload verification
	// samples: head, evenly spaced interior offsets, and the tail. A GOOD HEAD
	// IS NOT PROOF OF HEALTH — a live 3.2 GB file served 0%, 25% and 50%
	// correctly, returned a success status with ZERO bytes at 75%, and a
	// permanent 410 at the tail. Head-only verification would have called it
	// healthy. Tunable: raising it linearly raises sweep cost.
	repairProbeSamplePoints = 5

	// repairProbeReadTimeout bounds one file's whole payload verification (all
	// sample points) so a stalled provider degrades the probe to `unknown`
	// instead of hanging the sweep.
	repairProbeReadTimeout = 2 * time.Minute

	// repairIndeterminateRetry is how soon an entry whose CHECK reached no
	// verdict is re-examined. Far shorter than the recheck interval so
	// `unknown` cannot become a permanent resting state.
	repairIndeterminateRetry = 6 * time.Hour

	// repairProbeVersion is the identity of the probe algorithm implemented in
	// THIS file, stamped onto every verdict it writes so a verdict produced by an
	// older algorithm can be recognised and re-probed rather than trusted.
	//
	// BUMP IT WHENEVER THE PROBE LOGIC BELOW CHANGES IN A WAY THAT COULD CHANGE A
	// VERDICT — a new sentinel classified as dead, a different sample ladder, a
	// path that stops failing open. The constant itself lives in pkg/storage
	// because its READER, EntryHealth.IsDue, cannot import pkg/manager; bump it
	// there (storage.RepairProbeVersion) and this alias follows.
	repairProbeVersion = storage.RepairProbeVersion
)

// candidate is the unit of work for a sweep. One per entry-folder.
//
// item is loaded lazily: enumeration records only the entry name (cheap,
// index-only) so a sweep doesn't decode and hold every entry body at once.
// probeEntry populates item just before probing and the worker releases it
// straight after, bounding resident entry bodies to the in-flight worker count
// rather than the whole store.
type candidate struct {
	name       string
	item       *storage.EntryItem
	arrName    string
	arrKind    storage.ArrKind
	contentMap map[string]arr.ContentFile // file_name -> Arr metadata when source=arr
}

// healCache memoizes per-infohash auto-heal results within one sweep so
// duplicate torrent sightings don't trigger repeated re-inserts. A stored
// nil means "healed"; a non-nil error means "previously failed".
type healCache struct {
	sf      singleflight.Group
	results *xsync.Map[string, error] // infohash -> heal error (nil if healed)
}

func newHealCache() *healCache {
	return &healCache{results: xsync.NewMap[string, error]()}
}

// do runs fix at most once per infohash, deduplicating concurrent callers via
// singleflight and memoizing the result for subsequent calls.
func (c *healCache) do(infoHash string, fix func() error) error {
	if c == nil || infoHash == "" {
		return fix()
	}
	if v, ok := c.results.Load(infoHash); ok {
		return v
	}
	_, err, _ := c.sf.Do(infoHash, func() (any, error) {
		if v, ok := c.results.Load(infoHash); ok {
			return nil, v
		}
		err := fix()
		c.results.Store(infoHash, err)
		return nil, err
	})
	return err
}

// fileResult is the outcome of probing one file in an entry.
type fileResult struct {
	name     string
	infoHash string
	protocol config.Protocol
	healthy  bool
	broken   bool
	reason   string // populated only when broken or unknown
}

// Component names used as EntryHealth.ActionSkips keys.
const (
	componentRepair = "repair"
	componentPrune  = "prune"
	componentArrDelete = "arr_delete"
)

// Machine-readable reasons recorded when a component DECLINES to act, or when a
// probe reaches a verdict no future probe can change. Every one of these used
// to be a bare `continue` / `return` with no counter and no trace, which is how
// a correct refusal became indistinguishable from a broken action path.
const (
	// reasonRepairUnsupportedProtocol: REPAIR re-acquires by re-inserting the
	// item across debrid providers (Fixer.FixTorrent). Only a torrent can be
	// re-inserted — Entry.CanBeFixed() is literally IsTorrent(). Usenet has no
	// analogue: the articles either exist on the news servers or they do not,
	// and re-parsing the same NZB asks the same providers for the same message
	// ids. A dead nzb is recovered by ARR-DELETE (a fresh NZB from the indexer) or
	// PRUNE, never by REPAIR.
	reasonRepairUnsupportedProtocol = "repair_unsupported_protocol"
	// reasonRepairEntryMissing: the health record names an infohash whose entry
	// row is gone, so there is nothing to re-insert.
	reasonRepairEntryMissing = "repair_entry_missing"
	// reasonRepairNoInfohash: nothing broken on this entry carries an infohash,
	// so REPAIR has no handle to act on.
	reasonRepairNoInfohash = "repair_no_infohash"

	// reasonPruneNoBrokenFiles / reasonPrunePartialEntry / reasonPruneNoInfohash
	// are the three ways pruneEligible declines. reasonPrunePartialEntry is the
	// important one: refusing to delete a 13-file entry because 5 of its files
	// are dead is CORRECT — it keeps the surviving files' symlinks — and it is
	// also the single most confusing "PRUNE did nothing" outcome, because
	// nothing recorded that the refusal happened.
	reasonPruneNoBrokenFiles = "prune_no_broken_files"
	reasonPrunePartialEntry  = "prune_partial_entry"
	reasonPruneNoInfohash    = "prune_no_infohash"

	// reasonArrNoLink: no broken file resolved to an arr file record, so
	// there is no arr to delete from, blocklist in, or re-search.
	reasonArrNoLink = "arr_no_link"

	// reasonDeletionCapReached: the per-run destructive budget was exhausted
	// before this entry, so ALL destructive components were skipped for it.
	reasonDeletionCapReached = "deletion_cap_reached"

	// reasonEntryHasNoFiles is a STRUCTURAL verdict: the entry-item exists but
	// lists no probeable file (empty, or every file soft-deleted). Nothing can
	// be served from it and no probe can ever say otherwise.
	reasonEntryHasNoFiles = "entry_has_no_files"
)

// repairAttempt is the per-entry outcome of the REPAIR component. It exists so
// a repair that was ATTEMPTED AND FAILED is distinguishable from one that was
// never attempted: previously both produced reacquired=0, repair_failed=0 and
// last_repair_at=never, simultaneously, which is an unreadable run record.
type repairAttempt struct {
	reacquired  int    // infohashes successfully re-acquired
	failed      int    // re-acquire attempts that were made and errored
	unsupported int    // dead placements REPAIR structurally cannot re-acquire
	attempted   bool   // at least one re-acquire was actually invoked
	err         string // reason the last failed attempt failed
	skip        string // reason REPAIR declined, when it never attempted anything
}

// executeSweep is the body of a sweep: enumerate, filter due, probe, repair.
func (r *Repair) executeSweep(ctx context.Context, run *storage.RepairRun, opts RepairRunOptions, stopState *repairStopState) {
	cfg := r.cfg()
	log := r.logger.With().Str("run_id", run.ID).Logger()

	// Resolve the action set once from the configured REPAIR/PRUNE/ARR-DELETE knobs.
	// There is no master gate: the sweep is CHECK-only (probe + record, no
	// REPAIR/PRUNE/ARR-DELETE) exactly when all three knobs are off. A one-off run
	// (Run modal) may pass opts.Actions to override the configured knobs for that
	// run. This also decides what happens to whatever was found broken so far if
	// a StopSchedule cuts the sweep short.
	actions := resolveActions(cfg)
	if opts.Actions != nil {
		actions = opts.Actions.toActions(cfg)
	}

	// One destructive-deletion budget for the whole sweep. It bounds how many
	// entries this run may destructively act on (PRUNE decypharr-delete and/or
	// ARR-DELETE arr-delete), so a provider-wide false "unavailable" can't mass-act
	// on the entire due set in one run. Shared by the inline action pass and any
	// StopSchedule post-stop pass.
	budget := r.newDeletionBudget(run.ID)

	log.Info().
		Bool("repair", actions.repair).
		Bool("prune", actions.prune).
		Bool("arr_delete", actions.arrDelete).
		Msg("Sweep: selecting candidates (CHECK: whole managed library)")
	// CHECK always enumerates the whole hosted library via the managed path.
	// ARR-DELETE targeting (arr linkage) is merged in only when regrab is enabled.
	candidates, err := r.enumerateCandidates(ctx, cfg, actions)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			r.finishCancelledRepairSweep(ctx, run, stopState, actions, "context cancelled during selection", nil, budget)
			return
		}
		log.Error().Err(err).Msg("Sweep: enumeration failed")
		r.finalizeRun(run, storage.RepairRunFailed, err.Error(), "")
		return
	}
	if ctx.Err() != nil {
		r.finishCancelledRepairSweep(ctx, run, stopState, actions, "context cancelled after selection", nil, budget)
		return
	}

	due, skipped := r.filterDueCandidates(candidates, opts.IgnoreLastChecked)
	// The full candidate set is only needed to compute `due`; drop it now so the
	// EntryItems we filtered out don't pin memory for the whole probe pass.
	candidates = nil
	protocolScope := r.effectiveProtocolScope(opts)
	due = r.filterCandidatesByProtocol(due, protocolScope)
	run.Stats.Candidates = len(due)
	run.Stats.SkippedFresh = skipped

	// Order candidates by sweep tier, then oldest-checked-first WITHIN each tier
	// (see sweepTier): entries with no usable verdict, then broken, then routine
	// re-verification of everything already known healthy. A sweep is far more
	// likely to be truncated than to finish — a full pass is ~13 hours at the
	// probe rate — so the first minutes must spend themselves on the ~129 entries
	// that carry information, not on 60 arbitrary healthy ones.
	//
	// Forward progress is unchanged, just per-tier: any entry probed today gets
	// LastCheckedAt = now and moves to the back of its own tier, so tomorrow's
	// truncated sweep picks up where today's left off instead of re-rolling a
	// random subset of `due`.
	//
	// This slice also doubles as the candidate list considered by this run,
	// used to scope a stop-schedule repair pass.
	names := r.orderCandidatesBySweepPriority(due)

	run.Stage = storage.RepairStageProbing
	r.saveRun(run)
	log.Info().Int("due", len(due)).Int("skipped_fresh", skipped).Str("protocol", protocolScope).Bool("repair", actions.repair).Bool("prune", actions.prune).Bool("arr_delete", actions.arrDelete).Msg("Sweep: probing")

	heal := newHealCache()
	err = r.probeAndHealCandidates(ctx, run, due, names, heal, opts, actions, budget)
	due = nil
	if err != nil {
		if errors.Is(err, context.Canceled) {
			r.finishCancelledRepairSweep(ctx, run, stopState, actions, "context cancelled during probing", names, budget)
			return
		}
		log.Error().Err(err).Msg("Sweep: probing failed")
		r.finalizeRun(run, storage.RepairRunFailed, err.Error(), "")
		return
	}
	if ctx.Err() != nil {
		r.finishCancelledRepairSweep(ctx, run, stopState, actions, "context cancelled after probing", names, budget)
		return
	}

	recordBudgetStats(run, budget)
	r.finalizeRun(run, storage.RepairRunCompleted, "", "")
	log.Info().
		Int("probed", run.Stats.Probed).
		Int("broken", run.Stats.Broken).
		Int("healthy", run.Stats.Healthy).
		Int("reacquired", run.Stats.Reacquired).
		Int("repair_failed", run.Stats.RepairFailed).
		Int("repair_skipped_unsupported", run.Stats.RepairSkippedUnsupported).
		Int("pruned", run.Stats.Pruned).
		Int("prune_skipped_not_eligible", run.Stats.PruneSkippedNotEligible).
		
		Int("arr_deleted", run.Stats.ArrDeleted).
		Int("arr_blocklisted", run.Stats.ArrBlocklisted).
		Int("arr_searched", run.Stats.ArrSearched).
		Int("arr_delete_failed", run.Stats.ArrDeleteFailed).
		Int("arr_skipped_no_link", run.Stats.ArrSkippedNoLink).
		Int("deletions", run.Stats.Deletions).
		Int("deletion_cap_skipped", run.Stats.DeletionCapSkipped).
		Msg("Sweep: completed")
}

// finishCancelledRepairSweep is reached whenever the repair sweep's context is cancelled
// (StopRun, StopSchedule, or process shutdown). When the cancellation came
// from a StopSchedule firing, the run is finalized as completed (not
// cancelled) and, when autoRepair is on, a final repair pass runs over
// whatever this repair sweep found broken among the candidates it considered
// (names). When autoRepair is off, nothing further happens to those entries.
//
// A user-initiated StopRun already wrote RepairRunCancelled to storage before
// calling cancel; finalizeRun preserves that status regardless of what's
// passed here, so the StopRun path is unaffected.
func (r *Repair) finishCancelledRepairSweep(ctx context.Context, run *storage.RepairRun, stopState *repairStopState, actions repairActions, reason string, names []string, budget *repairDeletionBudget) {
	stopped := stopState != nil && stopState.get()
	if !stopped {
		r.finalizeRun(run, storage.RepairRunCancelled, "", reason)
		return
	}

	log := r.logger.With().Str("run_id", run.ID).Logger()
	log.Info().Bool("prune", actions.prune).Bool("arr_delete", actions.arrDelete).Msg("Repair sweep: stop schedule fired; finishing run")

	if actions.destructive() && len(names) > 0 {
		// Use a fresh, un-cancelled context for the final action pass: the
		// probe pass was cut short, but the destructive pass over what's already
		// known-broken is a short, bounded set of actions and should be
		// allowed to complete. Bound it so a misbehaving Arr can't hang.
		repairCtx, cancel := context.WithTimeout(detachedRepairContext(ctx, r.parentCtx), repairStopFinalRepairTimeout)
		defer cancel()

		// Require an arr file only when ARR-DELETE is the sole destructive action;
		// PRUNE can act on entries with no arr link, so don't filter them out.
		healths, _ := r.collectBrokenHealths(names, actions.arrDelete && !actions.prune)
		if healths.Size() > 0 {
			run.Stage = storage.RepairStageRepairing
			r.saveRun(run)
			r.repairBroken(repairCtx, run, healths, actions, budget)
		}
	}

	run.CancelReason = ""
	recordBudgetStats(run, budget)
	r.finalizeRun(run, storage.RepairRunCompleted, "", "stopped by schedule: "+reason)
}

// detachedRepairContext returns a context that is not already cancelled, for
// use by the post-stop repair pass. Falls back to the repair service's parent
// context (or background) when the run's own context has already been
// cancelled.
func detachedRepairContext(runCtx, parentCtx context.Context) context.Context {
	if runCtx.Err() == nil {
		return runCtx
	}
	if parentCtx != nil {
		return parentCtx
	}
	return context.Background()
}

// probeAndHealCandidates fans out across candidates with cfg.Repair.Workers
// concurrency. Each entry then probes its own files internally with at most
// repairFilesPerEntry concurrency, so total file probes in flight = workers × 2.
//
// names gives the iteration order (see orderCandidatesBySweepPriority): g.Go is
// called in this order, so with N workers the N highest-priority candidates —
// no-verdict first, then broken, oldest-checked-first within each tier — start
// first. If the run is cut short by a StopSchedule, the candidates that didn't
// get a chance to start retain that same order for the next repair sweep.
//
// The pipeline is folded into the per-entry pass. Per dead item, components are
// independent and knob-gated:
//   - REPAIR:  probeEntry runs the debrid re-acquire inline; on success the item
//     becomes healthy and the pipeline stops for it.
//   - PRUNE:   if still dead, delete the item decypharr-side only (no arr).
//   - ARR-DELETE: independently, if still dead, delete+blocklist+re-search via the
//     arr (whether or not PRUNE ran).
//
// PRUNE and ARR-DELETE are bounded per-item by the shared per-run deletion budget.
func (r *Repair) probeAndHealCandidates(ctx context.Context, run *storage.RepairRun, candidates map[string]*candidate, names []string, heal *healCache, opts RepairRunOptions, actions repairActions, budget *repairDeletionBudget) error {
	// run.Stats has plain int fields, so a single mutex guards every mutation
	// and the saveRun that follows it.
	var runMu sync.Mutex

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, r.workers()))

	for _, name := range names {
		c := candidates[name]
		if c == nil {
			continue
		}
		g.Go(func() error {
			if gctx.Err() != nil {
				return gctx.Err()
			}

			h, attempt := r.probeEntry(gctx, run.ID, c, heal, opts, actions.repair)
			if h == nil {
				// Entry body could not be loaded between enumeration and probe;
				// skip without counting. Release any loaded body.
				c.item = nil
				c.contentMap = nil
				return nil
			}

			// Still dead after REPAIR (re-acquire): run the destructive
			// components (PRUNE and/or ARR-DELETE) for just this item, bounded by
			// the per-run deletion budget. Independent and knob-gated.
			if h.Status == storage.HealthBroken {
				r.actOnDeadEntry(gctx, run, &runMu, name, h, actions, budget)
			}

			runMu.Lock()
			run.Stats.Probed++
			run.Stats.Reacquired += attempt.reacquired
			run.Stats.RepairFailed += attempt.failed
			run.Stats.RepairSkippedUnsupported += attempt.unsupported
			switch h.Status {
			case storage.HealthHealthy:
				run.Stats.Healthy++
			case storage.HealthBroken:
				run.Stats.Broken++
			case storage.HealthUnknown, storage.HealthUnsupported:
				run.Stats.Unknown++
			}
			r.saveRun(run)
			runMu.Unlock()

			// Release this entry's body so it can be collected immediately
			// rather than lingering until the run ends.
			c.item = nil
			c.contentMap = nil
			return nil
		})
	}
	return g.Wait()
}

// probeEntry probes one entry (CHECK): marks it repairing, probes its files
// (≤2 in parallel), runs the REPAIR re-acquire on broken torrents (only when
// repair is set), then persists final health. When REPAIR heals the item its
// rolled-up status becomes healthy, which is what stops the downstream
// PRUNE/ARR-DELETE pipeline for it.
func (r *Repair) probeEntry(ctx context.Context, runID string, c *candidate, heal *healCache, opts RepairRunOptions, repair bool) (*storage.EntryHealth, repairAttempt) {
	s := r.manager.storage
	// Lazily load the entry body. Enumeration only recorded the name, so the
	// store isn't fully decoded up front. A body that cannot be READ is a skip
	// (nil tells the worker not to count it); a body that reads fine but lists
	// nothing probeable is a real, countable verdict handled below.
	if c.item == nil {
		item, err := s.GetEntryItem(c.name)
		if err != nil || item == nil {
			// The entry body could not be loaded, so nothing can be verified.
			// Skipping silently used to leave a STALE `healthy` record in place
			// forever — an entry whose content had vanished from the mount kept
			// reporting healthy with last_failed_at never set. Downgrade any
			// such record to `unknown` instead: it stops asserting health while
			// staying non-actionable (a storage read failure must never trigger
			// PRUNE/ARR-DELETE).
			r.downgradeUnverifiableHealth(c.name)
			return nil, repairAttempt{}
		}
		c.item = item
	}

	h, _ := s.GetEntryHealth(c.name)
	if h == nil {
		h = &storage.EntryHealth{EntryName: c.name}
	}
	previous := h.Status

	// Live update: surface 'repairing' before we start the probes.
	h.PreviousStatus = previous
	h.Status = storage.HealthRepairing
	h.ActiveRunID = runID
	h.Protocol = ""
	r.saveHealth(h)

	names := orderedFilenames(c.item)
	if len(names) == 0 {
		// STRUCTURAL: the entry-item read fine but lists no probeable file —
		// empty, or every file soft-deleted. Nothing behind it can be served,
		// and no probe on any future run can say otherwise.
		//
		// This used to roll up `unknown` (rollupStatus of an empty result set),
		// which is a lie twice over: it asserted "this run could not reach a
		// verdict" when the verdict is certain, and `unknown` is non-actionable,
		// so the entry sat on the indeterminate retry forever, re-probed every
		// run, stamped, and instantly returning the same non-answer with
		// last_ok_at AND last_failed_at both never set.
		return r.recordStructurallyEmptyEntry(h, c.item)
	}

	results := r.probeFiles(ctx, c.item, names, opts)
	var attempt repairAttempt
	if repair {
		// REPAIR component: re-acquire dead items across providers. On success
		// the file's result flips to healthy so the entry rolls up healthy and
		// the destructive pipeline never runs for it.
		attempt = r.autoHealResults(ctx, results, heal)
	}

	broken := r.brokenFiles(c, results)
	final := rollupStatus(results)

	h.Status = final
	h.Structural = false
	h.FileCount = len(names)
	h.BrokenFiles = broken
	h.BrokenCount = len(broken)
	h.Fingerprint = storage.EntryItemRepairFingerprint(c.item)
	h.LastCheckedAt = time.Now()
	// Stamp WHICH algorithm reached this verdict, not just when. A future probe
	// change bumps repairProbeVersion and this record becomes due on sight.
	h.ProbeVersion = repairProbeVersion
	h.NextCheckDueAt = h.LastCheckedAt.Add(r.verdictRecheckDelay(final))
	h.Dirty = false
	h.DirtyReason = ""
	h.ActiveRunID = ""
	h.PreviousStatus = ""
	if proto := firstProtocol(results); proto != "" {
		h.Protocol = proto
	}
	switch final {
	case storage.HealthHealthy:
		h.LastOKAt = h.LastCheckedAt
		h.FailureReason = ""
	case storage.HealthBroken:
		h.LastFailedAt = h.LastCheckedAt
		h.FailureReason = topReason(broken)
	default:
		// `unknown` used to record NO reason at all: brokenFiles() only
		// collects results with broken=true, so the per-file reason that
		// produced the non-verdict (usenet_probe_indeterminate,
		// provider_probe_error, usenet_client_not_configured, …) was computed
		// and then dropped. An operator staring at a library of `unknown`
		// entries had literally nothing to go on. Carry the dominant reason.
		h.FailureReason = topIndeterminateReason(results)
	}
	if repair {
		r.applyRepairAttempt(h, attempt)
	}

	r.saveHealth(h)
	return h, attempt
}

// recordStructurallyEmptyEntry persists the terminal verdict for an entry-item
// that lists no probeable file.
//
// It records `broken`, which is a DESTRUCTIVE-ELIGIBLE class, so state the
// consequence plainly: an entry with zero probeable files carries zero
// BrokenFiles, therefore BrokenCount == 0, therefore pruneEligible() is false
// and entryHealthHasArrLink() is false. Neither PRUNE nor ARR-DELETE can act on
// it — it is honest and visible without being deletable. `broken` is chosen
// over `unknown` because the statement being made is "nothing here can be
// served", which is true and permanent, not "this run could not tell".
func (r *Repair) recordStructurallyEmptyEntry(h *storage.EntryHealth, item *storage.EntryItem) (*storage.EntryHealth, repairAttempt) {
	now := time.Now()
	h.Status = storage.HealthBroken
	h.Structural = true
	h.FileCount = 0
	h.BrokenFiles = nil
	h.BrokenCount = 0
	h.Fingerprint = storage.EntryItemRepairFingerprint(item)
	h.LastCheckedAt = now
	h.ProbeVersion = repairProbeVersion
	h.LastFailedAt = now
	h.FailureReason = reasonEntryHasNoFiles
	// A structural verdict takes the FULL recheck interval, never the short
	// indeterminate retry: there is nothing to retry sooner for. IsDue also
	// stops treating it as always-due, so it leaves the every-run treadmill.
	h.NextCheckDueAt = now.Add(r.verdictRecheckDelay(storage.HealthBroken))
	h.Dirty = false
	h.DirtyReason = ""
	h.ActiveRunID = ""
	h.PreviousStatus = ""
	r.saveHealth(h)
	return h, repairAttempt{}
}

// applyRepairAttempt folds the REPAIR component's outcome onto the health
// record. LastRepairAt is stamped whenever an attempt was MADE, successful or
// not — stamping only on success is what made a failed repair and a
// never-attempted repair look identical from storage.
func (r *Repair) applyRepairAttempt(h *storage.EntryHealth, attempt repairAttempt) {
	if h == nil {
		return
	}
	if attempt.attempted {
		h.LastRepairAt = time.Now()
	}
	h.LastRepairError = attempt.err
	h.SetActionSkip(componentRepair, attempt.skip)
}

// verdictRecheckDelay schedules the next CHECK. A definitive verdict (healthy
// or broken) waits the full recheck interval; an INDETERMINATE one does not.
//
// `unknown` means this run could not reach a verdict — a provider 429, an auth
// failure, a stalled substrate. Parking that for a week would turn `unknown`
// into a permanent resting state where an entry is neither trusted nor
// re-examined, which is how a silently-dead entry hides. Retry it soon instead,
// never later than the configured interval.
func (r *Repair) verdictRecheckDelay(status storage.HealthStatus) time.Duration {
	interval := r.recheckInterval()
	if status != storage.HealthUnknown {
		return interval
	}
	if repairIndeterminateRetry < interval {
		return repairIndeterminateRetry
	}
	return interval
}

// downgradeUnverifiableHealth clears a stale positive verdict for an entry the
// probe could not load at all. It never asserts broken: the load failure says
// nothing about the content, only that this run could not check it.
func (r *Repair) downgradeUnverifiableHealth(name string) {
	h, _ := r.manager.storage.GetEntryHealth(name)
	if h == nil || h.Status == storage.HealthUnknown {
		return
	}
	h.PreviousStatus = h.Status
	h.Status = storage.HealthUnknown
	h.ActiveRunID = ""
	h.FailureReason = "entry_item_unreadable"
	h.LastCheckedAt = time.Now()
	// This IS a CHECK outcome from the current algorithm ("could not verify"),
	// and it carries the short indeterminate retry. Stamping it is what lets that
	// retry actually gate scheduling instead of the record staying permanently
	// version-stale and re-attempted every single run.
	h.ProbeVersion = repairProbeVersion
	h.NextCheckDueAt = h.LastCheckedAt.Add(r.verdictRecheckDelay(storage.HealthUnknown))
	r.saveHealth(h)
}

// probeFiles fans per-file probes inside a single entry, capped at
// repairFilesPerEntry concurrent workers.
func (r *Repair) probeFiles(ctx context.Context, item *storage.EntryItem, names []string, opts RepairRunOptions) []fileResult {
	results := make([]fileResult, len(names))
	payloadProbe := selectPayloadProbeFiles(item, names)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(repairFilesPerEntry)
	for i, name := range names {
		g.Go(func() error {
			if gctx.Err() != nil {
				results[i] = fileResult{name: name, reason: "context_cancelled"}
				return nil
			}
			results[i] = r.probeFile(gctx, item, name, opts, payloadProbe[name])
			return nil
		})
	}
	_ = g.Wait()
	return results
}

// selectPayloadProbeFiles picks which files get the expensive "move real bytes"
// verification: exactly one per infohash, deterministically the first in probe
// order.
//
// The failure modes this verification exists to catch are placement-scoped, not
// file-scoped — an entry marked bad, an unresolvable/expired link, a stubbed
// post — so one verified file per infohash is enough to condemn the entry:
// rollupStatus fails the whole entry if ANY file is broken. Verifying every file
// would multiply a sweep's cost by the file count (a 23-file NZB would pull
// ~17 MB of segments per probe), which is not viable across a large library.
//
// The trade-off, stated plainly: a single dead file inside an otherwise-healthy
// multi-file entry is still caught only by the metadata-level probe (STAT /
// hoster check) that already ran on every file — the byte-level check does not
// run on the unselected siblings.
func selectPayloadProbeFiles(item *storage.EntryItem, names []string) map[string]bool {
	if item == nil {
		return nil
	}
	selected := make(map[string]bool, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		file := item.Files[name]
		if file == nil || file.InfoHash == "" {
			continue
		}
		if _, ok := seen[file.InfoHash]; ok {
			continue
		}
		seen[file.InfoHash] = struct{}{}
		selected[name] = true
	}
	return selected
}

// isDeadContentVerdict reports whether err is a DEFINITIVE statement that the
// content is gone, as opposed to a failure of the machinery that was asked.
// Only these may flip a file to broken; everything else (auth, rate limits,
// 5xx, timeouts, cancellations, unclassified failures) must degrade to
// `unknown`, which is non-actionable.
//
// Deliberately NOT included: link.CategoryPermanent as a whole. That category
// also covers ErrUnauthorized (a 401 is an ACCOUNT problem), and treating it as
// dead content would condemn an entire library the moment a token expired.
func isDeadContentVerdict(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// A usenet 430 or an article that decoded to no payload at all.
	if nntp.IsContentMissingError(err) {
		return true
	}
	if errors.Is(err, customerror.HosterUnavailableError) || errors.Is(err, debridTypes.EmptyDownloadLinkError) {
		return true
	}
	// One predicate, shared with the SERVE path. handlePropfind drops a child
	// from a collection listing on exactly this condition; the probe condemns a
	// file on exactly this condition. They were separate expressions once, and
	// they drifted: PROPFIND hid every child of an entry while the probe recorded
	// "no verdict" for the same files. See customerror.IsContentPermanentlyGone.
	return customerror.IsContentPermanentlyGone(err)
}

// deadContentReason names WHICH definitive verdict condemned a file, so the
// stored failure_reason distinguishes "the provider says the articles are gone"
// from "the hoster says the content is gone" without an operator having to
// re-run anything.
func deadContentReason(err error, fallback string) string {
	var custom *customerror.Error
	if errors.As(err, &custom) && custom.Code != "" {
		return custom.Code
	}
	return fallback
}

// countingWriter counts payload bytes without retaining them. Probe reads must
// prove bytes flowed, not keep them.
type countingWriter struct{ n int64 }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	return len(p), nil
}

// probeFile checks one file. NZB probes use usenet.CheckFile. Torrent probes
// use the provider CheckFile endpoint unless this run requests unrestrict-link
// probing.
func (r *Repair) probeFile(ctx context.Context, item *storage.EntryItem, name string, opts RepairRunOptions, payloadProbe bool) fileResult {
	file := item.Files[name]
	res := fileResult{name: name}

	if file == nil || file.InfoHash == "" {
		// A listed file with no infohash cannot be resolved to any placement or
		// NZB, so the read path can never serve it. That is a durable data
		// defect, not a transient substrate failure: it must be broken, not
		// `unknown`. Leaving it unknown let entries whose content is entirely
		// absent from the mount roll up healthy off their surviving siblings.
		res.broken = true
		res.reason = "missing_infohash"
		return res
	}
	res.infoHash = file.InfoHash

	entry, err := r.manager.GetEntry(file.InfoHash)
	if err != nil || entry == nil {
		// Same reasoning: the entry item still lists this file, but the entry
		// row it points at is gone, so there is no placement, no link and no
		// segment map behind it. Nothing can serve it.
		res.broken = true
		res.reason = "entry_not_found"
		return res
	}
	res.protocol = entry.Protocol
	if !repairProtocolMatches(r.effectiveProtocolScope(opts), entry.Protocol) {
		res.reason = "protocol_skipped"
		return res
	}

	if entry.IsNZB() {
		return r.probeNZBFile(ctx, entry, name, res, payloadProbe)
	}
	return r.probeTorrentFile(ctx, entry, file, name, res, opts, payloadProbe)
}

func (r *Repair) probeNZBFile(ctx context.Context, entry *storage.Entry, name string, res fileResult, payloadProbe bool) fileResult {
	if r.manager.usenet == nil {
		res.reason = "usenet_client_not_configured"
		return res
	}
	err := r.manager.usenet.CheckFile(ctx, entry.InfoHash, name)
	switch {
	case err == nil:
		// The STAT sample passed. That only proves the message ids exist; it
		// moved ZERO bytes, so it cannot see a post that is present but carries
		// no decodable payload. Fall through to the byte-level verification.
	case errors.Is(err, usenet.ErrNZBNotFound):
		// THE SEGMENT MAP IS GONE FROM DISK — decypharr lost its own index, which
		// says NOTHING about whether the content is still on the providers.
		//
		// This is checked FIRST and given its own reason on purpose. It used to
		// fall through to `usenet_probe_error`, sharing a single reason code with
		// the genuinely-dead case below; separating them is the whole point. A
		// lost index must never be able to masquerade as dead content, because
		// dead content is deletable and a missing local file is not a reason to
		// delete anything. Non-actionable: rolls up to `unknown`.
		res.reason = "usenet_meta_missing"
		return res
	case errors.Is(err, usenet.ErrAvailabilityIndeterminate):
		// The substrate failed, the content did not. Neither healthy nor broken
		// — rolls up to `unknown`, which no destructive component acts on.
		//
		// ORDERED AHEAD OF THE DEAD-CONTENT CASES DELIBERATELY. The two are
		// disjoint today (classifyAvailability formats the underlying error with
		// %v, not %w, so an indeterminate never wraps a typed content verdict),
		// but if that ever changes the SAFE outcome must be the one that wins by
		// default. Mis-reading a collapsed substrate as dead content deletes data;
		// mis-reading dead content as indeterminate only delays a cleanup.
		res.reason = "usenet_probe_indeterminate"
		return res
	case errors.Is(err, customerror.UsenetSegmentMissingError):
		res.broken = true
		res.reason = "usenet_segment_missing"
		return res
	case isDeadContentVerdict(err):
		// A DEFINITIVE statement that the content is gone — today, the durable
		// IsDeleted flag, which only a 430/423 or a zero-payload article can set.
		// The serve path already answers 410 Gone and hides this file from its
		// parent listing; before this case existed the probe called the identical
		// condition `usenet_probe_error` and left the entry non-actionable, so an
		// entry that served an EMPTY directory to every client was recorded as
		// "could not reach a verdict" rather than dead.
		res.broken = true
		res.reason = deadContentReason(err, "usenet_content_missing")
		return res
	default:
		// Everything unclassified — including a file with a genuinely empty
		// segment list — stays here: never healthy, never broken.
		res.reason = "usenet_probe_error"
		return res
	}

	if !payloadProbe {
		res.healthy = true
		return res
	}
	return r.verifyPayload(ctx, res, "usenet", func(probeCtx context.Context) (int64, error) {
		return r.manager.usenet.CheckFileReadable(probeCtx, entry.InfoHash, name, repairProbeReadBytes, repairProbeSamplePoints)
	})
}

func (r *Repair) probeTorrentFile(ctx context.Context, entry *storage.Entry, file *storage.File, name string, res fileResult, opts RepairRunOptions, payloadProbe bool) fileResult {
	// decypharr's own read path refuses a bad-marked entry outright ("can't
	// repair X since it's been marked as bad") before it even asks the
	// provider, so EVERY read of it fails. Bad is durable and only set after
	// re-insertion was already exhausted. No metadata-level probe — not the
	// hoster availability call, certainly not a HEAD served from cached
	// metadata in half a millisecond — ever notices, which is exactly how a
	// 100%-unreadable entry recorded healthy with broken_count 0.
	if entry.Bad {
		res.broken = true
		res.reason = "entry_marked_bad"
		return res
	}
	client := r.manager.ProviderClient(entry.ActiveProvider)
	if client == nil {
		res.reason = "provider_client_not_found"
		return res
	}
	if opts.UnrestrictLink {
		return r.probeTorrentFileByUnrestrict(ctx, entry, file, name, res, client, payloadProbe)
	}
	if !client.SupportsCheck() {
		res.reason = "provider_check_unsupported"
		return res
	}
	link := linkOf(entry, name)
	if link == "" {
		res.broken = true
		res.reason = "missing_provider_link"
		return res
	}
	err := client.CheckFile(ctx, file.InfoHash, link)
	switch {
	case err == nil:
		// Availability claimed. Prove it with bytes below.
	case errors.Is(err, customerror.HosterUnavailableError):
		res.broken = true
		res.reason = "hoster_unavailable"
		return res
	case errors.Is(err, debridTypes.ErrAvailabilityIndeterminate):
		// 401/403/429/5xx/transport: the provider never gave a verdict about the
		// content. Mirrors usenet.ErrAvailabilityIndeterminate — never healthy,
		// never broken, and re-checked soon rather than parked for a full
		// recheck interval (see probeEntry).
		res.reason = "provider_probe_indeterminate"
		return res
	default:
		res.reason = "provider_probe_error"
		return res
	}

	if !payloadProbe {
		res.healthy = true
		return res
	}
	return r.verifyPayload(ctx, res, "provider", func(probeCtx context.Context) (int64, error) {
		return r.readTorrentPayload(probeCtx, entry, file, name, client)
	})
}

func (r *Repair) probeTorrentFileByUnrestrict(ctx context.Context, entry *storage.Entry, file *storage.File, name string, res fileResult, client debrid.Client, payloadProbe bool) fileResult {
	placement := entry.GetActiveProvider()
	if placement == nil {
		res.reason = "placement_not_found"
		return res
	}
	placementFile := placement.Files[name]
	if placementFile == nil {
		res.reason = "placement_file_not_found"
		return res
	}
	if placementFile.Link == "" && placementFile.Id == "" {
		res.broken = true
		res.reason = "missing_provider_link"
		return res
	}

	downloadLink, err := client.GetDownloadLink(placement.ID, torrentProbeFile(placementFile, file))
	if err == nil && !downloadLink.Empty() {
		if !payloadProbe {
			res.healthy = true
			return res
		}
		// An unrestricted link that resolves still proves nothing about the
		// bytes behind it; fetch a bounded window of them.
		return r.verifyPayload(ctx, res, "provider", func(probeCtx context.Context) (int64, error) {
			return r.readPayloadFromURL(probeCtx, entry, file, name, downloadLink.DownloadLink)
		})
	}
	if errors.Is(err, debridTypes.ErrAvailabilityIndeterminate) {
		res.reason = "provider_probe_indeterminate"
		return res
	}
	if err == nil || errors.Is(err, debridTypes.EmptyDownloadLinkError) || errors.Is(err, customerror.HosterUnavailableError) {
		res.broken = true
		if errors.Is(err, customerror.HosterUnavailableError) {
			res.reason = "hoster_unavailable"
		} else {
			res.reason = "empty_download_link"
		}
		return res
	}
	res.reason = "unrestrict_link_error"
	return res
}

func torrentProbeFile(placementFile *storage.ProviderFile, file *storage.File) *debridTypes.File {
	return &debridTypes.File{
		Id:        placementFile.Id,
		Link:      placementFile.Link,
		Path:      placementFile.Path,
		Name:      file.Name,
		Size:      file.Size,
		ByteRange: file.ByteRange,
		Deleted:   file.Deleted,
	}
}

// verifyPayload runs one bounded real-payload read and folds its outcome into
// the three-way probe discipline:
//
//	bytes transferred            -> healthy
//	definitive dead-content      -> broken
//	zero bytes without an error  -> broken (a "successful" empty body is the
//	                                exact shape of the bug this exists to catch)
//	anything else (transport,    -> unknown; NEVER healthy, and never actionable
//	timeout, cancellation, auth)
func (r *Repair) verifyPayload(ctx context.Context, res fileResult, kind string, read func(context.Context) (int64, error)) fileResult {
	probeCtx, cancel := context.WithTimeout(ctx, repairProbeReadTimeout)
	defer cancel()

	n, err := read(probeCtx)
	switch {
	case err == nil && n > 0:
		res.healthy = true
	case isDeadContentVerdict(err):
		res.broken = true
		res.reason = kind + "_payload_missing"
	case err == nil:
		res.broken = true
		res.reason = kind + "_payload_empty"
	default:
		res.reason = kind + "_payload_indeterminate"
	}
	return res
}

// readTorrentPayload resolves a download link directly through the provider
// client and transfers a bounded window of real bytes.
//
// It deliberately does NOT go through Manager.Stream / linkService.GetLink: that
// path triggers re-insertion cycles as a side effect of a failed link, which a
// read-only CHECK must never do to a whole library.
func (r *Repair) readTorrentPayload(ctx context.Context, entry *storage.Entry, file *storage.File, name string, client debrid.Client) (int64, error) {
	placement := entry.GetActiveProvider()
	if placement == nil {
		return 0, fmt.Errorf("placement not found for entry %s", entry.InfoHash)
	}
	placementFile := placement.Files[name]
	if placementFile == nil {
		return 0, fmt.Errorf("placement file %q not found", name)
	}
	downloadLink, err := client.GetDownloadLink(placement.ID, torrentProbeFile(placementFile, file))
	if err != nil {
		return 0, err
	}
	if downloadLink.Empty() {
		return 0, debridTypes.EmptyDownloadLinkError
	}
	return r.readPayloadFromURL(ctx, entry, file, name, downloadLink.DownloadLink)
}

// readPayloadFromURL transfers a bounded window at each sampled offset through
// the same HTTP streaming code a client read uses, so a definitive 410 or a
// truncated/empty body is classified identically to a real playback failure.
//
// It samples a ladder of offsets rather than just the head: a file can serve its
// head perfectly and be dead mid-file (measured in production at the 75% mark),
// so a head-only read would report such a file healthy.
func (r *Repair) readPayloadFromURL(ctx context.Context, entry *storage.Entry, file *storage.File, name, url string) (int64, error) {
	if url == "" {
		return 0, debridTypes.EmptyDownloadLinkError
	}
	target := file
	if entry.Files != nil {
		if authoritative, ok := entry.Files[name]; ok && authoritative != nil {
			target = authoritative
		}
	}
	if target == nil {
		return 0, fmt.Errorf("file %q not found in entry %s", name, entry.InfoHash)
	}
	if target.Size <= 0 {
		return 0, fmt.Errorf("file %q has no readable bytes", name)
	}
	window := int64(repairProbeReadBytes)
	if window > target.Size {
		window = target.Size
	}

	var total int64
	for _, off := range usenet.SampleOffsets(target.Size, window, repairProbeSamplePoints) {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		counter := &countingWriter{}
		accepted := false
		err := r.manager.streamHTTPURL(ctx, url, entry.ActiveProvider, target, name, off, off+window-1, true, counter,
			func(*StreamMetadata) error {
				// Reached only after the upstream status was accepted, i.e. the
				// server promised this range.
				accepted = true
				return nil
			})
		if accepted && counter.n == 0 {
			// The upstream committed a success status for the range and then
			// delivered NOTHING. That is the exact signature of a dead region
			// (curl reports CURLE_PARTIAL_FILE), not a transport hiccup: a
			// dropped connection normally fails before the status or after some
			// bytes. Treat it as definitively gone rather than healthy.
			return total, customerror.NewContentGoneError(
				fmt.Errorf("file %q returned a success status with zero bytes at offset %d", name, off))
		}
		if err != nil {
			return total + counter.n, err
		}
		total += counter.n
	}
	return total, nil
}

// autoHealResults walks every broken infohash and tries one re-insert per
// infohash (singleflighted). On success, every file in that infohash group is
// marked healthy.
//
// The protocol gate lives on the ENTRY, not on the fileResult. The old filter
// (`res.protocol != config.ProtocolTorrent`) was wrong in both directions: it
// silently excluded every nzb entry — so an nzb could never be repaired and,
// worse, nothing said so — and it also excluded TORRENTS whose fileResult never
// got a protocol stamped (the `entry_not_found` path returns before
// `res.protocol = entry.Protocol`). Grouping by infohash and asking the
// authoritative Entry.CanBeFixed() fixes both and turns every exclusion into a
// counted, named skip instead of a bare `continue`.
//
// nzb is genuinely NOT repairable here, and that is a statement about usenet,
// not an oversight: REPAIR means "re-insert the item across debrid providers"
// (Fixer.FixTorrent, gated by Entry.CanBeFixed() == IsTorrent()). A usenet
// entry has no placement to re-insert; its articles either still exist on the
// news servers or they do not. Re-parsing the staged NZB asks the same
// providers for the same message ids and cannot resurrect a 430 or a
// payload-less article. A dead nzb is recovered by ARR-DELETE (a fresh NZB from
// the indexer) or PRUNE.
func (r *Repair) autoHealResults(ctx context.Context, results []fileResult, heal *healCache) repairAttempt {
	byHash := make(map[string][]int)
	noHash := false
	for i, res := range results {
		if !res.broken {
			continue
		}
		if res.infoHash == "" {
			noHash = true
			continue
		}
		byHash[res.infoHash] = append(byHash[res.infoHash], i)
	}
	if len(byHash) == 0 {
		if noHash {
			return repairAttempt{skip: reasonRepairNoInfohash}
		}
		return repairAttempt{}
	}

	var attempt repairAttempt
	for infoHash, indices := range byHash {
		if !r.tryReacquireInfoHash(ctx, infoHash, "", heal, &attempt) {
			continue
		}
		for _, i := range indices {
			results[i].broken = false
			results[i].healthy = true
			results[i].reason = "repaired"
		}
	}
	return attempt
}

// tryReacquireInfoHash runs the REPAIR component for ONE infohash and folds the
// outcome into attempt. It is the single definition of "what REPAIR can act
// on", shared by the inline sweep path (autoHealResults) and the batch
// no-reprobe path (reacquireDeadEntry) so the two cannot drift — they had
// already drifted into two different, and differently wrong, protocol filters.
//
// heal may be nil (the batch path has no per-run memo); healCache.do handles it.
// Every negative outcome is counted and named on attempt rather than dropped.
func (r *Repair) tryReacquireInfoHash(ctx context.Context, infoHash, entryName string, heal *healCache, attempt *repairAttempt) bool {
	log := r.logger.With().Str("component", "REPAIR").Str("infohash", infoHash).Logger()
	if entryName != "" {
		log = log.With().Str("entry", entryName).Logger()
	}

	entry, err := r.manager.GetEntry(infoHash)
	if err != nil || entry == nil {
		attempt.unsupported++
		attempt.skip = reasonRepairEntryMissing
		log.Debug().Msg("REPAIR: entry row is gone; nothing to re-acquire")
		return false
	}
	if !entry.CanBeFixed() {
		attempt.unsupported++
		attempt.skip = reasonRepairUnsupportedProtocol + ":" + string(entry.Protocol)
		log.Debug().Str("protocol", string(entry.Protocol)).
			Msg("REPAIR: protocol has no re-acquire path (only torrents can be re-inserted); leaving dead for PRUNE/ARR-DELETE")
		return false
	}

	attempt.attempted = true
	if err := heal.do(infoHash, func() error { return r.manager.ReinsertEntry(ctx, entry) }); err != nil {
		// A FAILED repair must be counted and named. As a bare `continue` it
		// produced reacquired=0 AND repair_failed=0 AND last_repair_at=never,
		// all at once — a run record in which a failed attempt and no attempt
		// at all look exactly the same.
		attempt.failed++
		attempt.err = err.Error()
		log.Debug().Err(err).Msg("REPAIR: re-acquire failed; leaving dead for PRUNE/ARR-DELETE")
		return false
	}
	attempt.reacquired++
	return true
}

// brokenFiles flattens broken results into BrokenFile records, attaching Arr
// identifiers so the repair pass can delete + re-search.
func (r *Repair) brokenFiles(c *candidate, results []fileResult) []storage.BrokenFile {
	out := make([]storage.BrokenFile, 0)
	for _, res := range results {
		if !res.broken {
			continue
		}
		bf := storage.BrokenFile{
			EntryName: c.name,
			FileName:  res.name,
			InfoHash:  res.infoHash,
			Protocol:  res.protocol,
			Reason:    res.reason,
		}
		if file, ok := c.item.Files[res.name]; ok && file != nil {
			bf.Size = file.Size
			if bf.InfoHash == "" {
				bf.InfoHash = file.InfoHash
			}
		}
		if cf, ok := c.contentMap[res.name]; ok {
			bf.ArrName = c.arrName
			bf.ArrKind = c.arrKind
			bf.MediaID = cf.Id
			bf.EpisodeID = cf.EpisodeId
			bf.ArrFileID = cf.FileId
			bf.TargetPath = cf.TargetPath
			bf.SourcePath = cf.Path
			if bf.Size == 0 {
				bf.Size = cf.Size
			}
		}
		out = append(out, bf)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FileName < out[j].FileName })
	return out
}

// rollupStatus collapses per-file results into a single EntryHealth status.
// Any broken file fails the entry; otherwise healthy wins over unknown.
func rollupStatus(results []fileResult) storage.HealthStatus {
	if len(results) == 0 {
		return storage.HealthUnknown
	}
	hasBroken, hasHealthy := false, false
	for _, res := range results {
		if res.broken {
			hasBroken = true
		}
		if res.healthy {
			hasHealthy = true
		}
	}
	switch {
	case hasBroken:
		return storage.HealthBroken
	case hasHealthy:
		return storage.HealthHealthy
	default:
		return storage.HealthUnknown
	}
}

func firstProtocol(results []fileResult) config.Protocol {
	for _, res := range results {
		if res.protocol != "" {
			return res.protocol
		}
	}
	return ""
}

// repairBroken runs the selected components over a set of already-known dead
// entries without reprobing. Used by the batch entry-points (FixBroken,
// media/entry rechecks) and the StopSchedule post-stop pass. Per entry the
// components apply in the same order the sweep uses:
//
//	REPAIR  — re-acquire across providers; on success the item is servable and
//	          the destructive components are skipped for it.
//	PRUNE / ARR-DELETE — if still dead, the destructive pass via actOnDeadEntry.
//
// The scheduled sweep instead applies REPAIR inline during probing (probeEntry)
// and only reaches actOnDeadEntry for what's still dead.
func (r *Repair) repairBroken(ctx context.Context, run *storage.RepairRun, healths *xsync.Map[string, *storage.EntryHealth], actions repairActions, budget *repairDeletionBudget) {
	var statsMu sync.Mutex
	healths.Range(func(name string, h *storage.EntryHealth) bool {
		if ctx != nil && ctx.Err() != nil {
			return false
		}
		if actions.repair && r.reacquireDeadEntry(ctx, run, &statsMu, name, h) {
			return true
		}
		r.actOnDeadEntry(ctx, run, &statsMu, name, h, actions, budget)
		return true
	})
}

// reacquireDeadEntry is the REPAIR component on the batch (no-reprobe) path used
// by FixBroken and the stop-schedule post-stop pass: it re-inserts the dead
// item's broken infohashes across providers. On success (every broken infohash
// re-acquired) it clears the entry's broken health so the next CHECK confirms
// it, records the Reacquired outcome, and returns true so the caller skips the
// destructive components for this item. Non-destructive: it never consumes a
// deletion slot and makes ZERO arr calls.
//
// THIS IS WHERE `/api/repair/fix` WENT INERT. The old selector was
//
//	if bf.Protocol == config.ProtocolTorrent && bf.InfoHash != "" { ... }
//	if len(hashes) == 0 { return false }
//
// so a FixBroken run with REPAIR selected over broken entries that are nzb — or
// whose BrokenFile.Protocol was never stamped, which is what the
// `missing_infohash` / `entry_not_found` probe paths produce — collected zero
// hashes and returned false WITHOUT a counter, a log line or a reason. Control
// then fell into actOnDeadEntry, which with prune=false and regrab=false
// returned just as silently. The result is a completed run over N candidates
// with every counter at zero and started_at == completed_at.
//
// Now: the protocol capability is asked of the authoritative Entry
// (CanBeFixed), every exclusion is counted and named, and a failed attempt
// stamps LastRepairAt so it is distinguishable from no attempt at all.
func (r *Repair) reacquireDeadEntry(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, name string, h *storage.EntryHealth) bool {
	if h == nil || h.Status != storage.HealthBroken {
		return false
	}
	hashes := make(map[string]struct{})
	for _, bf := range h.BrokenFiles {
		if bf.InfoHash != "" {
			hashes[bf.InfoHash] = struct{}{}
		}
	}

	var attempt repairAttempt
	if len(hashes) == 0 {
		attempt.skip = reasonRepairNoInfohash
	}
	// The entry is only healed when EVERY broken infohash re-acquires; a
	// partial success leaves it dead for the destructive components.
	healed := len(hashes) > 0
	for hash := range hashes {
		if ctx != nil && ctx.Err() != nil {
			healed = false
			break
		}
		if !r.tryReacquireInfoHash(ctx, hash, name, nil, &attempt) {
			healed = false
		}
	}

	if statsMu != nil && run != nil && (attempt.failed > 0 || attempt.unsupported > 0 || (healed && attempt.reacquired > 0)) {
		statsMu.Lock()
		run.Stats.RepairFailed += attempt.failed
		run.Stats.RepairSkippedUnsupported += attempt.unsupported
		if healed && attempt.reacquired > 0 {
			run.Stats.Reacquired++
		}
		r.saveRun(run)
		statsMu.Unlock()
	}

	if !healed || attempt.reacquired == 0 {
		// Record the attempt on the health record BEFORE returning so the
		// failure is durable, then let the destructive components have their
		// turn. markBrokenHealthCleared is not reached, so this save is the
		// only thing that writes the reason.
		r.applyRepairAttempt(h, attempt)
		r.saveHealth(h)
		return false
	}

	r.logger.Info().Str("component", "REPAIR").Str("entry", name).Msg("REPAIR: re-acquired dead item across providers")
	r.applyRepairAttempt(h, attempt)
	r.markBrokenHealthCleared(h, time.Now())
	return true
}

// entryHealthHasArrLink reports whether a dead entry carries the arr
// identifiers ARR-DELETE needs to delete + re-search. Best-effort: an entry with
// no resolved arr link simply can't be ARR-DELETE'd (logged, then skipped).
func entryHealthHasArrLink(h *storage.EntryHealth) bool {
	for _, bf := range h.BrokenFiles {
		if bf.ArrName != "" && bf.ArrFileID != 0 {
			return true
		}
	}
	return false
}

// pruneIneligibleReason returns "" when PRUNE may delete this entry
// decypharr-side, otherwise the machine-readable reason it may not.
//
// Only fully-broken entries (every file dead) are deletable, so a
// partially-broken entry keeps its healthy files' symlinks. Requires at least
// one infohash to delete by. THIS POLICY IS CORRECT AND UNCHANGED — refusing to
// delete a 13-file entry because 5 of its files are dead is the right call.
//
// What was wrong is that the refusal was invisible. A run over three
// partially-broken entries reported pruned=0 with no counter, no reason and
// nothing in the run record: identical to PRUNE being broken. The reason now
// lands on the health record (EntryHealth.ActionSkips) and the count lands on
// the run (RepairRunStats.PruneSkippedNotEligible).
func pruneIneligibleReason(h *storage.EntryHealth) string {
	if h == nil || h.BrokenCount == 0 {
		return reasonPruneNoBrokenFiles
	}
	if h.BrokenCount != h.FileCount {
		return reasonPrunePartialEntry
	}
	for _, bf := range h.BrokenFiles {
		if bf.InfoHash != "" {
			return ""
		}
	}
	return reasonPruneNoInfohash
}

// pruneEligible reports whether PRUNE may delete this entry decypharr-side.
func pruneEligible(h *storage.EntryHealth) bool { return pruneIneligibleReason(h) == "" }

// actOnDeadEntry runs the destructive pipeline for one dead item after CHECK
// found it dead and REPAIR (if enabled) failed to re-acquire it. PRUNE and
// ARR-DELETE are INDEPENDENT and individually knob-gated — ARR-DELETE is not gated
// behind PRUNE and vice-versa. Both are bounded by the shared per-run deletion
// budget: one slot is reserved per dead entry that undergoes any destructive
// action this run (a provider-wide false "unavailable" therefore can't mass-act
// on the whole due set in one run). A nil / unlimited budget (single-item
// paths) always grants. statsMu guards run.Stats across concurrent entries.
//
// INVARIANT: PRUNE (pruneDeadEntry) makes ZERO arr API calls — only ARR-DELETE
// (arrDeleteDeadEntry) touches the arr.
func (r *Repair) actOnDeadEntry(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, name string, h *storage.EntryHealth, actions repairActions, budget *repairDeletionBudget) {
	if h == nil || h.Status != storage.HealthBroken {
		return
	}

	// Every decline below is counted on the run and named on the health record.
	// A destructive component that correctly refuses to act is otherwise
	// indistinguishable from one that is broken: the operator sees pruned=0 and
	// has no way to learn whether PRUNE declined, errored, or never ran.
	pruneSkip := ""
	if actions.prune {
		pruneSkip = pruneIneligibleReason(h)
	}
	arrSkip := ""
	if actions.arrDelete && !entryHealthHasArrLink(h) {
		arrSkip = reasonArrNoLink
	}

	wantArrDelete := actions.arrDelete && arrSkip == ""
	wantPrune := actions.prune && pruneSkip == ""

	if arrSkip != "" {
		r.logger.Debug().Str("component", "ARR-DELETE").Str("entry", name).Msg("ARR-DELETE: no arr link resolved for dead item; cannot re-grab it")
	}
	if pruneSkip != "" {
		r.logger.Debug().Str("component", "PRUNE").Str("entry", name).
			Str("reason", pruneSkip).
			Int("broken_files", h.BrokenCount).Int("total_files", h.FileCount).
			Msg("PRUNE: declined to delete dead item decypharr-side (partial entries keep their surviving files' symlinks)")
	}

	if !wantArrDelete && !wantPrune {
		// Nothing destructive to do (no arr link and not prune-eligible): don't
		// consume a deletion slot. Non-destructive probes/re-inserts never count
		// against the cap.
		r.recordActionSkips(run, statsMu, h, pruneSkip, arrSkip)
		return
	}

	// Reserve one destructive slot for this dead entry (covers ARR-DELETE and/or
	// PRUNE). If the run's cap is exhausted, skip ALL destructive actions for
	// this entry and leave it dead so it is re-picked next run.
	if !budget.reserve() {
		if wantPrune {
			pruneSkip = reasonDeletionCapReached
		}
		if wantArrDelete {
			arrSkip = reasonDeletionCapReached
		}
		r.recordActionSkips(run, statsMu, h, pruneSkip, arrSkip)
		return
	}
	r.recordActionSkips(run, statsMu, h, pruneSkip, arrSkip)

	// ARR-DELETE first (arr-side), then PRUNE (decypharr-side). Both read only from
	// the in-memory health record, so order doesn't couple them; ARR-DELETE runs
	// whether or not PRUNE deletes the entry.
	if wantArrDelete {
		r.arrDeleteDeadEntry(ctx, run, statsMu, name, h, actions)
	}
	if wantPrune && r.pruneDeadEntry(name, h) {
		statsMu.Lock()
		run.Stats.Pruned++
		r.saveRun(run)
		statsMu.Unlock()
	}
}

// recordActionSkips persists why PRUNE / ARR-DELETE declined to act on this entry
// and counts the decline on the run. An empty reason CLEARS that component's
// stale skip, so the health record always describes the most recent run that
// considered the entry rather than accumulating history.
//
// A deletion-cap skip is deliberately NOT counted here: it already has its own
// run counter (DeletionCapSkipped, from the budget) and counting it as an
// eligibility decline would misreport a capped run as a policy refusal. The
// reason still lands on the health record so a per-entry lookup explains it.
func (r *Repair) recordActionSkips(run *storage.RepairRun, statsMu *sync.Mutex, h *storage.EntryHealth, pruneSkip, arrSkip string) {
	if h == nil {
		return
	}
	h.SetActionSkip(componentPrune, pruneSkip)
	h.SetActionSkip(componentArrDelete, arrSkip)

	countPrune := pruneSkip != "" && pruneSkip != reasonDeletionCapReached
	countRegrab := arrSkip != "" && arrSkip != reasonDeletionCapReached
	if (countPrune || countRegrab) && run != nil && statsMu != nil {
		statsMu.Lock()
		if countPrune {
			run.Stats.PruneSkippedNotEligible++
		}
		if countRegrab {
			run.Stats.ArrSkippedNoLink++
		}
		r.saveRun(run)
		statsMu.Unlock()
	}
	r.saveHealth(h)
}

// arrDeleteDeadEntry is the ARR-DELETE component: the ONLY arr-coupled action. For a
// dead item it deletes the arr file record, and — each only when separately
// enabled — blocklists the grab and/or triggers a search, per arr. It does not
// delete anything decypharr-side and does not verify the outcome
// (SearchMissing/MarkHistoryFailed only queue work; the next sweep verifies).
// statsMu guards run.Stats across concurrent entries.
func (r *Repair) arrDeleteDeadEntry(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, name string, h *storage.EntryHealth, actions repairActions) {
	// An entry's broken files normally all belong to one arr, but a merged
	// candidate can span more — group defensively.
	byArr := make(map[string][]arr.ContentFile)
	for _, bf := range h.BrokenFiles {
		if bf.ArrName == "" || bf.ArrFileID == 0 {
			continue
		}
		byArr[bf.ArrName] = append(byArr[bf.ArrName], arr.ContentFile{
			Id:        bf.MediaID,
			EpisodeId: bf.EpisodeID,
			FileId:    bf.ArrFileID,
			Name:      bf.FileName,
			Path:      bf.SourcePath,
			Size:      bf.Size,
			IsBroken:  true,
		})
	}
	if len(byArr) == 0 {
		return
	}

	for arrName, files := range byArr {
		if ctx != nil && ctx.Err() != nil {
			return
		}
		a := r.manager.arr.Get(arrName)
		if a == nil {
			continue
		}
		if r.repairArrFiles(ctx, run, statsMu, a, files, actions) {
			// Report exactly what was done. The old line asserted all three acts
			// unconditionally, so it claimed a blocklist and a re-search on runs
			// that performed neither — and after the split, neither is the
			// default. A log line that overstates the action is worse than none:
			// it is the record the operator reasons from afterwards.
			r.logger.Info().Str("component", "ARR-DELETE").Str("entry", name).Str("arr", arrName).
				Int("files", len(files)).
				Bool("blocklisted", actions.wantBlocklist()).
				Bool("searched", actions.wantSearch()).
				Msg("ARR-DELETE: deleted arr file records")
		}
	}
	h.LastRepairAt = time.Now()
	r.saveHealth(h)
}

// pruneDeadEntry is the PRUNE component: a decypharr-side-ONLY deletion. It
// removes the provider placements, the symlink/download folder (via the guarded
// deleteEntryFiles — the category-dir data-loss guard stays in the path), and
// the db entry through DeleteEntry(hash, true). It makes ZERO arr API calls, and
// there is no decypharr->arr coupling here. Only fully-broken entries reach here
// (pruneEligible), so a partially-broken entry keeps its healthy files. Returns
// true when at least one infohash was deleted decypharr-side, so the caller can
// record the PRUNE outcome.
//
// WHAT THIS DOES NOT DO — this comment previously claimed "the arr keeps the item
// MONITORED so its own next disk scan sees the file missing and re-searches".
// THAT IS FALSE. Traced in Sonarr/Radarr source: MediaFileTableCleanupService.Clean
// builds its on-disk key set from Directory.EnumerateFiles and compares with
// PathEqualityComparer — a pure string comparison, no stat, no target resolution.
// A DANGLING SYMLINK STILL ENUMERATES as a directory entry, so it is in the set,
// so the file row is KEPT. The arr therefore never notices, and MissingFromDisk
// never fires for it. Measured on the production host: 1,707 dangling symlinks
// against 17 MissingFromDisk events in Radarr's entire history, and those 17 were
// real deletions rather than dangling links. Independent clincher: Clean runs at
// DiskScanService.cs:134, BEFORE the size loop at :149 that throws
// FileNotFoundException on a dead link — for that throw to happen at all, the row
// must have survived Clean.
//
// So after PRUNE the arr is left holding a dangling symlink indefinitely. For an
// entry WITH an arr link, ARR-DELETE (when enabled) is what actually clears the arr
// side. For the regrab_no_arr_link population there is currently NO path that
// cleans it up from either side — closing that is arr-side work, not a reason to
// add coupling here.
func (r *Repair) pruneDeadEntry(name string, h *storage.EntryHealth) bool {
	hashes := make(map[string]struct{})
	for _, bf := range h.BrokenFiles {
		if bf.InfoHash != "" {
			hashes[bf.InfoHash] = struct{}{}
		}
	}
	if len(hashes) == 0 {
		return false
	}

	deleted := false
	for hash := range hashes {
		if err := r.manager.DeleteEntry(hash, true); err != nil {
			r.logger.Warn().Err(err).Str("component", "PRUNE").Str("entry", name).Str("infohash", hash).Msg("PRUNE: failed to delete dead entry decypharr-side")
			continue
		}
		deleted = true
		r.logger.Info().Str("component", "PRUNE").Str("entry", name).Str("infohash", hash).Msg("PRUNE: deleted dead entry decypharr-side (no arr call; any arr symlink is left dangling and the arr will NOT self-heal it)")
	}
	if !deleted {
		return false
	}

	// The entry is gone decypharr-side. Clear its now-orphan health record so
	// the dashboard stops counting it broken. If the item somehow still exists
	// (multi-infohash edge), just stamp the repair time instead.
	if _, err := r.manager.storage.GetEntryItem(name); err != nil {
		_ = r.manager.storage.DeleteEntryHealth(name)
	} else {
		h.LastRepairAt = time.Now()
		r.saveHealth(h)
	}
	return true
}

// repairArrFiles performs ARR-DELETE's arr-side work for one Arr: it ALWAYS deletes
// the broken file records, and then — only when separately enabled — searches for
// replacements and/or blocklists the grabs. Returns true when the delete
// succeeded (so the caller may consider the files handled). Concurrency is
// bounded by the sweep's worker count; Sonarr/Radarr handle that many in-flight
// API calls fine, and the actual search/grab work is paced by the Arr's own
// command queue regardless of how the calls arrive.
//
// THE THREE ACTS ARE INDEPENDENT, AND THE ARR ALWAYS ALLOWED THEM TO BE.
// `DELETE /api/v3/moviefile/{id}` takes no blocklist parameter, and a search can
// be dispatched immediately afterwards with no history or blocklist interaction
// whatsoever. The coupling was decypharr's alone.
//
// It arose from a shortcut: files whose grab-history record still existed were
// blocklisted via MarkHistoryFailed and NOT searched, because Sonarr/Radarr
// auto-re-search on a failed history row when "Redownload Failed" is on (the
// default). SearchMissing was called only for the leftovers. So the blocklist —
// a global, permanent record — was doing double duty as the search trigger for
// most files, and the two could not be separated by configuration.
//
// The consequence of that shortcut is why the split exists: every ordinary
// bytes-unavailable cleanup wrote a permanent global ban for a transient,
// provider-scoped fact, recorded in the arr as "Manually marked as failed" with
// no reason attached and therefore unauditable afterwards.
func (r *Repair) repairArrFiles(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, a *arr.Arr, files []arr.ContentFile, actions repairActions) bool {
	// Resolve grab-history ids up front — only needed for blocklisting, and only
	// worth the API calls when blocklisting is actually on. Deduped per arr: a
	// season-pack grab covers multiple broken files but needs one failed POST.
	historyIDs := make(map[int]struct{})
	if actions.wantBlocklist() {
		for _, f := range files {
			if ctx != nil && ctx.Err() != nil {
				return false
			}
			var mediaID int
			switch a.Type {
			case arr.Sonarr:
				mediaID = f.EpisodeId
			case arr.Radarr:
				mediaID = f.Id
			}
			if mediaID == 0 {
				continue
			}
			id, _, herr := a.FindGrabHistoryID(mediaID)
			if herr != nil || id == 0 {
				continue
			}
			historyIDs[id] = struct{}{}
		}
	}

	// ACT 1 — DELETE. Always runs; this is what ARR-DELETE now means on its own.
	// Clearing the EpisodeFile/MovieFile rows first also keeps any subsequent
	// search from being rejected by upgrade-only quality logic.
	if err := a.DeleteFiles(ctx, files); err != nil {
		r.logger.Warn().Err(err).Str("arr", a.Name).Msg("ARR-DELETE: DeleteFiles failed")
		statsMu.Lock()
		// ArrDeleteFailed, not RepairFailed: this is an arr-side ARR-DELETE delete
		// that errored, which is a different event from a REPAIR (re-acquire)
		// attempt failing. They shared a counter, so `repair_failed` could not
		// answer "did REPAIR fail?" — the question the operator actually had.
		run.Stats.ArrDeleteFailed += len(files)
		r.saveRun(run)
		statsMu.Unlock()
		return false
	}

	// ACT 2 — BLOCKLIST, only when explicitly enabled. Errors are non-fatal: the
	// rows are already cleared and a search (if enabled) can still recover.
	blocklisted := 0
	for id := range historyIDs {
		if ctx != nil && ctx.Err() != nil {
			break
		}
		if err := a.MarkHistoryFailed(id); err != nil {
			r.logger.Warn().Err(err).Str("arr", a.Name).Int("history_id", id).Msg("ARR-DELETE: MarkHistoryFailed failed")
			continue
		}
		blocklisted++
	}

	// ACT 3 — SEARCH, only when explicitly enabled, over EVERY deleted file.
	//
	// Not just the files that lack a grab record. Relying on MarkHistoryFailed's
	// auto-re-search for the rest is exactly the coupling this split removes: it
	// would make the search knob a silent no-op for the majority of files
	// whenever blocklisting is off, which is now the default.
	searched := 0
	if actions.wantSearch() && len(files) > 0 {
		if err := a.SearchMissing(ctx, files); err != nil {
			r.logger.Warn().Err(err).Str("arr", a.Name).Msg("ARR-DELETE: SearchMissing failed")
		} else {
			searched = len(files)
		}
	}

	statsMu.Lock()
	run.Stats.ArrDeleted += len(files)
	run.Stats.ArrBlocklisted += blocklisted
	run.Stats.ArrSearched += searched
	r.saveRun(run)
	statsMu.Unlock()
	return true
}

// === Candidate enumeration ===

// enumerateCandidates builds the CHECK candidate set. CHECK always enumerates
// the WHOLE hosted library via the managed path (every live entry-item, no arr
// / TMDB / symlink dependency), replacing the old arr-gated enumeration as the
// default detection. When ARR-DELETE is enabled, the arr enumeration is run once
// and its arr targeting (arrName/arrKind/contentMap) is merged onto the managed
// candidates so dead items can be routed to the arr; entries with no arr match
// are still CHECK'd/REPAIR'd/PRUNE'd, they just can't be ARR-DELETE'd. The
// configured cfg.Source no longer switches detection — it is retained for
// backward compat only.
func (r *Repair) enumerateCandidates(ctx context.Context, cfg config.RepairConfig, actions repairActions) (map[string]*candidate, error) {
	out, err := r.enumerateManagedCandidates(ctx)
	if err != nil {
		return out, err
	}
	if !actions.arrDelete {
		return out, nil
	}

	arrCands, arrErr := r.enumerateArrCandidates(ctx, cfg)
	if arrErr != nil {
		if errors.Is(arrErr, context.Canceled) {
			return out, arrErr
		}
		r.logger.Warn().Err(arrErr).Msg("Sweep: arr enumeration for ARR-DELETE targeting failed; dead items without an arr link can't be re-grabbed this run")
		return out, nil
	}
	mergeArrContext(out, arrCands)
	return out, nil
}

// mergeArrContext folds ARR-DELETE arr targeting (arrName/arrKind/contentMap) from
// an arr enumeration into the already-enumerated managed candidate set without
// retaining the arr enumeration's loaded entry bodies (managed loads bodies
// lazily per-entry to bound memory). Arr entries with no managed counterpart are
// ignored — the managed pass already covers the whole hosted library.
func mergeArrContext(dst, arrCands map[string]*candidate) {
	for name, ac := range arrCands {
		existing, ok := dst[name]
		if !ok {
			continue
		}
		if existing.arrName == "" {
			existing.arrName = ac.arrName
			existing.arrKind = ac.arrKind
		}
		if len(ac.contentMap) == 0 {
			continue
		}
		if existing.contentMap == nil {
			existing.contentMap = make(map[string]arr.ContentFile, len(ac.contentMap))
		}
		maps.Copy(existing.contentMap, ac.contentMap)
	}
}

func (r *Repair) filterCandidatesByProtocol(in map[string]*candidate, scope string) map[string]*candidate {
	if repairProtocolMatches(scope, config.ProtocolAll) {
		return in
	}
	out := make(map[string]*candidate, len(in))
	for name, c := range in {
		filtered := r.filterCandidateByProtocol(c, scope)
		if filtered != nil {
			out[name] = filtered
		}
	}
	return out
}

func (r *Repair) filterCandidateByProtocol(c *candidate, scope string) *candidate {
	if c == nil {
		return nil
	}
	// Restricted scope needs per-file protocols, so the body must be present.
	// For lazily-enumerated candidates load it here (only the due subset
	// reaches this point, so it doesn't reintroduce a whole-store decode).
	if c.item == nil {
		item, err := r.manager.GetEntryItem(c.name)
		if err != nil || item == nil {
			return nil
		}
		c.item = item
	}
	files := make(map[string]*storage.File, len(c.item.Files))
	for name, file := range c.item.Files {
		if file == nil || file.Deleted || file.InfoHash == "" {
			continue
		}
		entry, err := r.manager.GetEntry(file.InfoHash)
		if err != nil || entry == nil {
			continue
		}
		if repairProtocolMatches(scope, entry.Protocol) {
			files[name] = file
		}
	}
	if len(files) == 0 {
		return nil
	}

	item := *c.item
	item.Files = files
	filtered := *c
	filtered.item = &item
	if c.contentMap != nil {
		filtered.contentMap = make(map[string]arr.ContentFile, len(c.contentMap))
		for name, content := range c.contentMap {
			if _, ok := files[name]; ok {
				filtered.contentMap[name] = content
			}
		}
	}
	return &filtered
}

func (r *Repair) enumerateManagedCandidates(ctx context.Context) (map[string]*candidate, error) {
	// Names only: GetEntryItems walks the in-memory index without reading or
	// decoding any entry body. Bodies are loaded per-entry in probeEntry and
	// released by the worker, so the sweep never holds the whole store's worth
	// of decoded EntryItems in memory at once. Entries that turn out to be
	// empty are skipped when their body is loaded.
	out := make(map[string]*candidate)
	for name := range r.manager.storage.GetEntryItems() {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		default:
		}
		out[name] = &candidate{name: name}
	}
	return out, nil
}

func (r *Repair) enumerateArrCandidates(ctx context.Context, cfg config.RepairConfig) (map[string]*candidate, error) {
	out := make(map[string]*candidate)
	var mu sync.Mutex

	arrs := r.eligibleArrs(cfg.Arrs)
	if len(arrs) == 0 {
		return out, nil
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, a := range arrs {
		g.Go(func() error {
			sub, err := r.collectArrMediaCandidates(gctx, a, "")
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				r.logger.Warn().Err(err).Str("arr", a.Name).Msg("Sweep: GetMedia failed; skipping arr")
				return nil
			}
			mu.Lock()
			mergeCandidates(out, sub)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

// collectArrMediaCandidates resolves an Arr's media (or a specific media-id
// within that Arr) to entry-keyed candidates.
func (r *Repair) collectArrMediaCandidates(ctx context.Context, a *arr.Arr, mediaID string) (map[string]*candidate, error) {
	out := make(map[string]*candidate)
	media, err := a.GetMedia(ctx, mediaID)
	if err != nil {
		return nil, err
	}
	kind := arrKindFromType(a.Type)
	for _, content := range media {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		default:
		}
		for entryPath, files := range collectArrFiles(content) {
			name := filepath.Clean(filepath.Base(entryPath))
			item, err := r.manager.GetEntryItem(name)
			if err != nil || item == nil {
				continue
			}
			c, ok := out[name]
			if !ok {
				c = &candidate{
					name:       name,
					item:       item,
					arrName:    a.Name,
					arrKind:    kind,
					contentMap: make(map[string]arr.ContentFile),
				}
				out[name] = c
			}
			if c.contentMap == nil {
				c.contentMap = make(map[string]arr.ContentFile)
			}
			for _, f := range files {
				f.EntryName = name
				f.IsSymlink = true
				c.contentMap[f.TargetPath] = f
			}
		}
	}
	return out, nil
}

func mergeCandidates(dst, src map[string]*candidate) {
	for name, c := range src {
		existing, ok := dst[name]
		if !ok {
			dst[name] = c
			continue
		}
		if existing.arrName == "" {
			existing.arrName = c.arrName
			existing.arrKind = c.arrKind
		}
		if existing.contentMap == nil {
			existing.contentMap = make(map[string]arr.ContentFile)
		}
		maps.Copy(existing.contentMap, c.contentMap)
	}
}

func (r *Repair) eligibleArrs(filter []string) []*arr.Arr {
	all := r.manager.arr.GetAll()
	wanted := make(map[string]struct{}, len(filter))
	for _, name := range filter {
		if name = strings.TrimSpace(name); name != "" {
			wanted[name] = struct{}{}
		}
	}
	out := make([]*arr.Arr, 0, len(all))
	for _, a := range all {
		if a == nil || a.Host == "" || a.Token == "" || a.SkipRepair {
			continue
		}
		if len(wanted) > 0 {
			if _, ok := wanted[a.Name]; !ok {
				continue
			}
		}
		out = append(out, a)
	}
	return out
}

func (r *Repair) filterDueCandidates(in map[string]*candidate, ignoreLastChecked bool) (map[string]*candidate, int) {
	if ignoreLastChecked {
		return in, 0
	}
	recheck := r.recheckInterval()
	now := time.Now()
	out := make(map[string]*candidate, len(in))
	skipped := 0
	for name, c := range in {
		h, _ := r.manager.storage.GetEntryHealth(name)
		if h != nil && !h.IsDue(now, recheck) {
			skipped++
			continue
		}
		out[name] = c
	}
	return out, skipped
}

// sweepTier ranks a due candidate by how much PROBING IT RIGHT NOW is worth.
// Candidates are probed tier by tier; the oldest-checked-first ordering the
// sweep has always used applies WITHIN a tier rather than across the whole set.
//
// WHY THIS IS NOT ONE SORT KEY — the asymmetry, measured in production over a
// ~47,600-entry library:
//
//	fleet                                        healthy 47,543 · unknown 117 · broken 12
//	probe rate (multi-offset byte verification)   ~1 entry/sec
//	a full pass over every candidate              ≈ 13 HOURS
//	the 129 entries carrying no usable verdict    ≈ 2 MINUTES
//
// A single global oldest-LastCheckedAt-first sort — which is exactly what this
// used to be — does guarantee forward progress, and that property is worth
// keeping. But it buries those 2 minutes of actionable work at the back of a
// 13-hour queue, and it does so for a reason that gets WORSE the more attention
// an operator pays: examining an entry stamps its LastCheckedAt, and anything
// recently touched sorts to the rear. Force-rechecking 128 `unknown` entries
// pushed all 128 to the very end of a 47,672-entry queue. The ordering actively
// punished investigation.
//
// Run 0f97fa65 is the bill for that: started 06:59:02, cut by a stop schedule at
// 07:00:00, probed 60 of 47,672 — 60 arbitrary healthy entries, zero actionable
// work, and not one of the 129 entries anybody was waiting on. Those same 58
// seconds spent tier-first would have cleared most of the actionable backlog.
// Wall-clock stopping and candidate ordering are one feature, not two: a stop is
// only safe if the ordering front-loads what matters.
//
// DO NOT "simplify" this back into a single sort key.
type sweepTier int

const (
	// tierNoVerdict — NO USABLE VERDICT EXISTS. Either the entry was never
	// checked at all, or its stored status is not a verdict: `unknown` (this
	// run could not tell — a provider 429, an auth failure, a stalled
	// substrate), `repairing` (a record abandoned mid-probe by an interrupted
	// run), `stale`, or an empty/unrecognised status. Highest information gain
	// per probe: every one of these converts a non-answer into an answer, and
	// there are ~100 of them against ~47,500 settled entries.
	tierNoVerdict sweepTier = iota

	// tierBroken — `broken`. A verdict exists and it is ACTIONABLE NOW: this is
	// the only tier where REPAIR / PRUNE / ARR-DELETE have anything to do. It sits
	// behind tierNoVerdict only because an entry with no verdict may turn out to
	// be broken too, and finding that out is what makes it actionable.
	tierBroken

	// tierRoutine — everything else: `healthy`, plus `unsupported`, which
	// EntryHealth.IsDue deliberately groups with healthy. Routine
	// re-verification of entries that already carry a usable verdict. This tier
	// IS the 13 hours, and it is the only one that is safe to leave unfinished
	// when a stop schedule fires.
	tierRoutine
)

// sweepTierFor classifies one health record into its probe tier.
//
// A nil record (no health row at all) and a record whose LastCheckedAt is zero
// are BOTH tierNoVerdict: "never probed" is the strongest form of "no usable
// verdict", and it must not fall through to tierRoutine on the strength of a
// status field no probe ever wrote. This check therefore comes FIRST, ahead of
// the status switch.
func sweepTierFor(h *storage.EntryHealth) sweepTier {
	if h == nil || h.LastCheckedAt.IsZero() {
		return tierNoVerdict
	}
	switch h.Status {
	case storage.HealthBroken:
		return tierBroken
	case storage.HealthHealthy, storage.HealthUnsupported:
		return tierRoutine
	default:
		// storage.HealthUnknown, storage.HealthRepairing, storage.HealthStale,
		// "" and any status added later. Defaulting an UNRECOGNISED status to
		// tierNoVerdict is deliberate: a status this function does not know is,
		// by definition, not a verdict it can trust.
		return tierNoVerdict
	}
}

// sweepCandidate is one entry's sort key. tier is computed ONCE per candidate,
// outside the comparator.
type sweepCandidate struct {
	name          string
	tier          sweepTier
	lastCheckedAt time.Time
}

// sortSweepCandidates imposes the probe order in place: tier ascending, then
// LastCheckedAt ascending (oldest first — never-checked entries carry the zero
// time and lead their tier), then name ascending.
//
// The name tiebreak is what makes the order TOTAL rather than merely sorted:
// names are the keys of the candidate map, so they are unique, so no two
// elements ever compare equal and an unstable sort.Slice still yields exactly
// one possible permutation. Identical input therefore produces identical output
// no matter what order Go's map iteration handed the elements over in.
func sortSweepCandidates(items []sweepCandidate) {
	sort.Slice(items, func(i, j int) bool {
		a, b := &items[i], &items[j]
		if a.tier != b.tier {
			return a.tier < b.tier
		}
		if !a.lastCheckedAt.Equal(b.lastCheckedAt) {
			return a.lastCheckedAt.Before(b.lastCheckedAt)
		}
		return a.name < b.name
	})
}

// orderCandidatesBySweepPriority returns the names of `due` in probe order: by
// sweepTier first, then oldest-LastCheckedAt-first within each tier, then name.
//
// It is PURELY A PERMUTATION of `due` — every candidate goes in and every
// candidate comes out. It decides nothing about which entries are candidates,
// which are due, what verdict they get, or which components run on them.
//
// The forward-progress property the previous global oldest-first ordering
// existed for is preserved, just per-tier: probing an entry stamps
// LastCheckedAt = now, so it sorts to the back of ITS OWN tier for the next run,
// and a run cut short by a StopSchedule resumes at the head of the highest tier
// that still has work rather than re-treading the head it already probed.
//
// Cost at library scale: one GetEntryHealth per candidate (the read the previous
// ordering already performed, unchanged) plus one O(n log n) sort over a flat
// slice of {string, int, time.Time} — at ~47,600 entries that is ~2 MB in a
// single allocation and log₂n ≈ 16 comparisons per element, i.e. milliseconds
// against a 13-hour probe pass.
func (r *Repair) orderCandidatesBySweepPriority(due map[string]*candidate) []string {
	items := make([]sweepCandidate, 0, len(due))
	for name := range due {
		h, _ := r.manager.storage.GetEntryHealth(name)
		item := sweepCandidate{name: name, tier: sweepTierFor(h)}
		if h != nil {
			item.lastCheckedAt = h.LastCheckedAt
		}
		items = append(items, item)
	}
	sortSweepCandidates(items)
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.name
	}
	return out
}

// === Manual rechecks (webhooks + API) ===

func (r *Repair) collectBrokenHealths(names []string, requireArrFile bool) (*xsync.Map[string, *storage.EntryHealth], int) {
	wanted := make(map[string]struct{}, len(names))
	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			wanted[n] = struct{}{}
		}
	}

	healths := xsync.NewMap[string, *storage.EntryHealth]()
	_ = r.manager.storage.ForEachEntryHealth(func(h *storage.EntryHealth) error {
		if h == nil || h.Status != storage.HealthBroken {
			return nil
		}
		if len(wanted) > 0 {
			if _, ok := wanted[h.EntryName]; !ok {
				return nil
			}
		}
		if requireArrFile {
			if len(h.BrokenFiles) == 0 {
				return nil
			}
			hasArrFile := false
			for _, bf := range h.BrokenFiles {
				if bf.ArrName != "" && bf.ArrFileID != 0 {
					hasArrFile = true
					break
				}
			}
			if !hasArrFile {
				return nil
			}
		}
		healths.Store(h.EntryName, h)
		return nil
	})
	return healths, len(wanted)
}

func (r *Repair) markBrokenHealthCleared(h *storage.EntryHealth, at time.Time) {
	if h == nil {
		return
	}
	if _, err := r.manager.storage.GetEntryItem(h.EntryName); err != nil {
		_ = r.manager.storage.DeleteEntryHealth(h.EntryName)
		return
	}
	h.Status = storage.HealthUnknown
	h.BrokenFiles = nil
	h.FailureReason = ""
	h.LastRepairAt = at
	h.Dirty = false
	h.DirtyReason = ""
	h.NextCheckDueAt = time.Time{}
	r.saveHealth(h)
}

// FixBroken acts on currently-broken entries WITHOUT reprobing, applying the
// selected components: REPAIR (re-acquire across providers), PRUNE (delete
// decypharr-side only) and/or ARR-DELETE (arr delete + blocklist + search). When
// names is empty every entry with Status=broken is acted on. Returns the new
// RepairRun record immediately; the work runs in the background.
//
// Component precedence (resolveManualActions): an explicit selection runs
// exactly those components — single-component invocation (e.g. PRUNE-only) is
// supported; a nil selection falls back to the configured REPAIR/PRUNE/ARR-DELETE
// knobs — never force-all.
//
// Use this from the UI when a previous sweep already identified broken entries
// and the user wants to act on them without paying for another probe pass.
func (r *Repair) FixBroken(ctx context.Context, names []string, sel *ManualActions) (*storage.RepairRun, error) {
	if ctx == nil {
		ctx = r.parentCtx
	}

	// Pass the caller's REAL intent as the legacy fix flag instead of a
	// hardcoded true: the fallback to the configured knobs is what "no
	// selection supplied" means, and nothing else. Hardcoding true here is
	// what compounded the all-false footgun — an explicit "run no components"
	// selection resolved to the operator's configured (possibly destructive)
	// knobs. resolveManualActions now short-circuits that shape on its own, and
	// this keeps the two consistent: sel == nil ⇒ configured knobs, an explicit
	// all-false sel ⇒ no components ⇒ the error below.
	actions := r.resolveManualActions(sel, sel == nil)
	if !actions.any() {
		return nil, errors.New("no repair action selected: enable REPAIR, PRUNE, or ARR-DELETE")
	}

	// Require an arr link only when ARR-DELETE is the ONLY thing that can act:
	// PRUNE and REPAIR act on entries with no arr link too, so requiring one
	// would wrongly exclude prune-/repair-eligible broken entries.
	requireArr := actions.arrDelete && !actions.prune && !actions.repair
	healths, wantedCount := r.collectBrokenHealths(names, requireArr)
	if healths.Size() == 0 {
		return nil, errors.New("no fixable broken entries")
	}

	r.mu.Lock()
	if r.activeRunID != "" {
		id := r.activeRunID
		r.mu.Unlock()
		return nil, fmt.Errorf("repair already running (run %s)", id)
	}
	runCtx, cancel := context.WithCancel(ctx)
	scope := "all"
	if wantedCount > 0 {
		scope = fmt.Sprintf("%d", wantedCount)
	}
	run := &storage.RepairRun{
		ID:        uuid.NewString(),
		Trigger:   storage.RepairTriggerManual,
		Status:    storage.RepairRunRunning,
		Stage:     storage.RepairStageRepairing,
		StartedAt: time.Now(),
		Source:    fmt.Sprintf("fix-broken:%s:%s", scope, actions.label()),
	}
	run.Stats.Candidates = healths.Size()
	r.activeRunID = run.ID
	r.cancelRun = cancel
	r.mu.Unlock()

	if err := r.manager.storage.SaveRepairRun(run); err != nil {
		r.mu.Lock()
		r.activeRunID = ""
		r.cancelRun = nil
		r.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("failed to persist repair run: %w", err)
	}

	// FixBroken is a bulk path (it can act on every broken entry at once), so its
	// destructive components (PRUNE/ARR-DELETE) are bounded by the same per-run
	// deletion cap as the scheduled sweep: at most maxDeletionsPerRun entries get
	// deleted per invocation, the rest stay broken for the next run. REPAIR
	// re-acquires are non-destructive and never consume a slot.
	budget := r.newDeletionBudget(run.ID)

	r.runWG.Go(func() {
		defer func() {
			r.mu.Lock()
			if r.activeRunID == run.ID {
				r.activeRunID = ""
				r.cancelRun = nil
			}
			r.mu.Unlock()
			cancel()
		}()
		r.repairBroken(runCtx, run, healths, actions, budget)
		recordBudgetStats(run, budget)
		if runCtx.Err() != nil {
			r.finalizeRun(run, storage.RepairRunCancelled, "", "context cancelled during repair")
			return
		}
		r.finalizeRun(run, storage.RepairRunCompleted, "", "")
		r.logger.Info().
			Str("run_id", run.ID).
			Int("candidates", run.Stats.Candidates).
			Int("reacquired", run.Stats.Reacquired).
			Int("repair_failed", run.Stats.RepairFailed).
			Int("repair_skipped_unsupported", run.Stats.RepairSkippedUnsupported).
			Int("pruned", run.Stats.Pruned).
			Int("prune_skipped_not_eligible", run.Stats.PruneSkippedNotEligible).
			
			Int("arr_deleted", run.Stats.ArrDeleted).
			Int("arr_blocklisted", run.Stats.ArrBlocklisted).
			Int("arr_searched", run.Stats.ArrSearched).
			Int("arr_delete_failed", run.Stats.ArrDeleteFailed).
			Int("arr_skipped_no_link", run.Stats.ArrSkippedNoLink).
			Int("deletions", run.Stats.Deletions).
			Int("deletion_cap_skipped", run.Stats.DeletionCapSkipped).
			Msg("FixBroken: completed")
	})
	return run, nil
}

// RecheckEntry kicks off a recheck (CHECK) for a single entry and returns
// immediately with an in-progress EntryHealth ack. CHECK always runs; the
// selected components (sel / legacy fix flag, resolved via resolveManualActions)
// then act on it if it probes broken. A CHECK-only recheck (no components / no
// fix) just re-probes and records health.
func (r *Repair) RecheckEntry(ctx context.Context, entryName string, sel *ManualActions, fix bool) (*storage.EntryHealth, error) {
	if entryName == "" {
		return nil, errors.New("entry name is empty")
	}
	h, _ := r.manager.storage.GetEntryHealth(entryName)
	if h != nil && h.ActiveRunID != "" {
		return nil, fmt.Errorf("entry is being probed by run %s", h.ActiveRunID)
	}

	item, err := r.manager.GetEntryItem(entryName)
	if err != nil || item == nil {
		return nil, fmt.Errorf("entry %q not found", entryName)
	}

	actions := r.resolveManualActions(sel, fix)
	runID := "recheck-" + entryName
	c := &candidate{name: entryName, item: item}

	if ctx == nil {
		ctx = r.parentCtx
	}
	r.runWG.Go(func() {
		// Arr targeting is only needed when ARR-DELETE is selected.
		if actions.arrDelete {
			r.attachArrContext(ctx, c)
		}
		heal := newHealCache()
		// REPAIR runs inline during the probe (actions.repair). If it re-acquires
		// the item, final rolls up healthy and the destructive pass is skipped.
		final, _ := r.probeEntry(ctx, runID, c, heal, RepairRunOptions{}, actions.repair)
		if final == nil || final.Status != storage.HealthBroken || !actions.destructive() {
			return
		}
		pseudo := &storage.RepairRun{ID: runID, Stats: storage.RepairRunStats{}}
		var statsMu sync.Mutex
		// Carry a real budget even though a single entry is inherently bounded.
		// max_deletions_per_run is the ONLY guard against a mass-delete, so no
		// path that can reach a destructive action may run uncapped: nil here
		// meant this entry point silently opted out of the cap. actOnDeadEntry
		// applies only the destructive components (PRUNE/ARR-DELETE); REPAIR
		// already ran inline above.
		budget := r.newDeletionBudget(runID)
		r.actOnDeadEntry(ctx, pseudo, &statsMu, entryName, final, actions, budget)
		recordBudgetStats(pseudo, budget)
	})

	// Return an in-memory ack reflecting the freshly-started recheck. The
	// real EntryHealth in storage is updated by probeEntry shortly after.
	if h == nil {
		h = &storage.EntryHealth{EntryName: entryName}
	}
	h.Status = storage.HealthRepairing
	h.ActiveRunID = runID
	return h, nil
}

// RecheckMedia kicks off a recheck (CHECK) for every entry that an Arr's
// media-id resolves to and returns immediately with the in-progress RepairRun.
// The actual probing + action runs in the background so HTTP callers don't have
// to block. With arrName="" the first eligible Arr that resolves entries wins.
// CHECK always runs; the selected components (sel / legacy fix flag) then act on
// anything that probes broken. Honors the singleton run lock.
func (r *Repair) RecheckMedia(ctx context.Context, arrName, mediaID string, sel *ManualActions, fix bool) (*storage.RepairRun, error) {
	mediaID = strings.TrimSpace(mediaID)
	if mediaID == "" {
		return nil, errors.New("media_id is required")
	}
	if ctx == nil {
		ctx = r.parentCtx
	}

	// Validate arr selection synchronously so callers fail-fast on bad input.
	arrs, err := r.resolveArrsForMedia(arrName)
	if err != nil {
		return nil, err
	}
	actions := r.resolveManualActions(sel, fix)

	r.mu.Lock()
	if r.activeRunID != "" {
		id := r.activeRunID
		r.mu.Unlock()
		return nil, fmt.Errorf("repair already running (run %s)", id)
	}
	runCtx, cancel := context.WithCancel(ctx)
	run := &storage.RepairRun{
		ID:        uuid.NewString(),
		Trigger:   storage.RepairTriggerManual,
		Status:    storage.RepairRunRunning,
		Stage:     storage.RepairStageSelecting,
		StartedAt: time.Now(),
		Source:    fmt.Sprintf("media:%s/%s:%s", arrName, mediaID, actions.label()),
	}
	r.activeRunID = run.ID
	r.cancelRun = cancel
	r.mu.Unlock()

	if err := r.manager.storage.SaveRepairRun(run); err != nil {
		r.mu.Lock()
		r.activeRunID = ""
		r.cancelRun = nil
		r.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("failed to persist repair run: %w", err)
	}

	r.runWG.Go(func() {
		defer func() {
			r.mu.Lock()
			if r.activeRunID == run.ID {
				r.activeRunID = ""
				r.cancelRun = nil
			}
			r.mu.Unlock()
			cancel()
		}()
		r.executeRecheckMedia(runCtx, run, arrs, arrName, mediaID, actions)
	})
	return run, nil
}

// executeRecheckMedia is the body of a media recheck. Mirrors executeSweep
// but scoped to a specific media-id resolved through one or more Arrs. CHECK
// always runs; actions selects which components act on anything that probes
// broken.
func (r *Repair) executeRecheckMedia(ctx context.Context, run *storage.RepairRun, arrs []*arr.Arr, arrName, mediaID string, actions repairActions) {
	candidates := make(map[string]*candidate)
	var lastErr error
	for _, a := range arrs {
		if ctx.Err() != nil {
			break
		}
		sub, err := r.collectArrMediaCandidates(ctx, a, mediaID)
		if err != nil {
			lastErr = err
			r.logger.Trace().Err(err).Str("arr", a.Name).Str("media_id", mediaID).Msg("RecheckMedia: GetMedia failed")
			continue
		}
		mergeCandidates(candidates, sub)
		// When the caller didn't pin a specific Arr, the first Arr to resolve
		// non-empty entries wins. Avoids double-probing when sonarr+radarr
		// share a folder root.
		if arrName == "" && len(sub) > 0 {
			break
		}
	}

	if len(candidates) == 0 {
		msg := fmt.Sprintf("media id %q resolved no entries", mediaID)
		if lastErr != nil {
			msg += " (last error: " + lastErr.Error() + ")"
		}
		r.finalizeRun(run, storage.RepairRunCompleted, msg, "")
		return
	}

	run.Stats.Candidates = len(candidates)
	run.Stage = storage.RepairStageProbing
	r.saveRun(run)

	heal := newHealCache()
	mediaNames := make([]string, 0, len(candidates))
	for name := range candidates {
		mediaNames = append(mediaNames, name)
	}
	// A media id can resolve to a whole series' worth of entries, so this path
	// is NOT inherently bounded and must honour max_deletions_per_run exactly
	// like the scheduled sweep. It previously passed a nil (unlimited) budget,
	// which bypassed the cap entirely.
	budget := r.newDeletionBudget(run.ID)
	err := r.probeAndHealCandidates(ctx, run, candidates, mediaNames, heal, RepairRunOptions{}, actions, budget)
	recordBudgetStats(run, budget)
	candidates = nil
	if err != nil {
		if errors.Is(err, context.Canceled) {
			r.finalizeRun(run, storage.RepairRunCancelled, "", "context cancelled during probing")
			return
		}
		r.finalizeRun(run, storage.RepairRunFailed, err.Error(), "")
		return
	}
	if ctx.Err() != nil {
		r.finalizeRun(run, storage.RepairRunCancelled, "", "context cancelled during repair")
		return
	}

	r.finalizeRun(run, storage.RepairRunCompleted, "", "")
	r.logger.Info().
		Str("run_id", run.ID).
		Str("arr", arrName).
		Str("media_id", mediaID).
		Int("candidates", run.Stats.Candidates).
		Int("broken", run.Stats.Broken).
		Int("reacquired", run.Stats.Reacquired).
		Int("pruned", run.Stats.Pruned).
		
		Str("actions", actions.label()).
		Msg("RecheckMedia: completed")
}

func (r *Repair) resolveArrsForMedia(arrName string) ([]*arr.Arr, error) {
	if arrName != "" {
		a := r.manager.arr.Get(arrName)
		if a == nil {
			return nil, fmt.Errorf("arr %q not found", arrName)
		}
		if a.Host == "" || a.Token == "" {
			return nil, fmt.Errorf("arr %q is not configured", arrName)
		}
		if a.SkipRepair {
			return nil, fmt.Errorf("arr %q has skip_repair set", arrName)
		}
		return []*arr.Arr{a}, nil
	}
	all := r.eligibleArrs(nil)
	if len(all) == 0 {
		return nil, errors.New("no eligible arrs configured")
	}
	return all, nil
}

// attachArrContext walks Arrs looking for the entry's symlink targets so a
// single-entry fix can reach back into the Arr that owns the file.
func (r *Repair) attachArrContext(ctx context.Context, c *candidate) {
	for _, a := range r.eligibleArrs(nil) {
		if ctx.Err() != nil {
			return
		}
		media, err := a.GetMedia(ctx, "")
		if err != nil {
			continue
		}
		kind := arrKindFromType(a.Type)
		for _, content := range media {
			for entryPath, files := range collectArrFiles(content) {
				if filepath.Clean(filepath.Base(entryPath)) != c.name {
					continue
				}
				if c.contentMap == nil {
					c.contentMap = make(map[string]arr.ContentFile)
				}
				c.arrName = a.Name
				c.arrKind = kind
				for _, f := range files {
					f.EntryName = c.name
					f.IsSymlink = true
					c.contentMap[f.TargetPath] = f
				}
			}
		}
	}
}

// === helpers ===

func orderedFilenames(item *storage.EntryItem) []string {
	if item == nil {
		return nil
	}
	out := make([]string, 0, len(item.Files))
	for name, f := range item.Files {
		if f == nil || f.Deleted {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// topIndeterminateReason returns the dominant reason among results that reached
// NO verdict (neither healthy nor broken), so an `unknown` entry can say WHY it
// is unknown. Without it the per-file reason was computed by every probe path
// and then thrown away, because only broken results are flattened into
// BrokenFiles — leaving a library of `unknown` entries with an empty
// failure_reason and no way to tell a mis-configured usenet client from a
// rate-limited provider from a stalled substrate.
func topIndeterminateReason(results []fileResult) string {
	counts := make(map[string]int)
	for _, res := range results {
		if res.healthy || res.broken || res.reason == "" {
			continue
		}
		counts[res.reason]++
	}
	best, bestN := "", 0
	for reason, n := range counts {
		// Ties break on the lexicographically smaller reason so the recorded
		// value is deterministic across runs (map iteration is not).
		if n > bestN || (n == bestN && reason < best) {
			best, bestN = reason, n
		}
	}
	return best
}

func topReason(files []storage.BrokenFile) string {
	if len(files) == 0 {
		return ""
	}
	counts := make(map[string]int)
	for _, f := range files {
		if f.Reason != "" {
			counts[f.Reason]++
		}
	}
	best, bestN := "", 0
	for reason, n := range counts {
		if n > bestN {
			best = reason
			bestN = n
		}
	}
	if best != "" {
		return best
	}
	return files[0].Reason
}

// collectArrFiles groups Arr content files by their resolved symlink-target
// parent directory. The parent is the on-disk entry-folder name.
func collectArrFiles(media arr.Content) map[string][]arr.ContentFile {
	out := make(map[string][]arr.ContentFile)
	for _, f := range media.Files {
		target := readSymlinkTarget(f.Path)
		if target == "" {
			continue
		}
		f.IsSymlink = true
		dir, name := filepath.Split(target)
		f.TargetPath = name
		entryPath := filepath.Clean(dir)
		out[entryPath] = append(out[entryPath], f)
	}
	return out
}

func readSymlinkTarget(path string) string {
	path = filepath.Clean(path)
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		return ""
	}
	target, err := os.Readlink(path)
	if err != nil {
		return ""
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(path), target)
	}
	return target
}

func arrKindFromType(t arr.Type) storage.ArrKind {
	switch t {
	case arr.Sonarr:
		return storage.ArrKindSonarr
	case arr.Radarr:
		return storage.ArrKindRadarr
	case arr.Lidarr:
		return storage.ArrKindLidarr
	case arr.Readarr:
		return storage.ArrKindReadarr
	default:
		return storage.ArrKindOther
	}
}

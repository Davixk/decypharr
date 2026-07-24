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
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
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

// executeSweep is the body of a sweep: enumerate, filter due, probe, repair.
func (r *Repair) executeSweep(ctx context.Context, run *storage.RepairRun, opts RepairRunOptions, stopState *repairStopState) {
	cfg := r.cfg()
	log := r.logger.With().Str("run_id", run.ID).Logger()

	// Resolve the action set once from the configured REPAIR/PRUNE/RE-GRAB knobs.
	// There is no master gate: the sweep is CHECK-only (probe + record, no
	// REPAIR/PRUNE/RE-GRAB) exactly when all three knobs are off. A one-off run
	// (Run modal) may pass opts.Actions to override the configured knobs for that
	// run. This also decides what happens to whatever was found broken so far if
	// a StopSchedule cuts the sweep short.
	actions := resolveActions(cfg)
	if opts.Actions != nil {
		actions = repairActions{repair: opts.Actions.Repair, prune: opts.Actions.Prune, regrab: opts.Actions.Regrab}
	}

	// One destructive-deletion budget for the whole sweep. It bounds how many
	// entries this run may destructively act on (PRUNE decypharr-delete and/or
	// RE-GRAB arr-delete), so a provider-wide false "unavailable" can't mass-act
	// on the entire due set in one run. Shared by the inline action pass and any
	// StopSchedule post-stop pass.
	budget := r.newDeletionBudget(run.ID)

	log.Info().
		Bool("repair", actions.repair).
		Bool("prune", actions.prune).
		Bool("regrab", actions.regrab).
		Msg("Sweep: selecting candidates (CHECK: whole managed library)")
	// CHECK always enumerates the whole hosted library via the managed path.
	// RE-GRAB targeting (arr linkage) is merged in only when regrab is enabled.
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

	// Order candidates oldest-checked-first (never-checked entries sort
	// first, since their LastCheckedAt is the zero time). This is what makes
	// a StopSchedule-truncated repair sweep make guaranteed forward progress: any
	// entry probed today moves to the back of the queue (its LastCheckedAt
	// becomes "now"), so tomorrow's truncated repair sweep naturally picks up where
	// today's left off instead of re-rolling a random subset of `due`.
	//
	// This slice also doubles as the candidate list considered by this run,
	// used to scope a stop-schedule repair pass.
	names := r.orderCandidatesByLastChecked(due)

	run.Stage = storage.RepairStageProbing
	r.saveRun(run)
	log.Info().Int("due", len(due)).Int("skipped_fresh", skipped).Str("protocol", protocolScope).Bool("repair", actions.repair).Bool("prune", actions.prune).Bool("regrab", actions.regrab).Msg("Sweep: probing")

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
		Int("pruned", run.Stats.Pruned).
		Int("regrabbed", run.Stats.Regrabbed).
		Int("regrab_failed", run.Stats.RepairFailed).
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
	log.Info().Bool("prune", actions.prune).Bool("regrab", actions.regrab).Msg("Repair sweep: stop schedule fired; finishing run")

	if actions.destructive() && len(names) > 0 {
		// Use a fresh, un-cancelled context for the final action pass: the
		// probe pass was cut short, but the destructive pass over what's already
		// known-broken is a short, bounded set of actions and should be
		// allowed to complete. Bound it so a misbehaving Arr can't hang.
		repairCtx, cancel := context.WithTimeout(detachedRepairContext(ctx, r.parentCtx), repairStopFinalRepairTimeout)
		defer cancel()

		// Require an arr file only when RE-GRAB is the sole destructive action;
		// PRUNE can act on entries with no arr link, so don't filter them out.
		healths, _ := r.collectBrokenHealths(names, actions.regrab && !actions.prune)
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
// names gives the iteration order (see orderCandidatesByLastChecked):
// g.Go is called in this order, so with N workers the oldest-checked N
// candidates start first. If the run is cut short by a StopSchedule, the
// candidates that didn't get a chance to start remain oldest-first for the
// next repair sweep.
//
// The pipeline is folded into the per-entry pass. Per dead item, components are
// independent and knob-gated:
//   - REPAIR:  probeEntry runs the debrid re-acquire inline; on success the item
//     becomes healthy and the pipeline stops for it.
//   - PRUNE:   if still dead, delete the item decypharr-side only (no arr).
//   - RE-GRAB: independently, if still dead, delete+blocklist+re-search via the
//     arr (whether or not PRUNE ran).
//
// PRUNE and RE-GRAB are bounded per-item by the shared per-run deletion budget.
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

			h, reacquired := r.probeEntry(gctx, run.ID, c, heal, opts, actions.repair)
			if h == nil {
				// Entry vanished or had no files between enumeration and probe;
				// skip without counting. Release any loaded body.
				c.item = nil
				c.contentMap = nil
				return nil
			}

			// Still dead after REPAIR (re-acquire): run the destructive
			// components (PRUNE and/or RE-GRAB) for just this item, bounded by
			// the per-run deletion budget. Independent and knob-gated.
			if h.Status == storage.HealthBroken {
				r.actOnDeadEntry(gctx, run, &runMu, name, h, actions, budget)
			}

			runMu.Lock()
			run.Stats.Probed++
			run.Stats.Reacquired += reacquired
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
// PRUNE/RE-GRAB pipeline for it.
func (r *Repair) probeEntry(ctx context.Context, runID string, c *candidate, heal *healCache, opts RepairRunOptions, repair bool) (*storage.EntryHealth, int) {
	s := r.manager.storage
	// Lazily load the entry body. Enumeration only recorded the name, so the
	// store isn't fully decoded up front. A vanished or empty entry is a skip
	// (nil tells the worker not to count it).
	if c.item == nil {
		item, err := s.GetEntryItem(c.name)
		if err != nil || item == nil || len(item.Files) == 0 {
			return nil, 0
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
	results := r.probeFiles(ctx, c.item, names, opts)
	reacquired := 0
	if repair {
		// REPAIR component: re-acquire dead torrents across providers. On
		// success the file's result flips to healthy so the entry rolls up
		// healthy and the destructive pipeline never runs for it.
		reacquired = r.autoHealResults(ctx, results, heal)
	}

	broken := r.brokenFiles(c, results)
	final := rollupStatus(results)

	h.Status = final
	h.FileCount = len(names)
	h.BrokenFiles = broken
	h.BrokenCount = len(broken)
	h.Fingerprint = storage.EntryItemRepairFingerprint(c.item)
	h.LastCheckedAt = time.Now()
	h.NextCheckDueAt = h.LastCheckedAt.Add(r.recheckInterval())
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
	}

	r.saveHealth(h)
	return h, reacquired
}

// probeFiles fans per-file probes inside a single entry, capped at
// repairFilesPerEntry concurrent workers.
func (r *Repair) probeFiles(ctx context.Context, item *storage.EntryItem, names []string, opts RepairRunOptions) []fileResult {
	results := make([]fileResult, len(names))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(repairFilesPerEntry)
	for i, name := range names {
		g.Go(func() error {
			if gctx.Err() != nil {
				results[i] = fileResult{name: name, reason: "context_cancelled"}
				return nil
			}
			results[i] = r.probeFile(gctx, item, name, opts)
			return nil
		})
	}
	_ = g.Wait()
	return results
}

// probeFile checks one file. NZB probes use usenet.CheckFile. Torrent probes
// use the provider CheckFile endpoint unless this run requests unrestrict-link
// probing.
func (r *Repair) probeFile(ctx context.Context, item *storage.EntryItem, name string, opts RepairRunOptions) fileResult {
	file := item.Files[name]
	res := fileResult{name: name}

	if file == nil || file.InfoHash == "" {
		res.reason = "missing_infohash"
		return res
	}
	res.infoHash = file.InfoHash

	entry, err := r.manager.GetEntry(file.InfoHash)
	if err != nil || entry == nil {
		res.reason = "entry_not_found"
		return res
	}
	res.protocol = entry.Protocol
	if !repairProtocolMatches(r.effectiveProtocolScope(opts), entry.Protocol) {
		res.reason = "protocol_skipped"
		return res
	}

	if entry.IsNZB() {
		return r.probeNZBFile(ctx, entry, name, res)
	}
	return r.probeTorrentFile(ctx, entry, file, name, res, opts)
}

func (r *Repair) probeNZBFile(ctx context.Context, entry *storage.Entry, name string, res fileResult) fileResult {
	if r.manager.usenet == nil {
		res.reason = "usenet_client_not_configured"
		return res
	}
	err := r.manager.usenet.CheckFile(ctx, entry.InfoHash, name)
	if err == nil {
		res.healthy = true
		return res
	}
	if errors.Is(err, customerror.UsenetSegmentMissingError) {
		res.broken = true
		res.reason = "usenet_segment_missing"
	} else {
		res.reason = "usenet_probe_error"
	}
	return res
}

func (r *Repair) probeTorrentFile(ctx context.Context, entry *storage.Entry, file *storage.File, name string, res fileResult, opts RepairRunOptions) fileResult {
	client := r.manager.ProviderClient(entry.ActiveProvider)
	if client == nil {
		res.reason = "provider_client_not_found"
		return res
	}
	if opts.UnrestrictLink {
		return r.probeTorrentFileByUnrestrict(entry, file, name, res, client)
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
	if err == nil {
		res.healthy = true
		return res
	}
	if errors.Is(err, customerror.HosterUnavailableError) {
		res.broken = true
		res.reason = "hoster_unavailable"
	} else {
		res.reason = "provider_probe_error"
	}
	return res
}

func (r *Repair) probeTorrentFileByUnrestrict(entry *storage.Entry, file *storage.File, name string, res fileResult, client debrid.Client) fileResult {
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

	debridFile := &debridTypes.File{
		Id:        placementFile.Id,
		Link:      placementFile.Link,
		Path:      placementFile.Path,
		Name:      file.Name,
		Size:      file.Size,
		ByteRange: file.ByteRange,
		Deleted:   file.Deleted,
	}
	downloadLink, err := client.GetDownloadLink(placement.ID, debridFile)
	if err == nil && !downloadLink.Empty() {
		res.healthy = true
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

// autoHealResults walks broken torrent infohashes and tries one re-insert per
// infohash (singleflighted). On success, every file in that infohash group is
// marked healthy. Returns the number of infohashes REPAIR successfully
// re-acquired, for the run's Reacquired outcome counter.
func (r *Repair) autoHealResults(ctx context.Context, results []fileResult, heal *healCache) int {
	byHash := make(map[string][]int)
	for i, res := range results {
		if !res.broken || res.protocol != config.ProtocolTorrent || res.infoHash == "" {
			continue
		}
		byHash[res.infoHash] = append(byHash[res.infoHash], i)
	}
	if len(byHash) == 0 {
		return 0
	}
	reacquired := 0
	for infoHash, indices := range byHash {
		entry, err := r.manager.GetEntry(infoHash)
		if err != nil || entry == nil {
			continue
		}
		err = heal.do(infoHash, func() error {
			return r.manager.ReinsertEntry(ctx, entry)
		})
		if err != nil {
			continue
		}
		reacquired++
		for _, i := range indices {
			results[i].broken = false
			results[i].healthy = true
			results[i].reason = "repaired"
		}
	}
	return reacquired
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
//	PRUNE / RE-GRAB — if still dead, the destructive pass via actOnDeadEntry.
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
// item's broken torrent infohashes across providers. On success (every broken
// torrent re-acquired) it clears the entry's broken health so the next CHECK
// confirms it, records the Reacquired outcome, and returns true so the caller
// skips the destructive components for this item. Non-destructive: it never
// consumes a deletion slot and makes ZERO arr calls.
func (r *Repair) reacquireDeadEntry(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, name string, h *storage.EntryHealth) bool {
	if h == nil || h.Status != storage.HealthBroken {
		return false
	}
	hashes := make(map[string]struct{})
	for _, bf := range h.BrokenFiles {
		if bf.Protocol == config.ProtocolTorrent && bf.InfoHash != "" {
			hashes[bf.InfoHash] = struct{}{}
		}
	}
	if len(hashes) == 0 {
		return false
	}
	for hash := range hashes {
		if ctx != nil && ctx.Err() != nil {
			return false
		}
		entry, err := r.manager.GetEntry(hash)
		if err != nil || entry == nil {
			return false
		}
		if err := r.manager.ReinsertEntry(ctx, entry); err != nil {
			r.logger.Debug().Err(err).Str("component", "REPAIR").Str("entry", name).Str("infohash", hash).Msg("REPAIR: re-acquire failed; leaving dead for PRUNE/RE-GRAB")
			return false
		}
	}
	r.logger.Info().Str("component", "REPAIR").Str("entry", name).Msg("REPAIR: re-acquired dead item across providers")
	statsMu.Lock()
	run.Stats.Reacquired++
	r.saveRun(run)
	statsMu.Unlock()
	r.markBrokenHealthCleared(h, time.Now())
	return true
}

// entryHealthHasArrLink reports whether a dead entry carries the arr
// identifiers RE-GRAB needs to delete + re-search. Best-effort: an entry with
// no resolved arr link simply can't be RE-GRAB'd (logged, then skipped).
func entryHealthHasArrLink(h *storage.EntryHealth) bool {
	for _, bf := range h.BrokenFiles {
		if bf.ArrName != "" && bf.ArrFileID != 0 {
			return true
		}
	}
	return false
}

// pruneEligible reports whether PRUNE may delete this entry decypharr-side.
// Only fully-broken entries (every file dead) are deletable, so a
// partially-broken entry keeps its healthy files' symlinks. Requires at least
// one infohash to delete by.
func pruneEligible(h *storage.EntryHealth) bool {
	if h.BrokenCount == 0 || h.BrokenCount != h.FileCount {
		return false
	}
	for _, bf := range h.BrokenFiles {
		if bf.InfoHash != "" {
			return true
		}
	}
	return false
}

// actOnDeadEntry runs the destructive pipeline for one dead item after CHECK
// found it dead and REPAIR (if enabled) failed to re-acquire it. PRUNE and
// RE-GRAB are INDEPENDENT and individually knob-gated — RE-GRAB is not gated
// behind PRUNE and vice-versa. Both are bounded by the shared per-run deletion
// budget: one slot is reserved per dead entry that undergoes any destructive
// action this run (a provider-wide false "unavailable" therefore can't mass-act
// on the whole due set in one run). A nil / unlimited budget (single-item
// paths) always grants. statsMu guards run.Stats across concurrent entries.
//
// INVARIANT: PRUNE (pruneDeadEntry) makes ZERO arr API calls — only RE-GRAB
// (regrabDeadEntry) touches the arr.
func (r *Repair) actOnDeadEntry(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, name string, h *storage.EntryHealth, actions repairActions, budget *repairDeletionBudget) {
	if h == nil || h.Status != storage.HealthBroken {
		return
	}

	wantRegrab := actions.regrab && entryHealthHasArrLink(h)
	wantPrune := actions.prune && pruneEligible(h)
	if actions.regrab && !wantRegrab {
		r.logger.Debug().Str("component", "RE-GRAB").Str("entry", name).Msg("RE-GRAB: no arr link resolved for dead item; cannot re-grab it")
	}
	if !wantRegrab && !wantPrune {
		// Nothing destructive to do (no arr link and not prune-eligible): don't
		// consume a deletion slot. Non-destructive probes/re-inserts never count
		// against the cap.
		return
	}

	// Reserve one destructive slot for this dead entry (covers RE-GRAB and/or
	// PRUNE). If the run's cap is exhausted, skip ALL destructive actions for
	// this entry and leave it dead so it is re-picked next run.
	if !budget.reserve() {
		return
	}

	// RE-GRAB first (arr-side), then PRUNE (decypharr-side). Both read only from
	// the in-memory health record, so order doesn't couple them; RE-GRAB runs
	// whether or not PRUNE deletes the entry.
	if wantRegrab {
		r.regrabDeadEntry(ctx, run, statsMu, name, h)
	}
	if wantPrune && r.pruneDeadEntry(name, h) {
		statsMu.Lock()
		run.Stats.Pruned++
		r.saveRun(run)
		statsMu.Unlock()
	}
}

// regrabDeadEntry is the RE-GRAB component: the ONLY arr-coupled action. For a
// dead item it deletes the arr file record, blocklists the grab, and triggers a
// search, per arr. It does not delete anything decypharr-side and does not
// verify the outcome (SearchMissing/MarkHistoryFailed only queue work; the next
// sweep verifies). statsMu guards run.Stats across concurrent entries.
func (r *Repair) regrabDeadEntry(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, name string, h *storage.EntryHealth) {
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
		if r.repairArrFiles(ctx, run, statsMu, a, files) {
			r.logger.Info().Str("component", "RE-GRAB").Str("entry", name).Str("arr", arrName).Int("files", len(files)).Msg("RE-GRAB: deleted arr file records + blocklisted grab + queued re-search")
		}
	}
	h.LastRepairAt = time.Now()
	r.saveHealth(h)
}

// pruneDeadEntry is the PRUNE component: a decypharr-side-ONLY deletion. It
// removes the provider placements, the symlink/download folder (via the guarded
// deleteEntryFiles — the category-dir data-loss guard stays in the path), and
// the db entry through DeleteEntry(hash, true). It makes ZERO arr API calls: by
// design the arr keeps the item MONITORED so its own next disk scan sees the
// file missing and re-searches, with no decypharr->arr coupling. Only
// fully-broken entries reach here (pruneEligible), so a partially-broken entry
// keeps its healthy files. Returns true when at least one infohash was deleted
// decypharr-side, so the caller can record the PRUNE outcome.
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
		r.logger.Info().Str("component", "PRUNE").Str("entry", name).Str("infohash", hash).Msg("PRUNE: deleted dead entry decypharr-side (no arr call; arr keeps monitoring)")
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

// repairArrFiles deletes the broken files in one Arr, blocklists their grabs,
// and re-searches anything without a grab record. Returns true when the delete
// succeeded (so the caller may consider the files handled). Concurrency is
// bounded by the sweep's worker count; Sonarr/Radarr handle that many in-flight
// API calls fine, and the actual search/grab work is paced by the Arr's own
// command queue regardless of how the calls arrive.
func (r *Repair) repairArrFiles(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, a *arr.Arr, files []arr.ContentFile) bool {
	// Look up the grab history per broken file. Files whose grab record exists
	// get blocklisted via MarkHistoryFailed (which Sonarr/Radarr auto-re-searches
	// when "Redownload Failed" is on — the default). Files with no grab record
	// (history trimmed, manual import) fall back to an explicit SearchMissing.
	//
	// HistoryIDs are deduped per arr — a season-pack grab covers multiple broken
	// files but only needs one history/failed POST.
	historyIDs := make(map[int]struct{})
	needSearch := make([]arr.ContentFile, 0)
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
			needSearch = append(needSearch, f)
			continue
		}
		id, _, herr := a.FindGrabHistoryID(mediaID)
		if herr != nil || id == 0 {
			needSearch = append(needSearch, f)
			continue
		}
		historyIDs[id] = struct{}{}
	}

	// Clear the EpisodeFile/MovieFile rows first so the upcoming re-search isn't
	// rejected by upgrade-only quality logic.
	if err := a.DeleteFiles(ctx, files); err != nil {
		r.logger.Warn().Err(err).Str("arr", a.Name).Msg("Repair: DeleteFiles failed")
		statsMu.Lock()
		run.Stats.RepairFailed += len(files)
		r.saveRun(run)
		statsMu.Unlock()
		return false
	}

	// Blocklist each unique grab. Errors here are non-fatal: a missing blocklist
	// is bad but DeleteFiles already cleared the rows, so the fallback
	// SearchMissing below still has a chance to recover.
	for id := range historyIDs {
		if ctx != nil && ctx.Err() != nil {
			break
		}
		if err := a.MarkHistoryFailed(id); err != nil {
			r.logger.Warn().Err(err).Str("arr", a.Name).Int("history_id", id).Msg("Repair: MarkHistoryFailed failed")
		}
	}

	// SearchMissing only for files without a grab record. With one,
	// MarkHistoryFailed's auto-re-search covers the same ground without creating
	// an extra command row.
	if len(needSearch) > 0 {
		if err := a.SearchMissing(ctx, needSearch); err != nil {
			r.logger.Warn().Err(err).Str("arr", a.Name).Msg("Repair: SearchMissing fallback failed")
		}
	}

	statsMu.Lock()
	run.Stats.Regrabbed += len(files)
	r.saveRun(run)
	statsMu.Unlock()
	return true
}

// === Candidate enumeration ===

// enumerateCandidates builds the CHECK candidate set. CHECK always enumerates
// the WHOLE hosted library via the managed path (every live entry-item, no arr
// / TMDB / symlink dependency), replacing the old arr-gated enumeration as the
// default detection. When RE-GRAB is enabled, the arr enumeration is run once
// and its arr targeting (arrName/arrKind/contentMap) is merged onto the managed
// candidates so dead items can be routed to the arr; entries with no arr match
// are still CHECK'd/REPAIR'd/PRUNE'd, they just can't be RE-GRAB'd. The
// configured cfg.Source no longer switches detection — it is retained for
// backward compat only.
func (r *Repair) enumerateCandidates(ctx context.Context, cfg config.RepairConfig, actions repairActions) (map[string]*candidate, error) {
	out, err := r.enumerateManagedCandidates(ctx)
	if err != nil {
		return out, err
	}
	if !actions.regrab {
		return out, nil
	}

	arrCands, arrErr := r.enumerateArrCandidates(ctx, cfg)
	if arrErr != nil {
		if errors.Is(arrErr, context.Canceled) {
			return out, arrErr
		}
		r.logger.Warn().Err(arrErr).Msg("Sweep: arr enumeration for RE-GRAB targeting failed; dead items without an arr link can't be re-grabbed this run")
		return out, nil
	}
	mergeArrContext(out, arrCands)
	return out, nil
}

// mergeArrContext folds RE-GRAB arr targeting (arrName/arrKind/contentMap) from
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

// orderCandidatesByLastChecked returns the names of `due` sorted by
// EntryHealth.LastCheckedAt ascending - entries never checked (zero time)
// sort first, then least-recently-checked, etc. Ties (e.g. multiple
// never-checked entries) break on name for a stable, deterministic order
// across runs.
//
// This ordering is what lets a StopSchedule-truncated repair sweep make guaranteed
// forward progress across days: probing an entry updates its LastCheckedAt
// immediately, so it sorts to the back of tomorrow's queue.
func (r *Repair) orderCandidatesByLastChecked(due map[string]*candidate) []string {
	type ordered struct {
		name          string
		lastCheckedAt time.Time
	}
	items := make([]ordered, 0, len(due))
	for name := range due {
		var lastCheckedAt time.Time
		if h, _ := r.manager.storage.GetEntryHealth(name); h != nil {
			lastCheckedAt = h.LastCheckedAt
		}
		items = append(items, ordered{name: name, lastCheckedAt: lastCheckedAt})
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].lastCheckedAt.Equal(items[j].lastCheckedAt) {
			return items[i].lastCheckedAt.Before(items[j].lastCheckedAt)
		}
		return items[i].name < items[j].name
	})
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
// decypharr-side only) and/or RE-GRAB (arr delete + blocklist + search). When
// names is empty every entry with Status=broken is acted on. Returns the new
// RepairRun record immediately; the work runs in the background.
//
// Component precedence (resolveManualActions): an explicit selection runs
// exactly those components — single-component invocation (e.g. PRUNE-only) is
// supported; a nil selection falls back to the configured REPAIR/PRUNE/RE-GRAB
// knobs — never force-all.
//
// Use this from the UI when a previous sweep already identified broken entries
// and the user wants to act on them without paying for another probe pass.
func (r *Repair) FixBroken(ctx context.Context, names []string, sel *ManualActions) (*storage.RepairRun, error) {
	if ctx == nil {
		ctx = r.parentCtx
	}

	actions := r.resolveManualActions(sel, true)
	if !actions.any() {
		return nil, errors.New("no repair action selected: enable REPAIR, PRUNE, or RE-GRAB")
	}

	// Require an arr link only when RE-GRAB is the ONLY thing that can act:
	// PRUNE and REPAIR act on entries with no arr link too, so requiring one
	// would wrongly exclude prune-/repair-eligible broken entries.
	requireArr := actions.regrab && !actions.prune && !actions.repair
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
	// destructive components (PRUNE/RE-GRAB) are bounded by the same per-run
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
			Int("pruned", run.Stats.Pruned).
			Int("regrabbed", run.Stats.Regrabbed).
			Int("regrab_failed", run.Stats.RepairFailed).
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
		// Arr targeting is only needed when RE-GRAB is selected.
		if actions.regrab {
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
		// Single entry is inherently bounded, so it uses an unlimited budget
		// (nil) and is never blocked by the per-run cap. actOnDeadEntry applies
		// only the destructive components (PRUNE/RE-GRAB); REPAIR already ran
		// inline above.
		r.actOnDeadEntry(ctx, pseudo, &statsMu, entryName, final, actions, nil)
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
	// A media recheck is user-scoped to a single media id (inherently bounded),
	// so it runs with an unlimited budget (nil) and is never blocked by the cap.
	err := r.probeAndHealCandidates(ctx, run, candidates, mediaNames, heal, RepairRunOptions{}, actions, nil)
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
		Int("regrabbed", run.Stats.Regrabbed).
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

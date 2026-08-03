package manager

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/sirrobot01/decypharr/internal/config"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// ENUMERATE — bulk provider enumeration.
//
// A DISTINCT OPERATION, not a pre-filter for CHECK. It does not narrow, order
// or otherwise influence the candidate set CHECK probes; CHECK still walks the
// whole managed library afterwards exactly as it did before. The two reach
// verdicts by different means and neither is a cheaper version of the other:
//
//	CHECK      asks "can this be served?" and answers by moving real bytes at
//	           sampled offsets. Authoritative about content. ~13 hours for a
//	           full library pass at the measured probe rate.
//	ENUMERATE  asks "what does the provider say about its own state?" and
//	           answers by relaying it. Authoritative about the PLACEMENT, says
//	           nothing about content it does not mention. 8,997 entries fully
//	           status-classified in 41 requests and ~9 seconds, measured.
//
// The point is the second row's cost. An entry AllDebrid already labels
// "Expired - Files removed" needs no payload probe to be known dead, and
// waiting ~13 hours to rediscover by download what the provider will state in
// under a second is the gap this closes.
//
// ⚠️ ABSENCE IS NOT EVIDENCE — the contract on Client.GetAllTorrents, restated
// here because this is the caller that could most easily violate it. Every
// verdict below is driven by iterating the PROVIDER'S findings and looking up
// our entry. Nothing iterates our entries asking "is this missing from the
// provider list?", because a missing hash means nothing at all: enumeration may
// be partial, paginated short, or unsupported by that provider. A provider whose
// enumeration ERRORS contributes zero findings and is counted in
// EnumProvidersFailed — it never contributes "everything on it is dead".

const (
	// reasonProviderReportsDead prefixes the failure reason recorded by
	// ENUMERATE. The provider's own wording is appended, so the stored reason
	// reads e.g. `provider_reports_dead:Expired - Files removed` rather than a
	// generic code that loses which provider said what.
	reasonProviderReportsDead = "provider_reports_dead"

	// reasonEnumerateNotActiveProvider: the provider reporting this hash dead
	// is NOT the one currently serving our entry. A dead AllDebrid magnet is
	// irrelevant to an entry we already moved to RealDebrid, and acting on it
	// would condemn a working entry on the word of a provider it no longer
	// uses.
	reasonEnumerateNotActiveProvider = "enumerate_not_active_provider"

	// reasonEnumerateStalePlacement: the active provider matches but its
	// torrent ID does not, so the provider's dead record describes a DIFFERENT
	// submission than the one we hold. Declined rather than guessed.
	reasonEnumerateStalePlacement = "enumerate_stale_placement"
)

// providerDeadFinding is one POSITIVE finding: this provider, this torrent id,
// terminally dead, in the provider's own words.
type providerDeadFinding struct {
	provider string
	id       string
	status   string
}

// providerEnumeration is the whole result of one enumeration pass.
type providerEnumeration struct {
	// dead is keyed by infohash. Only positive findings ever land here.
	dead map[string]providerDeadFinding
	// scanned counts every torrent seen, dead or not — the denominator that
	// makes `dead` interpretable.
	scanned int
	// failed names providers whose enumeration errored. Their absence from
	// `dead` carries NO information.
	failed []string
}

// enumerateProviderTorrents asks every configured provider for its full torrent
// list, concurrently, and keeps the terminally-dead ones.
//
// A provider failure is isolated, never fatal: the pass continues with whatever
// the other providers returned, and the failure is recorded so a small `dead`
// set cannot be misread as a clean bill of health.
func (r *Repair) enumerateProviderTorrents(ctx context.Context) providerEnumeration {
	type providerResult struct {
		name    string
		dead    map[string]providerDeadFinding
		scanned int
		err     error
	}

	var clients []struct {
		name   string
		client debrid.Client
	}
	r.manager.Clients().Range(func(name string, client debrid.Client) bool {
		if client != nil {
			clients = append(clients, struct {
				name   string
				client debrid.Client
			}{name, client})
		}
		return true
	})
	// Deterministic order so concurrent findings for the same hash resolve the
	// same way on every run rather than by map-iteration luck.
	sort.Slice(clients, func(i, j int) bool { return clients[i].name < clients[j].name })

	results := make([]providerResult, len(clients))
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := providerResult{name: c.name, dead: map[string]providerDeadFinding{}}
			if ctx.Err() != nil {
				res.err = ctx.Err()
				results[i] = res
				return
			}
			torrents, err := c.client.GetAllTorrents()
			if err != nil {
				res.err = err
				results[i] = res
				return
			}
			for _, t := range torrents {
				if t == nil {
					continue
				}
				res.scanned++
				if !t.ProviderDead || t.InfoHash == "" {
					continue
				}
				res.dead[t.InfoHash] = providerDeadFinding{
					provider: c.name,
					id:       t.Id,
					status:   t.ProviderStatus,
				}
			}
			results[i] = res
		}()
	}
	wg.Wait()

	out := providerEnumeration{dead: map[string]providerDeadFinding{}}
	for _, res := range results {
		if res.err != nil {
			out.failed = append(out.failed, res.name)
			r.logger.Warn().Err(res.err).Str("component", "ENUMERATE").Str("provider", res.name).
				Msg("ENUMERATE: provider enumeration failed; it contributes NO findings this run. " +
					"This is not evidence that its torrents are healthy.")
			continue
		}
		out.scanned += res.scanned
		for hash, finding := range res.dead {
			// First provider wins by sorted name. A hash reported dead by two
			// providers is still resolved against our ACTIVE placement below,
			// so which one is recorded here only affects the reason text.
			if _, ok := out.dead[hash]; !ok {
				out.dead[hash] = finding
			}
		}
		r.logger.Debug().Str("component", "ENUMERATE").Str("provider", res.name).
			Int("scanned", res.scanned).Int("reported_dead", len(res.dead)).
			Msg("ENUMERATE: provider enumerated")
	}
	return out
}

// runProviderEnumeration is the ENUMERATE operation end to end: enumerate,
// resolve findings against our own entries, record verdicts, then hand the
// confirmed-dead set to the ordinary component pipeline.
//
// It writes exactly the same EntryHealth shape CHECK writes, which is what
// keeps REPAIR / PRUNE / ARR-DELETE untouched: they act on a broken health
// record and neither know nor care that this one was reached by asking rather
// than by probing.
func (r *Repair) runProviderEnumeration(ctx context.Context, run *storage.RepairRun, runMu *sync.Mutex, cfg config.RepairConfig, actions repairActions, budget *repairDeletionBudget) {
	log := r.logger.With().Str("run_id", run.ID).Str("component", "ENUMERATE").Logger()

	enum := r.enumerateProviderTorrents(ctx)

	runMu.Lock()
	run.Stats.EnumScanned = enum.scanned
	run.Stats.EnumReportedDead = len(enum.dead)
	run.Stats.EnumProvidersFailed = len(enum.failed)
	r.saveRun(run)
	runMu.Unlock()

	log.Info().
		Int("scanned", enum.scanned).
		Int("reported_dead", len(enum.dead)).
		Int("providers_failed", len(enum.failed)).
		Msg("ENUMERATE: provider enumeration complete")

	if len(enum.dead) == 0 {
		return
	}
	if ctx.Err() != nil {
		return
	}

	// Arr targeting is resolved once, and only when ARR-DELETE is actually
	// enabled — without it BrokenFile carries no ArrName/ArrFileID and
	// ARR-DELETE structurally cannot route, which would be a component that
	// silently declines on every entry.
	var arrContext map[string]*candidate
	if actions.arrDelete {
		arrCands, err := r.enumerateArrCandidates(ctx, cfg)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Warn().Err(err).Msg("ENUMERATE: arr enumeration failed; dead items cannot be arr-deleted this run")
		} else {
			arrContext = arrCands
		}
	}

	healths := xsync.NewMap[string, *storage.EntryHealth]()
	matched, marked := 0, 0
	for hash, finding := range enum.dead {
		if ctx.Err() != nil {
			return
		}
		name, h := r.recordProviderDeadVerdict(hash, finding, arrContext)
		if name == "" {
			continue
		}
		matched++
		if h != nil {
			marked++
			healths.Store(name, h)
		}
	}

	runMu.Lock()
	run.Stats.EnumMatched = matched
	run.Stats.EnumMarkedBroken = marked
	r.saveRun(run)
	runMu.Unlock()

	log.Info().Int("matched", matched).Int("marked_broken", marked).
		Msg("ENUMERATE: verdicts recorded")

	if healths.Size() == 0 || !actions.any() || ctx.Err() != nil {
		return
	}

	runMu.Lock()
	run.Stage = storage.RepairStageRepairing
	r.saveRun(run)
	runMu.Unlock()

	// The same pipeline CHECK's findings go through: REPAIR first (a dead
	// AllDebrid placement is exactly the case re-insertion across providers
	// exists for), then the destructive components for whatever is still dead.
	r.repairBroken(ctx, run, healths, actions, budget)
}

// recordProviderDeadVerdict resolves ONE provider finding against our own state
// and, when it genuinely concerns us, persists the broken verdict.
//
// Returns the entry-folder name when the finding matched one of our entries
// (whether or not a verdict was written) and the health record when one was.
//
// Every decline is named rather than dropped: a finding we correctly ignore and
// a finding we failed to process must not look the same afterwards.
func (r *Repair) recordProviderDeadVerdict(hash string, finding providerDeadFinding, arrContext map[string]*candidate) (string, *storage.EntryHealth) {
	log := r.logger.With().Str("component", "ENUMERATE").Str("infohash", hash).Str("provider", finding.provider).Logger()

	entry, err := r.manager.GetEntry(hash)
	if err != nil || entry == nil {
		// The provider holds a torrent we do not manage. Extremely common on a
		// shared account and not a problem: nothing of ours is affected.
		return "", nil
	}
	name := entry.GetFolder()
	if name == "" {
		return "", nil
	}

	// The provider making the claim must be the one currently serving this
	// entry. AllDebrid calling a magnet dead says nothing about an entry we
	// already moved to RealDebrid.
	if entry.ActiveProvider != finding.provider {
		log.Debug().Str("entry", name).Str("active_provider", entry.ActiveProvider).
			Msg("ENUMERATE: dead report is from a provider that is not serving this entry; ignoring")
		r.noteEnumerateSkip(name, reasonEnumerateNotActiveProvider)
		return name, nil
	}
	placement := entry.GetActiveProvider()
	if placement == nil {
		log.Debug().Str("entry", name).Msg("ENUMERATE: entry has no active placement; nothing to condemn")
		r.noteEnumerateSkip(name, reasonEnumerateNotActiveProvider)
		return name, nil
	}
	// A matching provider with a DIFFERENT torrent id means the provider's dead
	// record is about a submission we no longer hold. Decline rather than guess
	// which of the two is ours.
	if placement.ID != "" && finding.id != "" && placement.ID != finding.id {
		log.Debug().Str("entry", name).Str("our_id", placement.ID).Str("their_id", finding.id).
			Msg("ENUMERATE: dead report names a different submission than our placement; ignoring")
		r.noteEnumerateSkip(name, reasonEnumerateStalePlacement)
		return name, nil
	}

	item, err := r.manager.storage.GetEntryItem(name)
	if err != nil || item == nil {
		// Same discipline as probeEntry: a body we cannot READ is not evidence
		// about content. Downgrade any stale positive verdict instead of
		// asserting broken off a storage failure.
		r.downgradeUnverifiableHealth(name)
		return name, nil
	}

	files := orderedFilenames(item)
	if len(files) == 0 {
		// Structurally empty and the provider says dead: recordStructurallyEmptyEntry
		// already states this correctly (and, deliberately, non-destructively).
		h, _ := r.manager.storage.GetEntryHealth(name)
		if h == nil {
			h = &storage.EntryHealth{EntryName: name}
		}
		r.recordStructurallyEmptyEntry(h, item)
		return name, nil
	}

	reason := reasonProviderReportsDead
	if finding.status != "" {
		reason = reasonProviderReportsDead + ":" + finding.status
	}

	// The provider condemned the PLACEMENT, so every file behind it is
	// unservable — which is also what makes the entry prune-eligible
	// (pruneIneligibleReason requires BrokenCount == FileCount).
	broken := make([]storage.BrokenFile, 0, len(files))
	for _, fileName := range files {
		bf := storage.BrokenFile{
			EntryName: name,
			FileName:  fileName,
			InfoHash:  hash,
			Protocol:  entry.Protocol,
			Reason:    reason,
		}
		if f := item.Files[fileName]; f != nil {
			bf.Size = f.Size
			if f.InfoHash != "" {
				bf.InfoHash = f.InfoHash
			}
		}
		applyArrContext(&bf, arrContext[name], fileName)
		broken = append(broken, bf)
	}
	sort.Slice(broken, func(i, j int) bool { return broken[i].FileName < broken[j].FileName })

	h, _ := r.manager.storage.GetEntryHealth(name)
	if h == nil {
		h = &storage.EntryHealth{EntryName: name}
	}
	now := time.Now()
	h.PreviousStatus = ""
	h.Status = storage.HealthBroken
	h.Structural = false
	h.FileCount = len(files)
	h.BrokenFiles = broken
	h.BrokenCount = len(broken)
	h.Protocol = entry.Protocol
	h.Fingerprint = storage.EntryItemRepairFingerprint(item)
	h.LastCheckedAt = now
	h.LastFailedAt = now
	h.FailureReason = reason
	// NOT stamped with repairProbeVersion. That constant identifies the CHECK
	// payload-probe algorithm, and this verdict did not run it — claiming
	// otherwise would tell a later sweep that these entries had been
	// byte-verified by the current prober when they never were. Leaving it
	// unset keeps them due for a real CHECK.
	h.ProbeVersion = 0
	h.NextCheckDueAt = now.Add(r.verdictRecheckDelay(storage.HealthBroken))
	h.Dirty = false
	h.DirtyReason = ""
	h.ActiveRunID = ""
	h.SetActionSkip(componentRepair, "")
	r.saveHealth(h)

	log.Info().Str("entry", name).Str("provider_status", finding.status).
		Int("files", len(files)).
		Msg("ENUMERATE: provider reports this placement terminally dead; recorded broken")
	return name, h
}

// applyArrContext attaches arr routing to a BrokenFile when ARR-DELETE has
// enumeration context for it. Without ArrName + ArrFileID, entryHealthHasArrLink
// is false and ARR-DELETE declines with reasonArrNoLink.
func applyArrContext(bf *storage.BrokenFile, c *candidate, fileName string) {
	if bf == nil || c == nil {
		return
	}
	cf, ok := c.contentMap[fileName]
	if !ok {
		return
	}
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

// noteEnumerateSkip records why ENUMERATE declined to act on an entry it did
// match, without disturbing that entry's existing health verdict.
func (r *Repair) noteEnumerateSkip(name, reason string) {
	h, _ := r.manager.storage.GetEntryHealth(name)
	if h == nil {
		return
	}
	h.SetActionSkip(componentEnumerate, reason)
	r.saveHealth(h)
}

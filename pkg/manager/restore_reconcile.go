package manager

import (
	"context"
	"sort"
	"sync"

	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Restore reconciliation.
//
// THE BUG: rebuildQueuedTorrentJob decided whether to re-submit a queued
// torrent by looking at OUR record alone — placement present means adopt,
// placement absent means re-submit — which silently treats our own absence as
// proof of the provider's absence. Those are different claims. A crash between
// a successful provider add and our placement write leaves the provider holding
// the torrent and us believing nothing was ever submitted, so boot re-adds it.
//
// It is invisible on the current population by luck, not by design: all 2,932
// queued entries measured genuinely have no provider copy, so the branch that
// would misfire is never reached. Nothing about the mechanism prevents it.
//
// THE FIX: ask the providers. Enumerate every provider once, in bulk, and
// cross-reference against the queue before deciding.
//
// ⚠️ ABSENCE IS NOT EVIDENCE, and the whole design turns on it. Reconciliation
// acts ONLY on a positive sighting — "this provider holds this hash, so adopt
// instead of re-submitting". Every other outcome (hash not seen, enumeration
// failed, provider unreachable, per-entry fetch failed) falls through to the
// existing re-submit path unchanged. There is deliberately no branch anywhere
// below that concludes anything from a hash being missing, so a partial or
// failed enumeration degrades to today's behaviour rather than to a new wrong
// answer.
//
// MERGE DIRECTION: adopt provider status onto our entry, never replace our
// entry with the provider's object. The provider is authoritative about torrent
// state; our record is the only place the arr association, category, callback
// and action live, and losing those would orphan the arr's queue row. That is
// exactly what applyDebridTorrentToEntry does — it mutates our Entry in place.

// providerSighting records that a provider holds a given infohash in a state
// worth adopting.
type providerSighting struct {
	provider string
	id       string
	status   debridTypes.TorrentStatus
}

// restoreReconciliation is the provider-side view captured once at boot.
type restoreReconciliation struct {
	// sightings is keyed by infohash and contains ONLY positive findings.
	sightings map[string]providerSighting
	// answered / failed are recorded for the log line. Neither is consulted to
	// justify an action: a provider that failed simply contributes no
	// sightings, which is already the safe direction.
	answered []string
	failed   []string
	// adopted counts placements taken over instead of re-submitted. Reported by
	// the boot-restore summary so the narrow crash-window case is legible next
	// to the ordinary resume/rebuild counts, rather than being the only thing
	// restore ever says.
	adopted int
}

// queuedTorrentNeedsReconciliation reports whether any entry about to be
// rebuilt would reach the re-submit branch — a queued TORRENT that we hold no
// placement for. Enumerating providers is only worth its cost when at least one
// entry could actually be reconciled, so a queue made entirely of NZBs, or of
// torrents whose placements we already hold, skips it entirely.
func (m *Manager) queuedTorrentNeedsReconciliation(entries []*storage.Entry) bool {
	for _, entry := range entries {
		if entry == nil || entry.IsNZB() {
			continue
		}
		if entry.Status != debridTypes.TorrentStatusQueued {
			continue
		}
		if entry.ActiveProvider != "" && entry.GetActiveProvider() != nil {
			continue
		}
		return true
	}
	return false
}

// buildRestoreReconciliation enumerates every configured provider once and
// indexes what they hold. Providers are queried concurrently; a failure is
// isolated and costs only that provider's sightings.
//
// Terminally-dead copies are deliberately NOT indexed. Adopting a placement the
// provider already calls dead would resume a torrent that can never serve,
// whereas falling through to re-submit at least attempts recovery. So the
// positive finding this acts on is specifically "the provider holds a copy that
// is not dead", not merely "the provider knows this hash".
func (m *Manager) buildRestoreReconciliation(ctx context.Context) *restoreReconciliation {
	type result struct {
		name      string
		sightings map[string]providerSighting
		err       error
	}

	var clients []struct {
		name   string
		client debrid.Client
	}
	m.clients.Range(func(name string, client debrid.Client) bool {
		if client != nil {
			clients = append(clients, struct {
				name   string
				client debrid.Client
			}{name, client})
		}
		return true
	})
	if len(clients) == 0 {
		return nil
	}
	sort.Slice(clients, func(i, j int) bool { return clients[i].name < clients[j].name })

	results := make([]result, len(clients))
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := result{name: c.name, sightings: map[string]providerSighting{}}
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
				if t == nil || t.InfoHash == "" || t.Id == "" {
					continue
				}
				if t.ProviderDead {
					continue
				}
				res.sightings[t.InfoHash] = providerSighting{
					provider: c.name,
					id:       t.Id,
					status:   t.Status,
				}
			}
			results[i] = res
		}()
	}
	wg.Wait()

	rec := &restoreReconciliation{sightings: map[string]providerSighting{}}
	for _, res := range results {
		if res.err != nil {
			rec.failed = append(rec.failed, res.name)
			m.logger.Warn().Err(res.err).Str("provider", res.name).
				Msg("Restore reconciliation: provider enumeration failed; it contributes no sightings. " +
					"Entries it may hold will take the ordinary re-submit path, exactly as before this check existed.")
			continue
		}
		rec.answered = append(rec.answered, res.name)
		for hash, s := range res.sightings {
			if _, ok := rec.sightings[hash]; !ok {
				rec.sightings[hash] = s
			}
		}
	}

	m.logger.Info().
		Strs("providers_answered", rec.answered).
		Strs("providers_failed", rec.failed).
		Int("sightings", len(rec.sightings)).
		Msg("Restore reconciliation: provider state captured")

	if len(rec.sightings) == 0 && len(rec.answered) == 0 {
		// Nothing usable at all; keep nil so the restore path stays on its
		// pre-existing behaviour without a per-entry lookup.
		return nil
	}
	return rec
}

// adoptProviderPlacement is the ONLY action reconciliation takes. It returns a
// resume job when the provider positively holds this entry's torrent, and
// (nil, false) in every other case so the caller re-submits exactly as it did
// before.
//
// The bulk listing is used as a DECISION, not as the adopted data: RealDebrid's
// /torrents list carries no per-file information at all, so building a placement
// straight from it would fabricate a fileless placement. On a positive sighting
// the full object is fetched for that one entry and adopted. That cost is
// proportional to the number of entries actually reconciled — the rare
// crash-window case, measured at zero on the current population — not to the
// size of the library.
func (m *Manager) adoptProviderPlacement(entry *storage.Entry, rec *restoreReconciliation) (*Job, bool) {
	if rec == nil || entry == nil || entry.InfoHash == "" {
		return nil, false
	}
	sighting, ok := rec.sightings[entry.InfoHash]
	if !ok {
		// ABSENCE. Says nothing: the enumeration may be partial, the provider
		// may not enumerate failures, or the torrent may genuinely not exist.
		// Unchanged behaviour.
		return nil, false
	}
	client := m.ProviderClient(sighting.provider)
	if client == nil {
		return nil, false
	}
	remote, err := client.GetTorrent(sighting.id)
	if err != nil || remote == nil {
		m.logger.Warn().Err(err).
			Str("infohash", entry.InfoHash).
			Str("provider", sighting.provider).
			Str("provider_id", sighting.id).
			Msg("Restore reconciliation: provider holds this torrent but its details could not be fetched; re-submitting instead of adopting")
		return nil, false
	}
	if remote.Id == "" {
		remote.Id = sighting.id
	}
	if remote.Debrid == "" {
		remote.Debrid = sighting.provider
	}
	if remote.InfoHash == "" {
		remote.InfoHash = entry.InfoHash
	}

	// Adopt ONTO our entry. Category, Arr, CallbackURL, Action and AddedOn are
	// untouched by this call and must stay that way: our record is the only
	// source of truth for the arr association, and replacing the entry would
	// orphan the arr's queue row.
	applyDebridTorrentToEntry(entry, remote)
	if err := m.queue.Update(entry); err != nil {
		m.logger.Warn().Err(err).Str("infohash", entry.InfoHash).
			Msg("Restore reconciliation: adopted placement could not be persisted; re-submitting instead")
		return nil, false
	}

	rec.adopted++

	m.logger.Info().
		Str("infohash", entry.InfoHash).
		Str("provider", sighting.provider).
		Str("provider_id", sighting.id).
		Str("provider_status", string(remote.Status)).
		Msg("Restore reconciliation: provider already holds this torrent; adopted its state instead of re-submitting")

	return &Job{
		ID:             entry.InfoHash,
		Type:           JobTypeTorrent,
		Entry:          entry,
		ResumeExisting: true,
	}, true
}

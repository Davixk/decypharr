package manager

import (
	"strings"
	"sync"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// PROVIDER ORPHANS — items the provider holds that no local record claims.
//
// THE BLIND SPOT THIS CLOSES. Measured on a live box: RealDebrid reported 99
// active transfers while decypharr held a record for TWO of them. The other 97
// existed nowhere locally — not mislabelled, not in the wrong state, simply
// absent. Every one pinned a provider slot that nothing could ever release,
// because every release path in decypharr starts from a local entry.
//
// At that same instant /api/queue/consistency answered `consistent: true`. It
// was not wrong; it compares the local index against a local scan, and both
// agreed. But that makes the one health signal available structurally incapable
// of seeing the divergence that actually costs us: an account at 100/100 with
// nothing prunable and imports starved behind it. A checker that can only
// compare us against ourselves will always certify a self-consistent blindness.
//
// ⚠️ THIS ONLY REPORTS. It deletes nothing.
//
// That restraint is deliberate and is the whole design. "We have no record of
// it" is an ABSENCE, and absence is the one thing this codebase has repeatedly
// been burned by treating as evidence. A hash can be missing from our view for
// reasons that have nothing to do with it being abandoned: the add is still
// in flight, the record lives under a folder-alias key, a placement was written
// microseconds after the listing was taken. Deleting on that inference would
// destroy live downloads to reclaim slots.
//
// So the checks below are deliberately conservative in ONE direction. Every
// uncertainty resolves to "claimed", never to "orphan": both stores are
// consulted, placement IDs count as claims regardless of status, magnet content
// hashes are resolved for aliases, and anything younger than the grace window
// is exempt outright. An orphan reported here has survived all of that, and is
// still only a number for an operator to act on.
type providerOrphan struct {
	Provider string    `json:"provider"`
	ID       string    `json:"id"`
	InfoHash string    `json:"infohash"`
	Name     string    `json:"name,omitempty"`
	Status   string    `json:"status,omitempty"`
	Added    time.Time `json:"added,omitempty"`
	// Active reports whether the provider still has this in flight, i.e.
	// whether it is holding a download slot rather than just storage.
	Active bool `json:"active"`
}

// providerOrphanGrace exempts recently-created provider items. An add that
// succeeded remotely and has not yet had its placement written locally is
// indistinguishable from an orphan, and it is not one — it is the normal state
// of every torrent for a moment. The window is generous on purpose: a false
// orphan report invites an operator to delete a live download.
const providerOrphanGrace = 30 * time.Minute

// providerOrphanSample bounds how many are carried in the report. The COUNT is
// the actionable signal; the sample is for identifying what they are.
const providerOrphanSample = 50

// ProviderDivergence is the provider-side half of the consistency picture,
// captured by the last torrent refresh.
type ProviderDivergence struct {
	// CheckedAt is zero until a refresh has run. A zero value means UNKNOWN,
	// and must not be read as "no orphans" — the distinction is the same one
	// the fill cache draws between an absent count and a count of zero.
	CheckedAt time.Time                   `json:"checked_at,omitempty"`
	Providers map[string]ProviderOrphaned `json:"providers,omitempty"`
}

type ProviderOrphaned struct {
	// Held is how many items the provider reports in total.
	Held int `json:"held"`
	// Unclaimed is how many of those no local record accounts for.
	Unclaimed int `json:"unclaimed"`
	// UnclaimedActive is the subset still IN FLIGHT at the provider, and it is
	// the number that actually costs something.
	//
	// The distinction is not cosmetic. An unclaimed COMPLETED item consumes
	// storage and, on AllDebrid, a slot against the stored-item cap — annoying,
	// bounded, reclaimable at leisure. An unclaimed ACTIVE item holds a
	// concurrent DOWNLOAD slot, which is the scarce resource that stops new
	// grabs from starting. Reporting only the total invites reasoning about a
	// capacity problem using a number mostly made of things that are not
	// occupying capacity.
	UnclaimedActive int              `json:"unclaimed_active"`
	Sample          []providerOrphan `json:"sample,omitempty"`
}

type providerOrphanTracker struct {
	mu        sync.Mutex
	checkedAt time.Time
	byProvider map[string]ProviderOrphaned
}

func newProviderOrphanTracker() *providerOrphanTracker {
	return &providerOrphanTracker{byProvider: map[string]ProviderOrphaned{}}
}

func (t *providerOrphanTracker) record(provider string, held int, orphans []providerOrphan, now time.Time) {
	if t == nil {
		return
	}
	active := 0
	for _, orphan := range orphans {
		if orphan.Active {
			active++
		}
	}
	// ACTIVE ONES FIRST in the sample. The cap means a sample can be entirely
	// completed items while the handful holding download slots — the ones worth
	// looking at — never appear.
	sorted := make([]providerOrphan, 0, len(orphans))
	for _, orphan := range orphans {
		if orphan.Active {
			sorted = append(sorted, orphan)
		}
	}
	for _, orphan := range orphans {
		if !orphan.Active {
			sorted = append(sorted, orphan)
		}
	}
	sample := sorted
	if len(sample) > providerOrphanSample {
		sample = sample[:providerOrphanSample]
	}
	t.mu.Lock()
	t.checkedAt = now
	t.byProvider[provider] = ProviderOrphaned{
		Held:            held,
		Unclaimed:       len(orphans),
		UnclaimedActive: active,
		Sample:          sample,
	}
	t.mu.Unlock()
}

func (t *providerOrphanTracker) snapshot() ProviderDivergence {
	if t == nil {
		return ProviderDivergence{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := ProviderDivergence{
		CheckedAt: t.checkedAt,
		Providers: make(map[string]ProviderOrphaned, len(t.byProvider)),
	}
	for name, v := range t.byProvider {
		out.Providers[name] = v
	}
	return out
}

// ProviderDivergence exposes the last captured provider-vs-local diff.
func (m *Manager) ProviderDivergence() ProviderDivergence {
	return m.providerOrphans.snapshot()
}

// localClaims is every identity a local record asserts ownership of.
//
// It spans BOTH stores on purpose. The main store holds the library; the queue
// holds in-flight work, and in-flight is precisely the state an orphan
// candidate is in — so consulting only the main store would report every
// healthy download in progress as an abandoned item.
type localClaims struct {
	hashes      map[string]struct{}
	placementID map[string]struct{}
}

func (c localClaims) claims(hash, id string) bool {
	if id != "" {
		if _, ok := c.placementID[id]; ok {
			return true
		}
	}
	if hash == "" {
		return false
	}
	_, ok := c.hashes[strings.ToLower(hash)]
	return ok
}

func (c localClaims) add(entry *storage.Entry) {
	if entry == nil {
		return
	}
	if entry.InfoHash != "" {
		c.hashes[strings.ToLower(entry.InfoHash)] = struct{}{}
	}
	// A folder alias has a synthetic storage key but keeps the content magnet,
	// so its real provider hash is only reachable through that magnet. Without
	// this the alias's provider copy reads as unclaimed.
	if contentHash := utils.ExtractInfoHash(entry.Magnet); contentHash != "" {
		c.hashes[strings.ToLower(contentHash)] = struct{}{}
	}
	// Placement IDs count regardless of the placement's status. The question
	// here is ownership, not health: a local record pointing at a provider ID
	// is a claim on it even when that copy is dead.
	for _, placement := range entry.Providers {
		if placement != nil && placement.ID != "" {
			c.placementID[placement.ID] = struct{}{}
		}
	}
}

// collectLocalClaims scans both stores once.
func (m *Manager) collectLocalClaims() (localClaims, error) {
	claims := localClaims{
		hashes:      map[string]struct{}{},
		placementID: map[string]struct{}{},
	}
	if err := m.storage.ForEachBatch(refreshBatchSize, func(batch []*storage.Entry) error {
		for _, entry := range batch {
			claims.add(entry)
		}
		return nil
	}); err != nil {
		return claims, err
	}
	for _, entry := range m.queue.ListFilter("", config.ProtocolAll, "", nil, "", false) {
		claims.add(entry)
	}
	return claims, nil
}

// findProviderOrphans diffs a provider's full listing against every local claim.
//
// Returns orphans ONLY. A failure to build the local view returns an error and
// no orphans, because a partial claim set would manufacture them wholesale —
// exactly the direction that must never be guessed.
func (m *Manager) findProviderOrphans(provider string, remote []*debridTypes.Torrent, now time.Time) ([]providerOrphan, error) {
	claims, err := m.collectLocalClaims()
	if err != nil {
		return nil, err
	}

	orphans := make([]providerOrphan, 0)
	for _, t := range remote {
		if t == nil || t.Id == "" {
			continue
		}
		if claims.claims(t.InfoHash, t.Id) {
			continue
		}
		// Too young to judge. An add that succeeded remotely and has not yet
		// had its placement written is the normal state of every torrent for a
		// moment, and is indistinguishable from an orphan from here.
		if !t.Added.IsZero() && now.Sub(t.Added) < providerOrphanGrace {
			continue
		}
		orphans = append(orphans, providerOrphan{
			Provider: provider,
			ID:       t.Id,
			InfoHash: t.InfoHash,
			Name:     t.Name,
			Status:   t.ProviderStatus,
			Added:    t.Added,
			// ACTIVE MEANS HOLDING A DOWNLOAD SLOT — membership in the
			// provider's active set, not a negation of "downloaded".
			//
			// It was `!= Downloaded`, which swept in every ERRORED torrent.
			// Measured on a live account that was 640 of them, and the metric
			// built specifically to decide whether a capacity deadlock exists
			// reported 722 where the truth was 92. At 722 it says "everything
			// is a deadlock"; a number that can only say yes answers nothing.
			//
			// An errored or dead copy occupies STORAGE (and on AllDebrid a
			// stored-cap slot). It does not occupy a concurrent download slot,
			// which is the scarce resource this number exists to measure.
			Active: t.Status == debridTypes.TorrentStatusDownloading,
		})
	}
	return orphans, nil
}

// reportProviderOrphans records the diff and says so if there is anything to say.
func (m *Manager) reportProviderOrphans(provider string, remote []*debridTypes.Torrent) {
	now := time.Now()
	orphans, err := m.findProviderOrphans(provider, remote, now)
	if err != nil {
		// UNKNOWN, not zero. Leaving the previous snapshot in place is right:
		// overwriting it with an empty result would turn a failed check into a
		// clean bill of health.
		m.logger.Warn().Err(err).Str("debrid", provider).
			Msg("Could not build the local view to check for provider orphans; the previous result stands")
		return
	}
	m.providerOrphans.record(provider, len(remote), orphans, now)
	if len(orphans) == 0 {
		return
	}

	ids := make([]string, 0, min(10, len(orphans)))
	for _, o := range orphans[:min(10, len(orphans))] {
		ids = append(ids, o.ID)
	}
	active := 0
	for _, o := range orphans {
		if o.Active {
			active++
		}
	}
	m.logger.Warn().
		Str("debrid", provider).
		Int("unclaimed", len(orphans)).
		Int("unclaimed_active", active).
		Int("held", len(remote)).
		Strs("sample_ids", ids).
		Msg("Provider holds items no local record accounts for. unclaimed_active is the number that costs " +
			"capacity — those hold DOWNLOAD slots nothing here can release, because every release path " +
			"starts from a local entry. The remainder occupy storage only. Nothing has been deleted: " +
			"review them on the provider, or via /api/queue/consistency.")
}

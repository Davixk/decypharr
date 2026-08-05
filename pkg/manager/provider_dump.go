package manager

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// PROVIDER DUMP — the COMPLETE provider-vs-local picture, for a manual reconcile.
//
// ProviderDivergence carries counts and a 50-item sample, which is right for a
// health signal and useless for actually reconciling: you cannot fix 800 strays
// from a sample of 50. This returns every item, both directions, with enough
// per-row detail to decide what each one is.
//
// ⚠️ STRICTLY READ-ONLY. It deletes nothing, claims nothing, and changes no
// state whatsoever. Every judgement it enables is the operator's, made outside
// decypharr. That is doctrine, not caution: decypharr must never auto-prune
// unclaimed provider items, because "we have no record of it" is an ABSENCE, and
// a shared provider account makes that absence someone else's live download.
//
// It is also EXPENSIVE — one full enumeration per provider plus a full scan of
// both local stores — so it is an operator-triggered endpoint, never a sweep.

// ProviderDumpItem is one item the PROVIDER holds.
type ProviderDumpItem struct {
	Provider       string    `json:"provider"`
	ProviderID     string    `json:"provider_id"`
	InfoHash       string    `json:"infohash"`
	Name           string    `json:"name,omitempty"`
	ProviderStatus string    `json:"provider_status,omitempty"`
	Progress       float64   `json:"progress"`
	Added          time.Time `json:"added,omitempty"`

	// SlotConsuming uses activeCount semantics: membership in the provider's
	// ACTIVE set, which is the scarce resource.
	//
	// ⚠️ Explicitly NOT `status != downloaded`. That reading swept in every
	// errored torrent and, on a live account, reported 722 slot-holders where the
	// truth was 92. An errored copy costs storage, not a download slot.
	SlotConsuming bool `json:"slot_consuming"`

	// ClaimedBy is which local store accounts for this item: "queue", "main",
	// "both", or "none". "none" is the stray class.
	ClaimedBy string `json:"claimed_by"`
}

// LocalUnmatchedRow is the INVERSE direction: a local record whose provider
// placement the provider did not list back.
//
// ⚠️ ABSENCE IS NOT EVIDENCE, and this list is the one place in the codebase
// that reports on it anyway — deliberately, because the operator asked for the
// ghost rows and this is a report a human reads, not an input to any automatic
// action. A row here may be a genuine ghost (the provider item is gone, the
// local record is dangling) or an artefact of a short/partial enumeration.
// NOTHING may act on it automatically.
type LocalUnmatchedRow struct {
	EntryName  string `json:"entry_name,omitempty"`
	InfoHash   string `json:"infohash,omitempty"`
	Provider   string `json:"provider,omitempty"`
	ProviderID string `json:"provider_id,omitempty"`
	Store      string `json:"store"` // "queue" | "main"
	Status     string `json:"status,omitempty"`
}

type ProviderDumpProvider struct {
	Held            int `json:"held"`
	Unclaimed       int `json:"unclaimed"`
	UnclaimedActive int `json:"unclaimed_active"`

	// Error is set when this provider's enumeration FAILED. When it is set the
	// counts and lists below describe nothing and must not be read as "clean" —
	// a provider that could not be listed contributes no findings, never
	// "everything on it is fine".
	Error string `json:"error,omitempty"`

	Items          []ProviderDumpItem  `json:"items"`
	LocalUnmatched []LocalUnmatchedRow `json:"local_unmatched"`
}

type ProviderDump struct {
	GeneratedAt time.Time                       `json:"generated_at"`
	Providers   map[string]ProviderDumpProvider `json:"providers"`
	// Truncated names any provider whose item list was capped. Reported rather
	// than silently cut, so a short list is never mistaken for a short account.
	Truncated []string `json:"truncated,omitempty"`
}

// providerDumpMaxItems bounds one provider's item list. Generous — this exists
// to reconcile thousands of rows — but not unbounded, because the whole thing is
// serialised into one HTTP response.
const providerDumpMaxItems = 20000

// claimOrigin records WHICH local store asserts a claim, which plain
// localClaims deliberately does not track (it only answers yes/no).
//
// The distinction is what the reconcile needs: a queue-store claim is work in
// flight, a main-store claim is a library entry. A main-store row claiming a
// provider item that is dead or gone is a GHOST — measured as a ~40-item gap
// between AllDebrid's divergence count and a direct API count — and is very
// likely minted by the arr-less brand-new-torrent path.
type claimOrigin struct {
	hashes      map[string]string // lowercased hash -> "queue" | "main" | "both"
	placementID map[string]string
}

func newClaimOrigin() claimOrigin {
	return claimOrigin{hashes: map[string]string{}, placementID: map[string]string{}}
}

func mergeOrigin(existing, store string) string {
	if existing == "" {
		return store
	}
	if existing == store {
		return existing
	}
	return "both"
}

func (c claimOrigin) add(entry *storage.Entry, store string) {
	if entry == nil {
		return
	}
	if entry.InfoHash != "" {
		key := strings.ToLower(entry.InfoHash)
		c.hashes[key] = mergeOrigin(c.hashes[key], store)
	}
	// A folder alias keeps the content magnet; without resolving it the alias's
	// provider copy reads as unclaimed.
	if contentHash := utils.ExtractInfoHash(entry.Magnet); contentHash != "" {
		key := strings.ToLower(contentHash)
		c.hashes[key] = mergeOrigin(c.hashes[key], store)
	}
	for _, placement := range entry.Providers {
		if placement != nil && placement.ID != "" {
			c.placementID[placement.ID] = mergeOrigin(c.placementID[placement.ID], store)
		}
	}
}

// claimedBy answers which store claims this item, or "none".
func (c claimOrigin) claimedBy(hash, id string) string {
	origin := ""
	if id != "" {
		origin = mergeOrigin(origin, c.placementID[id])
	}
	if hash != "" {
		if byHash := c.hashes[strings.ToLower(hash)]; byHash != "" {
			origin = mergeOrigin(origin, byHash)
		}
	}
	if origin == "" {
		return "none"
	}
	return origin
}

// localPlacementRow is one local record's claim on a provider item, kept so the
// inverse direction can be computed without a second store scan.
type localPlacementRow struct {
	row      LocalUnmatchedRow
	provider string
	id       string
}

// collectClaimOrigins scans both stores ONCE, building the claim index and the
// list of local placements to check against the providers' listings.
func (m *Manager) collectClaimOrigins() (claimOrigin, []localPlacementRow, error) {
	origins := newClaimOrigin()
	var placements []localPlacementRow

	collect := func(entry *storage.Entry, store string) {
		if entry == nil {
			return
		}
		origins.add(entry, store)
		for name, placement := range entry.Providers {
			if placement == nil || placement.ID == "" {
				continue
			}
			placements = append(placements, localPlacementRow{
				row: LocalUnmatchedRow{
					EntryName:  entry.GetFolder(),
					InfoHash:   entry.InfoHash,
					Provider:   name,
					ProviderID: placement.ID,
					Store:      store,
					Status:     string(entry.State),
				},
				provider: name,
				id:       placement.ID,
			})
		}
	}

	if err := m.storage.ForEachBatch(refreshBatchSize, func(batch []*storage.Entry) error {
		for _, entry := range batch {
			collect(entry, "main")
		}
		return nil
	}); err != nil {
		return origins, nil, err
	}
	for _, entry := range m.queue.ListFilter("", config.ProtocolAll, "", nil, "", false) {
		collect(entry, "queue")
	}
	return origins, placements, nil
}

// ProviderDumpReport builds the complete reconcile picture.
//
// A provider whose enumeration fails is recorded with its Error set and
// contributes NO findings. It is never reported as clean — the same contract
// ENUMERATE holds, for the same reason.
func (m *Manager) ProviderDumpReport() (ProviderDump, error) {
	origins, placements, err := m.collectClaimOrigins()
	if err != nil {
		// A partial claim set would manufacture strays wholesale, which is the
		// one direction that must never be guessed.
		return ProviderDump{}, fmt.Errorf("could not build the local claim index: %w", err)
	}

	out := ProviderDump{
		GeneratedAt: time.Now(),
		Providers:   map[string]ProviderDumpProvider{},
	}

	// provider -> set of ids the provider actually listed back, for the inverse
	// direction. Only populated for providers that enumerated successfully.
	listed := map[string]map[string]struct{}{}

	m.clients.Range(func(name string, client debrid.Client) bool {
		if client == nil {
			return true
		}
		entry := ProviderDumpProvider{
			Items:          []ProviderDumpItem{},
			LocalUnmatched: []LocalUnmatchedRow{},
		}

		torrents, err := client.GetAllTorrents()
		if err != nil {
			entry.Error = err.Error()
			out.Providers[name] = entry
			return true
		}

		ids := make(map[string]struct{}, len(torrents))
		for _, t := range torrents {
			if t == nil || t.Id == "" {
				continue
			}
			ids[t.Id] = struct{}{}
			entry.Held++

			claimedBy := origins.claimedBy(t.InfoHash, t.Id)
			active := t.Status == debridTypes.TorrentStatusDownloading
			if claimedBy == "none" {
				entry.Unclaimed++
				if active {
					entry.UnclaimedActive++
				}
			}
			if len(entry.Items) < providerDumpMaxItems {
				entry.Items = append(entry.Items, ProviderDumpItem{
					Provider:       name,
					ProviderID:     t.Id,
					InfoHash:       t.InfoHash,
					Name:           t.Name,
					ProviderStatus: t.ProviderStatus,
					Progress:       t.Progress,
					Added:          t.Added,
					SlotConsuming:  active,
					ClaimedBy:      claimedBy,
				})
			}
		}
		if entry.Held > len(entry.Items) {
			out.Truncated = append(out.Truncated, name)
		}
		listed[name] = ids
		out.Providers[name] = entry
		return true
	})

	// Inverse direction: local placements the provider did not list back.
	// Skipped entirely for a provider that failed to enumerate — its silence
	// carries no information at all, and treating it as "the provider has none of
	// these" would report the entire library as ghosts.
	for _, p := range placements {
		ids, ok := listed[p.provider]
		if !ok {
			continue
		}
		if _, found := ids[p.id]; found {
			continue
		}
		entry := out.Providers[p.provider]
		entry.LocalUnmatched = append(entry.LocalUnmatched, p.row)
		out.Providers[p.provider] = entry
	}

	// Stable ordering: slot-consuming strays first, because those are what the
	// reconcile is actually chasing, then unclaimed, then everything else.
	for name, entry := range out.Providers {
		items := entry.Items
		sort.SliceStable(items, func(i, j int) bool {
			ri, rj := dumpRank(items[i]), dumpRank(items[j])
			if ri != rj {
				return ri < rj
			}
			return items[i].ProviderID < items[j].ProviderID
		})
		entry.Items = items
		out.Providers[name] = entry
	}
	sort.Strings(out.Truncated)

	return out, nil
}

func dumpRank(item ProviderDumpItem) int {
	switch {
	case item.ClaimedBy == "none" && item.SlotConsuming:
		return 0
	case item.ClaimedBy == "none":
		return 1
	case item.SlotConsuming:
		return 2
	default:
		return 3
	}
}

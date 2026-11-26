package manager

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Provider placements, made visible and individually removable.
//
// WHY THIS EXISTS. An entry can sit on more than one provider at once — a
// re-acquisition, a switch with keep_old, or an add that merged into an existing
// row all leave a second placement behind. Nothing in the API could show that
// map, and the only way to remove a placement was to delete the whole entry,
// which releases EVERY placement, the one the library is reading included.
// Migrating a library off a provider needs both: proof of which copies exist,
// and a way to drop exactly one of them.
//
// ⚠️ CASE TWINS. The main store used to key rows by the infohash string exactly
// as it arrived, so one hash seen in two cases could become two rows. New rows
// are now written lowercase and lookups resolve case-tolerantly (exact key
// first, then lowercase, then a legacy variant — see storage/case_keys.go), but
// twins written before that persist until the operator runs the case-twin
// merge. The report therefore names every OTHER row whose key equals the
// reported one case-insensitively, so a caller holding an uppercase id (Sonarr
// and Radarr report downloadId in uppercase) can see that two rows exist.

var (
	// ErrPlacementIsActive refuses to drop the placement the entry reads from.
	ErrPlacementIsActive = errors.New("placement is the entry's active provider")
	// ErrLastPlacement refuses to drop an entry's only placement.
	ErrLastPlacement = errors.New("placement is the entry's only one")
)

// PlacementReport is the read-only view of one entry's provider placements.
type PlacementReport struct {
	InfoHash       string          `json:"infohash"`
	Name           string          `json:"name"`
	Folder         string          `json:"folder"`
	Protocol       string          `json:"protocol"`
	ActiveProvider string          `json:"active_provider"`
	Bad            bool            `json:"bad"`
	FileCount      int             `json:"file_count"`
	Placements     []PlacementView `json:"placements"`
	// MissingFromActive lists entry files the active placement holds no record
	// for. Reads of those files fail, so a non-empty list after a switch means
	// the target named its files differently and library symlinks will dangle.
	MissingFromActive []string   `json:"missing_from_active,omitempty"`
	CaseTwins         []CaseTwin `json:"case_twins,omitempty"`
}

// PlacementView is one provider's copy of an entry.
type PlacementView struct {
	Provider     string     `json:"provider"`
	ID           string     `json:"id"`
	Status       string     `json:"status"`
	Active       bool       `json:"active"`
	Files        int        `json:"files"`
	AddedAt      time.Time  `json:"added_at"`
	DownloadedAt *time.Time `json:"downloaded_at,omitempty"`
}

// CaseTwin is another main-store row whose key equals this one case-insensitively.
type CaseTwin struct {
	InfoHash       string `json:"infohash"`
	Name           string `json:"name"`
	ActiveProvider string `json:"active_provider"`
}

// EntryPlacements reports an entry's placements, resolving the infohash with
// the store's case-tolerant rule. The report's InfoHash is the key of the row
// actually reported, and CaseTwins lists every other row of the same hash.
//
// When no row resolves, the error wraps storage.ErrEntryNotFound and any twins
// found by a case-insensitive scan are still returned, so the caller is never
// told a hash is gone while a row for it exists.
func (m *Manager) EntryPlacements(infohash string) (*PlacementReport, []CaseTwin, error) {
	if exists, _ := m.EntryExists(infohash); !exists {
		return nil, m.caseTwins(infohash), fmt.Errorf("%w: %s", storage.ErrEntryNotFound, infohash)
	}
	entry, err := m.GetEntry(infohash)
	if err != nil {
		return nil, m.caseTwins(infohash), err
	}
	twins := m.caseTwins(entry.InfoHash)

	report := &PlacementReport{
		InfoHash:       entry.InfoHash,
		Name:           entry.Name,
		Folder:         entry.GetFolder(),
		Protocol:       string(entry.Protocol),
		ActiveProvider: entry.ActiveProvider,
		Bad:            entry.Bad,
		CaseTwins:      twins,
	}
	for _, file := range entry.Files {
		if file != nil && !file.Deleted {
			report.FileCount++
		}
	}

	seen := make(map[string]struct{}, len(entry.Providers))
	for _, placement := range entry.Providers {
		if placement == nil {
			continue
		}
		key := placement.Provider + "\x00" + placement.ID
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		report.Placements = append(report.Placements, PlacementView{
			Provider:     placement.Provider,
			ID:           placement.ID,
			Status:       string(placement.Status),
			Active:       placement.Provider == entry.ActiveProvider,
			Files:        len(placement.Files),
			AddedAt:      placement.AddedAt,
			DownloadedAt: placement.DownloadedAt,
		})
	}
	sort.Slice(report.Placements, func(i, j int) bool {
		if report.Placements[i].Provider != report.Placements[j].Provider {
			return report.Placements[i].Provider < report.Placements[j].Provider
		}
		return report.Placements[i].ID < report.Placements[j].ID
	})

	if active := entry.GetActiveProvider(); active != nil {
		for name, file := range entry.Files {
			if file == nil || file.Deleted {
				continue
			}
			if active.Files[name] == nil {
				report.MissingFromActive = append(report.MissingFromActive, name)
			}
		}
		sort.Strings(report.MissingFromActive)
	}
	return report, twins, nil
}

// caseTwins lists rows whose key equals infohash case-insensitively but not
// exactly; pass the reported row's own key so it is not listed as its own twin.
// It reads metadata only, never a full row.
func (m *Manager) caseTwins(infohash string) []CaseTwin {
	var twins []CaseTwin
	_ = m.storage.ForEachMeta(func(meta *storage.EntryMetaInfo) error {
		if meta.InfoHash != infohash && strings.EqualFold(meta.InfoHash, infohash) {
			twins = append(twins, CaseTwin{InfoHash: meta.InfoHash, Name: meta.Name, ActiveProvider: meta.Provider})
		}
		return nil
	})
	sort.Slice(twins, func(i, j int) bool { return twins[i].InfoHash < twins[j].InfoHash })
	return twins
}

// MergeCaseTwins runs the one-time case-twin merge (storage.MergeCaseTwins):
// every hash still stored under a non-lowercase key is folded into one
// lowercase row, keeping the survivor's folder. With dryRun nothing is written.
//
// 🛑 OPERATOR ACTION ONLY, and it NEVER contacts a provider. It moves records
// between rows; no placement is released, so no reference check is needed.
// Placements are unioned onto the survivor before a twin is deleted, which is
// why it does not need copyEntryMu either: a release scan running on another
// hash can never see a copy with fewer referencing rows than before.
func (m *Manager) MergeCaseTwins(dryRun bool) (*storage.CaseTwinMergeReport, error) {
	report, err := m.storage.MergeCaseTwins(dryRun)
	if !dryRun && report != nil && (report.Merged > 0 || report.Rekeyed > 0) {
		// Cached listings still name the removed keys.
		m.refreshEntryCache()
	}
	return report, err
}

// DropPlacement removes ONE non-active placement from an entry and releases its
// provider copy, unless another row still references that copy.
//
// 🛑 IT REFUSES EVERY CASE IN WHICH IT COULD DESTROY THE COPY THE LIBRARY READS:
//   - the named provider is the ACTIVE one (switch first, then drop);
//   - it is the entry's ONLY placement (that is a delete, and DeleteEntry exists);
//   - the active placement is not `downloaded` (the copy being kept is not yet a
//     copy at all, so the one being dropped may be the only complete one).
//
// Each condition is checked twice: once on the caller's snapshot for a cheap
// refusal, and again inside the mutation fence, so a concurrent change between
// the two cannot turn an allowed drop into a destructive one.
//
// The provider release is reference-checked by removeProviderPlacementIfUnreferenced
// — the same guard SwitchTorrent's source cleanup and DeleteEntry use — and runs
// BEFORE the record changes. A failed release aborts the mutation, so the record
// never claims a copy is gone while it still exists. No arr is contacted.
func (m *Manager) DropPlacement(infohash, provider string) error {
	if exists, _ := m.EntryExists(infohash); !exists {
		return fmt.Errorf("%w: %s", storage.ErrEntryNotFound, infohash)
	}
	expected, err := m.GetEntry(infohash)
	if err != nil {
		return err
	}
	if err := checkPlacementDroppable(expected, provider); err != nil {
		return err
	}

	m.copyEntryMu.Lock()
	defer m.copyEntryMu.Unlock()
	_, present, err := m.storage.MutateEntrySnapshot(expected, func(current *storage.Entry) (bool, error) {
		if err := checkPlacementDroppable(current, provider); err != nil {
			return false, err
		}
		for _, placement := range current.Providers {
			if placement == nil || placement.Provider != provider {
				continue
			}
			if err := m.removeProviderPlacementIfUnreferenced(current.InfoHash, placement); err != nil {
				return false, err
			}
		}
		for key, placement := range current.Providers {
			if placement != nil && placement.Provider == provider {
				delete(current.Providers, key)
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("%w: %s was deleted during the drop", storage.ErrEntryNotFound, infohash)
	}
	// The provider's stored count just fell, so a fill snapshot taken before this
	// would keep reporting the account fuller than it is.
	m.fillCache.invalidate(provider)
	m.refreshEntryCache()
	return nil
}

func checkPlacementDroppable(entry *storage.Entry, provider string) error {
	found := false
	others := 0
	for _, placement := range entry.Providers {
		if placement == nil {
			continue
		}
		if placement.Provider == provider {
			found = true
		} else {
			others++
		}
	}
	if !found {
		return fmt.Errorf("%w: %s holds no placement for %s", storage.ErrPlacementNotFound, entry.InfoHash, provider)
	}
	if entry.ActiveProvider == provider {
		return fmt.Errorf("%w: %s reads from %s; switch it first", ErrPlacementIsActive, entry.InfoHash, provider)
	}
	if others == 0 {
		return fmt.Errorf("%w: %s on %s", ErrLastPlacement, entry.InfoHash, provider)
	}
	active := entry.GetActiveProvider()
	if active == nil || active.Status != debridTypes.TorrentStatusDownloaded {
		return fmt.Errorf("%w: %s's active placement is not downloaded, so %s may be its only complete copy",
			storage.ErrPlacementNotCompleted, entry.InfoHash, provider)
	}
	return nil
}

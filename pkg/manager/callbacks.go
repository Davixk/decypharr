package manager

import (
	"errors"
	"fmt"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

func (m *Manager) RemoveFromProvider(providerEntry *storage.ProviderEntry) error {
	if providerEntry == nil {
		return nil
	}
	if providerEntry.Provider == "" {
		return fmt.Errorf("cannot remove placement %s without a provider", providerEntry.ID)
	}
	if providerEntry.ID == "" {
		return fmt.Errorf("cannot remove %s placement without an id", providerEntry.Provider)
	}
	if providerEntry.Provider == "usenet" {
		return fmt.Errorf("cannot remove Usenet placement %s without its entry generation", providerEntry.ID)
	}

	client := m.ProviderClient(providerEntry.Provider)
	if client == nil {
		return fmt.Errorf("provider client %s not found for placement %s", providerEntry.Provider, providerEntry.ID)
	}
	return client.DeleteTorrent(providerEntry.ID)
}

func (m *Manager) RemoveTorrentPlacements(t *storage.Entry) error {
	m.copyEntryMu.Lock()
	defer m.copyEntryMu.Unlock()
	return m.removeTorrentPlacementsLocked(t)
}

// removeTorrentPlacementsLocked is used by deletion/COPY workflows that
// already hold copyEntryMu. Provider resources are removed only after their
// final durable main-row reference disappears.
func (m *Manager) removeTorrentPlacementsLocked(t *storage.Entry) error {
	if t == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(t.Providers))
	var errs []error
	for _, placement := range t.Providers {
		if placement == nil {
			continue
		}
		key := placement.Provider + "\x00" + placement.ID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if placement.Provider == "usenet" {
			if m.usenet != nil {
				if err := m.deleteUsenetPlacement(placement.ID, t.NZBGeneration); err != nil {
					errs = append(errs, err)
				}
			}
			continue
		}
		if err := m.removeProviderPlacementIfUnreferenced(t.InfoHash, placement); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// deleteUsenetPlacement removes NZB metadata and source artifacts for a
// deleted entry. Entries migrated from pre-generation releases carry a blank
// NZBGeneration; those adopt the metadata's current lifecycle instead of
// failing (a hard failure here used to orphan the metadata because the main
// row had already been removed). Missing metadata is treated as success so
// legacy deletions stay idempotent.
func (m *Manager) deleteUsenetPlacement(id, generation string) error {
	if generation != "" {
		return m.usenet.DeleteForGeneration(id, generation)
	}
	if err := m.usenet.Delete(id); err != nil {
		if errors.Is(err, usenet.ErrNZBNotFound) {
			return nil
		}
		return err
	}
	m.logger.Warn().Str("nzb", id).Msg("Deleted Usenet metadata for legacy entry without an NZB generation (adopt-on-delete)")
	return nil
}

// removeProviderPlacementIfUnreferenced must be called with copyEntryMu held.
// ownerInfohash is always skipped during the scan: Switcher invokes this just
// before atomically removing that owner's reference, and DeleteEntry invokes
// it as pre-delete cleanup while the owner row still exists.
func (m *Manager) removeProviderPlacementIfUnreferenced(ownerInfohash string, placement *storage.ProviderEntry) error {
	if placement == nil {
		return nil
	}
	referenced := false
	if err := m.storage.ForEach(func(entry *storage.Entry) error {
		if entry == nil || entry.InfoHash == ownerInfohash {
			return nil
		}
		for _, candidate := range entry.Providers {
			if candidate != nil && candidate.Provider == placement.Provider && candidate.ID == placement.ID {
				referenced = true
				return nil
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scan references for %s placement %s: %w", placement.Provider, placement.ID, err)
	}
	if referenced {
		return nil
	}
	return m.RemoveFromProvider(placement)
}

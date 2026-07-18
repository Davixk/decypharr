package manager

import (
	"context"
	"fmt"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

func (m *Manager) fixNZBFileSizes(ctx context.Context) {
	if m.usenet == nil {
		return
	}

	ids, err := m.usenet.NZBStorage().GetAllNZBIDs()
	if err != nil {
		m.logger.Warn().Err(err).Msg("Failed to list NZB IDs for size correction")
		return
	}

	updated := 0
	for _, id := range ids {
		select {
		case <-ctx.Done():
			return
		default:
		}

		nzb, changed, err := m.usenet.NormalizeNZBFileSizes(id)
		if err != nil {
			m.logger.Warn().Err(err).Str("nzb_id", id).Msg("Failed to normalize NZB metadata sizes")
			continue
		}
		if !changed || nzb == nil {
			continue
		}
		total := nzb.TotalSize

		entry, entryErr := m.storage.Get(nzb.ID)
		if entryErr == nil && entry != nil && entry.Protocol == config.ProtocolNZB {
			sizes, sizeErr := normalizedNZBEntrySizes(entry, nzb)
			if sizeErr != nil {
				m.logger.Warn().Err(sizeErr).Str("nzb_id", nzb.ID).Msg("Skipped stale NZB entry size correction")
				continue
			}
			if len(sizes) > 0 {
				// The helper checks the returned metadata snapshot, and the atomic
				// persistence helper checks again under the main-entry mutation lock.
				if _, err := m.persistUsenetFileSizesForGeneration(nzb.ID, nzb.Generation, sizes); err != nil {
					m.logger.Warn().Err(err).Str("nzb_id", nzb.ID).Int64("normalized_total", total).Msg("Failed to update entry during NZB size correction")
				}
			}
		}

		updated++
	}

	if updated > 0 {
		m.logger.Info().Int("updated", updated).Msg("Corrected NZB file sizes")
	}
}

func normalizedNZBEntrySizes(entry *storage.Entry, metadata *storage.NZB) (map[string]int64, error) {
	if entry == nil || metadata == nil {
		return nil, fmt.Errorf("entry and NZB metadata are required")
	}
	if entry.InfoHash != metadata.ID {
		return nil, fmt.Errorf("NZB metadata ID %q does not match entry %q", metadata.ID, entry.InfoHash)
	}
	if entry.NZBGeneration == "" || metadata.Generation == "" || entry.NZBGeneration != metadata.Generation {
		return nil, fmt.Errorf("%w: entry generation %q, metadata generation %q", usenet.ErrStaleNZBGeneration, entry.NZBGeneration, metadata.Generation)
	}

	sizes := make(map[string]int64, len(metadata.Files))
	for _, nzbFile := range metadata.Files {
		if _, ok := entry.Files[nzbFile.Name]; ok && nzbFile.Size > 0 {
			sizes[nzbFile.Name] = nzbFile.Size
		}
	}
	return sizes, nil
}

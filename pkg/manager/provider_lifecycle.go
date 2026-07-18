package manager

import (
	"errors"
	"fmt"
	"time"

	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func (m *Manager) persistLinkEntryBad(snapshot *storage.Entry) error {
	if snapshot == nil {
		return fmt.Errorf("entry is nil")
	}
	touched := false
	var errs []error
	_, mainPresent, mainErr := m.storage.MutateEntrySnapshot(snapshot, func(current *storage.Entry) (bool, error) {
		if current.Bad {
			return false, nil
		}
		current.Bad = true
		return true, nil
	})
	if mainErr == nil && mainPresent {
		touched = true
	} else if mainErr != nil && !errors.Is(mainErr, storage.ErrStaleEntryGeneration) {
		errs = append(errs, fmt.Errorf("mark main entry bad: %w", mainErr))
	}

	_, queuePresent, queueErr := m.storage.MutateQueuedSnapshot(snapshot, func(current *storage.Entry) (bool, error) {
		if current.Bad {
			return false, nil
		}
		current.Bad = true
		return true, nil
	})
	if queueErr == nil && queuePresent {
		touched = true
	} else if queueErr != nil && !errors.Is(queueErr, storage.ErrStaleEntryGeneration) {
		errs = append(errs, fmt.Errorf("mark queue entry bad: %w", queueErr))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if !touched {
		return fmt.Errorf("%w for entry %s", storage.ErrStaleEntryGeneration, snapshot.InfoHash)
	}
	return nil
}

// applyProviderTorrent merges only provider-owned remote state into an Entry.
// User and queue workflow fields are deliberately left untouched.
func applyProviderTorrent(entry *storage.Entry, remote *debridTypes.Torrent) {
	if entry == nil || remote == nil {
		return
	}
	if entry.Providers == nil {
		entry.Providers = make(map[string]*storage.ProviderEntry)
	}
	if entry.Files == nil {
		entry.Files = make(map[string]*storage.File)
	}

	addedOn := remote.Added
	if addedOn.IsZero() {
		addedOn = time.Now()
	}
	placement := entry.AddTorrentProvider(remote)
	placement.AddedAt = addedOn
	placement.Progress = remote.Progress
	if remote.Status == debridTypes.TorrentStatusDownloaded {
		downloadedAt := addedOn
		placement.DownloadedAt = &downloadedAt
	}

	for _, remoteFile := range remote.GetFiles() {
		file, exists := entry.Files[remoteFile.Name]
		if !exists || file == nil {
			entry.Files[remoteFile.Name] = &storage.File{
				Name:      remoteFile.Name,
				Size:      remoteFile.Size,
				ByteRange: cloneByteRange(remoteFile.ByteRange),
				Deleted:   remoteFile.Deleted,
				InfoHash:  entry.InfoHash,
				AddedOn:   addedOn,
			}
			continue
		}
		file.Size = remoteFile.Size
		file.ByteRange = cloneByteRange(remoteFile.ByteRange)
		file.Deleted = remoteFile.Deleted
		file.InfoHash = entry.InfoHash
		if file.AddedOn.IsZero() {
			file.AddedOn = addedOn
		}
	}

	if entry.ActiveProvider == "" || len(entry.Providers) == 1 {
		entry.ActiveProvider = remote.Debrid
	}
	if entry.ActiveProvider == remote.Debrid {
		entry.Status = remote.Status
		entry.Progress = remote.Progress
		entry.Speed = remote.Speed
		entry.Seeders = remote.Seeders
		entry.IsComplete = remote.Status == debridTypes.TorrentStatusDownloaded && len(placement.Files) > 0
		if remote.GetSize() > 0 {
			entry.Size = remote.GetSize()
			entry.Bytes = remote.GetSize()
		}
	}
	entry.UpdatedAt = time.Now()
}

// copyProviderView updates a long-lived caller snapshot after a successful
// commit without copying either store's private lifecycle tokens or queue-only
// workflow fields.
func copyProviderView(dst, src *storage.Entry) {
	if dst == nil || src == nil {
		return
	}
	dst.ActiveProvider = src.ActiveProvider
	dst.Providers = cloneProviderEntries(src.Providers)
	dst.Files = cloneEntryFiles(src.Files)
	dst.Size = src.Size
	dst.Bytes = src.Bytes
	dst.Status = src.Status
	dst.Progress = src.Progress
	dst.Speed = src.Speed
	dst.Seeders = src.Seeders
	dst.IsComplete = src.IsComplete
	dst.Bad = src.Bad
	dst.UpdatedAt = src.UpdatedAt
}

func cloneProviderEntries(source map[string]*storage.ProviderEntry) map[string]*storage.ProviderEntry {
	if source == nil {
		return nil
	}
	result := make(map[string]*storage.ProviderEntry, len(source))
	for name, placement := range source {
		if placement == nil {
			result[name] = nil
			continue
		}
		copyPlacement := *placement
		if placement.Files != nil {
			copyPlacement.Files = make(map[string]*storage.ProviderFile, len(placement.Files))
			for filename, file := range placement.Files {
				if file == nil {
					copyPlacement.Files[filename] = nil
					continue
				}
				copyFile := *file
				copyPlacement.Files[filename] = &copyFile
			}
		}
		result[name] = &copyPlacement
	}
	return result
}

func cloneEntryFiles(source map[string]*storage.File) map[string]*storage.File {
	if source == nil {
		return nil
	}
	result := make(map[string]*storage.File, len(source))
	for name, file := range source {
		if file == nil {
			result[name] = nil
			continue
		}
		copyFile := *file
		copyFile.ByteRange = cloneByteRange(file.ByteRange)
		result[name] = &copyFile
	}
	return result
}

func cloneByteRange(source *[2]int64) *[2]int64 {
	if source == nil {
		return nil
	}
	result := *source
	return &result
}

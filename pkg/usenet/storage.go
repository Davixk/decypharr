package usenet

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sourcegraph/conc/pool"
	"google.golang.org/protobuf/proto"
)

const (
	metaFileExtension = ".meta"
	metaDirName       = "meta"
	// metaMigrationMarker is written to the meta dir once all legacy proto
	// files have been upgraded to the v2 codec, so migration runs at most once.
	metaMigrationMarker = ".codec-v2.done"
)

// ErrStaleNZBGeneration means a long-running operation no longer owns the
// metadata currently stored under an NZB ID. Callers should stop that workflow;
// retrying it against the replacement would corrupt the new lifecycle.
var (
	ErrStaleNZBGeneration = errors.New("stale NZB generation")
	ErrNZBNotFound        = errors.New("NZB not found")
)

func newNZBGeneration() string { return uuid.NewString() }

// NewNZBGeneration returns an opaque ownership token that a queue can persist
// before ParseWithGeneration creates or updates any durable NZB metadata.
func NewNZBGeneration() string { return newNZBGeneration() }

const (
	NZBStatusPending     = "pending"
	NZBStatusParsing     = "parsing"
	NZBStatusDownloading = "downloading"
	NZBStatusCompleted   = "completed"
	NZBStatusFailed      = "failed"
)

// NZBStorage handles file-based persistence of NZB metadata using protobuf
type NZBStorage struct {
	metaDir string
	logger  zerolog.Logger
	mu      sync.RWMutex // Protects file operations and cached stats

	// lifecycle serializes read-modify-write operations for one NZB without
	// forcing unrelated IDs through the global metadata write lock. Lock order
	// is lifecycle entry first, then mu.
	lifecycle nzbLifecycleLockSet

	// Test seam used by deterministic concurrency tests. It is set before the
	// tested goroutines start and remains immutable while they run.
	prepareAfterReadHook      func(string)
	migrationBeforeCommitHook func(string)

	// Cached stats for fast Stats() reads without filesystem scans.
	metaCount      int
	metaTotalBytes int64
}

type nzbLifecycleLockSet struct {
	mu      sync.Mutex
	entries map[string]*nzbLifecycleLockEntry
}

type nzbLifecycleLockEntry struct {
	mu   sync.Mutex
	refs int
}

// lock returns an unlock function. Waiting callers count as references, so an
// entry cannot be removed and recreated while another goroutine still holds or
// waits on its mutex (the keyed-lock equivalent of preventing an ABA).
func (l *nzbLifecycleLockSet) lock(id string) func() {
	l.mu.Lock()
	if l.entries == nil {
		l.entries = make(map[string]*nzbLifecycleLockEntry)
	}
	entry := l.entries[id]
	if entry == nil {
		entry = &nzbLifecycleLockEntry{}
		l.entries[id] = entry
	}
	entry.refs++
	l.mu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		l.mu.Lock()
		entry.refs--
		if entry.refs == 0 && l.entries[id] == entry {
			delete(l.entries, id)
		}
		l.mu.Unlock()
	}
}

func (s *NZBStorage) lockNZBLifecycle(id string) func() {
	return s.lifecycle.lock(id)
}

type streamFilePreparation struct {
	size           int64
	advertisedSize int64
	streamableSize int64
	corrected      bool
	err            error
}

// NewNZBStorage creates a new file-based NZB storage
func NewNZBStorage() (*NZBStorage, error) {
	metaDir := filepath.Join(config.GetMainPath(), "usenet", metaDirName)
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create meta directory: %w", err)
	}

	s := &NZBStorage{
		metaDir: metaDir,
		logger:  logger.New("nzb-storage"),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.recalculateStatsLocked(); err != nil {
		return nil, fmt.Errorf("failed to initialize NZB stats cache: %w", err)
	}

	return s, nil
}

// metaFilePath returns the path for a given NZB ID
func (s *NZBStorage) metaFilePath(id string) string {
	return filepath.Join(s.metaDir, id+metaFileExtension)
}

// recalculateStatsLocked rebuilds cached stats by scanning metadata files.
// Caller must hold s.mu.
func (s *NZBStorage) recalculateStatsLocked() error {
	entries, err := os.ReadDir(s.metaDir)
	if err != nil {
		return fmt.Errorf("failed to read meta directory: %w", err)
	}

	count := 0
	var totalSize int64
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != metaFileExtension {
			continue
		}
		count++
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("failed to stat meta file %s: %w", entry.Name(), err)
		}
		totalSize += info.Size()
	}

	s.metaCount = count
	s.metaTotalBytes = totalSize
	return nil
}

// AddNZB saves an NZB to file storage
func (s *NZBStorage) AddNZB(nzb *storage.NZB) error {
	unlock := s.lockNZBLifecycle(nzb.ID)
	defer unlock()
	return s.addNZBWithLifecycleHeld(nzb)
}

func (s *NZBStorage) addNZBWithLifecycleHeld(nzb *storage.NZB) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if nzb == nil || nzb.ID == "" {
		return fmt.Errorf("NZB and ID are required")
	}

	// A populated generation denotes a mutation of one specific lifecycle.
	// Empty-generation callers retain AddNZB's historical replacement semantics
	// and rotate to a fresh token below.
	if nzb.Generation != "" {
		current, exists, err := s.readNZBIfPresentLocked(nzb.ID)
		if err != nil {
			return err
		}
		if exists && current.Generation != "" && current.Generation != nzb.Generation {
			return staleNZBGenerationError(nzb.ID, nzb.Generation, current.Generation)
		}
	}
	return s.writeNZBLocked(nzb)
}

// replaceNZBWithLifecycleHeld deliberately starts a new lifecycle even when an
// NZB with the same ID already exists. ParseWithID uses this for a genuinely new
// submission; resumed workers must use the conditional generation API instead.
func (s *NZBStorage) replaceNZBWithLifecycleHeld(nzb *storage.NZB) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if nzb == nil || nzb.ID == "" {
		return fmt.Errorf("NZB and ID are required")
	}
	if nzb.Generation == "" {
		nzb.Generation = newNZBGeneration()
	}
	return s.writeNZBLocked(nzb)
}

// writeNZBLocked atomically writes metadata and updates cached storage stats.
// Caller must hold s.mu.
func (s *NZBStorage) writeNZBLocked(nzb *storage.NZB) error {
	if nzb.Generation == "" {
		nzb.Generation = newNZBGeneration()
	}
	data, err := encodeNZBV2(nzb)
	if err != nil {
		return fmt.Errorf("failed to encode NZB: %w", err)
	}

	path := s.metaFilePath(nzb.ID)
	var oldSize int64
	alreadyExists := false
	if info, statErr := os.Stat(path); statErr == nil {
		alreadyExists = true
		oldSize = info.Size()
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("failed to stat existing NZB meta file: %w", statErr)
	}

	// Write atomically using temp file
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write NZB meta file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to rename NZB meta file: %w", err)
	}

	newSize := int64(len(data))
	if alreadyExists {
		s.metaTotalBytes += newSize - oldSize
	} else {
		s.metaCount++
		s.metaTotalBytes += newSize
	}

	return nil
}

func (s *NZBStorage) readNZBIfPresentLocked(id string) (*storage.NZB, bool, error) {
	data, err := os.ReadFile(s.metaFilePath(id))
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to read NZB metadata: %w", err)
	}
	nzb, err := decodeNZB(data)
	if err != nil {
		return nil, false, err
	}
	return nzb, true, nil
}

func staleNZBGenerationError(id, expected, actual string) error {
	return fmt.Errorf("%w for NZB %s (expected %q, current %q)", ErrStaleNZBGeneration, id, expected, actual)
}

func requireNZBGeneration(id, expected, actual string) error {
	if expected == actual {
		return nil
	}
	return staleNZBGenerationError(id, expected, actual)
}

// adoptOrRequireNZBGeneration performs the one allowed legacy transition.
// The caller must persist nzb before releasing the lifecycle lock when adopted
// is true; after that transition all comparisons are strict equality.
func adoptOrRequireNZBGeneration(nzb *storage.NZB, expected string) (adopted bool, err error) {
	if nzb.Generation == "" && expected != "" {
		nzb.Generation = expected
		return true, nil
	}
	return false, requireNZBGeneration(nzb.ID, expected, nzb.Generation)
}

func (s *NZBStorage) assertGenerationWithLifecycleHeld(id, expected string) (*storage.NZB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nzb, exists, err := s.readNZBIfPresentLocked(id)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrNZBNotFound, id)
	}
	adopted, err := adoptOrRequireNZBGeneration(nzb, expected)
	if err != nil {
		return nil, err
	}
	if adopted {
		if err := s.writeNZBLocked(nzb); err != nil {
			return nil, fmt.Errorf("persist adopted NZB generation: %w", err)
		}
	}
	return nzb, nil
}

func (s *NZBStorage) assertGenerationIfPresentWithLifecycleHeld(id, expected string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	nzb, exists, err := s.readNZBIfPresentLocked(id)
	if err != nil || !exists {
		return err
	}
	adopted, err := adoptOrRequireNZBGeneration(nzb, expected)
	if err != nil {
		return err
	}
	if adopted {
		return s.writeNZBLocked(nzb)
	}
	return nil
}

// AssertGeneration verifies that id still refers to expected. It is useful at
// the start of expensive work; final mutations perform the same check again.
func (s *NZBStorage) AssertGeneration(id, expected string) error {
	unlock := s.lockNZBLifecycle(id)
	defer unlock()
	_, err := s.assertGenerationWithLifecycleHeld(id, expected)
	return err
}

// markFilePermanentlyFailed records a definitive provider-side content
// failure while holding one lock across read-modify-write. This prevents two
// simultaneous 430s for different files from overwriting each other.
func (s *NZBStorage) markFilePermanentlyFailed(id, filename, reason string) error {
	unlock := s.lockNZBLifecycle(id)
	defer unlock()
	return s.markFilePermanentlyFailedWithLifecycleHeld(id, "", filename, reason)
}

func (s *NZBStorage) markFilePermanentlyFailedForGeneration(id, generation, filename, reason string) error {
	unlock := s.lockNZBLifecycle(id)
	defer unlock()
	return s.markFilePermanentlyFailedWithLifecycleHeld(id, generation, filename, reason)
}

func (s *NZBStorage) markFilePermanentlyFailedWithLifecycleHeld(id, generation, filename, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.metaFilePath(id))
	if err != nil {
		return fmt.Errorf("failed to read NZB metadata: %w", err)
	}
	nzb, err := decodeNZB(data)
	if err != nil {
		return err
	}
	if generation != "" {
		if _, err := adoptOrRequireNZBGeneration(nzb, generation); err != nil {
			return err
		}
	}

	found := false
	for i := range nzb.Files {
		if nzb.Files[i].Name == filename {
			nzb.Files[i].IsDeleted = true
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("file %s not found in NZB %s", filename, id)
	}
	nzb.IsBad = true
	nzb.Status = NZBStatusFailed
	nzb.FailMessage = reason
	return s.writeNZBLocked(nzb)
}

// reconcileFileSize atomically persists the segment-derived stream size.
func (s *NZBStorage) reconcileFileSize(id, filename string, size int64) error {
	unlock := s.lockNZBLifecycle(id)
	defer unlock()
	return s.reconcileFileSizeWithLifecycleHeld(id, filename, size)
}

func (s *NZBStorage) reconcileFileSizeWithLifecycleHeld(id, filename string, size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.metaFilePath(id))
	if err != nil {
		return fmt.Errorf("failed to read NZB metadata: %w", err)
	}
	nzb, err := decodeNZB(data)
	if err != nil {
		return err
	}
	for i := range nzb.Files {
		if nzb.Files[i].Name != filename {
			continue
		}
		old := nzb.Files[i].Size
		if old == size {
			return nil
		}
		nzb.Files[i].Size = size
		nzb.TotalSize += size - old
		return s.writeNZBLocked(nzb)
	}
	return fmt.Errorf("file %s not found in NZB %s", filename, id)
}

// prepareStreamFiles validates and reconciles a set of files under one per-NZB
// lifecycle lock and, when needed, one atomic write. Unrelated NZBs can decode
// and validate concurrently.
func (s *NZBStorage) prepareStreamFiles(id string, filenames []string) (map[string]streamFilePreparation, error) {
	unlock := s.lockNZBLifecycle(id)
	defer unlock()
	return s.prepareStreamFilesWithLifecycleHeld(id, "", filenames)
}

func (s *NZBStorage) prepareStreamFilesForGeneration(id, generation string, filenames []string) (map[string]streamFilePreparation, error) {
	unlock := s.lockNZBLifecycle(id)
	defer unlock()
	return s.prepareStreamFilesWithLifecycleHeld(id, generation, filenames)
}

func (s *NZBStorage) prepareStreamFilesWithLifecycleHeld(id, generation string, filenames []string) (map[string]streamFilePreparation, error) {
	if generation != "" {
		if _, err := s.assertGenerationWithLifecycleHeld(id, generation); err != nil {
			return nil, err
		}
	}
	s.mu.RLock()
	data, err := os.ReadFile(s.metaFilePath(id))
	s.mu.RUnlock()
	if err != nil {
		return nil, fmt.Errorf("failed to read NZB metadata: %w", err)
	}
	if s.prepareAfterReadHook != nil {
		s.prepareAfterReadHook(id)
	}
	nzb, err := decodeNZB(data)
	if err != nil {
		return nil, err
	}
	if generation != "" {
		if err := requireNZBGeneration(id, generation, nzb.Generation); err != nil {
			return nil, err
		}
	}

	requested := make(map[string]struct{}, len(filenames))
	for _, filename := range filenames {
		requested[filename] = struct{}{}
	}
	results, changed := prepareStreamFileResults(id, nzb, requested)
	if !changed {
		return results, nil
	}

	// The lifecycle lock prevents same-ID Add/Delete while validation runs. We
	// still re-read under the exclusive lock before correcting so migration or
	// any future non-lifecycle writer cannot be overwritten with stale bytes.
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err = os.ReadFile(s.metaFilePath(id))
	if err != nil {
		return nil, fmt.Errorf("failed to re-read NZB metadata for correction: %w", err)
	}
	nzb, err = decodeNZB(data)
	if err != nil {
		return nil, err
	}
	if generation != "" {
		if err := requireNZBGeneration(id, generation, nzb.Generation); err != nil {
			return nil, err
		}
	}
	results, changed = prepareStreamFileResults(id, nzb, requested)
	if !changed {
		return results, nil
	}

	var total int64
	for i := range nzb.Files {
		total += nzb.Files[i].Size
	}
	nzb.TotalSize = total
	if err := s.writeNZBLocked(nzb); err != nil {
		return nil, err
	}
	return results, nil
}

func prepareStreamFileResults(id string, nzb *storage.NZB, requested map[string]struct{}) (map[string]streamFilePreparation, bool) {
	results := make(map[string]streamFilePreparation, len(requested))
	changed := false
	for i := range nzb.Files {
		file := &nzb.Files[i]
		if _, ok := requested[file.Name]; !ok {
			continue
		}

		result := streamFilePreparation{advertisedSize: file.Size}
		if file.IsDeleted {
			result.err = customerror.NewArticleNotFoundError(fmt.Errorf("articles missing on provider for %q", file.Name))
			results[file.Name] = result
			continue
		}

		streamableSize, sizeErr := segmentDerivedFileSize(file)
		if sizeErr != nil {
			wrapped := customerror.NewPermanentError(fmt.Errorf("invalid usenet file metadata for %q: %w", file.Name, sizeErr))
			wrapped.Code = "usenet_metadata_invalid"
			result.err = wrapped
			results[file.Name] = result
			continue
		}

		result.streamableSize = streamableSize
		result.size = file.Size
		if result.size <= 0 || result.size > streamableSize {
			result.size = streamableSize
			result.corrected = true
			file.Size = result.size
			changed = true
		}
		results[file.Name] = result
	}

	for filename := range requested {
		if _, ok := results[filename]; !ok {
			results[filename] = streamFilePreparation{
				err: fmt.Errorf("file %s not found in NZB %s", filename, id),
			}
		}
	}
	return results, changed
}

// GetNZB retrieves an NZB from file storage
func (s *NZBStorage) GetNZB(id string) (*storage.NZB, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.metaFilePath(id)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrNZBNotFound, id)
		}
		return nil, fmt.Errorf("failed to read NZB meta file: %w", err)
	}

	return decodeNZB(data)
}

// GetNZBHeader retrieves an NZB without its segment map. It is far cheaper than
// GetNZB for the common case of only needing scalar/file metadata (status,
// path, file list). For legacy proto files it falls back to a full decode.
func (s *NZBStorage) GetNZBHeader(id string) (*storage.NZB, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.metaFilePath(id)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrNZBNotFound, id)
		}
		return nil, fmt.Errorf("failed to read NZB meta file: %w", err)
	}

	if isCodecV2(data) {
		return decodeNZBV2Header(data)
	}
	return decodeNZB(data)
}

// HasSegmentedFiles reports whether any non-deleted file in the durable meta
// carries at least one parsed segment. For v2 blobs it reads only the header
// region (the file table stores per-file segment counts), so callers such as
// the revival sweep can gauge rebuild viability without decompressing —
// or holding — multi-megabyte segment maps. Legacy proto files fall back to a
// full decode.
func (s *NZBStorage) HasSegmentedFiles(id string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(s.metaFilePath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return false, fmt.Errorf("%w: %s", ErrNZBNotFound, id)
		}
		return false, fmt.Errorf("failed to read NZB meta file: %w", err)
	}

	if isCodecV2(data) {
		nzb, counts, err := decodeNZBV2HeaderCounts(data)
		if err != nil {
			return false, err
		}
		for i := range nzb.Files {
			if !nzb.Files[i].IsDeleted && counts[i] > 0 {
				return true, nil
			}
		}
		return false, nil
	}

	nzb, err := decodeNZB(data)
	if err != nil {
		return false, err
	}
	for i := range nzb.Files {
		if !nzb.Files[i].IsDeleted && len(nzb.Files[i].Segments) > 0 {
			return true, nil
		}
	}
	return false, nil
}

// SampleFileMessageIDs returns the sampled message ids for a single file,
// used by availability/repair probes. For v2 blobs it decodes only that file's
// sampled ids (no numeric columns, no NZBSegment allocation, no other files),
// which keeps repair sweeps from holding full segment maps in memory. Legacy
// proto files fall back to a full decode. A nil slice with nil error means the
// file was not found or has no segments.
func (s *NZBStorage) SampleFileMessageIDs(id, filename string, percent int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.metaFilePath(id)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrNZBNotFound, id)
		}
		return nil, fmt.Errorf("failed to read NZB meta file: %w", err)
	}

	if isCodecV2(data) {
		ids, _, err := decodeFileMessageIDsSampled(data, filename, percent)
		return ids, err
	}

	// Legacy proto: full decode then sample in memory.
	nzb, err := decodeNZB(data)
	if err != nil {
		return nil, err
	}
	f := nzb.GetFileByName(filename)
	if f == nil || len(f.Segments) == 0 {
		return nil, nil
	}
	want := sampleIndices(len(f.Segments), percent)
	ids := make([]string, 0, len(want))
	for _, idx := range want {
		ids = append(ids, f.Segments[idx].MessageID)
	}
	return ids, nil
}

// decodeNZB decodes a meta blob, supporting both the v2 codec and legacy
// protobuf files (which migrate to v2 on their next write).
func decodeNZB(data []byte) (*storage.NZB, error) {
	if isCodecV2(data) {
		return decodeNZBV2(data)
	}

	var pb NZBProto
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, fmt.Errorf("failed to unmarshal NZB: %w", err)
	}
	return protoToNZB(&pb), nil
}

// DeleteNZB removes an NZB from file storage
func (s *NZBStorage) DeleteNZB(id string) error {
	unlock := s.lockNZBLifecycle(id)
	defer unlock()
	return s.deleteNZBWithLifecycleHeld(id, "")
}

func (s *NZBStorage) DeleteNZBForGeneration(id, generation string) error {
	unlock := s.lockNZBLifecycle(id)
	defer unlock()
	return s.deleteNZBWithLifecycleHeld(id, generation)
}

func (s *NZBStorage) deleteNZBWithLifecycleHeld(id, generation string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.metaFilePath(id)
	var oldSize int64
	alreadyExists := false
	if info, statErr := os.Stat(path); statErr == nil {
		alreadyExists = true
		oldSize = info.Size()
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("failed to stat NZB meta file before delete: %w", statErr)
	}
	if !alreadyExists {
		return fmt.Errorf("%w: %s", ErrNZBNotFound, id)
	}
	if generation != "" && alreadyExists {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return fmt.Errorf("failed to read NZB metadata before delete: %w", readErr)
		}
		current, decodeErr := decodeNZB(data)
		if decodeErr != nil {
			return decodeErr
		}
		if _, err := adoptOrRequireNZBGeneration(current, generation); err != nil {
			return err
		}
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete NZB meta file: %w", err)
	}

	if alreadyExists {
		if s.metaCount > 0 {
			s.metaCount--
		}
		s.metaTotalBytes -= oldSize
		if s.metaTotalBytes < 0 {
			s.metaTotalBytes = 0
		}
	}

	return nil
}

// ForEachNZB iterates over all NZBs in storage
func (s *NZBStorage) ForEachNZB(fn func(*storage.NZB) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, err := os.ReadDir(s.metaDir)
	if err != nil {
		return fmt.Errorf("failed to read meta directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != metaFileExtension {
			continue
		}

		path := filepath.Join(s.metaDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			s.logger.Warn().Err(err).Str("file", entry.Name()).Msg("Failed to read NZB meta file")
			continue
		}

		nzb, err := decodeNZB(data)
		if err != nil {
			s.logger.Warn().Err(err).Str("file", entry.Name()).Msg("Failed to decode NZB")
			continue
		}

		if err := fn(nzb); err != nil {
			return err
		}
	}

	return nil
}

// GetAllNZBIDs returns all NZB IDs in storage
func (s *NZBStorage) GetAllNZBIDs() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, err := os.ReadDir(s.metaDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read meta directory: %w", err)
	}

	var ids []string
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != metaFileExtension {
			continue
		}
		// Extract ID from filename (remove .meta extension)
		id := entry.Name()[:len(entry.Name())-len(metaFileExtension)]
		ids = append(ids, id)
	}

	return ids, nil
}

// Exists checks if an NZB exists in storage
func (s *NZBStorage) Exists(id string) bool {
	path := s.metaFilePath(id)
	_, err := os.Stat(path)
	return err == nil
}

// Count returns the number of NZBs in storage
func (s *NZBStorage) Count() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.metaCount, nil
}

// Stats returns storage statistics
func (s *NZBStorage) Stats() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]any{
		"count":       s.metaCount,
		"total_bytes": s.metaTotalBytes,
		"meta_dir":    s.metaDir,
	}
}

// MigrateLegacy rewrites any legacy protobuf .meta files to the v2 codec,
// reclaiming the ~4x size difference for NZBs that aren't otherwise re-saved.
//
// It runs at most once: a marker file is written after a clean pass, so
// subsequent calls (e.g. every restart) return immediately without scanning the
// directory. The heavy decode/encode work runs lock-free across a small worker
// pool; the storage lock is taken only briefly per file for a re-check + atomic
// rename, so a multi-thousand-file migration neither blocks startup nor starves
// concurrent readers. Each rewrite uses temp-file + atomic rename, so readers
// always observe a fully-decodable file (old proto or new v2). Decode failures
// are logged and skipped rather than aborting. Returns the number migrated.
func (s *NZBStorage) MigrateLegacy() (int, error) {
	if s.migrationMarkerExists() {
		return 0, nil
	}

	s.mu.RLock()
	entries, err := os.ReadDir(s.metaDir)
	s.mu.RUnlock()
	if err != nil {
		return 0, fmt.Errorf("failed to read meta directory: %w", err)
	}

	// Cheap first-byte probe (lock-free) to collect only the legacy files.
	var legacy []string
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != metaFileExtension {
			continue
		}
		path := filepath.Join(s.metaDir, entry.Name())
		v2, err := fileIsCodecV2(path)
		if err != nil {
			s.logger.Warn().Err(err).Str("file", entry.Name()).Msg("Migration: failed to probe file")
			continue
		}
		if !v2 {
			legacy = append(legacy, path)
		}
	}

	if len(legacy) == 0 {
		s.writeMigrationMarker()
		return 0, nil
	}

	s.logger.Info().Int("legacy", len(legacy)).Msg("Migration: upgrading legacy NZB meta to v2")

	var migrated, failed atomic.Int64
	pl := pool.New().WithMaxGoroutines(min(runtime.NumCPU(), 6))

	for _, path := range legacy {
		pl.Go(func() {
			ok, err := s.migrateFile(path)
			if err != nil {
				s.logger.Warn().Err(err).Str("file", filepath.Base(path)).Msg("Migration: failed to migrate file")
				failed.Add(1)
				return
			}
			if ok {
				if n := migrated.Add(1); n%1000 == 0 {
					s.logger.Info().Int64("migrated", n).Int("total", len(legacy)).Msg("Migration: progress")
				}
			}
		})
	}

	pl.Wait()

	// Recompute cached stats once from disk rather than racing per-file deltas.
	s.mu.Lock()
	_ = s.recalculateStatsLocked()
	s.mu.Unlock()

	if failed.Load() == 0 {
		s.writeMigrationMarker()
	}
	s.logger.Info().Int64("migrated", migrated.Load()).Int64("failed", failed.Load()).Msg("Migration: completed legacy NZB meta upgrade")
	return int(migrated.Load()), nil
}

// migrateFile re-encodes one legacy proto meta file to v2. The expensive
// read/decode/encode runs lock-free; the storage lock is held only for the
// final re-check + atomic rename so a concurrent AddNZB can't be clobbered
// (AddNZB always writes v2, so a file that became v2 meanwhile is skipped).
func (s *NZBStorage) migrateFile(path string) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, fmt.Errorf("read: %w", err)
	}
	if isCodecV2(data) {
		return false, nil
	}

	nzb, err := decodeNZB(data)
	if err != nil {
		return false, fmt.Errorf("decode: %w", err)
	}
	out, err := encodeNZBV2(nzb)
	if err != nil {
		return false, fmt.Errorf("encode: %w", err)
	}

	// Unique temp name so it can't collide with AddNZB's "<path>.tmp".
	tmpPath := path + ".v2tmp"
	if err := os.WriteFile(tmpPath, out, 0644); err != nil {
		return false, fmt.Errorf("write temp: %w", err)
	}
	if s.migrationBeforeCommitHook != nil {
		s.migrationBeforeCommitHook(path)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// If AddNZB rewrote this file as v2 while we were encoding, its content is
	// newer — don't overwrite it with our re-encoded older copy.
	cur, checkErr := fileIsCodecV2(path)
	if checkErr != nil {
		_ = os.Remove(tmpPath)
		if os.IsNotExist(checkErr) {
			// Delete won while the legacy bytes were being encoded. Renaming the
			// temporary conversion here would resurrect the deleted NZB.
			return false, nil
		}
		return false, fmt.Errorf("recheck before migration commit: %w", checkErr)
	}
	if cur {
		_ = os.Remove(tmpPath)
		return false, nil
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return false, fmt.Errorf("rename: %w", err)
	}
	return true, nil
}

func (s *NZBStorage) migrationMarkerPath() string {
	return filepath.Join(s.metaDir, metaMigrationMarker)
}

func (s *NZBStorage) migrationMarkerExists() bool {
	_, err := os.Stat(s.migrationMarkerPath())
	return err == nil
}

func (s *NZBStorage) writeMigrationMarker() {
	if err := os.WriteFile(s.migrationMarkerPath(), []byte("v2\n"), 0644); err != nil {
		s.logger.Warn().Err(err).Msg("Migration: failed to write completion marker")
	}
}

// fileIsCodecV2 cheaply reports whether a meta file already uses the v2 codec
// by reading only its first byte.
func fileIsCodecV2(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	var b [1]byte
	n, err := f.Read(b[:])
	if err != nil && err != io.EOF {
		return false, err
	}
	return n == 1 && b[0] == codecMagicV2, nil
}

// ============================================================================
// Conversion functions between storage.NZB and NZBProto
// ============================================================================

func nzbToProto(nzb *storage.NZB) *NZBProto {
	pb := &NZBProto{
		Id:               nzb.ID,
		Name:             nzb.Name,
		Title:            nzb.Title,
		Path:             nzb.Path,
		TotalSize:        nzb.TotalSize,
		DatePostedUnix:   nzb.DatePosted.Unix(),
		Category:         nzb.Category,
		Groups:           nzb.Groups,
		Downloaded:       nzb.Downloaded,
		AddedOnUnix:      nzb.AddedOn.Unix(),
		LastActivityUnix: nzb.LastActivity.Unix(),
		Status:           nzb.Status,
		Progress:         nzb.Progress,
		Percentage:       nzb.Percentage,
		SizeDownloaded:   nzb.SizeDownloaded,
		Eta:              nzb.ETA,
		Speed:            nzb.Speed,
		CompletedOnUnix:  nzb.CompletedOn.Unix(),
		IsBad:            nzb.IsBad,
		Storage:          nzb.Storage,
		FailMessage:      nzb.FailMessage,
		Password:         nzb.Password,
		Generation:       nzb.Generation,
	}

	pb.Files = make([]*NZBFileProto, len(nzb.Files))
	for i, f := range nzb.Files {
		pb.Files[i] = nzbFileToProto(&f)
	}

	return pb
}

func nzbFileToProto(f *storage.NZBFile) *NZBFileProto {
	pb := &NZBFileProto{
		NzbId:         f.NzbID,
		Name:          f.Name,
		InternalPath:  f.InternalPath,
		Size:          f.Size,
		StartOffset:   f.StartOffset,
		Groups:        f.Groups,
		FileType:      string(f.FileType),
		Password:      f.Password,
		IsDeleted:     f.IsDeleted,
		IsStored:      f.IsStored,
		SegmentSize:   f.SegmentSize,
		EncryptionKey: f.EncryptionKey,
		EncryptionIv:  f.EncryptionIV,
		IsEncrypted:   f.IsEncrypted,
	}

	pb.Segments = make([]*NZBSegmentProto, len(f.Segments))
	for i, s := range f.Segments {
		pb.Segments[i] = &NZBSegmentProto{
			Number:           int32(s.Number),
			MessageId:        s.MessageID,
			Bytes:            s.Bytes,
			StartOffset:      s.StartOffset,
			EndOffset:        s.EndOffset,
			Group:            s.Group,
			SegmentDataStart: s.SegmentDataStart,
		}
	}

	return pb
}

func protoToNZB(pb *NZBProto) *storage.NZB {
	nzb := &storage.NZB{
		ID:             pb.Id,
		Name:           pb.Name,
		Title:          pb.Title,
		Path:           pb.Path,
		TotalSize:      pb.TotalSize,
		DatePosted:     time.Unix(pb.DatePostedUnix, 0),
		Category:       pb.Category,
		Groups:         pb.Groups,
		Downloaded:     pb.Downloaded,
		AddedOn:        time.Unix(pb.AddedOnUnix, 0),
		LastActivity:   time.Unix(pb.LastActivityUnix, 0),
		Status:         pb.Status,
		Progress:       pb.Progress,
		Percentage:     pb.Percentage,
		SizeDownloaded: pb.SizeDownloaded,
		ETA:            pb.Eta,
		Speed:          pb.Speed,
		CompletedOn:    time.Unix(pb.CompletedOnUnix, 0),
		IsBad:          pb.IsBad,
		Storage:        pb.Storage,
		FailMessage:    pb.FailMessage,
		Password:       pb.Password,
		Generation:     pb.Generation,
	}

	nzb.Files = make([]storage.NZBFile, len(pb.Files))
	for i, f := range pb.Files {
		nzb.Files[i] = protoToNZBFile(f)
	}

	return nzb
}

func protoToNZBFile(pb *NZBFileProto) storage.NZBFile {
	f := storage.NZBFile{
		NzbID:         pb.NzbId,
		Name:          pb.Name,
		InternalPath:  pb.InternalPath,
		Size:          pb.Size,
		StartOffset:   pb.StartOffset,
		Groups:        pb.Groups,
		FileType:      storage.NZBFileType(pb.FileType),
		Password:      pb.Password,
		IsDeleted:     pb.IsDeleted,
		IsStored:      pb.IsStored,
		SegmentSize:   pb.SegmentSize,
		EncryptionKey: pb.EncryptionKey,
		EncryptionIV:  pb.EncryptionIv,
		IsEncrypted:   pb.IsEncrypted,
	}

	f.Segments = make([]storage.NZBSegment, len(pb.Segments))
	for i, s := range pb.Segments {
		f.Segments[i] = storage.NZBSegment{
			Number:           int(s.Number),
			MessageID:        s.MessageId,
			Bytes:            s.Bytes,
			StartOffset:      s.StartOffset,
			EndOffset:        s.EndOffset,
			Group:            s.Group,
			SegmentDataStart: s.SegmentDataStart,
		}
	}

	return f
}

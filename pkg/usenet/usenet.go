package usenet

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/fs"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

const (
	bufferSize = 256 * 1024 // 256KB buffer for streaming
)

var streamBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, bufferSize)
	},
}

func acquireStreamBuffer() []byte {
	buf := streamBufferPool.Get().([]byte)
	if cap(buf) < bufferSize {
		buf = make([]byte, bufferSize)
	}
	return buf[:bufferSize]
}

func releaseStreamBuffer(buf []byte) {
	if buf == nil {
		return
	}
	if cap(buf) < bufferSize {
		return
	}
	streamBufferPool.Put(buf[:bufferSize])
}

type fsEntry struct {
	fs            *fs.FS
	volumes       []*types.Volume
	generation    string                  // Durable NZB lifecycle that supplied the segment map
	reader        fs.PrefetchableReaderAt // Shared reader with prefetch capability
	readerSize    int64                   // Size of the volume
	readerCleanup func()                  // Cleanup function for reader
	readerOnce    sync.Once               // Ensures reader is created exactly once
	cleanupOnce   sync.Once               // Ensures a retired entry is closed exactly once
	readerErr     error                   // Error from reader creation (if any)
	refCount      atomic.Int32
	lastAccessed  atomic.Int64 // Unix timestamp
	retired       atomic.Bool  // Fences out new users after metadata invalidation
	unmapped      atomic.Bool  // Allows cleanup only after exact map removal
}

func (fe *fsEntry) cleanup() {
	fe.cleanupOnce.Do(func() {
		if fe.readerCleanup != nil {
			fe.readerCleanup()
			fe.readerCleanup = nil
			fe.reader = nil
		}
	})
}

func (fe *fsEntry) cleanupIfUnused() {
	if fe.retired.Load() && fe.unmapped.Load() && fe.refCount.Load() == 0 {
		fe.cleanup()
	}
}

// acquire takes a reference unless the entry has been retired. The second
// retired check closes the race where retirement starts after the first check;
// in that case the speculative reference is immediately returned.
func (fe *fsEntry) acquire() bool {
	if fe.retired.Load() {
		return false
	}
	fe.refCount.Add(1)
	if fe.retired.Load() {
		fe.release()
		return false
	}
	return true
}

func (fe *fsEntry) release() {
	fe.refCount.Add(-1)
	fe.lastAccessed.Store(utils.NowUnix())
	fe.cleanupIfUnused()
}

// getOrCreateReader returns the shared reader, creating it lazily on first use.
// Uses sync.Once to ensure the reader is created exactly once even under concurrent access.
func (fe *fsEntry) getOrCreateReader() (fs.PrefetchableReaderAt, int64, error) {
	fe.readerOnce.Do(func() {
		var readerAt fs.PrefetchableReaderAt
		var size int64
		var cleanup func()
		var err error

		// Single volume optimization - skip multi-volume overhead
		if len(fe.volumes) == 1 {
			readerAt, size, cleanup, err = fe.fs.CreateReaderAtForVolume(fe.volumes[0])
		} else {
			// Multi-volume case - need to create reader differently
			// For now, fall back to io.ReaderAt (no prefetch for multi-volume)
			var plainReaderAt io.ReaderAt
			plainReaderAt, size, cleanup, err = fe.fs.CreateReaderAt()
			if err != nil {
				fe.readerErr = err
				return
			}
			// Wrap in a no-op prefetchable reader
			readerAt = &noPrefetchReader{ReaderAt: plainReaderAt}
		}

		if err != nil {
			fe.readerErr = err
			return
		}

		fe.reader = readerAt
		fe.readerSize = size
		fe.readerCleanup = cleanup
	})

	if fe.readerErr != nil {
		return nil, 0, fe.readerErr
	}
	// cleanup() nils the reader after the Once has fired; a caller racing a
	// shutdown-path cleanup must get an error, not a nil interface.
	if fe.reader == nil {
		return nil, 0, fmt.Errorf("reader has been closed")
	}
	return fe.reader, fe.readerSize, nil
}

// noPrefetchReader wraps io.ReaderAt for cases where prefetch isn't available
type noPrefetchReader struct {
	io.ReaderAt
}

func (n *noPrefetchReader) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	nr, err := n.ReaderAt.ReadAt(p, off)
	if ctxErr := ctx.Err(); ctxErr != nil && nr == 0 {
		return 0, ctxErr
	}
	return nr, err
}

func (n *noPrefetchReader) Prefetch(ctx context.Context, off, length int64) {
	// No-op for multi-volume readers
}

type contextSectionReader struct {
	ctx   context.Context
	r     fs.PrefetchableReaderAt
	base  int64
	limit int64
	off   int64
}

func newContextSectionReader(ctx context.Context, r fs.PrefetchableReaderAt, off, length int64) *contextSectionReader {
	if ctx == nil {
		ctx = context.Background()
	}
	return &contextSectionReader{
		ctx:   ctx,
		r:     r,
		base:  off,
		limit: length,
	}
}

func (r *contextSectionReader) Read(p []byte) (int, error) {
	if r.off >= r.limit {
		return 0, io.EOF
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	remaining := r.limit - r.off
	if int64(len(p)) > remaining {
		p = p[:int(remaining)]
	}
	n, err := r.r.ReadAtContext(r.ctx, p, r.base+r.off)
	r.off += int64(n)
	if err == io.EOF && r.off < r.limit {
		return n, io.ErrUnexpectedEOF
	}
	if err == nil && r.off >= r.limit {
		return n, io.EOF
	}
	return n, err
}

type Usenet struct {
	nntp                     *nntp.Client
	logger                   zerolog.Logger
	metadataDir              string
	nzbStorage               *NZBStorage   // File-based NZB metadata storage
	maxConnections           int           // Connections allocated per streaming file
	processingMaxConnections int           // Connections allocated per file for parsing and NZB downloads
	prefetchSize             int64         // Streaming prefetch size in bytes
	readTimeout              time.Duration // Maximum idle time for one stream read (0 = disabled)
	downloadTimeout          time.Duration // Cap on one segment download attempt (0 = disabled)
	failedFiles              *xsync.Map[string, error]
	preparedSizes            *xsync.Map[string, int64]

	// Test seam for deterministic lifecycle/cache publication races. Tests set
	// this before starting goroutines and leave it immutable while they run.
	lifecycleTestHook func(operation, nzoID string)

	fs *xsync.Map[string, *fsEntry]
}

// fsKey builds a cache key for fs map entries efficiently.
// Uses direct byte slice manipulation to avoid strings.Builder overhead.
func fsKey(nzoID, filename string) string {
	// Single allocation: nzoID + "::" + filename
	buf := make([]byte, len(nzoID)+2+len(filename))
	n := copy(buf, nzoID)
	buf[n] = ':'
	buf[n+1] = ':'
	copy(buf[n+2:], filename)
	return string(buf)
}

func (u *Usenet) lockNZBLifecycle(nzoID string) func() {
	return u.nzbStorage.lockNZBLifecycle(nzoID)
}

// clearNZBHotCaches invalidates every file-level result and cached reader for
// an NZB, including filenames that disappeared in a replacement generation.
// The caller holds the NZB lifecycle lock through its durable metadata write
// and this invalidation.
func (u *Usenet) clearNZBHotCaches(nzoID string) {
	prefix := nzoID + "::"
	if u.failedFiles != nil {
		u.failedFiles.Range(func(key string, _ error) bool {
			if strings.HasPrefix(key, prefix) {
				u.failedFiles.Delete(key)
			}
			return true
		})
	}
	if u.preparedSizes != nil {
		u.preparedSizes.Range(func(key string, _ int64) bool {
			if strings.HasPrefix(key, prefix) {
				u.preparedSizes.Delete(key)
			}
			return true
		})
	}
	u.retireFSForNZB(nzoID)
}

// retireFSEntry prevents new users from acquiring entry, removes it only when
// it is still the value mapped at key, and lets active users finish before the
// reader is closed. The idle janitor also uses this path, so retirement and
// acquisition synchronize through the entry atomics rather than a lifecycle
// lock alone.
func (u *Usenet) retireFSEntry(key string, entry *fsEntry) {
	if entry == nil {
		return
	}
	entry.retired.Store(true)
	if u.fs != nil {
		u.fs.Compute(key, func(current *fsEntry, loaded bool) (*fsEntry, xsync.ComputeOp) {
			if loaded && current == entry {
				return nil, xsync.DeleteOp
			}
			return current, xsync.CancelOp
		})
	}
	entry.unmapped.Store(true)
	entry.cleanupIfUnused()
}

func (u *Usenet) retireFSForNZB(nzoID string) {
	if u.fs == nil {
		return
	}
	prefix := nzoID + "::"
	u.fs.Range(func(key string, entry *fsEntry) bool {
		if strings.HasPrefix(key, prefix) {
			u.retireFSEntry(key, entry)
		}
		return true
	})
}

func (u *Usenet) runLifecycleTestHook(operation, nzoID string) {
	if u.lifecycleTestHook != nil {
		u.lifecycleTestHook(operation, nzoID)
	}
}

// New creates a new usenet instance
func New() (*Usenet, error) {
	cfg := config.Get()
	usenetConfig := cfg.Usenet
	if len(usenetConfig.Providers) == 0 {
		return nil, fmt.Errorf("no usenet providers configured")
	}
	_logger := logger.New("usenet")

	metadataDir := filepath.Join(config.GetMainPath(), "usenet", "nzbs")
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create metadata dir: %w", err)
	}

	// Create file-based NZB storage
	nzbStorage, err := NewNZBStorage()
	if err != nil {
		return nil, fmt.Errorf("failed to create NZB storage: %w", err)
	}

	// One-time (idempotent) upgrade of any legacy protobuf meta files to the v2
	// codec. Runs in the background so it never blocks startup; atomic rewrites
	// keep concurrent reads safe throughout.
	go func() {
		if _, err := nzbStorage.MigrateLegacy(); err != nil {
			nzbStorage.logger.Warn().Err(err).Msg("Legacy NZB meta migration failed")
		}
	}()

	// Create NNTP client with retry configuration
	client, err := nntp.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	maxConns := usenetConfig.MaxConnections
	if maxConns <= 0 {
		maxConns = 10
	}
	processingMaxConns := usenetConfig.ProcessingMaxConnections
	if processingMaxConns <= 0 {
		processingMaxConns = maxConns
	}

	prefetchSize, err := config.ParseSize(usenetConfig.ReadAhead)
	if err != nil {
		prefetchSize = 16 * 1024 * 1024 // Default to 16MB
	}
	readTimeout, err := parseTimeoutSetting(usenetConfig.ReadTimeout, 30*time.Second)
	if err != nil {
		_logger.Warn().Err(err).Str("read_timeout", usenetConfig.ReadTimeout).
			Msg("Invalid usenet read_timeout; using 30s")
	}
	downloadTimeout, err := parseTimeoutSetting(usenetConfig.DownloadTimeout, 60*time.Second)
	if err != nil {
		_logger.Warn().Err(err).Str("download_timeout", usenetConfig.DownloadTimeout).
			Msg("Invalid usenet download_timeout; using 60s")
	}

	u := &Usenet{
		nzbStorage:               nzbStorage,
		nntp:                     client,
		logger:                   _logger,
		metadataDir:              metadataDir,
		maxConnections:           maxConns,
		processingMaxConnections: processingMaxConns,
		prefetchSize:             prefetchSize,
		readTimeout:              readTimeout,
		downloadTimeout:          downloadTimeout,
		fs:                       xsync.NewMap[string, *fsEntry](),
		failedFiles:              xsync.NewMap[string, error](),
		preparedSizes:            xsync.NewMap[string, int64](),
	}

	// clean streams dir
	u.initStreamsDir(cfg.Usenet.DiskBufferPath)

	// Start background cleanup for idle sessions
	go u.cleanupIdleFS()

	return u, nil
}

func (u *Usenet) initStreamsDir(streamsDir string) {
	if err := os.RemoveAll(streamsDir); err != nil && !os.IsNotExist(err) {
		return
	}
	if err := os.MkdirAll(streamsDir, 0755); err != nil {
		return
	}
}

func (u *Usenet) createEntry(file *storage.NZBFile, generation string) (*fsEntry, error) {
	volumes := GetFileVolumes(file)
	if len(volumes) == 0 {
		return nil, fmt.Errorf("no volumes available for file %s", file.Name)
	}

	fsCtx := context.Background()

	usenetFS, err := fs.NewFS(fsCtx, u.nntp, u.maxConnections, u.prefetchSize, volumes, u.logger, fs.WithDownloadTimeout(u.downloadTimeout))
	if err != nil {
		return nil, fmt.Errorf("failed to create usenet FS: %w", err)
	}

	return &fsEntry{
		fs:         usenetFS,
		volumes:    volumes,
		generation: generation,
	}, nil
}

// getOrCreateEntry holds the per-NZB lifecycle lock through metadata lookup and
// cache publication, so Delete or replacement cannot miss an in-flight entry
// built from the previous metadata generation.
func (u *Usenet) getOrCreateEntry(ctx context.Context, nzoID, filename string) (*fsEntry, error) {
	return u.getOrCreateEntryForGeneration(ctx, nzoID, "", filename)
}

func (u *Usenet) getOrCreateEntryForGeneration(ctx context.Context, nzoID, generation, filename string) (*fsEntry, error) {
	unlockLifecycle := u.lockNZBLifecycle(nzoID)
	defer unlockLifecycle()
	if generation != "" {
		if _, err := u.nzbStorage.assertGenerationWithLifecycleHeld(nzoID, generation); err != nil {
			return nil, err
		}
	}

	key := fsKey(nzoID, filename)

	// Fast path: entry already exists and has not been retired by metadata
	// invalidation or idle cleanup.
	if entry, ok := u.fs.Load(key); ok {
		if generation != "" && entry.generation != generation {
			u.retireFSEntry(key, entry)
		} else if entry.acquire() {
			entry.lastAccessed.Store(utils.NowUnix())
			return entry, nil
		}
	}

	// Slow path: need to create entry
	file, durableGeneration, err := u.getFileForGeneration(nzoID, generation, filename)
	if err != nil {
		return nil, err
	}

	// Pre-checks
	if err := u.preStreamChecks(file); err != nil {
		return nil, err
	}

	newEntry, err := u.createEntry(file, durableGeneration)
	if err != nil {
		return nil, err
	}
	// Publish with the creator's reference already installed. Otherwise the
	// idle janitor can observe refCount == 0 between LoadOrStore and return,
	// retire the entry, and leave the caller holding a closed reader.
	newEntry.refCount.Store(1)
	newEntry.lastAccessed.Store(utils.NowUnix())
	u.runLifecycleTestHook("fs-before-publish", nzoID)

	// Atomically store only if key doesn't exist (prevents race condition)
	for {
		actual, loaded := u.fs.LoadOrStore(key, newEntry)
		if !loaded {
			// We won the race - use our new entry
			u.runLifecycleTestHook("fs-publish", nzoID)
			return newEntry, nil
		}
		// Another goroutine created the entry first - use theirs.
		// Retire our unpublished candidate and return its creator reference.
		if actual.generation != durableGeneration {
			u.retireFSEntry(key, actual)
			if err := ctx.Err(); err != nil {
				newEntry.retired.Store(true)
				newEntry.unmapped.Store(true)
				newEntry.release()
				return nil, err
			}
			runtime.Gosched()
			continue
		}
		if actual.acquire() {
			newEntry.retired.Store(true)
			newEntry.unmapped.Store(true)
			newEntry.release()
			actual.lastAccessed.Store(utils.NowUnix())
			return actual, nil
		}
		// The mapped entry is retired; retry until exact-pointer removal lets
		// our candidate be stored.
		if err := ctx.Err(); err != nil {
			newEntry.retired.Store(true)
			newEntry.unmapped.Store(true)
			newEntry.release()
			return nil, err
		}
		runtime.Gosched()
	}
}

// releaseFS releases the exact entry acquired by the caller. Looking it up by
// key would decrement a replacement generation after the old entry is retired.
func (u *Usenet) releaseFS(entry *fsEntry) {
	if entry != nil {
		entry.release()
	}
}

// cleanupIdleFS removes sessions with refCount=0 that haven't been used recently
func (u *Usenet) cleanupIdleFS() {
	// Keep a warm reader through short pauses, then tear it down. Usenet segment
	// buffering is only for active latency hiding; stale buffers should disappear
	// quickly instead of behaving like a VFS cache.
	const idleThreshold = int64(120) // 2 minutes idle
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		now := utils.NowUnix()

		u.fs.Range(func(key string, entry *fsEntry) bool {
			if entry.refCount.Load() == 0 {
				lastUsed := entry.lastAccessed.Load()
				if now-lastUsed > idleThreshold {
					// Retirement fences out a concurrent Stream that already
					// loaded this entry. If it acquired first, it may finish;
					// otherwise its speculative reference is rolled back.
					u.retireFSEntry(key, entry)
				}
			}
			return true
		})
	}
}

// Parse processes an NZB for download/streaming (quick parse, defers archive extraction)
func (u *Usenet) Parse(ctx context.Context, name string, content []byte, category string) (*storage.NZB, map[string]*parser.FileGroup, error) {
	return u.ParseWithID(ctx, "", name, content, category)
}

// ParseWithID parses an NZB using a caller-provided ID. Supplying the ID lets
// the manager expose a queued entry before the active-download worker starts.
func (u *Usenet) ParseWithID(ctx context.Context, id, name string, content []byte, category string) (*storage.NZB, map[string]*parser.FileGroup, error) {
	return u.parseWithGeneration(ctx, id, "", name, content, category, true)
}

// ParseWithGeneration resumes a queued NZB only if the metadata under id still
// belongs to generation. Unlike ParseWithID it never replaces a different
// lifecycle, so a restored worker cannot overwrite a same-ID delete/re-add.
func (u *Usenet) ParseWithGeneration(ctx context.Context, id, generation, name string, content []byte, category string) (*storage.NZB, map[string]*parser.FileGroup, error) {
	if id == "" {
		return nil, nil, fmt.Errorf("NZB ID is required")
	}
	if generation == "" {
		return nil, nil, fmt.Errorf("NZB generation is required")
	}
	return u.parseWithGeneration(ctx, id, generation, name, content, category, false)
}

func (u *Usenet) parseWithGeneration(ctx context.Context, id, generation, name string, content []byte, category string, replace bool) (*storage.NZB, map[string]*parser.FileGroup, error) {
	if len(content) == 0 {
		return nil, nil, fmt.Errorf("NZB content is empty")
	}
	var unlockLifecycle func()
	if !replace && id != "" {
		// Keep this per-ID lock through validation, network parsing, source write,
		// and commit. Otherwise Delete can win after this check and the final add
		// can resurrect the deleted generation from an absent metadata file.
		unlockLifecycle = u.lockNZBLifecycle(id)
		defer unlockLifecycle()
		if err := u.nzbStorage.assertGenerationIfPresentWithLifecycleHeld(id, generation); err != nil {
			return nil, nil, err
		}
		u.runLifecycleTestHook("parse-generation-checked", id)
	}

	// Validate NZB content
	if err := validateNZB(content); err != nil {
		return nil, nil, fmt.Errorf("invalid NZB content: %w", err)
	}

	// Create parser with the manager
	prs := parser.NewParser(u.nntp, u.processingMaxConnections, u.logger.With().Str("component", "parser").Logger())

	// Quick parse: defer archive extraction for async processing
	nzb, groups, err := prs.Parse(ctx, name, content)
	if err != nil {
		return nil, nil, err
	}
	if id != "" {
		nzb.ID = id
	}
	if replace {
		nzb.Generation = newNZBGeneration()
	} else {
		nzb.Generation = generation
	}
	if unlockLifecycle == nil {
		unlockLifecycle = u.lockNZBLifecycle(nzb.ID)
		defer unlockLifecycle()
	}
	previousSourcePath := ""
	if current, currentErr := u.nzbStorage.GetNZBHeader(nzb.ID); currentErr == nil && current != nil {
		previousSourcePath = current.Path
	}

	nzb.Category = category
	nzb.Status = NZBStatusParsing
	// Save NZB file to disk
	nzbPath, err := u.saveNZBFile(nzb.ID, nzb.Generation, content)
	if err != nil {
		return nil, nil, err
	}
	nzb.Path = nzbPath

	// Mark as processing
	if err := u.markAsProcessing(nzb); err != nil {
		// Don't leave a managed source orphaned after marker creation fails.
		_ = os.Remove(nzbPath)
		return nil, nil, fmt.Errorf("failed to mark NZB as processing: %w", err)
	}

	var persistErr error
	if replace {
		// Keep the generation chosen above because it is already embedded in the
		// source path and returned to the queue as the ownership token.
		persistErr = u.nzbStorage.replaceNZBWithLifecycleHeld(nzb)
	} else {
		persistErr = u.nzbStorage.addNZBWithLifecycleHeld(nzb)
	}
	if persistErr != nil {
		_ = os.Remove(nzbPath + ".processing")
		_ = os.Remove(nzbPath)
		return nil, nil, fmt.Errorf("failed to save NZB to storage: %w", persistErr)
	}
	if previousSourcePath != "" && previousSourcePath != nzbPath {
		removeNZBSourceArtifacts(previousSourcePath)
	}
	u.clearNZBHotCaches(nzb.ID)

	u.logger.Info().
		Str("nzb_id", nzb.ID).
		Str("name", nzb.Name).
		Int("groups", len(groups)).
		Msg("Successfully parsed NZB file")
	return nzb, groups, nil
}

// Process processes archive files in an NZB (full parse)
func (u *Usenet) Process(ctx context.Context, nzb *storage.NZB, groups map[string]*parser.FileGroup) (*storage.NZB, error) {
	if nzb == nil {
		return nil, fmt.Errorf("NZB metadata is required")
	}
	if err := u.nzbStorage.AssertGeneration(nzb.ID, nzb.Generation); err != nil {
		return nzb, fmt.Errorf("refusing to process stale NZB metadata: %w", err)
	}
	u.logger.Info().
		Str("nzb_id", nzb.ID).
		Str("name", nzb.Name).
		Msg("Processing archive files in NZB")

	// Create parser with the manager
	prs := parser.NewParser(u.nntp, u.processingMaxConnections, u.logger.With().Str("component", "parser").Logger())
	// Process the groups (archives)
	updatedNZB, err := prs.Process(ctx, nzb, groups)
	if err != nil {
		processErr := fmt.Errorf("failed to process NZB archives: %w", err)
		if markErr := u.markAsFailed(nzb, err); markErr != nil {
			return nzb, errors.Join(processErr, fmt.Errorf("record NZB processing failure: %w", markErr))
		}
		return nzb, processErr
	}

	// Post-parse availability gate: probe a sample of each content file's
	// segments before declaring the NZB complete. Segments can go missing
	// between the original parse and now; without this gate they slip through
	// to Sonarr/Radarr and only surface later as failed ffprobes. Connection
	// errors are non-fatal here (CheckFileAvailability returns nil for those),
	// so a provider hiccup won't wrongly fail an import — only a definitively
	// missing segment (gone on every provider) fails the NZB.
	if err := u.checkNZBAvailability(ctx, updatedNZB); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// Cancellation is a workflow interruption, not evidence that the NZB
			// is bad. Leave durable metadata resumable.
			return updatedNZB, ctxErr
		}
		availabilityErr := fmt.Errorf("availability check failed: %w", err)
		if markErr := u.markAsFailed(updatedNZB, err); markErr != nil {
			return updatedNZB, errors.Join(availabilityErr, fmt.Errorf("record NZB availability failure: %w", markErr))
		}
		return updatedNZB, availabilityErr
	}

	// Mark as completed
	if err := u.markAsCompleted(updatedNZB); err != nil {
		return updatedNZB, fmt.Errorf("failed to mark NZB as completed: %w", err)
	}

	u.logger.Info().
		Str("nzb_id", updatedNZB.ID).
		Str("name", updatedNZB.Name).
		Int("files", len(updatedNZB.Files)).
		Msg("Successfully processed NZB archives (full parse)")
	return updatedNZB, nil
}

// checkAvailability samples each content file's segments (via the same
// repair-bank-gated BatchStat path as CheckFile) and returns an error if any
// file is definitively unavailable — i.e. a sampled segment is missing on
// every provider. Recovery/noise files (par2, ignore), deleted files, and
// segment-less entries are skipped so the gate fails only on genuinely missing
// playable content. Connection-only failures are treated as non-fatal by
// CheckFileAvailability, so they do not fail the NZB. It returns on the first
// definitively-missing file (fail fast).
func (u *Usenet) checkNZBAvailability(ctx context.Context, nzb *storage.NZB) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	samplePercent := config.Get().Usenet.ImportAvailabilitySamplePercent
	for i := range nzb.Files {
		file := &nzb.Files[i]
		if file.IsDeleted || len(file.Segments) == 0 {
			continue
		}
		switch file.FileType {
		case storage.NZBFileTypePar2, storage.NZBFileTypeIgnore:
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := u.CheckFileAvailability(ctx, file, samplePercent); err != nil {
			u.logger.Warn().
				Err(err).
				Str("nzb_id", nzb.ID).
				Str("file", file.Name).
				Msg("Post-parse availability check failed; marking NZB unavailable")
			return fmt.Errorf("file %q unavailable: %w", file.Name, err)
		}
	}
	return nil
}

// CheckFile probes the availability of a single NZB file. Connection use is
// gated by the NNTP client's repair bank so concurrent probes don't starve
// streaming traffic.
func (u *Usenet) CheckFile(ctx context.Context, nzoID, filename string) error {
	// Repair/availability probes only need a sample of one file's message ids.
	// Decode just those (no numeric columns, no NZBSegment structs, no other
	// files) so a full sweep doesn't hold whole segment maps in memory.
	samplePercent := config.Get().Usenet.AvailabilitySamplePercent
	messageIDs, err := u.nzbStorage.SampleFileMessageIDs(nzoID, filename, samplePercent)
	if err != nil {
		return fmt.Errorf("failed to sample file segments: %w", err)
	}
	if len(messageIDs) == 0 {
		return fmt.Errorf("file has no Segments: %s", filename)
	}
	return u.checkAvailability(ctx, filename, messageIDs)
}

func (u *Usenet) CheckFileAvailability(ctx context.Context, file *storage.NZBFile, samplePercent int) error {
	return u.checkAvailability(ctx, file.Name, u.sampleSegments(file.Segments, samplePercent))
}

// checkAvailability batch-STATs the given sampled message ids. The NNTP client
// gates each worker through its internal repair bank so concurrent availability
// checks don't starve streaming connections.
func (u *Usenet) checkAvailability(ctx context.Context, fileName string, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	result, err := u.nntp.BatchStat(ctx, messageIDs)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		// Connection/system error - log and continue (don't fail availability check)
		u.logger.Warn().
			Err(err).
			Str("file", fileName).
			Msg("Non-fatal error during availability check, ignoring")
		return nil
	}

	// Check if all sampled segments are available.
	// Distinguish genuine article-not-found from connection errors:
	//   TotalCount = FoundCount + notFoundCount + ErrorCount
	// Only treat a file as unavailable when segments are definitively missing
	// (notFoundCount > 0). Connection errors mean we couldn't check — treat
	// those the same as the top-level error path above (non-fatal, skip check).
	if !result.AllAvailable() {
		notFoundCount := result.TotalCount - result.FoundCount - result.ErrorCount
		if result.ErrorCount > 0 && notFoundCount == 0 {
			// All failures were connection errors, not missing articles.
			return nil
		}
		// At least some segments are definitively missing.
		u.logger.Warn().
			Str("file", fileName).
			Int("sampled_segments", len(messageIDs)).
			Int("available_segments", result.FoundCount).
			Int("missing_segments", notFoundCount).
			Int("error_count", result.ErrorCount).
			Msg("File is unavailable - one or more segments are missing")
		return customerror.UsenetSegmentMissingError
	}

	return nil
}

// sampleSegments returns a sample of segment message IDs based on the given
// percentage. Always includes first and last segments, then uniformly samples
// from the middle (see sampleIndices).
func (u *Usenet) sampleSegments(segments []storage.NZBSegment, percent int) []string {
	idx := sampleIndices(len(segments), percent)
	if len(idx) == 0 {
		return nil
	}
	out := make([]string, len(idx))
	for i, j := range idx {
		out[i] = segments[j].MessageID
	}
	return out
}

func (u *Usenet) Stop() {
	u.logger.Info().Msg("Stopping Usenet")
}

// Close closes all usenet resources including NNTP connections
func (u *Usenet) Close() error {
	u.logger.Info().Msg("Closing Usenet NNTP client")

	// Close NNTP client FIRST to force-close all active connections.
	// This unblocks any in-flight StreamBody/TCP reads in prefetch workers,
	// allowing SegmentFetcher.Close() (prefetchWg.Wait()) to complete without hanging.
	if u.nntp != nil {
		if err := u.nntp.Close(); err != nil {
			u.logger.Warn().Err(err).Msg("Failed to close NNTP client")
		}
	}

	// Cleanup all active FS entries (fetcher.Close() now completes quickly
	// because connections were already force-closed above)
	u.fs.Range(func(key string, entry *fsEntry) bool {
		entry.cleanup()
		return true
	})
	u.fs.Clear()

	u.logger.Info().Msg("Usenet closed")
	return nil
}

func (u *Usenet) getFile(nzoID, filename string) (*storage.NZBFile, error) {
	file, _, err := u.getFileForGeneration(nzoID, "", filename)
	return file, err
}

func (u *Usenet) getFileForGeneration(nzoID, generation, filename string) (*storage.NZBFile, string, error) {
	nzb, err := u.nzbStorage.GetNZB(nzoID)
	if err != nil {
		return nil, "", fmt.Errorf("metadata load failed: %w", err)
	}
	if generation != "" {
		if err := requireNZBGeneration(nzoID, generation, nzb.Generation); err != nil {
			return nil, "", err
		}
	}
	for i := range nzb.Files {
		source := nzb.Files[i]
		if source.Name != filename {
			continue
		}
		if source.IsDeleted {
			return nil, "", customerror.NewArticleNotFoundError(fmt.Errorf("articles missing on provider for %q", filename))
		}
		file := source
		if file.NzbID == "" {
			file.NzbID = nzoID
		}
		return &file, nzb.Generation, nil
	}
	return nil, "", fmt.Errorf("file %s not found in NZB %s", filename, nzoID)
}

// IsFilePermanentlyFailed checks both the hot failure cache and durable NZB
// metadata. Callers use it before committing HTTP headers.
func (u *Usenet) IsFilePermanentlyFailed(nzoID, filename string) error {
	return u.isFilePermanentlyFailedForGeneration(nzoID, "", filename)
}

// IsFilePermanentlyFailedForGeneration performs the same durable check while
// rejecting a caller that belongs to an older lifecycle.
func (u *Usenet) IsFilePermanentlyFailedForGeneration(nzoID, generation, filename string) error {
	if generation == "" {
		return fmt.Errorf("NZB generation is required")
	}
	return u.isFilePermanentlyFailedForGeneration(nzoID, generation, filename)
}

func (u *Usenet) isFilePermanentlyFailedForGeneration(nzoID, generation, filename string) error {
	unlockLifecycle := u.lockNZBLifecycle(nzoID)
	defer unlockLifecycle()
	if generation != "" {
		if _, err := u.nzbStorage.assertGenerationWithLifecycleHeld(nzoID, generation); err != nil {
			return err
		}
	}

	key := fsKey(nzoID, filename)
	if cause, ok := u.failedFiles.Load(key); ok {
		return customerror.NewArticleNotFoundError(cause)
	}
	// PrepareStream populated this only after checking durable metadata. Any
	// failure discovered later in this process populates failedFiles first, so
	// a known-good file can skip repeated header reads on ranged requests.
	if u.preparedSizes != nil {
		if _, ok := u.preparedSizes.Load(key); ok {
			return nil
		}
	}
	nzb, err := u.nzbStorage.GetNZBHeader(nzoID)
	if err != nil {
		return nil
	}
	for i := range nzb.Files {
		if nzb.Files[i].Name == filename && nzb.Files[i].IsDeleted {
			cause := fmt.Errorf("articles missing on provider for %q", filename)
			u.failedFiles.Store(key, cause)
			return customerror.NewArticleNotFoundError(cause)
		}
	}
	return nil
}

func segmentDerivedFileSize(file *storage.NZBFile) (int64, error) {
	if file == nil || len(file.Segments) == 0 {
		return 0, fmt.Errorf("file has no segments")
	}
	var total int64
	hasOffsets := false
	for _, seg := range file.Segments {
		if seg.Bytes <= 0 {
			return 0, fmt.Errorf("segment %d has invalid size %d", seg.Number, seg.Bytes)
		}
		total += seg.Bytes
		if seg.StartOffset != 0 || seg.EndOffset != 0 {
			hasOffsets = true
		}
	}
	if !hasOffsets {
		return total, nil
	}
	expectedStart := int64(0)
	for _, seg := range file.Segments {
		if seg.StartOffset != expectedStart {
			return 0, fmt.Errorf("segment %d starts at %d, expected %d", seg.Number, seg.StartOffset, expectedStart)
		}
		if seg.EndOffset < seg.StartOffset || seg.EndOffset-seg.StartOffset+1 != seg.Bytes {
			return 0, fmt.Errorf("segment %d has inconsistent byte range %d-%d for %d bytes", seg.Number, seg.StartOffset, seg.EndOffset, seg.Bytes)
		}
		expectedStart = seg.EndOffset + 1
	}
	if expectedStart != total {
		return 0, fmt.Errorf("segment ranges total %d bytes, segment sizes total %d", expectedStart, total)
	}
	return total, nil
}

// PrepareStream validates durable failure state and clamps an advertised size
// that exceeds the contiguous segment map before a caller writes headers. A
// smaller advertised size is legitimate for encrypted stored files, whose
// segment map can include cipher padding, so it must never be expanded here.
func (u *Usenet) PrepareStream(nzoID, filename string) (int64, error) {
	return u.prepareStreamForGeneration(nzoID, "", filename)
}

func (u *Usenet) PrepareStreamForGeneration(nzoID, generation, filename string) (int64, error) {
	if generation == "" {
		return 0, fmt.Errorf("NZB generation is required")
	}
	return u.prepareStreamForGeneration(nzoID, generation, filename)
}

func (u *Usenet) prepareStreamForGeneration(nzoID, generation, filename string) (int64, error) {
	sizes, fileErrors, err := u.prepareStreamsForGeneration(nzoID, generation, []string{filename})
	if err != nil {
		return 0, err
	}
	if fileErr := fileErrors[filename]; fileErr != nil {
		return 0, fileErr
	}
	size, ok := sizes[filename]
	if !ok {
		return 0, fmt.Errorf("file %s was not prepared in NZB %s", filename, nzoID)
	}
	return size, nil
}

// PrepareStreams is the batch form of PrepareStream. Cached results avoid disk
// work on later listings; unresolved files share one metadata decode and write.
func (u *Usenet) PrepareStreams(nzoID string, filenames []string) (map[string]int64, map[string]error, error) {
	return u.prepareStreamsForGeneration(nzoID, "", filenames)
}

func (u *Usenet) PrepareStreamsForGeneration(nzoID, generation string, filenames []string) (map[string]int64, map[string]error, error) {
	if generation == "" {
		return nil, nil, fmt.Errorf("NZB generation is required")
	}
	return u.prepareStreamsForGeneration(nzoID, generation, filenames)
}

func (u *Usenet) prepareStreamsForGeneration(nzoID, generation string, filenames []string) (map[string]int64, map[string]error, error) {
	unlockLifecycle := u.lockNZBLifecycle(nzoID)
	defer unlockLifecycle()
	if generation != "" {
		if _, err := u.nzbStorage.assertGenerationWithLifecycleHeld(nzoID, generation); err != nil {
			return nil, nil, err
		}
	}

	sizes := make(map[string]int64, len(filenames))
	fileErrors := make(map[string]error)
	unresolved := make([]string, 0, len(filenames))

	for _, filename := range filenames {
		key := fsKey(nzoID, filename)
		if cause, ok := u.failedFiles.Load(key); ok {
			fileErrors[filename] = customerror.NewArticleNotFoundError(cause)
			continue
		}
		if u.preparedSizes != nil {
			if size, ok := u.preparedSizes.Load(key); ok {
				sizes[filename] = size
				continue
			}
		}
		unresolved = append(unresolved, filename)
	}
	if len(unresolved) == 0 {
		return sizes, fileErrors, nil
	}

	prepared, err := u.nzbStorage.prepareStreamFilesWithLifecycleHeld(nzoID, generation, unresolved)
	if err != nil {
		return nil, nil, fmt.Errorf("prepare Usenet metadata for %s: %w", nzoID, err)
	}
	u.runLifecycleTestHook("prepare-publish", nzoID)
	for _, filename := range unresolved {
		result := prepared[filename]
		key := fsKey(nzoID, filename)
		if result.err != nil {
			fileErrors[filename] = result.err
			var permanent *customerror.Error
			if errors.As(result.err, &permanent) && permanent.Code == "usenet_article_missing" {
				u.failedFiles.Store(key, result.err)
				if u.preparedSizes != nil {
					u.preparedSizes.Delete(key)
				}
			}
			continue
		}

		size := result.size
		if u.preparedSizes != nil {
			actual, _ := u.preparedSizes.LoadOrStore(key, size)
			size = actual
		}
		sizes[filename] = size
		if result.corrected {
			if u.fs != nil {
				if entry, ok := u.fs.Load(key); ok {
					u.retireFSEntry(key, entry)
				}
			}
			u.logger.Warn().
				Str("nzo_id", nzoID).
				Str("file", filename).
				Int64("advertised_size", result.advertisedSize).
				Int64("segment_size", result.streamableSize).
				Msg("Clamped Usenet file size before streaming")
		}
	}
	return sizes, fileErrors, nil
}

func (u *Usenet) getFiles(nzoID string, filenames []string) (map[string]*storage.NZBFile, error) {
	nzb, err := u.nzbStorage.GetNZB(nzoID)
	if err != nil {
		return nil, fmt.Errorf("metadata load failed: %w", err)
	}

	requested := make(map[string]struct{}, len(filenames))
	for _, filename := range filenames {
		requested[filename] = struct{}{}
	}

	files := make(map[string]*storage.NZBFile, len(requested))
	for i := range nzb.Files {
		source := nzb.Files[i]
		if source.IsDeleted {
			continue
		}
		if _, ok := requested[source.Name]; !ok {
			continue
		}
		file := source
		if file.NzbID == "" {
			file.NzbID = nzoID
		}
		files[file.Name] = &file
	}
	return files, nil
}

func (u *Usenet) preStreamChecks(file *storage.NZBFile) error {
	// Check if we have Segments
	if len(file.Segments) == 0 {
		return fmt.Errorf("file has no Segments: %s", file.Name)
	}

	// Check if file was marked as failed previously
	if cause, ok := u.failedFiles.Load(fsKey(file.NzbID, file.Name)); ok {
		return customerror.NewArticleNotFoundError(cause)
	}

	return nil
}

// Stream streams a file using the new streaming system with caching and worker limiting
func (u *Usenet) Stream(ctx context.Context, nzoID, filename string, start, end int64, writer io.Writer) error {
	return u.streamForGeneration(ctx, nzoID, "", filename, start, end, writer, nil)
}

// StreamReadyInfo describes the exact acquired reader and effective range. A
// caller may safely commit response headers from this value: streaming proceeds
// through the same retained reader handle after the callback returns.
type StreamReadyInfo struct {
	Size  int64
	Start int64
	End   int64
}

type StreamReadyFunc func(StreamReadyInfo) error

// StreamForGeneration guarantees that both the segment map used for the read
// and any durable failure it records belong to generation.
func (u *Usenet) StreamForGeneration(ctx context.Context, nzoID, generation, filename string, start, end int64, writer io.Writer) error {
	if generation == "" {
		return fmt.Errorf("NZB generation is required")
	}
	return u.streamForGeneration(ctx, nzoID, generation, filename, start, end, writer, nil)
}

// StreamForGenerationReady acquires the exact generation-bound reader first,
// reports its authoritative size/range to onReady, then copies bytes through
// that same retained handle. onReady is never called on preparation failure or
// after the requested generation has been replaced.
func (u *Usenet) StreamForGenerationReady(ctx context.Context, nzoID, generation, filename string, start, end int64, writer io.Writer, onReady StreamReadyFunc) error {
	if generation == "" {
		return fmt.Errorf("NZB generation is required")
	}
	return u.streamForGeneration(ctx, nzoID, generation, filename, start, end, writer, onReady)
}

func (u *Usenet) streamForGeneration(ctx context.Context, nzoID, generation, filename string, start, end int64, writer io.Writer, onReady StreamReadyFunc) error {
	deadline := newProgressDeadline(ctx, u.readTimeout)
	defer deadline.Close()
	ctx = deadline.Context
	if err := u.isFilePermanentlyFailedForGeneration(nzoID, generation, filename); err != nil {
		return err
	}

	if start < 0 {
		start = 0
	}
	if end < start {
		return fmt.Errorf("invalid byte range %d-%d", start, end)
	}

	ufsEntry, err := u.getOrCreateEntryForGeneration(ctx, nzoID, generation, filename)
	if err != nil {
		return fmt.Errorf("failed to get or create file system: %w", err)
	}
	defer u.releaseFS(ufsEntry)

	// Use start/end directly - file segments are already positioned correctly
	rangeStart := start
	rangeEnd := end

	// get shared reader from entry (created once, reused by all streams)
	readerAt, readerSize, err := ufsEntry.getOrCreateReader()
	if err != nil {
		return fmt.Errorf("failed to get reader: %w", err)
	}
	if rangeEnd >= readerSize {
		rangeEnd = readerSize - 1
	}
	if rangeEnd < rangeStart {
		return fmt.Errorf("invalid reader byte range %d-%d for size %d", rangeStart, rangeEnd, readerSize)
	}
	u.runLifecycleTestHook("stream-reader-acquired", nzoID)
	readyInfo := StreamReadyInfo{Size: readerSize, Start: rangeStart, End: rangeEnd}
	if generation != "" {
		// Serialize the final current-generation check with replacement through
		// the readiness callback. Replacement may proceed once headers are
		// committed; the retained fsEntry remains safe for this response, and a
		// later 430 is still conditionally fenced by its generation.
		unlockLifecycle := u.lockNZBLifecycle(nzoID)
		if _, generationErr := u.nzbStorage.assertGenerationWithLifecycleHeld(nzoID, generation); generationErr != nil {
			unlockLifecycle()
			return generationErr
		}
		if onReady != nil {
			if readyErr := onReady(readyInfo); readyErr != nil {
				unlockLifecycle()
				return readyErr
			}
		}
		unlockLifecycle()
	} else if onReady != nil {
		if err := onReady(readyInfo); err != nil {
			return err
		}
	}

	length := rangeEnd - rangeStart + 1

	// Check context before starting
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Prefetch only a bounded read-ahead window from the requested start,
	// NOT the entire range. Queuing a whole multi-GB file would flood the
	// fixed-depth prefetch channel with head segments and starve reads that
	// land elsewhere (e.g. ffprobe seeking to the moov atom at EOF). The
	// per-read sliding window in readAtPlain advances this as playback
	// progresses; PreCache separately warms the head and tail.
	prefetchLen := length
	if u.prefetchSize > 0 && prefetchLen > u.prefetchSize {
		prefetchLen = u.prefetchSize
	}
	readerAt.Prefetch(ctx, rangeStart, prefetchLen)

	section := newContextSectionReader(ctx, readerAt, rangeStart, length)
	buf := acquireStreamBuffer()
	defer releaseStreamBuffer(buf)

	// Use a safe copy loop that checks context and validates read counts
	_, err = safeCopyBuffer(ctx, progressWriter{Writer: writer, progress: deadline.Progress}, section, buf)

	// Handle context cancellation explicitly
	if err != nil && ctx.Err() != nil {
		return contextError(ctx)
	}

	// Mark file as failed if article not found (permanent error)
	if err != nil && nntp.IsArticleNotFoundError(err) {
		return u.recordPermanentArticleFailureForGeneration(nzoID, ufsEntry.generation, filename, err)
	}

	return err
}

func (u *Usenet) recordPermanentArticleFailure(nzoID, filename string, articleErr error) error {
	return u.recordPermanentArticleFailureForGeneration(nzoID, "", filename, articleErr)
}

func (u *Usenet) recordPermanentArticleFailureForGeneration(nzoID, generation, filename string, articleErr error) error {
	unlockLifecycle := u.lockNZBLifecycle(nzoID)
	defer unlockLifecycle()

	cause := fmt.Errorf("articles missing on provider for %q: %w", filename, articleErr)
	if persistErr := u.nzbStorage.markFilePermanentlyFailedWithLifecycleHeld(nzoID, generation, filename, cause.Error()); persistErr != nil {
		// Do not acknowledge/cache a permanent 410 until it is durable. A
		// transient metadata write failure must be retried after this request
		// instead of disappearing on process restart.
		return fmt.Errorf("persist permanent usenet failure for %q: %w (article error: %v)", filename, persistErr, cause)
	}

	u.runLifecycleTestHook("failure-publish", nzoID)
	key := fsKey(nzoID, filename)
	if u.fs != nil {
		if entry, ok := u.fs.Load(key); ok {
			u.retireFSEntry(key, entry)
		}
	}
	u.failedFiles.Store(key, cause)
	if u.preparedSizes != nil {
		u.preparedSizes.Delete(key)
	}
	return customerror.NewArticleNotFoundError(cause)
}

// safeCopyBuffer copies from src to dst using buf, with context checking and
// validation of read counts to prevent panics from corrupted readers during shutdown.
func safeCopyBuffer(ctx context.Context, dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	var release func()
	if len(buf) == 0 {
		buf = acquireStreamBuffer()
		release = func() { releaseStreamBuffer(buf) }
	}
	if release != nil {
		defer release()
	}
	bufLen := len(buf)

	for {
		// Check context before each read
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}

		nr, er := src.Read(buf)

		// Validate read count - this catches corrupted readers during shutdown
		if nr < 0 {
			return written, fmt.Errorf("reader returned negative count: %d", nr)
		}
		if nr > bufLen {
			// Reader returned more bytes than buffer capacity - this would panic
			// Return error instead of panicking
			return written, fmt.Errorf("reader returned invalid count %d (buffer size %d)", nr, bufLen)
		}

		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nw > nr {
				nw = 0
				if ew == nil {
					ew = fmt.Errorf("invalid write count: %d", nw)
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

// Touch validates that the first segment of a file is available via NNTP STAT
func (u *Usenet) Touch(ctx context.Context, nzoID, filename string) error {
	file, err := u.getFile(nzoID, filename)
	if err != nil {
		return fmt.Errorf("failed to get file: %w", err)
	}

	if err := u.preStreamChecks(file); err != nil {
		return err
	}

	// Check if we have Segments
	if len(file.Segments) == 0 {
		return fmt.Errorf("file has no Segments: %s", filename)
	}

	// get first segment
	firstSeg := file.Segments[0]
	// Run STAT command to check if article exists
	_, _, err = u.nntp.Stat(ctx, firstSeg.MessageID)
	if err != nil {
		return fmt.Errorf("segment not available: %w", err)
	}
	return nil
}

// PreCache creates a file system entry and pre-fetches head and tail segments.
// This warms up the cache to reduce latency for subsequent reads (e.g. ffprobe).
// Uses the shared entry/reader so the cache is available for Stream calls.
func (u *Usenet) PreCache(ctx context.Context, nzoID, filename string) error {
	// Use shared entry (same as Stream)
	entry, err := u.getOrCreateEntry(ctx, nzoID, filename)
	if err != nil {
		return fmt.Errorf("failed to get or create entry: %w", err)
	}
	defer u.releaseFS(entry)

	if len(entry.volumes) == 0 {
		return fmt.Errorf("no volumes available for file %s", filename)
	}

	fileSize := entry.volumes[0].Size

	// Calculate how much to read for head and tail
	headSize := int64(2 * 1024 * 1024) // 2MB head (~3 segments)
	tailSize := int64(2 * 1024 * 1024) // 2MB tail (~3 segments)

	if headSize > fileSize {
		headSize = fileSize
	}

	// get shared reader from entry
	readerAt, _, err := entry.getOrCreateReader()
	if err != nil {
		return fmt.Errorf("failed to get reader: %w", err)
	}

	// Pre-fetch head segments using Prefetch (non-blocking segment download)
	readerAt.Prefetch(ctx, 0, headSize)

	// Pre-fetch tail segments (if file is large enough)
	if fileSize > headSize+tailSize {
		tailOffset := fileSize - tailSize
		readerAt.Prefetch(ctx, tailOffset, tailSize)
	}

	return nil
}

// Stats returns nntp statistics
func (u *Usenet) Stats() map[string]any {
	stats := u.nntp.Stats()
	stats["readers"] = u.fs.Size()
	stats["nzb_storage"] = u.nzbStorage.Stats()
	return stats
}

// GetNZB returns NZB metadata by ID
func (u *Usenet) GetNZB(id string) (*storage.NZB, error) {
	return u.nzbStorage.GetNZB(id)
}

// AssertGeneration atomically adopts expected into legacy metadata or verifies
// strict equality for already-versioned metadata.
func (u *Usenet) AssertGeneration(id, expected string) error {
	if expected == "" {
		return fmt.Errorf("NZB generation is required")
	}
	return u.nzbStorage.AssertGeneration(id, expected)
}

// NormalizeNZBFileSizes corrects impossible advertised file sizes from the
// segment map while holding the NZB lifecycle lock across the full
// read-modify-write operation. The returned NZB is the version that was read
// (and, when changed, persisted). Callers must use this method instead of a
// GetNZB/modify/NZBStorage.AddNZB sequence, which can overwrite a concurrent
// failure or replacement with stale metadata.
func (u *Usenet) NormalizeNZBFileSizes(id string) (*storage.NZB, bool, error) {
	unlockLifecycle := u.lockNZBLifecycle(id)
	defer unlockLifecycle()

	nzb, err := u.nzbStorage.GetNZB(id)
	if err != nil {
		return nil, false, err
	}
	changed, _, err := normalizeNZBFileSizes(nzb)
	if err != nil {
		return nil, false, err
	}
	if !changed {
		return nzb, false, nil
	}

	u.runLifecycleTestHook("normalize-write", id)
	if err := u.nzbStorage.addNZBWithLifecycleHeld(nzb); err != nil {
		return nil, false, fmt.Errorf("persist normalized NZB metadata: %w", err)
	}
	u.clearNZBHotCaches(id)
	return nzb, true, nil
}

func normalizeNZBFileSizes(nzb *storage.NZB) (bool, int64, error) {
	if nzb == nil {
		return false, 0, nil
	}

	changed := false
	var total int64
	for i := range nzb.Files {
		file := &nzb.Files[i]
		if len(file.Segments) > 0 {
			streamSize, err := segmentDerivedFileSize(file)
			if err != nil {
				return false, 0, fmt.Errorf("normalize file size for %q: %w", file.Name, err)
			}
			if file.Size <= 0 || file.Size > streamSize {
				file.Size = streamSize
				changed = true
			}
		}
		total += file.Size
	}
	if nzb.TotalSize != total {
		nzb.TotalSize = total
		changed = true
	}
	return changed, total, nil
}

// GetNZBHeader returns NZB metadata without its segment map. Use this when only
// scalar fields or the file list are needed (status, path, sizes); it avoids
// decoding/allocating the multi-megabyte segment data.
func (u *Usenet) GetNZBHeader(id string) (*storage.NZB, error) {
	return u.nzbStorage.GetNZBHeader(id)
}

// ForEachNZB iterates over all NZBs
func (u *Usenet) ForEachNZB(fn func(*storage.NZB) error) error {
	return u.nzbStorage.ForEachNZB(fn)
}

// NZBStorage returns the underlying NZB storage
func (u *Usenet) NZBStorage() *NZBStorage {
	return u.nzbStorage
}

// SpeedTest runs a speed test for a specific NNTP provider
// It finds a segment from a processed NZB to download for real speed measurement
func (u *Usenet) SpeedTest(ctx context.Context, providerHost string) nntp.SpeedTestResult {
	// Try to find a segment from any processed NZB for the speed test
	messageID := u.findTestSegment()
	return u.nntp.SpeedTest(ctx, providerHost, messageID)
}

// findTestSegment looks for a segment from any processed NZB to use for speed testing
func (u *Usenet) findTestSegment() string {
	var messageID string

	// Iterate through NZBs to find a usable segment
	_ = u.nzbStorage.ForEachNZB(func(nzb *storage.NZB) error {
		for _, file := range nzb.Files {
			if file.IsDeleted || len(file.Segments) == 0 {
				continue
			}
			// Use the first segment we find
			messageID = file.Segments[0].MessageID
			// Return an error to stop iteration (not a real error)
			return fmt.Errorf("found")
		}
		return nil
	})

	return messageID
}

// GetSpeedTestResults returns all stored speed test results
func (u *Usenet) GetSpeedTestResults() map[string]nntp.SpeedTestResult {
	return u.nntp.GetSpeedTestResults()
}

func generationPathToken(generation string) string {
	sum := sha256.Sum256([]byte(generation))
	return fmt.Sprintf("%x", sum[:12])
}

func (u *Usenet) generationArtifactPath(id, generation, suffix string) string {
	return filepath.Join(u.metadataDir, id+"."+generationPathToken(generation)+suffix)
}

func (u *Usenet) removeGenerationSourceArtifacts(id, generation string) {
	if id == "" || generation == "" {
		return
	}
	removeNZBSourceArtifacts(u.generationArtifactPath(id, generation, ".queued"))
	removeNZBSourceArtifacts(u.generationArtifactPath(id, generation, ".source"))
}

func removeNZBSourceArtifacts(path string) {
	if path == "" {
		return
	}
	_ = os.Remove(path)
	_ = os.Remove(path + ".processing")
	_ = os.Remove(path + ".processed")
	_ = os.Remove(path + ".failed")
}

func (u *Usenet) saveNZBFile(id, generation string, content []byte) (string, error) {
	// Store the raw source keyed by the bounded NZB ID rather than the
	// (untrusted, arbitrarily long) display name. ext4 caps a path component at
	// 255 bytes; a long release name plus a ".processing"/".importing"/".queued"
	// marker suffix blew past that limit, which failed the rename, wedged the
	// refresh watcher, and left truncated fragment files behind. The UUID keeps
	// every derived name comfortably under the cap.
	// Keep managed in-flight sources outside the watched .nzb extension. A
	// watcher scan between file creation and marker creation could otherwise
	// claim/rename this source and enqueue a duplicate.
	path := u.generationArtifactPath(id, generation, ".source")
	if err := os.WriteFile(path, content, 0644); err != nil {
		return "", fmt.Errorf("failed to save NZB file to disk: %w", err)
	}
	return path, nil
}

// StageNZB persists a queued NZB before an active-download worker starts.
func (u *Usenet) StageNZB(id string, content []byte) (string, error) {
	return u.StageNZBForGeneration(id, newNZBGeneration(), content)
}

// StageNZBForGeneration keeps queued sources from different same-ID
// lifecycles in distinct files, so cleanup by an old worker cannot remove the
// replacement's staged bytes.
func (u *Usenet) StageNZBForGeneration(id, generation string, content []byte) (string, error) {
	if id == "" {
		return "", fmt.Errorf("NZB ID is required")
	}
	if generation == "" {
		return "", fmt.Errorf("NZB generation is required")
	}
	// Keep the staged file off the .nzb extension so the metadata-directory
	// watcher does not treat a pending active-download job as an unmanaged import.
	path := u.generationArtifactPath(id, generation, ".queued")
	if err := os.WriteFile(path, content, 0644); err != nil {
		return "", fmt.Errorf("failed to stage NZB file: %w", err)
	}
	return path, nil
}

func normalizedNZBArtifactPath(path string) string {
	abs, err := filepath.Abs(path)
	if err == nil {
		path = abs
	}
	path = filepath.Clean(path)
	if runtime.GOOS == "windows" {
		path = strings.ToLower(path)
	}
	return path
}

func isManagedQueuedNZBName(name string) bool {
	if !strings.HasSuffix(name, ".queued") {
		return false
	}
	stem := strings.TrimSuffix(name, ".queued")
	dot := strings.LastIndexByte(stem, '.')
	if dot <= 0 {
		return false
	}
	token := stem[dot+1:]
	if len(token) != 24 {
		return false
	}
	for i := range len(token) {
		if (token[i] < '0' || token[i] > '9') && (token[i] < 'a' || token[i] > 'f') {
			return false
		}
	}
	return true
}

// CleanupOrphanedStagedNZBs removes only managed generation-qualified queue
// artifacts that are not referenced by a durable queue row. It is intended to
// run synchronously before persisted jobs are restored, closing the crash gap
// between StageNZBForGeneration and Queue.Add without touching user files or a
// live replacement generation.
func (u *Usenet) CleanupOrphanedStagedNZBs(livePaths []string) (int, error) {
	live := make(map[string]struct{}, len(livePaths))
	for _, path := range livePaths {
		if path != "" {
			live[normalizedNZBArtifactPath(path)] = struct{}{}
		}
	}

	entries, err := os.ReadDir(u.metadataDir)
	if err != nil {
		return 0, fmt.Errorf("read NZB metadata dir for staged cleanup: %w", err)
	}

	removed := 0
	var cleanupErrors []error
	for _, entry := range entries {
		if entry.IsDir() || !isManagedQueuedNZBName(entry.Name()) {
			continue
		}
		path := filepath.Join(u.metadataDir, entry.Name())
		if _, ok := live[normalizedNZBArtifactPath(path)]; ok {
			continue
		}
		if err := os.Remove(path); err != nil {
			if !os.IsNotExist(err) {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("remove orphan staged NZB %s: %w", path, err))
			}
			continue
		}
		removed++
	}
	return removed, errors.Join(cleanupErrors...)
}

// RemoveStagedNZB removes a queued source file after it has been parsed.
func (u *Usenet) RemoveStagedNZB(path string) {
	if path != "" {
		_ = os.Remove(path)
	}
}

func (u *Usenet) markAsProcessing(nzb *storage.NZB) error {
	// Mark as processing by creating a marker file with the NZB ID
	markerPath := nzb.Path + ".processing"
	if err := os.WriteFile(markerPath, []byte(nzb.ID), 0644); err != nil {
		return fmt.Errorf("failed to create processing marker: %w", err)
	}
	return nil
}

func (u *Usenet) markAsCompleted(nzb *storage.NZB) error {
	unlockLifecycle := u.lockNZBLifecycle(nzb.ID)
	defer unlockLifecycle()

	current, err := u.nzbStorage.GetNZB(nzb.ID)
	if err != nil {
		// In particular, never recreate metadata after a concurrent Delete.
		return fmt.Errorf("load current NZB before completion: %w", err)
	}
	if _, err := adoptOrRequireNZBGeneration(current, nzb.Generation); err != nil {
		return err
	}
	if current.IsBad || current.Status == NZBStatusFailed {
		return fmt.Errorf("refusing to complete NZB %s after durable failure: %s", nzb.ID, current.FailMessage)
	}
	for i := range current.Files {
		if current.Files[i].IsDeleted {
			return fmt.Errorf("refusing to complete NZB %s after file %q permanently failed", nzb.ID, current.Files[i].Name)
		}
	}
	// Preserve a size correction committed after this parser snapshot was
	// created, but only when the immutable segment layout still identifies the
	// same logical file.
	currentFiles := make(map[string]*storage.NZBFile, len(current.Files))
	for i := range current.Files {
		currentFiles[current.Files[i].Name] = &current.Files[i]
	}
	sizesMerged := false
	for i := range nzb.Files {
		durable := currentFiles[nzb.Files[i].Name]
		if durable != nil && durable.Size > 0 && sameNZBSegments(durable.Segments, nzb.Files[i].Segments) && durable.Size != nzb.Files[i].Size {
			nzb.Files[i].Size = durable.Size
			sizesMerged = true
		}
	}
	if sizesMerged {
		var total int64
		for i := range nzb.Files {
			total += nzb.Files[i].Size
		}
		nzb.TotalSize = total
	}

	nzb.Status = NZBStatusCompleted

	// The parsed segment map (.meta) is the only artifact needed for streaming
	// and repair, so the raw .nzb source file is dead weight once the NZB
	// completes — delete it (and its processing marker) immediately. Path is
	// cleared so a later Delete()/watch scan ignores the now-absent file; with
	// the source gone there is nothing for ClaimNewNZBs to re-import, so no
	// .processed marker is needed.
	if nzb.Path != "" {
		if err := os.Remove(nzb.Path); err != nil && !os.IsNotExist(err) {
			u.logger.Warn().Err(err).Str("path", nzb.Path).Msg("Failed to delete NZB source file after completion")
		}
		_ = os.Remove(nzb.Path + ".processing")
		nzb.Path = ""
	}

	if err := u.nzbStorage.addNZBWithLifecycleHeld(nzb); err != nil {
		return fmt.Errorf("failed to save NZB to storage: %w", err)
	}
	u.clearNZBHotCaches(nzb.ID)
	return nil
}

func sameNZBSegments(a, b []storage.NZBSegment) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (u *Usenet) markAsFailed(nzb *storage.NZB, err error) error {
	unlockLifecycle := u.lockNZBLifecycle(nzb.ID)
	defer unlockLifecycle()

	// Apply the status change to a fresh durable snapshot. The parser-owned NZB
	// may predate a size correction or permanent file failure, and writing that
	// long-lived value would silently undo the newer metadata.
	current, loadErr := u.nzbStorage.GetNZB(nzb.ID)
	if loadErr != nil {
		// In particular, never recreate metadata after a concurrent Delete.
		return fmt.Errorf("load current NZB before failure update: %w", loadErr)
	}
	if _, generationErr := adoptOrRequireNZBGeneration(current, nzb.Generation); generationErr != nil {
		return generationErr
	}
	failMessage := err.Error()
	if current.IsBad && current.FailMessage != "" {
		failMessage = current.FailMessage
	}
	current.Status = NZBStatusFailed
	current.FailMessage = failMessage
	if writeErr := u.nzbStorage.addNZBWithLifecycleHeld(current); writeErr != nil {
		return fmt.Errorf("failed to mark NZB as failed in storage: %w", writeErr)
	}
	u.clearNZBHotCaches(current.ID)
	nzb.Status = current.Status
	nzb.FailMessage = current.FailMessage

	// Remove processing marker if exists
	processingMarker := current.Path + ".processing"
	_ = os.Remove(processingMarker)

	// Remove the nzb file itself, as it's considered failed
	if current.Path != "" {
		if removeErr := os.Remove(current.Path); removeErr != nil && !os.IsNotExist(removeErr) {
			u.logger.Warn().Err(removeErr).Str("path", current.Path).Msg("Failed to delete NZB file from disk after failure")
		}
		_ = os.Remove(current.Path + ".processing")
	}
	return nil
}

func (u *Usenet) Delete(nzoID string) error {
	return u.delete(nzoID, "")
}

// DeleteForGeneration removes metadata and source artifacts only while the
// caller still owns the current lifecycle.
func (u *Usenet) DeleteForGeneration(nzoID, generation string) error {
	if generation == "" {
		return fmt.Errorf("NZB generation is required")
	}
	return u.delete(nzoID, generation)
}

func (u *Usenet) delete(nzoID, generation string) error {
	unlockLifecycle := u.lockNZBLifecycle(nzoID)
	defer unlockLifecycle()

	nzb, err := u.nzbStorage.GetNZBHeader(nzoID)
	if err != nil {
		if generation != "" && errors.Is(err, ErrNZBNotFound) {
			// Generation-qualified cleanup is intentionally idempotent. A retry
			// after a crash may observe that its exact resource was already
			// removed; a live replacement would still be present and fail the
			// generation assertion below instead of being touched.
			u.removeGenerationSourceArtifacts(nzoID, generation)
			u.clearNZBHotCaches(nzoID)
			return nil
		}
		return fmt.Errorf("failed to get NZB: %w", err)
	}
	if generation != "" {
		nzb, err = u.nzbStorage.assertGenerationWithLifecycleHeld(nzoID, generation)
		if err != nil {
			return err
		}
	}
	artifactGeneration := generation
	if artifactGeneration == "" {
		artifactGeneration = nzb.Generation
	}
	u.removeGenerationSourceArtifacts(nzoID, artifactGeneration)

	// Delete NZB XML file from disk
	if nzb.Path != "" {
		if err := os.Remove(nzb.Path); err != nil && !os.IsNotExist(err) {
			u.logger.Warn().Err(err).Str("path", nzb.Path).Msg("Failed to delete NZB file from disk")
		}

		// Delete marker files
		processedMarker := nzb.Path + ".processed"
		_ = os.Remove(processedMarker)
		failedMarker := nzb.Path + ".failed"
		_ = os.Remove(failedMarker)
		_ = os.Remove(nzb.Path + ".processing")
	}

	// Delete from file-based storage
	if err := u.nzbStorage.deleteNZBWithLifecycleHeld(nzoID, generation); err != nil {
		return fmt.Errorf("failed to delete NZB from storage: %w", err)
	}
	u.clearNZBHotCaches(nzoID)
	return nil
}

// PendingNZB is an unmanaged NZB file claimed by the metadata-directory watcher.
type PendingNZB struct {
	Name    string
	Path    string
	Content []byte
}

// ClaimNewNZBs moves unmanaged NZB files out of the watched extension and
// returns them for submission to the shared active-download queue.
func (u *Usenet) ClaimNewNZBs() ([]PendingNZB, error) {
	entries, err := os.ReadDir(u.metadataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata dir: %w", err)
	}

	var pending []PendingNZB
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		claimedPath := filepath.Join(u.metadataDir, name)
		if strings.HasSuffix(name, ".nzb.importing") {
			name = strings.TrimSuffix(name, ".importing")
		} else {
			if filepath.Ext(name) != ".nzb" {
				continue
			}
			path := filepath.Join(u.metadataDir, name)
			if fileExists(path+".processed") || fileExists(path+".processing") || fileExists(path+".failed") {
				continue
			}
			claimedPath = path + ".importing"
			if err := os.Rename(path, claimedPath); err != nil {
				if os.IsNotExist(err) {
					continue
				}
				// Skip this entry instead of aborting the whole scan. A single
				// poison file (e.g. a name so long that appending ".importing"
				// exceeds the filesystem limit) previously failed every refresh
				// and permanently blocked all other pending NZBs.
				u.logger.Error().Err(err).Str("name", name).Msg("Failed to claim NZB; skipping")
				continue
			}
		}

		content, err := os.ReadFile(claimedPath)
		if err != nil {
			u.logger.Error().Err(err).Str("path", claimedPath).Msg("Failed to read claimed NZB")
			continue
		}
		pending = append(pending, PendingNZB{Name: name, Path: claimedPath, Content: content})
	}

	if len(pending) > 0 {
		u.logger.Info().Int("count", len(pending)).Msg("Found new NZB files to queue")
	}
	return pending, nil
}

// RemoveClaimedNZB removes a watched source after it has been staged by the queue.
func (u *Usenet) RemoveClaimedNZB(path string) {
	if path != "" {
		_ = os.Remove(path)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

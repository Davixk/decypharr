package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/internal/retry"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

const (
	streamBufferSize = 256 * 1024
)

// streamBufPool provides reusable buffers for streaming to reduce GC pressure.
// Each buffer is 256KB - prevents per-request allocations.
var streamBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, streamBufferSize)
		return &buf
	},
}

// ActiveStream represents a currently active streaming file
type ActiveStream struct {
	ID         string `json:"id"`
	EntryName  string `json:"entry_name"`
	FileName   string `json:"file_name"`
	FileSize   int64  `json:"file_size"`
	Source     string `json:"source"` // "torrent" or "nzb"
	StartedAt  int64  `json:"started_at"`
	LastActive int64  `json:"last_active"` // Last activity timestamp (for observability)
	Debrid     string `json:"debrid,omitempty"`
	Client     string `json:"client,omitempty"` // Client identifier (User-Agent for WebDAV, "DFS" for DFS)
}

// === Active Streams Tracking ===

// registerStream registers an active stream for observability.
// Returns the stream ID so the caller can remove it when streaming completes.
func (m *Manager) registerStream(entryName, fileName string, fileSize int64, source, debrid, client string) string {
	// Use deterministic ID to ensure a single entry per file
	streamID := entryName + ":" + fileName
	now := utils.NowUnix()

	stream := &ActiveStream{
		ID:         streamID,
		EntryName:  entryName,
		FileName:   fileName,
		FileSize:   fileSize,
		Source:     source,
		StartedAt:  now,
		LastActive: now,
		Debrid:     debrid,
		Client:     client,
	}

	m.activeStreams.Store(streamID, stream)
	return streamID
}

// unregisterStream removes an active stream entry if it exists.
func (m *Manager) unregisterStream(streamID string) {
	if streamID == "" {
		return
	}
	m.activeStreams.Delete(streamID)
}

// GetActiveStreams returns all currently active streams.
func (m *Manager) GetActiveStreams() []*ActiveStream {
	var streams []*ActiveStream
	m.activeStreams.Range(func(_ string, stream *ActiveStream) bool {
		streams = append(streams, stream)
		return true
	})
	return streams
}

// GetActiveStreamsCount returns the number of active streams.
func (m *Manager) GetActiveStreamsCount() int {
	return m.activeStreams.Size()
}

type StreamError struct {
	Err       error
	Retryable bool
	LinkError bool // true if we should try a new link
}

func (e StreamError) Error() string {
	return e.Err.Error()
}

// StreamMetadata describes the headers/status for a streaming response before data flows.
type StreamMetadata struct {
	Header        http.Header
	StatusCode    int
	ContentLength int64
}

// StreamReadyFunc allows callers to copy headers/status before streaming begins.
type StreamReadyFunc func(*StreamMetadata) error

// isConnectionError checks if the error is related to connection issues
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	// Check for common connection errors
	if strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection refused") {
		return true
	}

	// Check for net.Error types
	var netErr net.Error
	return errors.As(err, &netErr)
}

// Stream streams a file from an entry to the provided writer within the specified byte range.
// rangeRequested records whether the caller supplied a Range request; it is
// intentionally distinct from any backing File.ByteRange used to fetch data.
// client identifies the caller (e.g., User-Agent for WebDAV, "DFS" for DFS mount).
func (m *Manager) Stream(ctx context.Context, entry *storage.Entry, filename string, start, end int64, rangeRequested bool, writer io.Writer, onReady StreamReadyFunc, client string) error {
	if writer == nil {
		return fmt.Errorf("writer is nil")
	}
	if entry == nil {
		return retry.Unrecoverable(fmt.Errorf("entry is nil"))
	}

	// get file info for size
	file, ok := entry.Files[filename]
	if !ok {
		return retry.Unrecoverable(fmt.Errorf("file %s not found", filename))
	}
	if entry.Protocol == config.ProtocolNZB {
		preparedEntry, prepErr := m.prepareUsenetStreamEntry(entry.InfoHash, filename, entry.NZBGeneration)
		if prepErr != nil {
			return retry.Unrecoverable(prepErr)
		}
		entry = preparedEntry
		file, ok = entry.Files[filename]
		if !ok {
			return retry.Unrecoverable(fmt.Errorf("file %s not found after preparing Usenet stream", filename))
		}
	}
	start, end, err := normalizeStreamRange(file.Size, start, end)

	if err != nil {
		return retry.Unrecoverable(fmt.Errorf("invalid stream range for file %s: %w", filename, err))
	}

	// Route based on protocol
	if entry.Protocol == config.ProtocolNZB {
		return m.streamUsenet(ctx, entry, filename, start, end, rangeRequested, writer, onReady)
	}

	// Default to HTTP streaming for torrents
	return m.streamHTTP(ctx, entry, filename, start, end, rangeRequested, writer, onReady)
}

// PrepareFileInfo validates a remote Usenet file before an HTTP/WebDAV caller
// derives response metadata from it. It returns a copy of FileInfo so cached
// directory entries are never mutated concurrently by request handlers.
func (m *Manager) PrepareFileInfo(entry *storage.Entry, info *FileInfo) (*storage.Entry, *FileInfo, error) {
	if entry == nil {
		return nil, nil, fmt.Errorf("entry is nil")
	}
	if info == nil {
		return nil, nil, fmt.Errorf("file info is nil")
	}
	if entry.Protocol != config.ProtocolNZB {
		return entry, info, nil
	}

	preparedEntry, err := m.prepareUsenetStreamEntry(entry.InfoHash, info.name, entry.NZBGeneration)
	if err != nil {
		return nil, nil, err
	}
	file, ok := preparedEntry.Files[info.name]
	if !ok {
		return nil, nil, fmt.Errorf("file %s not found after preparing Usenet metadata", info.name)
	}

	preparedInfo := *info
	preparedInfo.size = file.Size
	return preparedEntry, &preparedInfo, nil
}

// PrepareFileInfos batches collection metadata preparation by entry. The
// returned slices align with infos; callers can omit a permanently failed
// child while still returning healthy siblings.
func (m *Manager) PrepareFileInfos(infos []FileInfo) ([]FileInfo, []error) {
	prepared := append([]FileInfo(nil), infos...)
	fileErrors := make([]error, len(infos))
	groups := make(map[string][]int)

	for i := range prepared {
		info := &prepared[i]
		if info.isDir || len(info.content) != 0 {
			continue
		}
		if info.infohash == "" {
			fileErrors[i] = fmt.Errorf("remote file %s/%s has no entry infohash", info.parent, info.name)
			continue
		}
		groups[info.infohash] = append(groups[info.infohash], i)
	}

	for infohash, indexes := range groups {
		entry, err := m.storage.Get(infohash)
		if err != nil {
			for _, index := range indexes {
				fileErrors[index] = err
			}
			continue
		}
		if entry.Protocol != config.ProtocolNZB {
			continue
		}
		if m.usenet == nil {
			for _, index := range indexes {
				fileErrors[index] = fmt.Errorf("usenet client not configured")
			}
			continue
		}
		generation, generationErr := m.ensureNZBGeneration(entry)
		if generationErr != nil {
			for _, index := range indexes {
				fileErrors[index] = generationErr
			}
			continue
		}

		names := make([]string, 0, len(indexes))
		seen := make(map[string]struct{}, len(indexes))
		for _, index := range indexes {
			name := prepared[index].name
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				names = append(names, name)
			}
		}
		sizes, preparationErrors, batchErr := m.usenet.PrepareStreamsForGeneration(infohash, generation, names)
		if batchErr != nil {
			for _, index := range indexes {
				fileErrors[index] = batchErr
			}
			continue
		}

		corrections := make(map[string]int64, len(sizes))
		permanentFailures := make(map[string]usenetFileFailure)
		for _, index := range indexes {
			name := prepared[index].name
			if prepErr := preparationErrors[name]; prepErr != nil {
				var permanent *customerror.Error
				if errors.As(prepErr, &permanent) && permanent.IsPermanent() {
					permanentFailures[name] = usenetFileFailure{
						cause:           prepErr,
						articlesMissing: permanent.Code == "usenet_article_missing",
					}
				}
				fileErrors[index] = prepErr
				continue
			}
			size, ok := sizes[name]
			if !ok {
				fileErrors[index] = fmt.Errorf("file %s was not prepared in entry %s", name, infohash)
				continue
			}
			corrections[name] = size
		}

		if len(permanentFailures) > 0 {
			if persistErr := m.markUsenetStreamFailuresForGeneration(infohash, generation, permanentFailures); persistErr != nil {
				for _, index := range indexes {
					name := prepared[index].name
					if _, failed := permanentFailures[name]; failed {
						fileErrors[index] = fmt.Errorf("persist manager Usenet failure for %q: %w (original error: %v)", name, persistErr, fileErrors[index])
					}
				}
			}
		}

		if len(corrections) == 0 {
			continue
		}
		updated, persistErr := m.persistUsenetFileSizesForGeneration(infohash, generation, corrections)
		if persistErr != nil {
			for _, index := range indexes {
				if fileErrors[index] == nil {
					fileErrors[index] = persistErr
				}
			}
			continue
		}
		for _, index := range indexes {
			if fileErrors[index] != nil {
				continue
			}
			if file, ok := updated.Files[prepared[index].name]; ok {
				prepared[index].size = file.Size
			}
		}
	}
	return prepared, fileErrors
}

func (m *Manager) prepareUsenetStreamEntry(infohash, filename, expectedGeneration string) (*storage.Entry, error) {
	if m.usenet == nil {
		return nil, fmt.Errorf("usenet client not configured")
	}
	if m.storage == nil {
		return nil, fmt.Errorf("manager storage is not configured")
	}

	entry, err := m.storage.Get(infohash)
	if err != nil {
		return nil, fmt.Errorf("load authoritative entry %s: %w", infohash, err)
	}
	if expectedGeneration != "" && entry.NZBGeneration != expectedGeneration {
		return nil, fmt.Errorf("%w for manager entry %s (expected %q, current %q)", usenet.ErrStaleNZBGeneration, infohash, expectedGeneration, entry.NZBGeneration)
	}
	_, ok := entry.Files[filename]
	if !ok {
		return nil, fmt.Errorf("file %s not found in entry %s", filename, infohash)
	}
	generation, err := m.ensureNZBGeneration(entry)
	if err != nil {
		return nil, err
	}
	if expectedGeneration != "" && generation != expectedGeneration {
		return nil, fmt.Errorf("%w for manager entry %s (expected %q, current %q)", usenet.ErrStaleNZBGeneration, infohash, expectedGeneration, generation)
	}

	actualSize, prepErr := m.usenet.PrepareStreamForGeneration(infohash, generation, filename)
	if prepErr != nil {
		var permanent *customerror.Error
		if errors.As(prepErr, &permanent) && permanent.IsPermanent() {
			if persistErr := m.markUsenetStreamFailureForGeneration(infohash, generation, filename, prepErr, permanent.Code == "usenet_article_missing"); persistErr != nil {
				// Do not expose the permanent status until the manager's
				// authoritative entry has durably recorded it.
				return nil, fmt.Errorf("persist manager Usenet failure for %q: %w (original error: %v)", filename, persistErr, prepErr)
			}
		}
		return nil, prepErr
	}
	// Always pass through the atomic persistence helper. Even when the main
	// entry is already correct, this retries a previously failed queue mirror.
	return m.persistUsenetFileSizeForGeneration(infohash, generation, filename, actualSize)
}

func (m *Manager) persistUsenetFileSize(infohash, filename string, size int64) (*storage.Entry, error) {
	return m.persistUsenetFileSizes(infohash, map[string]int64{filename: size})
}

func (m *Manager) persistUsenetFileSizeForGeneration(infohash, generation, filename string, size int64) (*storage.Entry, error) {
	return m.persistUsenetFileSizesForGeneration(infohash, generation, map[string]int64{filename: size})
}

func (m *Manager) persistUsenetFileSizes(infohash string, sizes map[string]int64) (*storage.Entry, error) {
	return m.persistUsenetFileSizesForGeneration(infohash, "", sizes)
}

func (m *Manager) persistUsenetFileSizesForGeneration(infohash, generation string, sizes map[string]int64) (*storage.Entry, error) {
	for filename, size := range sizes {
		if size <= 0 {
			return nil, fmt.Errorf("invalid reconciled Usenet size %d for %q", size, filename)
		}
	}

	mainChanged := false
	entry, present, err := m.storage.MutateEntryIfPresent(infohash, func(entry *storage.Entry) (bool, error) {
		if generation != "" && entry.NZBGeneration != generation {
			return false, fmt.Errorf("stale NZB generation %q for main entry %s (current %q)", generation, infohash, entry.NZBGeneration)
		}
		var mutateErr error
		mainChanged, mutateErr = applyUsenetFileSizes(entry, sizes)
		return mainChanged, mutateErr
	})
	if err != nil {
		return nil, fmt.Errorf("persist reconciled Usenet sizes in main entry: %w", err)
	}
	if !present {
		return nil, fmt.Errorf("entry %s was deleted before Usenet size reconciliation", infohash)
	}

	_, _, queueErr := m.storage.MutateQueuedIfPresent(infohash, func(queued *storage.Entry) (bool, error) {
		if generation != "" && queued.NZBGeneration != generation {
			return false, fmt.Errorf("stale NZB generation %q for queue entry %s (current %q)", generation, infohash, queued.NZBGeneration)
		}
		return applyUsenetFileSizes(queued, sizes)
	})
	if queueErr != nil {
		m.logger.Error().Err(queueErr).Str("entry", entry.Name).Msg("Failed to synchronize reconciled Usenet sizes to queue")
	}

	if mainChanged {
		m.refreshEntryCache()
	}
	return entry, nil
}

func applyUsenetFileSizes(entry *storage.Entry, sizes map[string]int64) (bool, error) {
	if entry == nil {
		return false, fmt.Errorf("entry is nil")
	}
	changed := false
	for filename, size := range sizes {
		file, ok := entry.Files[filename]
		if !ok {
			return false, fmt.Errorf("file %s not found in entry %s", filename, entry.InfoHash)
		}
		if file.Size != size {
			file.Size = size
			changed = true
		}
	}
	var total int64
	for _, entryFile := range entry.Files {
		total += entryFile.Size
	}
	if entry.Size != total {
		entry.Size = total
		changed = true
	}
	if entry.Bytes != total {
		entry.Bytes = total
		changed = true
	}
	return changed, nil
}

func (m *Manager) refreshEntryCache() {
	if m.entry != nil && m.config != nil {
		m.entry.Refresh()
	}
}

// TrackStream registers an active stream for observability and returns the stream ID.
// Call UntrackStream with the returned ID when streaming completes.
func (m *Manager) TrackStream(entry *storage.Entry, filename, client string) string {
	if entry == nil {
		return ""
	}
	file, ok := entry.Files[filename]
	if !ok {
		return ""
	}

	var source, debrid string
	if entry.Protocol == config.ProtocolNZB {
		source = "nzb"
	} else {
		source = "torrent"
		debrid = entry.ActiveProvider
	}

	return m.registerStream(entry.Name, filename, file.Size, source, debrid, client)
}

// UntrackStream removes a previously-registered active stream if the ID is non-empty.
func (m *Manager) UntrackStream(streamID string) {
	m.unregisterStream(streamID)
}

type httpStreamPlan struct {
	logicalStart      int64
	logicalEnd        int64
	upstreamStart     int64
	upstreamEnd       int64
	clientRequested   bool
	upstreamRequested bool
}

func newHTTPStreamPlan(file *storage.File, start, end int64, rangeRequested bool) (httpStreamPlan, error) {
	var plan httpStreamPlan
	if file == nil {
		return plan, fmt.Errorf("stream file is nil")
	}
	if file.Size <= 0 {
		return plan, fmt.Errorf("invalid file size %d", file.Size)
	}
	if start < 0 || end < start || end >= file.Size {
		return plan, fmt.Errorf("invalid logical byte range %d-%d for file size %d", start, end, file.Size)
	}
	if !rangeRequested && (start != 0 || end != file.Size-1) {
		return plan, fmt.Errorf("partial logical byte range %d-%d is missing client range intent", start, end)
	}

	plan = httpStreamPlan{
		logicalStart:      start,
		logicalEnd:        end,
		upstreamStart:     start,
		upstreamEnd:       end,
		clientRequested:   rangeRequested,
		upstreamRequested: rangeRequested,
	}
	if file.ByteRange == nil {
		return plan, nil
	}

	backingStart, backingEnd := file.ByteRange[0], file.ByteRange[1]
	if backingStart < 0 || backingEnd < backingStart {
		return httpStreamPlan{}, fmt.Errorf("invalid backing byte range %d-%d", backingStart, backingEnd)
	}
	maxLogicalOffset := backingEnd - backingStart
	if file.Size-1 > maxLogicalOffset {
		return httpStreamPlan{}, fmt.Errorf("backing byte range %d-%d is shorter than logical file size %d", backingStart, backingEnd, file.Size)
	}
	if end > maxLogicalOffset {
		return httpStreamPlan{}, fmt.Errorf("logical byte range %d-%d exceeds backing byte range %d-%d", start, end, backingStart, backingEnd)
	}

	plan.upstreamStart = backingStart + start
	plan.upstreamEnd = backingStart + end
	plan.upstreamRequested = true
	return plan, nil
}

// streamHTTP handles streaming for torrent files via HTTP.
func (m *Manager) streamHTTP(ctx context.Context, torrent *storage.Entry, filename string, start, end int64, rangeRequested bool, writer io.Writer, onReady StreamReadyFunc) error {
	file, ok := torrent.Files[filename]
	if !ok {
		return fmt.Errorf("file not found in entry: %s", filename)
	}

	// Get the validated download link using the link service.
	downloadLink, err := m.linkService.GetLink(ctx, torrent, filename)
	if err != nil {
		return fmt.Errorf("failed to get download link: %w", err)
	}
	return m.streamHTTPURL(ctx, downloadLink.DownloadLink, torrent.ActiveProvider, file, filename, start, end, rangeRequested, writer, onReady)
}

// streamRangeIgnoredMaxDiscard bounds how many upstream bytes may be discarded
// to reach the requested offset when an upstream answers a Range request with
// 200 OK (full body). Small offsets are served by skipping the prefix so CDNs
// that ignore Range headers keep working; beyond this bound the request is
// retried once and only then failed, because discarding gigabytes to honor a
// mid-file seek would stall playback anyway. Overridable in tests.
var streamRangeIgnoredMaxDiscard = int64(8 << 20)

// streamHTTPURL keeps link resolution separate from the byte-accurate HTTP
// transfer. Tests exercise this layer with a real HTTP server.
func (m *Manager) streamHTTPURL(ctx context.Context, url, provider string, file *storage.File, filename string, start, end int64, rangeRequested bool, writer io.Writer, onReady StreamReadyFunc) error {
	plan, err := newHTTPStreamPlan(file, start, end, rangeRequested)
	if err != nil {
		return retry.Unrecoverable(StreamError{Err: err, Retryable: false})
	}
	expectedLen := plan.logicalEnd - plan.logicalStart + 1

	// Progress-based idle deadline for the whole debrid transfer. One timer of
	// duration debridReadTimeout() covers the wait for the first byte
	// (connection + headers) and mid-stream idle; it is reset on every byte
	// delivered to the client (streamProgressWriter) and fires only after the
	// full timeout elapses with ZERO delivery. The HTTP request and copy run on
	// streamCtx so a fire aborts the in-flight resp.Body read; a slow but
	// progressing stream never trips it. timeout <= 0 ("off"/"0"/"none") is a
	// pure passthrough with no watchdog goroutine and streamCtx == ctx.
	deadline := newStreamProgressDeadline(ctx, m.debridReadTimeout())
	defer deadline.Close()
	streamCtx := deadline.Context

	resp, reqErr := m.doRequest(streamCtx, url, plan.upstreamStart, plan.upstreamEnd, plan.upstreamRequested)
	if reqErr != nil {
		return m.stallOr(deadline, reqErr)
	}
	if plan.upstreamRequested && resp.StatusCode == http.StatusOK && plan.upstreamStart > streamRangeIgnoredMaxDiscard {
		// The upstream ignored the Range request and the offset is too large
		// to reach by discarding. Give it exactly one more chance to honor the
		// range before giving up.
		resp.Body.Close()
		m.logger.Warn().
			Str("provider", provider).
			Str("file", filename).
			Int64("offset", plan.upstreamStart).
			Msg("Upstream ignored Range request with a large offset; retrying once")
		resp, reqErr = m.doRequest(streamCtx, url, plan.upstreamStart, plan.upstreamEnd, plan.upstreamRequested)
		if reqErr != nil {
			return m.stallOr(deadline, reqErr)
		}
	}
	defer resp.Body.Close()

	discardPrefix, responseErr := m.resolveHTTPStreamResponse(resp, plan, expectedLen, provider, filename)
	if responseErr != nil {
		// A definitive dead status (404/410) is classified by
		// resolveHTTPStreamResponse as a permanent *customerror.Error. Surface it
		// unwrapped (only marked unrecoverable) so errors.As in the WebDAV layer
		// still maps it to 410 Gone; everything else keeps the historical
		// StreamError wrapping.
		var permanent *customerror.Error
		if errors.As(responseErr, &permanent) {
			return retry.Unrecoverable(responseErr)
		}
		return retry.Unrecoverable(StreamError{Err: responseErr, Retryable: false})
	}
	if discardPrefix > 0 {
		// Skip the prefix before committing headers so a failure here can
		// still surface as a proper error response. The discard reads through
		// the same idle deadline (progress pulses keep a healthy discard alive).
		discardWriter := streamProgressWriter{Writer: io.Discard, progress: deadline.Progress}
		if _, discardErr := io.CopyN(discardWriter, resp.Body, discardPrefix); discardErr != nil {
			return m.stallOr(deadline, classifyStreamTransferError(ctx, discardErr))
		}
	}

	if onReady != nil {
		header := resp.Header.Clone()
		header.Del("Content-Range")
		header.Set("Content-Length", strconv.FormatInt(expectedLen, 10))
		header.Set("Accept-Ranges", "bytes")
		// A stored byte range may come from an archive URL whose upstream media
		// type describes the container, not the logical file being served.
		header.Set("Content-Type", utils.GetContentType(filename))
		statusCode := http.StatusOK
		if plan.clientRequested {
			statusCode = http.StatusPartialContent
			header.Set("Content-Range", buildContentRange(plan.logicalStart, plan.logicalEnd, file.Size))
		}
		if readyErr := onReady(&StreamMetadata{
			Header:        header,
			StatusCode:    statusCode,
			ContentLength: expectedLen,
		}); readyErr != nil {
			return retry.Unrecoverable(readyErr)
		}
	}

	bufPtr := streamBufPool.Get().(*[]byte)
	buf := *bufPtr
	defer streamBufPool.Put(bufPtr)

	// Wrap the client writer so each successful delivery resets the idle
	// deadline; read from streamCtx-bound resp.Body so a deadline fire aborts a
	// stalled read.
	progressWriter := streamProgressWriter{Writer: writer, progress: deadline.Progress}
	n, copyErr := io.CopyBuffer(progressWriter, io.LimitReader(resp.Body, expectedLen), buf)
	if n < expectedLen && copyErr == nil {
		copyErr = io.ErrUnexpectedEOF
	}
	if copyErr == nil || copyErr == io.EOF {
		return nil
	}
	return m.stallOr(deadline, classifyStreamTransferError(ctx, copyErr))
}

// stallOr returns the ErrDebridReadStalled sentinel (logged, unrecoverable) when
// the idle deadline fired, otherwise it returns fallback unchanged. It is called
// at every debrid-stream exit that the deadline could have aborted so an idle
// timeout is surfaced distinctly instead of being masked as a silent context
// cancellation. The fallback is still computed with the caller context so a
// genuine client disconnect keeps its historical (silent) classification.
func (m *Manager) stallOr(deadline *streamProgressDeadline, fallback error) error {
	if stall := deadline.stallCause(); stall != nil {
		m.logger.Warn().Err(stall).Msg("Debrid stream stalled with no byte progress; aborting read")
		return retry.Unrecoverable(stall)
	}
	return fallback
}

// classifyStreamTransferError mirrors the historical copy-error handling:
// caller cancellation is unrecoverable, network/timeout errors are retryable,
// anything else is unrecoverable to avoid infinite retry loops.
func classifyStreamTransferError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return retry.Unrecoverable(ctx.Err())
	}
	if isConnectionError(err) || strings.Contains(err.Error(), "timeout") {
		return err
	}
	return retry.Unrecoverable(err)
}

// resolveHTTPStreamResponse restores the tolerance the pre-validation streamer
// had for imperfect debrid CDNs while still rejecting responses it would also
// have choked on. It returns how many leading upstream bytes must be discarded
// before the requested offset begins.
//
//   - 200 for a ranged request: the upstream ignored the Range header and sent
//     the full body. Serve the requested window from it by skipping the offset
//     prefix (bounded by streamRangeIgnoredMaxDiscard; the copy loop stops at
//     the range length).
//   - Inexact or unparseable Content-Range/Content-Length: WARN and proceed as
//     the pre-validation code did; the copy loop still enforces the exact
//     logical length.
//   - Any other status: hard failure, exactly as before.
func (m *Manager) resolveHTTPStreamResponse(resp *http.Response, plan httpStreamPlan, expectedLen int64, provider, filename string) (int64, error) {
	if resp == nil {
		return 0, fmt.Errorf("upstream HTTP response is nil")
	}
	discardPrefix := int64(0)
	switch {
	case resp.StatusCode == http.StatusGone:
		// HTTP 410 Gone is the ONLY status treated as a definitive dead verdict:
		// the debrid link resolved but the upstream affirmatively signals the
		// content is permanently gone. Classify it as a permanent 410 so the
		// WebDAV layer maps it to 410 Gone (before the first byte) and the retry
		// loop treats it as non-retryable. 404 is deliberately NOT treated as
		// dead — debrid CDNs also return 404 for merely expired/refetchable
		// links, and a false "dead" verdict on a still-live file would violate
		// the read-deadline safety bar. 404, 403/expired, and every other status
		// keep their existing (non-permanent) behavior below.
		return 0, customerror.NewContentGoneError(fmt.Errorf("debrid content gone: HTTP %d for %q (provider %s)", resp.StatusCode, filename, provider))
	case plan.upstreamRequested && resp.StatusCode == http.StatusPartialContent:
		contentRange := resp.Header.Get("Content-Range")
		start, end, err := parseContentRange(contentRange)
		if err != nil {
			m.logger.Warn().Err(err).
				Str("provider", provider).
				Str("file", filename).
				Str("content_range", contentRange).
				Msg("Upstream returned an unparseable Content-Range; proceeding with the requested range")
		} else if start != plan.upstreamStart || end != plan.upstreamEnd {
			m.logger.Warn().
				Str("provider", provider).
				Str("file", filename).
				Str("content_range", contentRange).
				Int64("requested_start", plan.upstreamStart).
				Int64("requested_end", plan.upstreamEnd).
				Msg("Upstream Content-Range does not match the requested range; proceeding")
		}
	case plan.upstreamRequested && resp.StatusCode == http.StatusOK:
		if plan.upstreamStart > streamRangeIgnoredMaxDiscard {
			return 0, fmt.Errorf("upstream ignored requested byte range %d-%d and offset %d exceeds the %d byte discard bound",
				plan.upstreamStart, plan.upstreamEnd, plan.upstreamStart, streamRangeIgnoredMaxDiscard)
		}
		discardPrefix = plan.upstreamStart
		m.logger.Warn().
			Str("provider", provider).
			Str("file", filename).
			Int64("discard", discardPrefix).
			Msg("Upstream ignored Range request; serving the requested window from the full response")
	case plan.upstreamRequested:
		return 0, fmt.Errorf("unexpected HTTP status %d for ranged request", resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return 0, fmt.Errorf("unexpected HTTP status %d for full request", resp.StatusCode)
	}
	if discardPrefix == 0 && resp.ContentLength >= 0 && resp.ContentLength != expectedLen {
		m.logger.Warn().
			Str("provider", provider).
			Str("file", filename).
			Int64("content_length", resp.ContentLength).
			Int64("expected", expectedLen).
			Msg("Upstream content length does not match the requested length; proceeding")
	}
	return discardPrefix, nil
}

func parseContentRange(value string) (int64, int64, error) {
	fields := strings.Fields(value)
	if len(fields) != 2 || !strings.EqualFold(fields[0], "bytes") {
		return 0, 0, fmt.Errorf("expected bytes content range, got %q", value)
	}
	interval, total, ok := strings.Cut(fields[1], "/")
	if !ok || interval == "" || total == "" {
		return 0, 0, fmt.Errorf("malformed content range %q", value)
	}
	startText, endText, ok := strings.Cut(interval, "-")
	if !ok || startText == "" || endText == "" {
		return 0, 0, fmt.Errorf("malformed content range interval %q", interval)
	}
	start, err := strconv.ParseInt(startText, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, fmt.Errorf("invalid content range start %q", startText)
	}
	end, err := strconv.ParseInt(endText, 10, 64)
	if err != nil || end < start {
		return 0, 0, fmt.Errorf("invalid content range end %q", endText)
	}
	if total != "*" {
		totalSize, totalErr := strconv.ParseInt(total, 10, 64)
		if totalErr != nil || totalSize <= end {
			return 0, 0, fmt.Errorf("invalid content range size %q", total)
		}
	}
	return start, end, nil
}

// streamUsenet handles streaming for NZB files via usenet
func (m *Manager) streamUsenet(ctx context.Context, entry *storage.Entry, filename string, start, end int64, rangeRequested bool, writer io.Writer, onReady StreamReadyFunc) error {
	if m.usenet == nil {
		return retry.Unrecoverable(fmt.Errorf("usenet client not configured"))
	}

	_, ok := entry.Files[filename]
	if !ok {
		return retry.Unrecoverable(fmt.Errorf("file not found in entry: %s", filename))
	}
	if entry.NZBGeneration == "" {
		return retry.Unrecoverable(fmt.Errorf("NZB generation is missing for %s", entry.InfoHash))
	}
	var ready func(usenet.StreamReadyInfo) error
	if onReady != nil {
		ready = func(info usenet.StreamReadyInfo) error {
			return onReady(newUsenetStreamMetadata(info, filename, rangeRequested))
		}
	}

	// The ready callback fires only after the exact generation-bound reader is
	// acquired. Headers and bytes therefore come from the same retained handle.
	err := m.usenet.StreamForGenerationReady(ctx, entry.InfoHash, entry.NZBGeneration, filename, start, end, writer, ready)
	if err != nil && nntp.IsContentMissingError(err) {
		if persistErr := m.markUsenetStreamFailureForGeneration(entry.InfoHash, entry.NZBGeneration, filename, err, true); persistErr != nil {
			return errors.Join(err, fmt.Errorf("persist manager Usenet failure: %w", persistErr))
		}
	}
	return err
}

func newUsenetStreamMetadata(info usenet.StreamReadyInfo, filename string, rangeRequested bool) *StreamMetadata {
	contentLength := info.End - info.Start + 1
	statusCode := http.StatusOK
	header := make(http.Header, 4)
	header.Set("Accept-Ranges", "bytes")
	header.Set("Content-Length", strconv.FormatInt(contentLength, 10))
	if rangeRequested {
		statusCode = http.StatusPartialContent
		header.Set("Content-Range", buildContentRange(info.Start, info.End, info.Size))
	}
	header.Set("Content-Type", utils.GetContentType(filename))
	return &StreamMetadata{Header: header, StatusCode: statusCode, ContentLength: contentLength}
}

func (m *Manager) markUsenetStreamFailure(infohash, filename string, cause error, articlesMissing bool) error {
	return m.markUsenetStreamFailures(infohash, map[string]usenetFileFailure{
		filename: {cause: cause, articlesMissing: articlesMissing},
	})
}

func (m *Manager) markUsenetStreamFailureForGeneration(infohash, generation, filename string, cause error, articlesMissing bool) error {
	return m.markUsenetStreamFailuresForGeneration(infohash, generation, map[string]usenetFileFailure{
		filename: {cause: cause, articlesMissing: articlesMissing},
	})
}

type usenetFileFailure struct {
	cause           error
	articlesMissing bool
}

func (m *Manager) markUsenetStreamFailures(infohash string, failures map[string]usenetFileFailure) error {
	return m.markUsenetStreamFailuresForGeneration(infohash, "", failures)
}

func (m *Manager) markUsenetStreamFailuresForGeneration(infohash, generation string, failures map[string]usenetFileFailure) error {
	if infohash == "" {
		return fmt.Errorf("entry infohash is empty")
	}
	if m.storage == nil {
		return fmt.Errorf("manager storage is not configured")
	}
	if len(failures) == 0 {
		return fmt.Errorf("Usenet failures are empty")
	}

	names := make([]string, 0, len(failures))
	for filename, failure := range failures {
		if filename == "" {
			return fmt.Errorf("Usenet failure filename is empty")
		}
		if failure.cause == nil {
			return fmt.Errorf("Usenet failure cause for %q is nil", filename)
		}
		names = append(names, filename)
	}
	sort.Strings(names)

	parts := make([]string, 0, len(names))
	for _, filename := range names {
		failure := failures[filename]
		if failure.articlesMissing {
			parts = append(parts, fmt.Sprintf("articles missing on provider for %q", filename))
		} else {
			parts = append(parts, fmt.Sprintf("usenet file %q failed: %v", filename, failure.cause))
		}
	}
	messageText := parts[0]
	if len(parts) > 1 {
		messageText = "multiple usenet files failed: " + strings.Join(parts, "; ")
	}
	message := errors.New(messageText)

	mainChanged := false
	persist := func() error {
		_, present, mutateErr := m.storage.MutateEntryIfPresent(infohash, func(entry *storage.Entry) (bool, error) {
			if generation != "" && entry.NZBGeneration != generation {
				return false, fmt.Errorf("stale NZB generation %q for main entry %s (current %q)", generation, infohash, entry.NZBGeneration)
			}
			mainChanged = applyUsenetStreamFailure(entry, message)
			return mainChanged, nil
		})
		if mutateErr != nil {
			return fmt.Errorf("persist Usenet failure in main entry: %w", mutateErr)
		}
		if !present {
			return fmt.Errorf("entry %s was deleted before Usenet failure persistence", infohash)
		}
		var queueErr error
		if m.queue != nil {
			_, _, queueErr = m.queue.mutateTerminalLocked(infohash, func(queued *storage.Entry) bool {
				if generation != "" && queued.NZBGeneration != generation {
					return false
				}
				return applyUsenetStreamFailure(queued, message)
			})
		} else {
			// Lightweight managers used by focused tests have no Queue wrapper.
			// Storage still provides an atomic optional-mirror mutation, so retain
			// the same persistence guarantee without dereferencing a nil queue.
			_, _, queueErr = m.storage.MutateQueuedIfPresent(infohash, func(queued *storage.Entry) (bool, error) {
				if generation != "" && queued.NZBGeneration != generation {
					return false, nil
				}
				return applyUsenetStreamFailure(queued, message), nil
			})
		}
		if queueErr != nil {
			return fmt.Errorf("synchronize Usenet stream failure to queue: %w", queueErr)
		}
		return nil
	}
	var err error
	if m.queue != nil {
		err = m.queue.withLifecycle(infohash, persist)
	} else {
		err = persist()
	}
	if err != nil {
		return err
	}

	if mainChanged {
		m.refreshEntryCache()
	}
	return nil
}

func applyUsenetStreamFailure(entry *storage.Entry, message error) bool {
	// A durable Usenet content failure is terminal for the whole entry. Do not
	// make its identity depend on whether this request observed one failed file
	// or a collection of them: alternating HEAD/listing requests must not churn
	// LastError or inflate ErrorCount.
	if entry.State == storage.EntryStateError && entry.Bad {
		return false
	}
	entry.MarkAsError(message)
	entry.Bad = true
	return true
}

func normalizeStreamRange(size, start, end int64) (int64, int64, error) {
	if size <= 0 {
		return 0, 0, fmt.Errorf("invalid file size %d", size)
	}

	if start < 0 {
		start = 0
	}
	if end == -1 || end >= size {
		end = size - 1
	}
	if start >= size {
		return 0, 0, fmt.Errorf("requested start %d beyond file size %d", start, size)
	}
	if end < start {
		return 0, 0, fmt.Errorf("invalid byte range %d-%d", start, end)
	}
	return start, end, nil
}

func (m *Manager) doRequest(ctx context.Context, url string, start, end int64, rangeRequested bool) (*http.Response, error) {
	var resp *http.Response

	err := retry.Do(
		func() error {
			req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if reqErr != nil {
				return retry.Unrecoverable(StreamError{Err: reqErr, Retryable: false})
			}

			// Backing ranges are explicit. A logical full-file request without a
			// stored subrange must not be converted into an upstream Range request.
			if rangeRequested {
				req.Header.Set("Range", buildHTTPRange(start, end))
			}

			// Set optimized headers for streaming
			req.Header.Set("Connection", "keep-alive")
			req.Header.Set("Accept-Encoding", "identity") // Disable compression for streaming
			req.Header.Set("Cache-Control", "no-cache")

			var doErr error
			resp, doErr = m.streamClient.Do(req)
			if doErr != nil {
				// Check if it's a connection error that we should retry
				if isConnectionError(doErr) {
					return doErr
				}
				return retry.Unrecoverable(StreamError{Err: doErr, Retryable: true})
			}
			return nil
		},
		retry.Context(ctx),
		retry.Attempts(uint(m.config.Retries)+1),
		retry.Delay(config.DefaultRetryDelay),
		retry.MaxDelay(config.DefaultRetryDelayMax),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
	)

	if err != nil {
		return nil, StreamError{Err: fmt.Errorf("connection retry exhausted: %w", err), Retryable: true}
	}
	return resp, nil
}

func buildContentRange(start, end, total int64) string {
	var b strings.Builder
	b.Grow(64)
	b.WriteString("bytes ")
	b.WriteString(strconv.FormatInt(start, 10))
	b.WriteByte('-')
	if end >= start {
		b.WriteString(strconv.FormatInt(end, 10))
	} else {
		b.WriteByte('*')
	}
	b.WriteByte('/')
	b.WriteString(strconv.FormatInt(total, 10))
	return b.String()
}

func buildHTTPRange(start, end int64) string {
	var b strings.Builder
	b.Grow(32)
	b.WriteString("bytes=")
	b.WriteString(strconv.FormatInt(start, 10))
	b.WriteByte('-')
	if end >= start {
		b.WriteString(strconv.FormatInt(end, 10))
	}
	return b.String()
}

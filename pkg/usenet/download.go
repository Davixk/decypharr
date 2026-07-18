package usenet

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sourcegraph/conc/pool"
)

// segmentResult holds a fetched segment and its index for ordered writing
type segmentResult struct {
	index int
	data  []byte
	err   error
}

// ProgressCallback is called periodically during download with progress info
// downloaded: total bytes written so far, speed: bytes per second (estimated)
type ProgressCallback func(downloaded int64, speed int64)

// Download downloads a file by fetching segments in parallel and streaming to writer in order.
// Bytes flow to the writer progressively as in-order segments complete - no waiting for all segments.
// If progressCallback is provided, it will be called after each segment write with current progress.
func (u *Usenet) Download(ctx context.Context, nzoID, filename string, writer io.Writer, progressCallback ProgressCallback) error {
	return u.downloadForGeneration(ctx, nzoID, "", filename, writer, progressCallback)
}

// DownloadForGeneration rejects stale work before reading metadata, fences any
// permanent failure write, and verifies ownership again before reporting a
// successful local file. Concurrent files of the same NZB remain parallel.
func (u *Usenet) DownloadForGeneration(ctx context.Context, nzoID, generation, filename string, writer io.Writer, progressCallback ProgressCallback) error {
	if generation == "" {
		return fmt.Errorf("NZB generation is required")
	}
	return u.downloadForGeneration(ctx, nzoID, generation, filename, writer, progressCallback)
}

func (u *Usenet) downloadForGeneration(ctx context.Context, nzoID, generation, filename string, writer io.Writer, progressCallback ProgressCallback) error {
	downloadCtx, cancelDownload := context.WithCancel(ctx)
	defer cancelDownload()

	// get file metadata
	file, _, err := u.getFileForGeneration(nzoID, generation, filename)
	if err != nil {
		return fmt.Errorf("failed to get file: %w", err)
	}

	if len(file.Segments) == 0 {
		return fmt.Errorf("file has no segments: %s", file.Name)
	}
	var expectedBytes int64
	for _, segment := range file.Segments {
		if segment.Bytes <= 0 {
			return fmt.Errorf("segment %d has invalid expected size %d", segment.Number, segment.Bytes)
		}
		expectedBytes += segment.Bytes
		if expectedBytes < 0 {
			return fmt.Errorf("download size overflow for %s", filename)
		}
	}

	// Track progress
	var completedSegments atomic.Int64
	var downloadedBytes atomic.Int64

	// Channel for segment results - buffered to allow parallel fetching ahead
	resultChan := make(chan segmentResult, max(u.processingMaxConnections, 1)*2)

	// Map to hold out-of-order segments waiting to be written
	pendingSegments := make(map[int][]byte)
	var pendingMu sync.Mutex
	nextToWrite := 0

	// Error tracking
	var writeErr error
	var writeErrMu sync.Mutex
	setWriteErr := func(err error) {
		if err == nil {
			return
		}
		writeErrMu.Lock()
		if writeErr == nil {
			writeErr = err
			cancelDownload()
		}
		writeErrMu.Unlock()
	}
	hasWriteErr := func() bool {
		writeErrMu.Lock()
		defer writeErrMu.Unlock()
		return writeErr != nil
	}

	// Writer goroutine - writes segments in order as they arrive
	var writerWg sync.WaitGroup
	writerWg.Go(func() {
		for result := range resultChan {
			if hasWriteErr() {
				continue
			}
			if result.err != nil {
				setWriteErr(result.err)
				continue
			}

			pendingMu.Lock()
			pendingSegments[result.index] = result.data

			// Write all consecutive segments starting from nextToWrite
			for {
				data, exists := pendingSegments[nextToWrite]
				if !exists {
					break
				}
				delete(pendingSegments, nextToWrite)
				pendingMu.Unlock()

				// Write to output. A short write with nil error violates io.Writer's
				// contract and must never be accepted as a complete segment.
				n, err := writeDownloadedSegment(writer, data)
				if err != nil {
					setWriteErr(fmt.Errorf("write failed at segment %d: %w", nextToWrite, err))
					pendingMu.Lock()
					break
				}

				completedSegments.Add(1)
				downloaded := downloadedBytes.Add(int64(n))
				nextToWrite++

				// Call progress callback if provided
				if progressCallback != nil {
					// Estimate speed (rough: assume ~1s per segment batch)
					completed := completedSegments.Load()
					speed := downloaded / max(1, completed) * int64(max(u.processingMaxConnections, 1))
					progressCallback(downloaded, speed)
				}

				pendingMu.Lock()
			}
			pendingMu.Unlock()
		}
	})

	// Fetch segments in parallel
	p := pool.New().WithContext(downloadCtx).WithMaxGoroutines(max(u.processingMaxConnections, 1))

	for idx, segment := range file.Segments {
		segIdx := idx
		seg := segment

		p.Go(func(ctx context.Context) error {
			// Check for write errors
			writeErrMu.Lock()
			if writeErr != nil {
				writeErrMu.Unlock()
				return writeErr
			}
			writeErrMu.Unlock()

			// Check context
			if ctx.Err() != nil {
				return ctx.Err()
			}

			// Fetch segment using manager with failover
			var data []byte
			err := u.nntp.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
				d, e := conn.GetDecodedBody(seg.MessageID)
				data = d
				return e
			})
			if err != nil {
				select {
				case resultChan <- segmentResult{index: segIdx, err: fmt.Errorf("segment %d: %w", segIdx, err)}:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			data, err = normalizeDownloadedSegment(seg, data)
			if err != nil {
				select {
				case resultChan <- segmentResult{index: segIdx, err: fmt.Errorf("segment %d: %w", segIdx, err)}:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			select {
			case resultChan <- segmentResult{index: segIdx, data: data}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}

	// Wait for all fetches to complete, then close result channel
	fetchErr := p.Wait()
	close(resultChan)

	// Wait for writer to finish
	writerWg.Wait()

	// Check for errors
	if writeErr != nil {
		if generation != "" && nntp.IsArticleNotFoundError(writeErr) {
			return u.recordPermanentArticleFailureForGeneration(nzoID, generation, filename, writeErr)
		}
		return writeErr
	}
	if fetchErr != nil {
		return fetchErr
	}
	if err := verifyDownloadComplete(completedSegments.Load(), len(file.Segments), downloadedBytes.Load(), expectedBytes); err != nil {
		return err
	}
	if generation != "" {
		if err := u.AssertGeneration(nzoID, generation); err != nil {
			return fmt.Errorf("NZB generation changed during download: %w", err)
		}
	}

	u.logger.Info().
		Str("file", filename).
		Int64("bytes", downloadedBytes.Load()).
		Msg("Download complete")

	return nil
}

func normalizeDownloadedSegment(segment storage.NZBSegment, data []byte) ([]byte, error) {
	if segment.Bytes <= 0 {
		return nil, fmt.Errorf("invalid expected size %d", segment.Bytes)
	}
	if segment.SegmentDataStart < 0 || segment.SegmentDataStart > int64(len(data)) {
		return nil, fmt.Errorf("offset %d exceeds decoded size %d", segment.SegmentDataStart, len(data))
	}
	data = data[segment.SegmentDataStart:]
	if int64(len(data)) < segment.Bytes {
		return nil, fmt.Errorf("decoded %d bytes, expected %d: %w", len(data), segment.Bytes, io.ErrUnexpectedEOF)
	}
	return data[:segment.Bytes], nil
}

func writeDownloadedSegment(writer io.Writer, data []byte) (int, error) {
	n, err := writer.Write(data)
	if n < 0 || n > len(data) {
		return n, fmt.Errorf("invalid writer count %d for %d bytes", n, len(data))
	}
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return n, err
}

func verifyDownloadComplete(completed int64, expectedSegments int, downloaded, expectedBytes int64) error {
	if completed != int64(expectedSegments) {
		return fmt.Errorf("download completed %d of %d segments", completed, expectedSegments)
	}
	if downloaded != expectedBytes {
		return fmt.Errorf("download wrote %d bytes, expected %d: %w", downloaded, expectedBytes, io.ErrUnexpectedEOF)
	}
	return nil
}

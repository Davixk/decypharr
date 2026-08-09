package webdav

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func (h *Handler) StreamResponse(entry *storage.Entry, info *manager.FileInfo, w http.ResponseWriter, r *http.Request) error {
	var err error
	entry, info, err = h.preparer.PrepareFileInfo(entry, info)
	if err != nil {
		var customErr *customerror.Error
		if errors.As(err, &customErr) {
			customErr.HeadersWritten = false
			return customErr
		}
		return customerror.NewError(err, http.StatusInternalServerError, "server.internal_error", false, false)
	}
	return h.streamPreparedResponse(entry, info, w, r)
}

func (h *Handler) streamPreparedResponse(entry *storage.Entry, info *manager.FileInfo, w http.ResponseWriter, r *http.Request) error {
	start, end, rangeErr := prepareRangeResponse(w, info.Size(), r)
	if rangeErr != nil {
		return rangeErr
	}

	// Extract client identifier from User-Agent header
	client := r.UserAgent()
	if client == "" {
		client = "Unknown"
	}

	streamID := h.manager.TrackStream(entry, info.Name(), client)
	if streamID != "" {
		defer h.manager.UntrackStream(streamID)
	}

	headersWritten := false
	err := h.manager.Stream(r.Context(), entry, info.Name(), start, end, r.Header.Get("Range") != "", w, func(meta *manager.StreamMetadata) error {
		// Manager.Stream revalidates Usenet metadata immediately before it
		// invokes onReady. Delay every success/entity header until that final
		// preparation succeeds so a concurrent delete/failure cannot leave a
		// 410 response carrying stale media headers.
		setEntityHeaders(w, info)
		if err := h.handleSuccessfulResponse(w, meta, start, end); err != nil {
			return err
		}
		headersWritten = true
		return nil
	}, client)
	if err != nil {
		var customErr *customerror.Error
		if errors.As(err, &customErr) {
			customErr.HeadersWritten = headersWritten
			return customErr
		}

		return customerror.NewError(err, http.StatusInternalServerError, "server.internal_error", false, headersWritten)
	}
	return nil
}

func prepareRangeResponse(w http.ResponseWriter, size int64, r *http.Request) (int64, int64, error) {
	start, end, err := getRange(size, r)
	if err == nil {
		return start, end, nil
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
	return 0, 0, customerror.NewError(err, http.StatusRequestedRangeNotSatisfiable, "server.range_not_satisfiable", false, false)
}

func (h *Handler) handleSuccessfulResponse(w http.ResponseWriter, meta *manager.StreamMetadata, start, end int64) error {
	statusCode := http.StatusOK
	if meta != nil {
		if meta.Header != nil {
			if contentLength := meta.Header.Get("Content-Length"); contentLength != "" {
				w.Header().Set("Content-Length", contentLength)
			} else if meta.ContentLength > 0 {
				w.Header().Set("Content-Length", fmt.Sprintf("%d", meta.ContentLength))
			}

			if contentRange := meta.Header.Get("Content-Range"); contentRange != "" {
				w.Header().Set("Content-Range", contentRange)
			}

			if contentType := meta.Header.Get("Content-Type"); contentType != "" {
				w.Header().Set("Content-Type", contentType)
			}
		}
		if meta.StatusCode != 0 {
			statusCode = meta.StatusCode
		} else if start > 0 || end > 0 {
			statusCode = http.StatusPartialContent
		}
	} else if start > 0 || end > 0 {
		statusCode = http.StatusPartialContent
	}

	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(statusCode)
	return nil
}

func getRange(size int64, r *http.Request) (int64, int64, error) {
	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		// Signal downstream streaming code to serve the entire file
		return 0, -1, nil
	}

	ranges, err := parseRange(rangeHeader, size)
	if err != nil || len(ranges) != 1 {
		if err == nil {
			err = fmt.Errorf("exactly one satisfiable byte range is required")
		}
		return 0, 0, fmt.Errorf("invalid Range header %q: %w", rangeHeader, err)
	}

	return ranges[0].start, ranges[0].end, nil
}

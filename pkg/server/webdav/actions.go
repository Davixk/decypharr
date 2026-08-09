package webdav

import (
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// handlePropfind answers a listing under a ceiling.
//
// A listing used to have no deadline of ANY kind. Only byte-streams did, so a
// stalled metadata call had nothing to trip and the handler simply waited on
// whatever the upstream did — which for Usenet entries is a live per-file
// preparation against the NNTP providers. A wedged listing takes down `ls`, an
// *arr scan and a Plex refresh exactly as hard as a wedged read, and it never
// produced an error any of them could act on.
func (h *Handler) handlePropfind(current *manager.FileInfo, children []manager.FileInfo, w http.ResponseWriter, r *http.Request) {
	if current == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	// One ceiling, applied to each backend phase below. The two phases cannot
	// both do backend work on any real route: `current` is only a non-directory
	// on /{group}/{torrent}/{file}, and that route resolves with children == nil,
	// so the child batch is an empty no-op there. The configured number is
	// therefore the wait an operator actually experiences, not half of it.
	ceiling := h.metadataReadTimeout()

	if !current.IsDir() && current.IsRemote() {
		prepared, ok := awaitBounded(ceiling, func() preparedFile {
			entry, info, err := h.prepareRemoteFile(current)
			return preparedFile{entry: entry, info: info, err: err}
		})
		if !ok {
			h.writeBackendTimeout(w, "propfind_self", ceiling)
			return
		}
		if prepared.err != nil {
			h.writeMetadataError(w, prepared.err)
			return
		}
		current = prepared.info
	}

	var preparedChildren []manager.FileInfo
	if propfindIncludesChildren(r) {
		result, ok := awaitBounded(ceiling, func() preparedBatch {
			infos, errs := h.preparer.PrepareFileInfos(children)
			return preparedBatch{infos: infos, errors: errs}
		})
		if !ok {
			h.writeBackendTimeout(w, "propfind_children", ceiling)
			return
		}
		batch, batchErrors := result.infos, result.errors
		preparedChildren = make([]manager.FileInfo, 0, len(batch))
		for i := range batch {
			if err := batchErrors[i]; err != nil {
				if customerror.IsContentPermanentlyGone(err) {
					// A permanently unavailable resource must not be advertised
					// as healthy in a collection listing.
					//
					// THIS IS THE SAME PREDICATE THE REPAIR PROBE CONDEMNS A FILE
					// WITH (isDeadContentVerdict -> IsContentPermanentlyGone). It
					// was an inline 410 check here and a separate code switch
					// there, and they drifted: this loop hid every child of an
					// entry while the probe reported "no verdict" for the same
					// files, so an entry serving an EMPTY directory to every
					// client sat un-actioned. Keep them one call.
					continue
				}
				h.writeMetadataError(w, err)
				return
			}
			preparedChildren = append(preparedChildren, batch[i])
		}
	}

	cleanPath := path.Clean(r.URL.Path)
	sb := convertToXML(cleanPath, current, preparedChildren)
	// Set headers
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Vary", "Accept-Encoding")

	// Set status code and write response
	w.WriteHeader(http.StatusMultiStatus) // 207 MultiStatus
	_, _ = w.Write(sb.Bytes())
}

func propfindIncludesChildren(r *http.Request) bool {
	return !strings.EqualFold(strings.TrimSpace(r.Header.Get("Depth")), "0")
}

func (h *Handler) handleGet(current *manager.FileInfo, w http.ResponseWriter, r *http.Request) {
	if current.IsDir() {
		http.Error(w, "Bad Request: Cannot GET a directory", http.StatusBadRequest)
		return
	}
	h.handleDownload(current, w, r)
}

func (h *Handler) handleDelete(current *manager.FileInfo, w http.ResponseWriter, r *http.Request) {
	if err := h.manager.RemoveEntry(current); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent) // 204 No Content
}

func (h *Handler) handleHead(info *manager.FileInfo, w http.ResponseWriter, r *http.Request) {
	if info == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	if !info.IsDir() && info.IsRemote() {
		// Same ceiling as PROPFIND: HEAD is a metadata request that reaches the
		// same preparer, and a media player probing a file must never be the
		// thing that wedges on a stalled backend.
		ceiling := h.metadataReadTimeout()
		prepared, ok := awaitBounded(ceiling, func() preparedFile {
			entry, prepInfo, err := h.prepareRemoteFile(info)
			return preparedFile{entry: entry, info: prepInfo, err: err}
		})
		if !ok {
			h.writeBackendTimeout(w, "head", ceiling)
			return
		}
		if prepared.err != nil {
			h.writeMetadataError(w, prepared.err)
			return
		}
		info = prepared.info
	}

	w.Header().Set("Content-Type", utils.GetContentType(info.Name()))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size()))
	w.Header().Set("Last-Modified", info.ModTime().UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) prepareRemoteFile(info *manager.FileInfo) (*storage.Entry, *manager.FileInfo, error) {
	if info == nil {
		return nil, nil, fmt.Errorf("file info is nil")
	}
	if info.IsDir() || !info.IsRemote() {
		return nil, info, nil
	}

	var (
		entry *storage.Entry
		err   error
	)
	if info.InfoHash() != "" {
		entry, err = h.manager.GetEntry(info.InfoHash())
	} else {
		entry, err = h.manager.GetEntryByName(info.Parent(), info.Name())
	}
	if err != nil {
		return nil, nil, err
	}
	if entry == nil {
		return nil, nil, fmt.Errorf("entry for %s/%s is nil", info.Parent(), info.Name())
	}
	return h.preparer.PrepareFileInfo(entry, info)
}

func (h *Handler) writeMetadataError(w http.ResponseWriter, err error) {
	var customErr *customerror.Error
	if errors.As(err, &customErr) {
		setTransientRetryAfter(w, customErr)
		http.Error(w, customErr.Error(), customErr.StatusCode())
		return
	}
	http.Error(w, "Internal Server Error", http.StatusInternalServerError)
}

// setTransientRetryAfter attaches a backoff hint to errors the type system
// already says are transient (Retryable and not Permanent). It is driven off
// that existing classification rather than off the status code on purpose:
// TrafficExceededError is also a 503 but clears on the provider's billing
// cycle, not in five seconds, and it is deliberately NOT marked retryable — so
// it correctly gets no hint. Advertising a wrong one would be worse than none.
func setTransientRetryAfter(w http.ResponseWriter, err *customerror.Error) {
	if err == nil || !err.IsRetryable() {
		return
	}
	w.Header().Set("Retry-After", transientRetryAfter)
}

func (h *Handler) handleCopy(current *manager.FileInfo, w http.ResponseWriter, r *http.Request, delete bool) {
	handleCopyRequest(current, w, r, delete, h.manager.CopyEntryWithOverwrite)
}

type copyEntryFunc func(entry *manager.FileInfo, destination string, move, overwrite bool) (bool, error)

func handleCopyRequest(current *manager.FileInfo, w http.ResponseWriter, r *http.Request, move bool, copyEntry copyEntryFunc) {
	if current == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	destHeader := r.Header.Get("Destination")
	if strings.TrimSpace(destHeader) == "" {
		http.Error(w, "Bad Request: Missing Destination header", http.StatusBadRequest)
		return
	}
	destPath, err := parseDestinationPath(destHeader)
	if err != nil {
		http.Error(w, "Bad Request: Invalid Destination header", http.StatusBadRequest)
		return
	}
	overwrite, err := parseOverwriteHeader(r.Header.Get("Overwrite"))
	if err != nil {
		http.Error(w, "Bad Request: Invalid Overwrite header", http.StatusBadRequest)
		return
	}

	created, err := copyEntry(current, destPath, move, overwrite)
	if err != nil {
		switch {
		case errors.Is(err, manager.ErrCopyDestinationExists):
			http.Error(w, "Precondition Failed", http.StatusPreconditionFailed)
		case errors.Is(err, manager.ErrCopyDestinationParentMissing), errors.Is(err, manager.ErrCopyUnsupported), errors.Is(err, manager.ErrCopySourceActive):
			http.Error(w, "Conflict", http.StatusConflict)
		default:
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
		return
	}
	if created {
		w.WriteHeader(http.StatusCreated)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parseDestinationPath(header string) (string, error) {
	raw := strings.TrimSpace(header)
	destination, err := url.Parse(raw)
	if err != nil || destination.Opaque != "" || destination.RawQuery != "" || destination.Fragment != "" {
		return "", fmt.Errorf("invalid Destination URI")
	}
	if destination.IsAbs() {
		if destination.Host == "" || (destination.Scheme != "http" && destination.Scheme != "https") {
			return "", fmt.Errorf("unsupported Destination URI")
		}
	} else if destination.Host != "" || !strings.HasPrefix(raw, "/") {
		return "", fmt.Errorf("Destination must be an absolute URI or absolute path")
	}
	if destination.Path == "" || strings.ContainsRune(destination.Path, '\x00') {
		return "", fmt.Errorf("Destination path is empty")
	}
	escapedPath := strings.ToLower(destination.EscapedPath())
	if strings.Contains(escapedPath, "%2f") || strings.Contains(escapedPath, "%5c") || strings.ContainsRune(destination.Path, '\\') {
		return "", fmt.Errorf("Destination path contains an encoded separator")
	}
	clean := path.Clean(destination.Path)
	if clean == "." || clean == "/" {
		return "", fmt.Errorf("Destination path has no resource name")
	}
	return clean, nil
}

func parseOverwriteHeader(header string) (bool, error) {
	switch strings.ToUpper(strings.TrimSpace(header)) {
	case "", "T":
		return true, nil
	case "F":
		return false, nil
	default:
		return false, fmt.Errorf("invalid Overwrite value")
	}
}

func (h *Handler) handleOptions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", "OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, COPY, MOVE, PROPFIND")
	w.Header().Set("DAV", "1, 2")
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDownload(info *manager.FileInfo, w http.ResponseWriter, r *http.Request) {
	if !info.IsRemote() {
		setEntityHeaders(w, info)
		// Write .Content disposition for local files
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename*=UTF-8''%s", utils.PathUnescape(info.Name())))
		_, _ = w.Write(info.Content())
		return
	}

	originalInfo := info
	// The PREPARE phase is bounded like any other metadata call; the transfer
	// that follows is not, and must not be — a healthy stream is long by design
	// and has its own idle deadline (debrid_read_timeout). What this ceiling
	// covers is the window before a single byte exists, which nothing else
	// watches.
	ceiling := h.metadataReadTimeout()
	prepared, ok := awaitBounded(ceiling, func() preparedFile {
		entry, preparedInfo, err := h.prepareRemoteFile(info)
		return preparedFile{entry: entry, info: preparedInfo, err: err}
	})
	if !ok {
		h.writeBackendTimeout(w, "download_prepare", ceiling)
		return
	}
	if prepared.err != nil {
		h.handleDownloadError(originalInfo, w, prepared.err)
		return
	}
	entry, info := prepared.entry, prepared.info
	if err := h.streamPreparedResponse(entry, info, w, r); err != nil {
		h.handleDownloadError(info, w, err)
	}
}

func setEntityHeaders(w http.ResponseWriter, info *manager.FileInfo) {
	w.Header().Set("ETag", fmt.Sprintf("\"%x-%x\"", info.ModTime().Unix(), info.Size()))
	w.Header().Set("Last-Modified", info.ModTime().UTC().Format(http.TimeFormat))
	ext := filepath.Ext(info.Name())
	if contentType := mime.TypeByExtension(ext); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
}

func (h *Handler) handleDownloadError(info *manager.FileInfo, w http.ResponseWriter, err error) {
	logKey := fmt.Sprintf("%s/%s", info.Parent(), info.Name())

	var streamErr *customerror.Error
	if errors.As(err, &streamErr) {
		if !streamErr.HeadersWritten {
			setTransientRetryAfter(w, streamErr)
			http.Error(w, streamErr.Error(), streamErr.StatusCode())
		}
		if !streamErr.IsSilent() {
			h.logger.Rate(logKey).Error().Err(err).Msgf("Error streaming file: %s", logKey)
		}
		return
	}

	if !customerror.IsSilentError(err) {
		h.logger.Rate(logKey).Error().Err(err).Msgf("Error streaming file: %s", logKey)
	}
	http.Error(w, "Internal Server Error", http.StatusInternalServerError)
}

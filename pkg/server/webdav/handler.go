package webdav

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func init() {
	chi.RegisterMethod("PROPFIND")
	chi.RegisterMethod("PROPPATCH")
	chi.RegisterMethod("MKCOL")
	chi.RegisterMethod("COPY")
	chi.RegisterMethod("MOVE")
	chi.RegisterMethod("LOCK")
	chi.RegisterMethod("UNLOCK")
}

const (
	PROPFIND = "PROPFIND"
)

type Handler struct {
	logger   *logger.RateLimitedLogger
	manager  *manager.Manager
	metadata entryMetadataResolver
	preparer entryPreparer
}

// entryPreparer is the seam for the only metadata work that can reach a
// backend. Everything else a listing does is local store iteration; these two
// calls are the ones that, for Usenet entries, fan out to the NNTP providers
// with no deadline of their own. Split out as an interface so the ceiling that
// now guards them can be driven directly in tests with a preparer that stalls,
// which is the only way to prove the ceiling actually releases the handler.
type entryPreparer interface {
	PrepareFileInfo(*storage.Entry, *manager.FileInfo) (*storage.Entry, *manager.FileInfo, error)
	PrepareFileInfos([]manager.FileInfo) ([]manager.FileInfo, []error)
}

type entryMetadataResolver interface {
	RootInfo() *manager.FileInfo
	GetEntries() []manager.FileInfo
	GetEntryNode(string) *manager.FileInfo
	GetEntryChildren(string) (*manager.FileInfo, []manager.FileInfo)
	GetEntryInfo(string) (*manager.FileInfo, error)
	GetTorrentChildren(string) (*manager.FileInfo, []manager.FileInfo)
	GetTorrentFile(string, string) (*manager.FileInfo, error)
}

func NewHandler(mgr *manager.Manager) *Handler {
	log := logger.NewRateLimitedLogger(logger.WithLogger(logger.New("webdav")))
	h := &Handler{
		logger:   log,
		manager:  mgr,
		metadata: mgr,
		preparer: mgr,
	}
	return h
}

func (h *Handler) readinessMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-h.manager.IsReady():
			// WebDAV is ready, proceed
			next.ServeHTTP(w, r)
		default:
			// WebDAV is still initializing
			w.Header().Set("Retry-After", "5")
			http.Error(w, "WebDAV service is initializing, please try again shortly", http.StatusServiceUnavailable)
		}
	})
}

func (h *Handler) Routes() chi.Router {
	r := chi.NewRouter()
	r.Use(h.readinessMiddleware)
	r.Use(h.commonMiddleware)
	r.Use(middleware.AllowContentEncoding("gzip"))
	// Always install the auth middleware; whether it actually enforces auth is
	// decided live per-request from config, so toggling UseAuth/EnableWebdavAuth
	// takes effect without rebuilding the router (no restart).
	r.Use(h.authMiddleware)

	h.registerRoutes(r)
	return r
}

// registerRoutes installs the resource routes on r. Split out from Routes so the
// exact production route table can be driven directly in tests, without the
// readiness gate that only a fully started Manager can open.
func (h *Handler) registerRoutes(r chi.Router) {
	r.HandleFunc("/", h.handleRoot)
	r.HandleFunc("/{group}", h.handleGroup)
	r.HandleFunc("/{group}/{torrent}", h.handleTorrentFolder)
	r.HandleFunc("/{group}/{torrent}/{file}", h.handleTorrentFile)
	r.HandleFunc("/stream/{group}/{torrent}/{file}", h.handleTorrentFile)
}

func (h *Handler) IsDisabled() bool {
	cfg := config.Get()
	return cfg.DisableWebDav
}

func (h *Handler) handler(current *manager.FileInfo, children []manager.FileInfo, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "HEAD":
		h.handleHead(current, w, r)
	case "GET":
		if current == nil {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}
		h.handleGet(current, w, r)
	case "DELETE":
		h.handleDelete(current, w, r)
	case PROPFIND:
		h.handlePropfind(current, children, w, r)
	case "COPY":
		h.handleCopy(current, w, r, false)
	case "OPTIONS":
		h.handleOptions(w, r)
	case "MOVE":
		h.handleCopy(current, w, r, true)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
}

func (h *Handler) handleRoot(w http.ResponseWriter, r *http.Request) {
	current := h.metadata.RootInfo()
	var children []manager.FileInfo
	if requestNeedsChildren(r) {
		children = h.metadata.GetEntries()
	}
	h.handler(current, children, w, r)
}

func (h *Handler) handleGroup(w http.ResponseWriter, r *http.Request) {
	group := utils.PathUnescape(chi.URLParam(r, "group"))
	currentInfo, rawEntries := h.resolveGroupMetadata(group, requestNeedsChildren(r))
	h.handler(currentInfo, rawEntries, w, r)

}

func (h *Handler) handleTorrentFolder(w http.ResponseWriter, r *http.Request) {
	torrent := utils.PathUnescape(chi.URLParam(r, "torrent"))

	currentInfo, children := h.resolveTorrentMetadata(torrent, requestNeedsChildren(r))
	h.handler(currentInfo, children, w, r)
}

func (h *Handler) handleTorrentFile(w http.ResponseWriter, r *http.Request) {
	torrent := utils.PathUnescape(chi.URLParam(r, "torrent"))
	file := utils.PathUnescape(chi.URLParam(r, "file"))
	currentInfo, err := h.metadata.GetTorrentFile(torrent, file)
	if err != nil || currentInfo == nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	h.handler(currentInfo, nil, w, r)
}

func requestNeedsChildren(r *http.Request) bool {
	return r.Method == PROPFIND && propfindIncludesChildren(r)
}

func (h *Handler) resolveGroupMetadata(group string, includeChildren bool) (*manager.FileInfo, []manager.FileInfo) {
	if includeChildren {
		return h.metadata.GetEntryChildren(group)
	}
	return h.metadata.GetEntryNode(group), nil
}

func (h *Handler) resolveTorrentMetadata(torrent string, includeChildren bool) (*manager.FileInfo, []manager.FileInfo) {
	if includeChildren {
		return h.metadata.GetTorrentChildren(torrent)
	}
	current, err := h.metadata.GetEntryInfo(torrent)
	if err != nil {
		return nil, nil
	}
	return current, nil
}

func (h *Handler) commonMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("DAV", "1, 2")
		w.Header().Set("Allow", "OPTIONS, PROPFIND, GET, HEAD, POST, PUT, DELETE, MKCOL, PROPPATCH, COPY, MOVE, LOCK, UNLOCK")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "OPTIONS, GET, PROPFIND, HEAD, POST, PUT, DELETE, MKCOL, PROPPATCH, COPY, MOVE, LOCK, UNLOCK")
		w.Header().Set("Access-Control-Allow-Headers", "Depth, Content-Type, Authorization")

		next.ServeHTTP(w, r)
	})
}

func (h *Handler) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read the auth toggles live so changes apply without a restart.
		cfg := config.Get()
		if !cfg.UseAuth || !cfg.EnableWebdavAuth {
			next.ServeHTTP(w, r)
			return
		}

		username, password, ok := r.BasicAuth()
		if !ok || !config.VerifyAuth(username, password) {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

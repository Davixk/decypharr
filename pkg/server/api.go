package server

import (
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"sort"
	"strconv"
	"strings"

	json "github.com/bytedance/sonic"

	"github.com/go-chi/chi/v5"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/version"
	"github.com/sourcegraph/conc/iter"
	"golang.org/x/crypto/bcrypt"
)

type mountCacheCleaner interface {
	CleanupCache() (map[string]any, error)
}

type mountCachePurger interface {
	PurgeCache() (map[string]any, error)
}

func (s *Server) handleGetArrs(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, s.manager.Arr().GetAll(), http.StatusOK)
}

func (s *Server) handleAddContent(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	arrName := r.FormValue("arr")
	action := r.FormValue("action")
	debridName := r.FormValue("debrid")
	callbackUrl := r.FormValue("callbackUrl")
	downloadFolder := r.FormValue("downloadFolder")
	if downloadFolder == "" {
		downloadFolder = config.Get().DownloadFolder
	}
	skipMultiSeason := r.FormValue("skipMultiSeason") == "true"

	dlUncached := r.FormValue("downloadUncached") == "true"
	var downloadUncached *bool
	if dlUncached {
		downloadUncached = &dlUncached
	}
	rmTrackerUrls := r.FormValue("rmTrackerUrls") == "true"

	// Check config setting - if always remove tracker URLs is enabled, force it to true
	cfg := config.Get()
	if cfg.AlwaysRmTrackerUrls {
		rmTrackerUrls = true
	}

	_arr := s.manager.Arr().Get(arrName)
	if _arr == nil {
		// These are not found in the config. They are throwaway arrs.
		_arr = arr.New(arrName, "", "", false, downloadUncached, "", "")
	}

	// Unified task type for all content types
	type addTask struct {
		taskType   string // "torrent", "nzbURL", "nzbFile"
		magnet     *utils.Magnet
		nzbContent []byte
		name       string
		source     string // for error messages
	}

	var tasks []addTask

	// Collect torrent URLs
	if urls := r.FormValue("urls"); urls != "" {
		for u := range strings.SplitSeq(urls, "\n") {
			if trimmed := strings.TrimSpace(u); trimmed != "" {
				magnet, err := utils.GetMagnetFromUrl(trimmed, rmTrackerUrls)
				if err != nil {
					tasks = append(tasks, addTask{
						taskType: "error",
						source:   fmt.Sprintf("Failed to parse URL %s: %v", trimmed, err),
					})
					continue
				}
				tasks = append(tasks, addTask{taskType: "torrent", magnet: magnet, source: fmt.Sprintf("URL %s", trimmed)})
			}
		}
	}

	// Collect torrent files
	if files := r.MultipartForm.File["files"]; len(files) > 0 {
		for _, fileHeader := range files {
			file, err := fileHeader.Open()
			if err != nil {
				tasks = append(tasks, addTask{
					taskType: "error",
					source:   fmt.Sprintf("Failed to open file %s: %v", fileHeader.Filename, err),
				})
				continue
			}

			magnet, err := utils.GetMagnetFromFile(file, fileHeader.Filename, rmTrackerUrls)
			if err != nil {
				tasks = append(tasks, addTask{
					taskType: "error",
					source:   fmt.Sprintf("Failed to parse torrent file %s: %v", fileHeader.Filename, err),
				})
				continue
			}
			tasks = append(tasks, addTask{taskType: "torrent", magnet: magnet, source: fmt.Sprintf("File %s", fileHeader.Filename), name: fileHeader.Filename})
		}
	}

	// Collect NZB URLs
	if nzbURLs := r.FormValue("nzbURLs"); nzbURLs != "" {
		for u := range strings.SplitSeq(nzbURLs, "\n") {
			if trimmed := strings.TrimSpace(u); trimmed != "" {
				filename, content, err := utils.DownloadFile(trimmed, utils.WithHeader("User-Agent", s.nzbUserAgent))
				if err != nil {
					tasks = append(tasks, addTask{
						taskType: "error",
						source:   fmt.Sprintf("Failed to fetch NZB from URL %s: %v", trimmed, err),
					})
					continue
				}
				tasks = append(tasks, addTask{taskType: "nzb", nzbContent: content, name: filename, source: fmt.Sprintf("NZB URL %s", trimmed)})
			}
		}
	}

	// Collect NZB files
	if nzbFiles := r.MultipartForm.File["nzbFiles"]; len(nzbFiles) > 0 {
		for _, fileHeader := range nzbFiles {
			content, err := getNZBContentFromFile(fileHeader)
			if err != nil {
				tasks = append(tasks, addTask{
					taskType: "error",
					source:   fmt.Sprintf("Failed to read NZB file %s: %v", fileHeader.Filename, err),
				})
				continue
			}
			tasks = append(tasks, addTask{taskType: "nzb", nzbContent: content, source: fmt.Sprintf("NZB File %s", fileHeader.Filename), name: fileHeader.Filename})
		}
	}

	// Parse all tasks in parallel using iter.Map
	mapper := iter.Mapper[addTask, *manager.ImportRequest]{
		MaxGoroutines: min(len(tasks), 10),
	}

	results := mapper.Map(tasks, func(task *addTask) *manager.ImportRequest {
		switch task.taskType {
		case "error":
			// Task already failed during collection phase
			return &manager.ImportRequest{
				Status: "error",
				Error:  fmt.Sprintf("Failed to import torrent %s: %v", task.name, task.magnet),
			}

		case "torrent":
			importReq := manager.NewTorrentRequest(debridName, downloadFolder, task.magnet, _arr, config.DownloadAction(action), downloadUncached, callbackUrl, manager.ImportTypeAPI, skipMultiSeason)
			if err := s.manager.AddNewTorrent(ctx, importReq); err != nil {
				s.logger.Error().Err(err).Str("source", task.source).Msg("Failed to add torrent")
				importReq.Error = err.Error()
				importReq.Status = "error"
			}
			return importReq

		case "nzb":
			importReq := manager.NewNZBRequest(task.name, downloadFolder, task.nzbContent, _arr, config.DownloadAction(action), callbackUrl, manager.ImportTypeAPI, skipMultiSeason)
			nzoID, err := s.manager.AddNewNZB(ctx, importReq)
			if err != nil {
				s.logger.Error().Err(err).Str("source", task.source).Msg("Failed to add NZB")
				importReq.Error = err.Error()
				importReq.Status = "error"
			}
			importReq.Id = nzoID
			return importReq

		default:
			return nil
		}
	})

	// Filter out nil results
	filtered := make([]*manager.ImportRequest, 0, len(results))
	for _, r := range results {
		if r != nil {
			filtered = append(filtered, r)
		}
	}

	utils.JSONResponse(w, filtered, http.StatusOK)
}

func getNZBContentFromFile(fileHeader *multipart.FileHeader) ([]byte, error) {
	file, err := fileHeader.Open()
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read NZB content
	nzbContent, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return nzbContent, nil
}

func (s *Server) handleGetVersion(w http.ResponseWriter, r *http.Request) {
	v := version.GetInfo()
	utils.JSONResponse(w, v, http.StatusOK)
}

func (s *Server) handleRunMountCacheCleanup(w http.ResponseWriter, r *http.Request) {
	mountMgr := s.manager.MountManager()
	if mountMgr == nil || !mountMgr.IsReady() {
		http.Error(w, "Mount is not ready", http.StatusServiceUnavailable)
		return
	}

	cleaner, ok := mountMgr.(mountCacheCleaner)
	if !ok {
		http.Error(w, "Manual cache cleanup is only available for DFS mounts", http.StatusBadRequest)
		return
	}

	cleanupStats, err := cleaner.CleanupCache()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to run mount cache cleanup")
		http.Error(w, "Failed to run mount cache cleanup", http.StatusInternalServerError)
		return
	}

	if s.stats != nil {
		s.stats.Refresh()
	}

	utils.JSONResponse(w, map[string]any{
		"status": "success",
		"cache":  cleanupStats,
	}, http.StatusOK)
}

func (s *Server) handlePurgeMountCache(w http.ResponseWriter, r *http.Request) {
	mountMgr := s.manager.MountManager()
	if mountMgr == nil || !mountMgr.IsReady() {
		http.Error(w, "Mount is not ready", http.StatusServiceUnavailable)
		return
	}

	purger, ok := mountMgr.(mountCachePurger)
	if !ok {
		http.Error(w, "Cache purge is only available for DFS mounts", http.StatusBadRequest)
		return
	}

	purgeStats, err := purger.PurgeCache()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to purge mount cache")
		http.Error(w, "Failed to purge mount cache", http.StatusInternalServerError)
		return
	}

	if s.stats != nil {
		s.stats.Refresh()
	}

	utils.JSONResponse(w, map[string]any{
		"status": "success",
		"cache":  purgeStats,
	}, http.StatusOK)
}

func (s *Server) handleGetTorrents(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters for server-side filtering, sorting, and pagination
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 100 {
		limit = 20
	}

	search := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("search")))
	category := strings.TrimSpace(r.URL.Query().Get("category"))
	state := strings.TrimSpace(r.URL.Query().Get("state"))
	sortBy := strings.TrimSpace(r.URL.Query().Get("sort_by"))
	sortOrder := strings.TrimSpace(r.URL.Query().Get("sort_order"))

	if sortBy == "" {
		sortBy = "added_on"
	}
	if sortOrder == "" {
		sortOrder = "desc"
	}

	// GetReader all torrents
	allTorrents := s.manager.Queue().ListFilter("", config.ProtocolAll, "", nil, "added_on", false)
	for _, t := range allTorrents {
		t.Sanitize()
	}

	// Apply filters
	filteredTorrents := make([]*storage.Entry, 0)
	for _, t := range allTorrents {
		// Search filter - search in name and hash
		if search != "" {
			searchIn := strings.ToLower(t.Name + " " + t.InfoHash)
			if !strings.Contains(searchIn, search) {
				continue
			}
		}

		// Category filter
		if category != "" && t.Category != category {
			continue
		}

		// State filter
		if state != "" && t.State != storage.TorrentState(state) {
			continue
		}

		filteredTorrents = append(filteredTorrents, t)
	}

	// Apply sorting
	sortQueuedTorrents(filteredTorrents, sortBy, sortOrder)

	// Calculate pagination
	total := len(filteredTorrents)
	totalPages := (total + limit - 1) / limit
	offset := (page - 1) * limit

	// Apply pagination
	var paginatedTorrents []*storage.Entry
	if offset < total {
		end := min(offset+limit, total)
		paginatedTorrents = filteredTorrents[offset:end]
	} else {
		paginatedTorrents = []*storage.Entry{}
	}

	// GetReader unique categories
	categorySet := make(map[string]bool)
	for _, t := range allTorrents {
		if t.Category != "" {
			categorySet[t.Category] = true
		}
	}

	categories := make([]string, 0, len(categorySet))
	for c := range categorySet {
		categories = append(categories, c)
	}

	utils.JSONResponse(w, map[string]any{
		"torrents":    paginatedTorrents,
		"total":       total,
		"page":        page,
		"limit":       limit,
		"total_pages": totalPages,
		"has_prev":    page > 1,
		"has_next":    page < totalPages,
		"categories":  categories,
	}, http.StatusOK)
}

// sortQueuedTorrents sorts torrents based on the given field and order
func sortQueuedTorrents(torrents []*storage.Entry, sortBy, sortOrder string) {
	if len(torrents) == 0 {
		return
	}

	less := func(i, j int) bool {
		var result bool
		switch sortBy {
		case "name":
			result = strings.ToLower(torrents[i].Name) < strings.ToLower(torrents[j].Name)
		case "size":
			result = torrents[i].Size < torrents[j].Size
		case "added_on":
			result = torrents[i].AddedOn.Before(torrents[j].AddedOn)
		case "progress":
			result = torrents[i].Progress < torrents[j].Progress
		case "category":
			result = strings.ToLower(torrents[i].Category) < strings.ToLower(torrents[j].Category)
		case "state":
			result = torrents[i].State < torrents[j].State
		default:
			result = torrents[i].AddedOn.Before(torrents[j].AddedOn)
		}

		if sortOrder == "desc" {
			return !result
		}
		return result
	}

	sort.Slice(torrents, less)
}

func (s *Server) handleDeleteTorrent(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	removeFromDebrid := r.URL.Query().Get("removeFromDebrid") == "true"
	if hash == "" {
		http.Error(w, "No hash provided", http.StatusBadRequest)
		return
	}
	var cleanup func(torrent *storage.Entry) error

	if removeFromDebrid {
		cleanup = func(t *storage.Entry) error {
			exists, _ := s.manager.EntryExists(t.InfoHash)
			if exists {
				// Remove the entry from manager fully, which will handle removing from debrid and deleting the entry
				return s.manager.DeleteEntry(t.InfoHash, true)
			}
			return s.manager.RemoveTorrentPlacements(t)
		}
	}

	if err := s.manager.Queue().Delete(hash, cleanup); err != nil {
		s.logger.Error().Err(err).Str("hash", hash).Msg("Failed to delete entry from queue")
		http.Error(w, "Failed to delete entry from queue", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteTorrents(w http.ResponseWriter, r *http.Request) {
	hashesStr := r.URL.Query().Get("hashes")
	removeFromDebrid := r.URL.Query().Get("removeFromDebrid") == "true"
	if hashesStr == "" {
		http.Error(w, "No hashes provided", http.StatusBadRequest)
		return
	}
	hashes := strings.Split(hashesStr, ",")
	var cleanup func(torrent *storage.Entry) error
	if removeFromDebrid {
		cleanup = func(t *storage.Entry) error {
			exists, _ := s.manager.EntryExists(t.InfoHash)
			if exists {
				// Remove the entry from manager fully, which will handle removing from debrid and deleting the entry
				return s.manager.DeleteEntry(t.InfoHash, true)
			}
			return s.manager.RemoveTorrentPlacements(t)
		}
	}
	if err := s.manager.Queue().DeleteWhere("", config.ProtocolAll, "", hashes, cleanup); err != nil {
		s.logger.Error().Err(err).Msg("Failed to delete torrents")
		http.Error(w, "Failed to delete torrents", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	arrStorage := s.manager.Arr()
	cfg := config.Get()
	cfg.Arrs = arrStorage.SyncToConfig()

	// Create response with API token info
	type ConfigResponse struct {
		*config.Config
		APIToken     string `json:"api_token,omitempty"`
		AuthUsername string `json:"auth_username,omitempty"`
	}

	response := &ConfigResponse{Config: cfg}

	// AddOrUpdate API token and auth information
	auth := cfg.GetAuth()
	if auth != nil {
		if auth.APIToken != "" {
			response.APIToken = auth.APIToken
		}
		response.AuthUsername = auth.Username
	}

	utils.JSONResponse(w, response, http.StatusOK)
}

// configUpdateMode selects how a submitted document is combined with the stored
// resource. It is the ONLY difference between the verbs: every config-writing
// handler funnels through one implementation and passes one of these, so merge
// and replace can never drift into two divergent code paths.
type configUpdateMode int

const (
	// mergeUpdate is PATCH: the body is a PARTIAL document. A key absent from
	// it keeps its current value at every nesting level; a key that is present
	// — including an explicit zero/false/empty — is applied. Arrays and maps
	// are not element-merged: a posted list is the caller's complete list and
	// replaces wholesale, an absent one is preserved.
	mergeUpdate configUpdateMode = iota
	// replaceUpdate is PUT: the body is the COMPLETE document. Whatever the
	// caller omitted reverts to its zero/default value — that is what PUT
	// means, and the handler does not quietly soften it. See
	// applyRepairConfigUpdate for why clearing the destructive-action knobs
	// this way is safe: their zero values are the conservative defaults.
	replaceUpdate
)

// handlePatchConfig serves PATCH /api/config: a partial update. Fields absent
// from the body are PRESERVED.
func (s *Server) handlePatchConfig(w http.ResponseWriter, r *http.Request) {
	s.applyConfigUpdate(w, r, mergeUpdate)
}

// handleReplaceConfig serves PUT /api/config: a full replacement. Fields absent
// from the body revert to their zero/default value.
func (s *Server) handleReplaceConfig(w http.ResponseWriter, r *http.Request) {
	s.applyConfigUpdate(w, r, replaceUpdate)
}

// applyConfigUpdate is the single implementation behind PATCH and PUT
// /api/config. Only the merge step below depends on mode; everything after it —
// the preserved auth fields, the Arr filtering, Save (which applies defaults)
// and the restart-vs-live decision — is identical for every verb, so a replace
// can never bypass a check a merge performs.
func (s *Server) applyConfigUpdate(w http.ResponseWriter, r *http.Request, mode configUpdateMode) {
	var newConfig config.Config
	body, err := readRequiredJSONBody(r, &newConfig)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to decode config update request")
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	currentConfig := config.Get()

	if mode == mergeUpdate {
		// PATCH: a key absent from the body means "keep the current value";
		// only explicitly submitted values (including empty ones such as
		// "debrids": []) overwrite. Without this merge, a partial update
		// replaced every omitted section with its zero value and Save wiped it
		// from disk.
		//
		// The merge recurses INTO submitted sections, so
		// `{"repair":{"enabled":true}}` does not replace the whole repair block
		// — which would silently zero max_deletions_per_run (the
		// destructive-action cap), stop_schedule, prune and regrab. Slices and
		// maps are the exception: a submitted array/object is the caller's
		// complete list and replaces wholesale.
		if err := newConfig.PreserveMissingSections(currentConfig, body); err != nil {
			s.logger.Error().Err(err).Msg("Failed to merge config update request")
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	// PUT skips the merge entirely: the decoded document IS the new config, so
	// every key the caller left out is already at its zero value here.

	// Basic validation
	if newConfig.BindAddress == "" {
		newConfig.BindAddress = "0.0.0.0"
	}
	if newConfig.Port == "" {
		newConfig.Port = "8282"
	}

	// Preserve fields that shouldn't be overwritten by frontend
	newConfig.Auth = currentConfig.GetAuth()
	// The frontend config form doesn't include use_auth or enable_webdav_auth,
	// so they would be zero-valued (false) in the decoded payload. Preserve
	// them from the live config so auth isn't silently disabled on every save.
	newConfig.UseAuth = currentConfig.UseAuth
	newConfig.EnableWebdavAuth = currentConfig.EnableWebdavAuth

	// Filter out empty or incomplete arrs
	validArrs := make([]config.Arr, 0, len(newConfig.Arrs))
	for _, a := range newConfig.Arrs {
		if a.Name != "" && a.Host != "" && a.Token != "" {
			if err := utils.ValidateURL(a.Host); err != nil {
				http.Error(w, fmt.Sprintf("Invalid Arr host for %q: %v", a.Name, err), http.StatusBadRequest)
				return
			}
			validArrs = append(validArrs, a)
		}
	}
	newConfig.Arrs = validArrs

	// Save the updated config. This also applies defaults to newConfig, so the
	// restart comparison below sees a fully-normalized config on both sides.
	if err := newConfig.Save(); err != nil {
		s.logger.Error().Err(err).Msg("Failed to save config")
		http.Error(w, "Error saving config: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Only restart when a field that needs it actually changed (HTTP bind,
	// debrid/usenet clients, or the mount). For everything else, apply the new
	// config live so users aren't disrupted by a full restart on every save.
	restarted := config.Get().RequiresRestart(&newConfig)
	if restarted {
		go s.Restart()
	} else {
		config.Get().ApplyRuntime(&newConfig)
		// Only expose Arr policy changes after the complete configuration is
		// durably saved and applied. A failed write must never enable cleanup.
		s.manager.Arr().SyncFromConfig(newConfig.Arrs)
		// Reschedule/reapply the repair sweep if its settings changed.
		if svc := s.manager.Repair(); svc != nil {
			if err := svc.ApplyConfig(); err != nil {
				s.logger.Warn().Err(err).Msg("Failed to apply repair config after live update")
			}
		}
	}

	utils.JSONResponse(w, map[string]any{"status": "success", "restarted": restarted}, http.StatusOK)
}

func (s *Server) handleGetRepairConfig(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, config.Get().Repair, http.StatusOK)
}

// handlePatchRepairConfig serves PATCH /api/repair/config: a partial update.
// A key absent from the body keeps its current value; an explicitly submitted
// one (including an explicit zero/false/empty) is applied.
func (s *Server) handlePatchRepairConfig(w http.ResponseWriter, r *http.Request) {
	s.applyRepairConfigUpdate(w, r, mergeUpdate)
}

// handleReplaceRepairConfig serves PUT /api/repair/config: a full replacement.
// The submitted document IS the new repair config, so every key the caller
// omitted reverts to its zero value.
//
// PUT used to merge, which was a lie: a client that PUT a partial document
// reasonably expects the omitted fields to be cleared, and silently preserving
// them made PUT indistinguishable from PATCH. The merge behaviour now lives on
// PATCH, where it belongs.
//
// Clearing by omission is safe here BECAUSE the destructive-action knobs are
// designed so their zero value is the conservative one: max_deletions_per_run 0
// resolves to the default cap of 100 (never unlimited — that is -1),
// prune/regrab false means "delete nothing", repair nil resolves to true
// (re-acquire, non-destructive) and stop_schedule "" only means the sweep runs
// to completion. A replace therefore cannot produce a MORE destructive config
// than the caller asked for, and it runs the same validation the merge path
// does — see applyRepairConfigUpdate.
func (s *Server) handleReplaceRepairConfig(w http.ResponseWriter, r *http.Request) {
	s.applyRepairConfigUpdate(w, r, replaceUpdate)
}

// applyRepairConfigUpdate is the single implementation behind PATCH and PUT
// /api/repair/config. Only the merge step depends on mode; the validation below
// then runs on the RESULTING document either way — i.e. on what will actually
// be saved — so a PATCH that omits "schedule" while the stored schedule is
// valid is accepted, and a PUT that omits it while enabling repair is rejected
// with 400 rather than saving a config that cannot schedule.
func (s *Server) applyRepairConfigUpdate(w http.ResponseWriter, r *http.Request, mode configUpdateMode) {
	var req config.RepairConfig
	body, err := readRequiredJSONBody(r, &req)
	if err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	cfg := config.Get()
	current := cfg.Repair

	if mode == mergeUpdate {
		// Fields the caller did not submit keep their current values. The
		// Repair *bool tri-state is preserved exactly: an absent "repair" key
		// keeps the current pointer (nil included).
		if err := req.PreserveMissingFields(current, body); err != nil {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	// replaceUpdate keeps the decoded document as-is: omitted keys are already
	// at their zero value, and Repair is nil (unset ⇒ defaults to true).

	if req.Enabled {
		if strings.TrimSpace(req.Schedule) == "" {
			http.Error(w, "Schedule is required when repair is enabled", http.StatusBadRequest)
			return
		}
		if _, err := utils.ConvertToJobDef(req.Schedule); err != nil {
			http.Error(w, fmt.Sprintf("Invalid schedule: %v", err), http.StatusBadRequest)
			return
		}
		if req.RecheckInterval != "" {
			if _, err := utils.ParseDuration(req.RecheckInterval); err != nil {
				http.Error(w, fmt.Sprintf("Invalid recheck_interval: %v", err), http.StatusBadRequest)
				return
			}
		}
		if req.Source != "" && req.Source != config.RepairSourceArr && req.Source != config.RepairSourceManaged {
			http.Error(w, "Invalid source (must be 'arr' or 'managed')", http.StatusBadRequest)
			return
		}
	}
	if req.NNTPConnectionPercent < 0 || req.NNTPConnectionPercent > 100 {
		http.Error(w, "Invalid nntp_connection_percent (must be between 0 and 100)", http.StatusBadRequest)
		return
	}

	cfg.Repair = req
	if err := cfg.Save(); err != nil {
		cfg.Repair = current // don't leave the process running a config that isn't on disk
		s.logger.Error().Err(err).Msg("Failed to save repair config")
		http.Error(w, "Failed to save config: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if s.manager != nil {
		if svc := s.manager.Repair(); svc != nil {
			if err := svc.ApplyConfig(); err != nil {
				s.logger.Warn().Err(err).Msg("Failed to apply repair config")
				http.Error(w, "Saved, but failed to apply: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}

	utils.JSONResponse(w, cfg.Repair, http.StatusOK)
}

func (s *Server) handleRepairStatus(w http.ResponseWriter, r *http.Request) {
	svc := s.manager.Repair()
	if svc == nil {
		utils.JSONResponse(w, manager.RepairStatus{}, http.StatusOK)
		return
	}
	utils.JSONResponse(w, svc.Status(), http.StatusOK)
}

// maxOptionalJSONBody bounds how much of a request body is read before it is
// rejected as malformed. It applies to optional and required bodies alike.
const maxOptionalJSONBody = 1 << 20 // 1 MiB

// readBoundedJSONBody reads at most maxOptionalJSONBody bytes of r.Body. It is
// the one place both the optional and the required decoder get their bytes, so
// they cannot drift apart on the limit.
func readBoundedJSONBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	return io.ReadAll(io.LimitReader(r.Body, maxOptionalJSONBody))
}

// decodeOptionalJSONBody decodes an OPTIONAL JSON request body into dst.
//
// An absent, empty, or whitespace-only body is accepted and leaves dst at its
// zero value (these endpoints document "no body ⇒ defaults"). Anything else
// MUST be well-formed JSON: a truncated document such as "{" surfaces from the
// decoder as io.EOF / io.ErrUnexpectedEOF, and the previous
// `err != nil && err != io.EOF` guard let exactly that case fall through to a
// zero-value request. On the repair endpoints a zero-value request does not
// mean "do nothing" — it means "act on EVERY broken entry with the configured
// knobs" (handleFixBroken) or "run a full default sweep" (handleRunRepair), so
// a typo'd body could launch real, potentially destructive work. Reject it.
func decodeOptionalJSONBody(r *http.Request, dst any) error {
	if r.Body == nil || r.ContentLength == 0 {
		return nil
	}
	raw, err := readBoundedJSONBody(r)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(raw)) == "" {
		return nil
	}
	return json.ConfigDefault.Unmarshal(raw, dst)
}

// readRequiredJSONBody is decodeOptionalJSONBody's REQUIRED twin, used by the
// config-writing endpoints. It decodes into dst and also returns the raw bytes,
// which the merge step needs: key PRESENCE (not the decoded value) is what
// separates "leave this field alone" from "clear it", and that information is
// gone once the body has been decoded into a struct.
//
// Unlike the optional decoder, an absent/empty/whitespace-only body is an error
// here: on a config endpoint a zero-value document is not "do nothing", it is
// "replace the whole config with zeros". A present-but-malformed body is
// rejected for the same reason it is on the optional path — it must never be
// silently downgraded to defaults.
func readRequiredJSONBody(r *http.Request, dst any) ([]byte, error) {
	raw, err := readBoundedJSONBody(r)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(raw)) == "" {
		return nil, errors.New("a JSON object is required")
	}
	if err := json.ConfigDefault.Unmarshal(raw, dst); err != nil {
		return nil, err
	}
	return raw, nil
}

func (s *Server) handleRunRepair(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IgnoreLastChecked bool   `json:"ignore_last_checked,omitempty"`
		Force             bool   `json:"force,omitempty"`
		Repair            *bool  `json:"repair,omitempty"`
		Prune             *bool  `json:"prune,omitempty"`
		ArrDelete         *bool  `json:"arr_delete,omitempty"`
		Regrab            *bool  `json:"regrab,omitempty"` // Deprecated: use arr_delete.
		Search            *bool  `json:"search,omitempty"`    // ARR-DELETE sub-action; nil = configured.
		Blocklist         *bool  `json:"blocklist,omitempty"` // ARR-DELETE sub-action; nil = configured.
		AutoRepair        *bool  `json:"auto_repair,omitempty"` // Deprecated: back-compat only.
		UnrestrictLink    bool   `json:"unrestrict_link,omitempty"`
		Protocol          string `json:"protocol,omitempty"`
	}
	if err := decodeOptionalJSONBody(r, &req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	ignoreLastChecked := req.IgnoreLastChecked || req.Force
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("ignore_last_checked"))) {
	case "1", "true", "yes", "on":
		ignoreLastChecked = true
	}
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("force"))) {
	case "1", "true", "yes", "on":
		ignoreLastChecked = true
	}
	repair := req.Repair
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("repair"))) {
	case "1", "true", "yes", "on":
		v := true
		repair = &v
	case "0", "false", "no", "off":
		v := false
		repair = &v
	}
	prune := req.Prune
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("prune"))) {
	case "1", "true", "yes", "on":
		v := true
		prune = &v
	case "0", "false", "no", "off":
		v := false
		prune = &v
	}
	// arr_delete, with `regrab` accepted as the deprecated alias. An explicit
	// arr_delete always wins so a client that sends both is not surprised by the
	// stale key.
	arrDelete := req.ArrDelete
	if arrDelete == nil {
		arrDelete = req.Regrab
	}
	for _, key := range []string{"regrab", "arr_delete"} {
		switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get(key))) {
		case "1", "true", "yes", "on":
			v := true
			arrDelete = &v
		case "0", "false", "no", "off":
			v := false
			arrDelete = &v
		}
	}
	search := req.Search
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("search"))) {
	case "1", "true", "yes", "on":
		v := true
		search = &v
	case "0", "false", "no", "off":
		v := false
		search = &v
	}
	blocklist := req.Blocklist
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("blocklist"))) {
	case "1", "true", "yes", "on":
		v := true
		blocklist = &v
	case "0", "false", "no", "off":
		v := false
		blocklist = &v
	}
	autoRepair := req.AutoRepair
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("auto_repair"))) {
	case "1", "true", "yes", "on":
		v := true
		autoRepair = &v
	case "0", "false", "no", "off":
		v := false
		autoRepair = &v
	}
	// Resolve the one-off per-component override for this run. Explicit
	// REPAIR/PRUNE/ARR-DELETE keys (JSON body or query param) win: if any is present
	// build an explicit override with each absent key defaulting to false.
	// Otherwise fall back to the deprecated auto_repair flag for old clients:
	// true → use the configured knobs (nil override); false → explicit all-false
	// (CHECK-only). With nothing set, use the configured knobs (nil override).
	//
	// search / blocklist are ARR-DELETE sub-actions and deliberately do NOT appear
	// in the switch condition: naming only a sub-action selects no component, so
	// on its own it must not manufacture an override. They ride along on an
	// override that a real component triggered, and are nil (= use the configured
	// knob) otherwise.
	var actions *manager.ManualActions
	switch {
	case repair != nil || prune != nil || arrDelete != nil:
		actions = &manager.ManualActions{
			Repair:    repair != nil && *repair,
			Prune:     prune != nil && *prune,
			ArrDelete: arrDelete != nil && *arrDelete,
			Search:    search,
			Blocklist: blocklist,
		}
	case autoRepair != nil && !*autoRepair:
		actions = &manager.ManualActions{}
	}
	unrestrictLink := req.UnrestrictLink
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("unrestrict_link"))) {
	case "1", "true", "yes", "on":
		unrestrictLink = true
	case "0", "false", "no", "off":
		unrestrictLink = false
	}
	protocolScope := strings.ToLower(strings.TrimSpace(req.Protocol))
	if queryProtocol := strings.TrimSpace(r.URL.Query().Get("protocol")); queryProtocol != "" {
		protocolScope = strings.ToLower(queryProtocol)
	}
	switch protocolScope {
	case "", "all", "both", "torrent", "nzb":
		if protocolScope == "both" {
			protocolScope = "all"
		}
	default:
		http.Error(w, "Invalid protocol; expected all, torrent, or nzb", http.StatusBadRequest)
		return
	}

	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	id, err := svc.RunNow(manager.RepairRunOptions{
		IgnoreLastChecked: ignoreLastChecked,
		Actions:           actions,
		UnrestrictLink:    unrestrictLink,
		ProtocolScope:     protocolScope,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	utils.JSONResponse(w, map[string]string{"run_id": id}, http.StatusOK)
}

func (s *Server) handleStopRepair(w http.ResponseWriter, r *http.Request) {
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	if err := svc.StopRun(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleDebridClients lists the debrid clients the runtime actually holds.
//
// Config listing a provider is not evidence that provider is usable: a client
// whose construction fails is skipped at startup and simply absent afterwards.
// This is the debrid analogue of /api/arrs — without it, "configured" and
// "registered" cannot be told apart from outside.
func (s *Server) handleDebridClients(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, s.manager.RegisteredDebridClients(), http.StatusOK)
}

// handleDebridChain reports the provider chain a torrent add would actually
// walk for one arr, resolved through the same selection the add path uses.
//
// When fallback is enabled the chain should contain every registered client, so
// exhausting one provider moves to the next. A single-provider chain here is
// the failure, and this shows which of the possible causes produced it:
// selected_debrid not matching any registered client, a provider missing from
// the registry, or fallback being off on the runtime arr.
func (s *Server) handleDebridChain(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, s.manager.DiagnoseDebridChain(chi.URLParam(r, "arr")), http.StatusOK)
}

// handleQueueConsistency reconciles queue index membership against a full scan.
//
// Queue.Add rejects a duplicate using a bare index lookup, while every listing
// an arr polls comes from a scan. When those disagree an entry is both
// "already exists" and invisible, which cannot be distinguished from outside:
// probing by re-adding a magnet answers through debrid submission, so the
// result is confounded by provider cache state rather than index state. This
// answers the index question directly, and yields a real count instead of an
// inferred one.
func (s *Server) handleQueueConsistency(w http.ResponseWriter, r *http.Request) {
	report, err := s.manager.Storage().QueueConsistency()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, report, http.StatusOK)
}

// handleQueueKeyState answers index membership and scan visibility for one
// infohash, with no debrid interaction.
func (s *Server) handleQueueKeyState(w http.ResponseWriter, r *http.Request) {
	diagnosis, err := s.manager.Storage().QueueKeyState(chi.URLParam(r, "infohash"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	utils.JSONResponse(w, diagnosis, http.StatusOK)
}

func (s *Server) handleListRepairRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.manager.Storage().ListRepairRuns()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, runs, http.StatusOK)
}

func (s *Server) handleGetRepairRun(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "No run ID provided", http.StatusBadRequest)
		return
	}
	run, err := s.manager.Storage().GetRepairRun(id)
	if err != nil {
		http.Error(w, "Run not found", http.StatusNotFound)
		return
	}
	utils.JSONResponse(w, run, http.StatusOK)
}

func (s *Server) handleClearRepairRuns(w http.ResponseWriter, r *http.Request) {
	if err := s.manager.Storage().ClearRepairRuns(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListEntryHealth(w http.ResponseWriter, r *http.Request) {
	statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
	out := make([]*storage.EntryHealth, 0)
	_ = s.manager.Storage().ForEachEntryHealth(func(state *storage.EntryHealth) error {
		if statusFilter != "" && string(state.Status) != statusFilter {
			return nil
		}
		out = append(out, state)
		return nil
	})
	sort.Slice(out, func(i, j int) bool {
		return out[i].EntryName < out[j].EntryName
	})
	utils.JSONResponse(w, out, http.StatusOK)
}

func (s *Server) handleGetEntryHealth(w http.ResponseWriter, r *http.Request) {
	name := utils.PathUnescape(chi.URLParam(r, "name"))
	if name == "" {
		http.Error(w, "No entry name provided", http.StatusBadRequest)
		return
	}
	state, err := s.manager.Storage().GetEntryHealth(name)
	if err != nil {
		http.Error(w, "Entry health not found", http.StatusNotFound)
		return
	}
	utils.JSONResponse(w, state, http.StatusOK)
}

func (s *Server) handleRecheckMedia(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Arr     string             `json:"arr"`
		MediaID string             `json:"media_id"`
		Fix     bool               `json:"fix"`
		Actions *repairActionsBody `json:"actions,omitempty"`
	}
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.MediaID) == "" {
		http.Error(w, "media_id is required", http.StatusBadRequest)
		return
	}
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	run, err := svc.RecheckMedia(s.manager.Context(), strings.TrimSpace(req.Arr), strings.TrimSpace(req.MediaID), req.Actions.toManager(), resolveLegacyFixFlag(req.Actions, req.Fix))
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		// Returning the run record (when present) gives the caller the
		// failure detail captured in storage as well as the message.
		if run != nil {
			utils.JSONResponse(w, map[string]any{
				"error": err.Error(),
				"run":   run,
			}, status)
			return
		}
		http.Error(w, err.Error(), status)
		return
	}
	utils.JSONResponse(w, run, http.StatusOK)
}

func (s *Server) handleRecheckEntry(w http.ResponseWriter, r *http.Request) {
	name := utils.PathUnescape(chi.URLParam(r, "name"))
	if name == "" {
		http.Error(w, "No entry name provided", http.StatusBadRequest)
		return
	}
	// CHECK-only by default. An optional {"actions": {...}} body selects the
	// component(s) to apply if the entry probes broken; the legacy ?fix=true
	// query still maps to the configured knobs.
	var req struct {
		Actions *repairActionsBody `json:"actions,omitempty"`
	}
	if err := decodeOptionalJSONBody(r, &req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	fix := resolveLegacyFixFlag(req.Actions, r.URL.Query().Get("fix") == "true")
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	state, err := svc.RecheckEntry(s.manager.Context(), name, req.Actions.toManager(), fix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	utils.JSONResponse(w, state, http.StatusOK)
}

// repairActionsBody is the per-component action selection accepted by the
// manual fix endpoints. A nil *repairActionsBody (the "actions" key absent)
// means "not specified" → the manager falls back to the configured
// REPAIR/PRUNE/ARR-DELETE knobs (never force-all). Present-but-all-false is an
// explicit "no components": CHECK-only on the recheck endpoints, and a 400 on
// /api/repair/fix, which has nothing to do without a component. See
// explicitNone.
type repairActionsBody struct {
	Repair bool `json:"repair"`
	Prune  bool `json:"prune"`

	// ArrDelete selects the arr-side component; Regrab is its deprecated alias,
	// accepted so clients written before the rename keep working. Read them
	// through arrDeleteSelected(), never directly.
	ArrDelete bool `json:"arr_delete"`
	Regrab    bool `json:"regrab"` // Deprecated: use ArrDelete.

	// Search / Blocklist override ARR-DELETE's sub-actions for this one request.
	// Both are *bool: absent means "not specified", which uses the configured
	// knob. They are NOT part of explicitNone() — naming only a sub-action
	// selects no component, and must not make an otherwise-empty selection look
	// non-empty, or {"blocklist":true} alone would slip past the explicit-none
	// guard and resolve to the operator's configured components.
	Search    *bool `json:"search,omitempty"`
	Blocklist *bool `json:"blocklist,omitempty"`
}

func (b *repairActionsBody) toManager() *manager.ManualActions {
	if b == nil {
		return nil
	}
	return &manager.ManualActions{
		Repair: b.Repair, Prune: b.Prune, ArrDelete: b.arrDeleteSelected(),
		Search: b.Search, Blocklist: b.Blocklist,
	}
}

// explicitNone reports that the request carried an "actions" object naming NO
// component — an explicit "do nothing". It is deliberately distinct from a nil
// *repairActionsBody ("actions" absent), which means "unspecified" and falls
// back to the configured knobs.
//
// The manager's resolveManualActions cannot tell the two apart (sel.any() is
// false either way) and so honors the legacy fix flag for both. On
// /api/repair/fix — where the manager passes fix=true unconditionally — that
// turned an explicit all-false selection into a run of the operator's
// configured REPAIR/PRUNE/ARR-DELETE knobs: the exact opposite of what was asked,
// and destructive if prune/regrab are on. Handlers must therefore never pass
// fix=true when this is true.
func (b *repairActionsBody) explicitNone() bool {
	return b != nil && !b.Repair && !b.Prune && !b.arrDeleteSelected()
}

// arrDeleteSelected accepts either the current key or the deprecated alias.
func (b *repairActionsBody) arrDeleteSelected() bool {
	return b != nil && (b.ArrDelete || b.Regrab)
}

// resolveLegacyFixFlag decides what to pass as the manager's legacy fix flag.
// An explicit all-false "actions" selection forces it off so the request
// resolves to CHECK-only, matching POST /api/repair/run, where the equivalent
// all-false JSON already yields CHECK-only. "actions" absent leaves the flag
// untouched, preserving the documented "configured knobs" behavior.
func resolveLegacyFixFlag(actions *repairActionsBody, fix bool) bool {
	if actions.explicitNone() {
		return false
	}
	return fix
}

// handleFixBroken acts on currently-broken entries WITHOUT reprobing. Body:
// {"names": ["..."], "actions": {"repair":bool,"prune":bool,"regrab":bool}}.
// Empty/missing names ⇒ act on every broken entry. A specified "actions" runs
// exactly those components (single-component invocation supported, e.g.
// PRUNE-only); omitting "actions" falls back to the configured
// REPAIR/PRUNE/ARR-DELETE knobs. An "actions" object naming NO component is an
// explicit no-op and is rejected with 400 rather than silently promoted to the
// configured knobs.
func (s *Server) handleFixBroken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Names   []string           `json:"names,omitempty"`
		Actions *repairActionsBody `json:"actions,omitempty"`
	}
	// Body is optional: absent ⇒ act on every broken entry with the configured
	// knobs. A PRESENT but malformed body is rejected, never silently defaulted.
	if err := decodeOptionalJSONBody(r, &req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	// An "actions" object that is PRESENT but names no component is an explicit
	// "do nothing". This endpoint only ever acts (it never re-probes) and the
	// manager resolves it with fix=true unconditionally, so passing it through
	// would run the configured knobs — possibly PRUNE/ARR-DELETE — on every broken
	// entry. Refuse it here, with the same message the manager uses when a
	// selection resolves to no components.
	if req.Actions.explicitNone() {
		http.Error(w, "no repair action selected: enable REPAIR, PRUNE, or ARR-DELETE", http.StatusBadRequest)
		return
	}
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	run, err := svc.FixBroken(s.manager.Context(), req.Names, req.Actions.toManager())
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	utils.JSONResponse(w, run, http.StatusOK)
}

func (s *Server) handleClearRepairState(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Statuses []string `json:"statuses"`
	}
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	statuses := make([]storage.HealthStatus, 0, len(req.Statuses))
	for _, raw := range req.Statuses {
		status, ok := parseRepairHealthStatus(raw)
		if !ok {
			http.Error(w, "Invalid repair health status: "+raw, http.StatusBadRequest)
			return
		}
		statuses = append(statuses, status)
	}
	if len(statuses) == 0 {
		http.Error(w, "At least one status is required", http.StatusBadRequest)
		return
	}

	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	result, err := svc.ClearStates(statuses)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	utils.JSONResponse(w, result, http.StatusOK)
}

func parseRepairHealthStatus(raw string) (storage.HealthStatus, bool) {
	switch storage.HealthStatus(strings.ToLower(strings.TrimSpace(raw))) {
	case storage.HealthHealthy:
		return storage.HealthHealthy, true
	case storage.HealthBroken:
		return storage.HealthBroken, true
	case storage.HealthRepairing:
		return storage.HealthRepairing, true
	case storage.HealthStale:
		return storage.HealthStale, true
	case storage.HealthUnknown:
		return storage.HealthUnknown, true
	case storage.HealthUnsupported:
		return storage.HealthUnsupported, true
	default:
		return "", false
	}
}

func (s *Server) handleRefreshAPIToken(w http.ResponseWriter, _ *http.Request) {
	token, err := s.refreshAPIToken()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to refresh API token")
		http.Error(w, "Failed to refresh token: "+err.Error(), http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, map[string]any{
		"token":   token,
		"message": "API token refreshed successfully",
	}, http.StatusOK)
}

func (s *Server) handleUpdateAuth(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username        string `json:"username"`
		Password        string `json:"password"`
		ConfirmPassword string `json:"confirm_password"`
	}
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cfg := config.Get()
	auth := cfg.GetAuth()
	if auth == nil {
		auth = &config.Auth{}
	}

	// Check if trying to disable authentication (both empty)
	if req.Username == "" && req.Password == "" {
		// Disable authentication
		cfg.UseAuth = false
		auth.Username = ""
		auth.Password = ""
		if err := cfg.SaveAuth(auth); err != nil {
			s.logger.Error().Err(err).Msg("Failed to save auth config")
			http.Error(w, "Failed to save authentication settings", http.StatusInternalServerError)
			return
		}
		if err := cfg.Save(); err != nil {
			s.logger.Error().Err(err).Msg("Failed to save config")
			http.Error(w, "Failed to save configuration", http.StatusInternalServerError)
			return
		}

		utils.JSONResponse(w, map[string]string{
			"message": "Authentication disabled successfully",
		}, http.StatusOK)
		return
	}

	// Validate required fields
	if req.Username == "" {
		http.Error(w, "Username is required", http.StatusBadRequest)
		return
	}
	if req.Password == "" {
		http.Error(w, "Password is required", http.StatusBadRequest)
		return
	}
	if req.Password != req.ConfirmPassword {
		http.Error(w, "Passwords do not match", http.StatusBadRequest)
		return
	}

	// Hash the password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to hash password")
		http.Error(w, "Failed to process password", http.StatusInternalServerError)
		return
	}

	// Update auth settings
	auth.Username = req.Username
	auth.Password = string(hashedPassword)
	cfg.UseAuth = true

	// Save auth config
	if err := cfg.SaveAuth(auth); err != nil {
		s.logger.Error().Err(err).Msg("Failed to save auth config")
		http.Error(w, "Failed to save authentication settings", http.StatusInternalServerError)
		return
	}

	// Save main config
	if err := cfg.Save(); err != nil {
		s.logger.Error().Err(err).Msg("Failed to save config")
		http.Error(w, "Failed to save configuration", http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, map[string]string{
		"message": "Authentication settings updated successfully",
	}, http.StatusOK)
}

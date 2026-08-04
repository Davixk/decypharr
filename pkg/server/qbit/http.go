package qbit

import (
	"errors"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func (q *QBit) handleLogin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	cfg := config.Get()
	username := r.FormValue("username")
	password := r.FormValue("password")
	a, err := q.authenticate(getCategory(ctx), username, password)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if cfg.UseAuth {
		cookie := &http.Cookie{
			Name:     "SID",
			Value:    createSID(a.Host, a.Token),
			Path:     "/",
			SameSite: http.SameSiteNoneMode,
		}
		http.SetCookie(w, cookie)
	}
	_, _ = w.Write([]byte("Ok."))
}

func (q *QBit) handleVersion(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte("v4.3.2"))
}

func (q *QBit) handleWebAPIVersion(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte("2.7"))
}

func (q *QBit) handlePreferences(w http.ResponseWriter, r *http.Request) {
	preferences := getAppPreferences()

	preferences.SavePath = q.downloadFolder
	preferences.TempPath = filepath.Join(q.downloadFolder, "temp")

	utils.JSONResponse(w, preferences, http.StatusOK)
}

func (q *QBit) handleBuildInfo(w http.ResponseWriter, r *http.Request) {
	res := BuildInfo{
		Bitness:    64,
		Boost:      "1.75.0",
		Libtorrent: "1.2.11.0",
		Openssl:    "1.1.1i",
		Qt:         "5.15.2",
		Zlib:       "1.2.11",
	}
	utils.JSONResponse(w, res, http.StatusOK)
}

func (q *QBit) handleShutdown(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleTorrentsInfo(w http.ResponseWriter, r *http.Request) {
	//log all url params
	ctx := r.Context()
	category := getCategory(ctx)
	// qBittorrent's "filter" parameter defaults to "all", meaning no filtering.
	// "all" is not a TorrentState, so passing it straight through matched no
	// entry and returned an empty list to any client that sent it explicitly.
	// (strings.Trim(s, "") was also a no-op — an empty cutset trims nothing.)
	state := strings.TrimSpace(r.URL.Query().Get("filter"))
	if strings.EqualFold(state, "all") {
		state = ""
	}
	hashes := getHashes(ctx)

	// Convert hashes to filter function
	torrents := q.manager.Queue().ListFilter(category, config.ProtocolTorrent, storage.TorrentState(state), hashes, "added_on", false)
	qbitTorrents := make([]Torrent, len(torrents))
	for i, t := range torrents {
		qbitTorrents[i] = convertToQBitTorrentTorrent(t)
	}
	utils.JSONResponse(w, qbitTorrents, http.StatusOK)
}

func (q *QBit) handleTorrentsAdd(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Parse form based on content type
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "multipart/form-data") {
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			q.logger.Error().Err(err).Msgf("Error parsing multipart form")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else if strings.Contains(contentType, "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			q.logger.Error().Err(err).Msgf("Error parsing form")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		http.Error(w, "Invalid content type", http.StatusBadRequest)
		return
	}

	cfg := config.Get()
	action := cfg.DefaultDownloadAction
	if strings.ToLower(r.FormValue("sequentialDownload")) == "true" {
		action = config.DownloadActionDownload
	}

	rmTrackerUrls := strings.ToLower(r.FormValue("firstLastPiecePrio")) == "true"

	// Check config setting - if always remove tracker URLs is enabled, force it to true
	if q.alwaysRemoveTrackerURLS {
		rmTrackerUrls = true
	}

	debridName := r.FormValue("debrid")
	category := r.FormValue("category")
	_arr := getArrFromContext(ctx)
	if _arr == nil {
		// Arr is not in context
		_arr = arr.New(category, "", "", false, nil, "", "")
	}
	atleastOne := false

	// Handle magnet URLs
	if urls := r.FormValue("urls"); urls != "" {
		var urlList []string
		for u := range strings.SplitSeq(urls, "\n") {
			urlList = append(urlList, strings.TrimSpace(u))
		}
		for _, url := range urlList {
			if err := q.addMagnet(ctx, url, _arr, debridName, action, cfg.Notifications.CallbackURL, rmTrackerUrls, cfg.SkipMultiSeason); err != nil {
				q.logger.Debug().Msgf("Error adding magnet: %s", err.Error())
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			atleastOne = true
		}
	}

	// Handle torrent files
	if r.MultipartForm != nil && r.MultipartForm.File != nil {
		if files := r.MultipartForm.File["torrents"]; len(files) > 0 {
			for _, fileHeader := range files {
				if err := q.addTorrent(ctx, fileHeader, _arr, debridName, action, cfg.Notifications.CallbackURL, rmTrackerUrls, cfg.SkipMultiSeason); err != nil {
					q.logger.Debug().Err(err).Str("torrent", fileHeader.Filename).Msgf("Error adding torrent")
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				atleastOne = true
			}
		}
	}

	if !atleastOne {
		http.Error(w, "No valid URLs or torrents provided", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleTorrentsDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hashes := getHashes(ctx)

	if len(hashes) == 0 {
		http.Error(w, "No hashes provided", http.StatusBadRequest)
		return
	}
	// deleteFiles is qBittorrent's "also remove the downloaded data", and the
	// *arrs already send it when they abandon a download.
	//
	// IGNORING IT WAS THE PROVIDER-SLOT LEAK. This endpoint passed a nil cleanup
	// unconditionally, so the queue row went and the provider transfer kept
	// running — forever, holding a slot nothing could reclaim, because every
	// release path in decypharr starts from a local entry that no longer
	// existed. Measured on a live account: 94 of 96 active RealDebrid transfers
	// had no local record, 93 still downloading, 67 between 50% and 99%
	// complete. The *arrs' own stalled-download handling is the likely trigger,
	// which means the cleanup was causing the congestion.
	//
	// For a debrid client the provider copy IS the downloaded data, so honouring
	// the flag means releasing the placement. That is not a guess about what the
	// operator wants — it is doing what the caller explicitly asked for.
	_ = r.ParseForm()
	deleteFiles := strings.EqualFold(strings.TrimSpace(r.FormValue("deleteFiles")), "true")

	var cleanup func(*storage.Entry) error
	if deleteFiles {
		cleanup = func(entry *storage.Entry) error {
			// The PLACEMENT only. The main-store row, if one exists, is the
			// library record and is not this endpoint's to remove: the caller
			// asked to drop a queue item, and conflating the two would delete
			// library content during a routine *arr queue removal.
			if err := q.manager.RemoveTorrentPlacements(entry); err != nil {
				// REPORTED, BUT NOT FATAL TO THE DELETE — and this is a
				// judgement, so here is the reasoning.
				//
				// Failing the request leaves the queue row AND the placement,
				// and the *arr retries the same call forever against the same
				// broken condition. Nothing improves and the row becomes
				// undeletable. Completing the delete leaks the placement, which
				// is what this whole fix exists to stop.
				//
				// What breaks the tie is that a leaked placement is now
				// RECOVERABLE: the provider-sourced stall prune finds abandoned
				// transfers from the provider's own active list and needs no
				// local record to act. So the worst case degrades to "an orphan
				// the other sweep reaps", not "an orphan nothing can ever see"
				// — which was the old behaviour, silently, on every delete.
				q.logger.Error().Err(err).
					Str("infohash", entry.InfoHash).
					Str("provider", entry.ActiveProvider).
					Msg("Could not release the provider placement for a deleted queue entry. The queue row is " +
						"removed as requested; the provider copy may still be holding a slot. The " +
						"provider-sourced stall prune reaps transfers like this from the provider's own " +
						"list, so it is recoverable — but repeated occurrences point at a provider or " +
						"credential problem worth fixing.")
			}
			return nil
		}
	}

	for _, hash := range hashes {
		// An entry that is already absent is a satisfied delete, not a failure.
		// This previously matched on the message text containing "not found",
		// which happened to work only because both the storage and store-level
		// sentinels are worded that way — rewording either would silently turn
		// a tolerated absence back into a 500. Match the sentinel instead.
		err := q.manager.Queue().Delete(hash, cleanup)
		if err != nil && !errors.Is(err, storage.ErrEntryNotFound) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleTorrentsPause(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hashes := getHashes(ctx)
	for _, hash := range hashes {
		torrent, err := q.manager.Queue().GetTorrent(hash)
		if err != nil {
			continue
		}
		go q.PauseTorrent(torrent)
	}

	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleTorrentsResume(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hashes := getHashes(ctx)
	for _, hash := range hashes {
		torrent, err := q.manager.Queue().GetTorrent(hash)
		if err != nil {
			continue
		}
		go q.ResumeTorrent(torrent)
	}

	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleTorrentRecheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hashes := getHashes(ctx)
	for _, hash := range hashes {
		torrent, err := q.manager.Queue().GetTorrent(hash)
		if err != nil {
			continue
		}
		go q.RefreshTorrent(torrent)
	}

	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleCategories(w http.ResponseWriter, r *http.Request) {
	var categories = map[string]TorrentCategory{}
	for _, cat := range q.categories {
		path := filepath.Join(q.downloadFolder, cat)
		categories[cat] = TorrentCategory{
			Name:     cat,
			SavePath: path,
		}
	}
	utils.JSONResponse(w, categories, http.StatusOK)
}

func (q *QBit) handleCreateCategory(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Failed to parse form data", http.StatusBadRequest)
		return
	}

	name := r.Form.Get("category")
	if name == "" {
		http.Error(w, "No name provided", http.StatusBadRequest)
		return
	}

	q.categories = append(q.categories, name)

	utils.JSONResponse(w, nil, http.StatusOK)
}

func (q *QBit) handleTorrentProperties(w http.ResponseWriter, r *http.Request) {
	hash := r.URL.Query().Get("hash")
	torrent, err := q.manager.Queue().GetTorrent(hash)
	if err != nil {
		http.Error(w, "Entry not found", http.StatusNotFound)
		return
	}

	properties := q.GetTorrentProperties(torrent)
	utils.JSONResponse(w, properties, http.StatusOK)
}

func (q *QBit) handleTorrentFiles(w http.ResponseWriter, r *http.Request) {
	hash := r.URL.Query().Get("hash")
	torrent, err := q.manager.Queue().GetTorrent(hash)
	if err != nil {
		http.Error(w, "Entry not found", http.StatusNotFound)
		return
	}
	utils.JSONResponse(w, getTorrentFiles(torrent), http.StatusOK)
}

func (q *QBit) handleSetCategory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	category := getCategory(ctx)
	hashes := getHashes(ctx)
	if len(hashes) == 0 {
		http.Error(w, "No hashes provided", http.StatusBadRequest)
		return
	}
	filterFunc := q.manager.Queue().ListFilterFunc("", config.ProtocolTorrent, "", hashes)

	updateFunc := func(t *storage.Entry) bool {
		if t.Category != category {
			t.Category = category
			return true
		}
		return false
	}

	if err := q.manager.Queue().UpdateWhere(filterFunc, updateFunc); err != nil {
		q.logger.Warn().Err(err).Msgf("Error adding torrent")
		http.Error(w, "Failed to update torrents", http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, nil, http.StatusOK)
}

func (q *QBit) handleAddTorrentTags(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Failed to parse form data", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	hashes := getHashes(ctx)
	tags := strings.Split(r.FormValue("tags"), ",")
	for i, tag := range tags {
		tags[i] = strings.TrimSpace(tag)
	}
	torrents := q.manager.Queue().ListFilter("", config.ProtocolTorrent, "", hashes, "", false)
	for _, t := range torrents {
		q.setTorrentTags(t, tags)
	}
	utils.JSONResponse(w, nil, http.StatusOK)
}

func (q *QBit) handleRemoveTorrentTags(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Failed to parse form data", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	hashes := getHashes(ctx)
	tags := strings.Split(r.FormValue("tags"), ",")
	for i, tag := range tags {
		tags[i] = strings.TrimSpace(tag)
	}
	torrents := q.manager.Queue().ListFilter("", config.ProtocolTorrent, "", hashes, "", false)
	for _, torrent := range torrents {
		q.removeTorrentTags(torrent, tags)

	}
	utils.JSONResponse(w, nil, http.StatusOK)
}

func (q *QBit) handleGetTags(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, q.Tags, http.StatusOK)
}

func (q *QBit) handleCreateTags(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Failed to parse form data", http.StatusBadRequest)
		return
	}
	tags := strings.Split(r.FormValue("tags"), ",")
	for i, tag := range tags {
		tags[i] = strings.TrimSpace(tag)
	}
	q.addTags(tags)
	utils.JSONResponse(w, nil, http.StatusOK)
}

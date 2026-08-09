package qbit

import (
	"errors"
	"net"
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

// deleteCaller is the attribution attached to a shim delete.
type deleteCaller struct {
	agent string // User-Agent, trimmed
	addr  string // remote address, host only
	auth  string // how the caller authenticated — NEVER the credential itself
}

// describeDeleteCaller identifies who asked for a delete, for attribution only.
//
// 🔴 IT MUST NEVER LOG A CREDENTIAL. An API key or session cookie in a log line
// is a leaked secret that outlives the investigation it was added for, gets
// copied into bug reports, and survives in log shipping nobody audits. So `auth`
// records the SCHEME that was used — apikey / basic / cookie / none — and never
// a byte of the value.
//
// That is enough for the question being asked. Distinguishing "sonarr's own
// client" from "the operator's resolver script" is answered by the User-Agent
// and the source address; knowing WHICH key was presented adds nothing to it.
func describeDeleteCaller(r *http.Request) deleteCaller {
	c := deleteCaller{
		agent: strings.TrimSpace(r.UserAgent()),
		addr:  r.RemoteAddr,
		auth:  "none",
	}
	if c.agent == "" {
		c.agent = "(no user-agent)"
	}
	// Host only: the ephemeral port changes per connection and identifies
	// nothing, while making every line look distinct.
	if host, _, err := net.SplitHostPort(c.addr); err == nil && host != "" {
		c.addr = host
	}
	switch {
	case r.Header.Get("X-Api-Key") != "":
		c.auth = "apikey"
	case strings.HasPrefix(strings.ToLower(r.Header.Get("Authorization")), "basic "):
		c.auth = "basic"
	case r.Header.Get("Authorization") != "":
		c.auth = "authorization-header"
	default:
		if _, err := r.Cookie("SID"); err == nil {
			c.auth = "cookie"
		}
	}
	return c
}

func (q *QBit) handleTorrentsDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hashes := getHashes(ctx)

	if len(hashes) == 0 {
		http.Error(w, "No hashes provided", http.StatusBadRequest)
		return
	}
	// 🔴 THIS ENDPOINT MUST NEVER RELEASE A PROVIDER COPY, WHATEVER deleteFiles SAYS.
	//
	// It used to. The reasoning looked sound — for a debrid client the provider
	// copy IS the downloaded data, so honouring qBittorrent's "also remove the
	// downloaded data" seemed like simply doing what the caller asked. It was
	// wrong, and it destroyed the library.
	//
	// AN *ARR'S ROUTINE POST-IMPORT CLEANUP SENDS deleteFiles=true. By then it has
	// already imported the release — as a symlink pointing INTO the mount. So the
	// "downloaded data" the caller means (its own scratch copy) and the bytes the
	// library now depends on are THE SAME BYTES. Releasing the provider copy
	// deletes the file the library was just built from. Measured on this deployment:
	// 2,592 provider-copy releases in 24h, MissingFromDisk reaps climbing 56/day to
	// 8,302/day, each one re-searched and re-grabbed. Sonarr imports, one second
	// later its remove-completed fires, and the episode it just filed is gone.
	//
	// A guard existed and did not help, which is worth stating exactly:
	// removeProviderPlacementIfUnreferenced asks "does another decypharr ROW share
	// this debrid torrent?" It knows nothing about library symlinks. A normal
	// imported release has one row, the owner is skipped by construction, so the
	// scan finds nothing and the release proceeds. The row was guarded; the CONTENT
	// the symlink resolves to never was.
	//
	// Upstream never did this: its handler parses hashes only and passes a nil
	// cleanup. That is not an oversight on their part, it is the correct semantics
	// for a client whose data lives behind a mount — and the SAB shim here already
	// agreed with upstream, so the two shims disagreeing was itself the tell.
	//
	// The slot leak that motivated the original change is real but belongs
	// elsewhere: the provider-sourced stall prune reaps abandoned transfers from
	// the provider's OWN active list with no local record needed. That did not
	// exist when this was written; it does now, so this path is redundant as well
	// as destructive. Provider-copy release belongs to repair/prune and explicit
	// operator actions — every one of which knows whether content is still needed.
	//
	// deleteFiles is still PARSED and LOGGED. Knowing what callers send is how the
	// above was diagnosed, and it costs nothing to keep.
	_ = r.ParseForm()
	rawDeleteFiles := r.FormValue("deleteFiles")
	deleteFiles := strings.EqualFold(strings.TrimSpace(rawDeleteFiles), "true")
	caller := describeDeleteCaller(r)

	for _, hash := range hashes {
		// LOGGED ON SUCCESS, not just on failure.
		//
		// This endpoint used to be completely silent when it worked, and so is
		// the arr-side queue cleanup that drives most of its traffic. That made
		// an entire class of disappearance unattributable: an investigation
		// traced 130 in-flight downloads and found 62 that simply stopped
		// having log lines, while the provider went on downloading them. They
		// may well have been deleted right here — there was no way to tell.
		//
		// A removal that leaves no trace cannot be reasoned about afterwards,
		// and three separate root-cause theories died for want of exactly this
		// line. It records who was removed and whether the provider copy went
		// with them.
		// NIL CLEANUP, UNCONDITIONALLY — matching upstream. See the block above for
		// why deleteFiles must not reach the provider from here.
		err := q.manager.Queue().Delete(hash, nil)
		switch {
		case err == nil:
			// ⚠️ THE ATTRIBUTION IS THE POINT, NOT THE OUTCOME.
			//
			// The flag no longer changes what this endpoint DOES, so it would be
			// easy to argue for dropping it from the line. Keep it. Reading these
			// fields is how the library destruction was diagnosed at all: the
			// 170-false / 108-true split over four hours is what made the parameter
			// the subject, and the caller fields are what identified an operator
			// cron rather than an *arr as the source of the false ones. The next
			// question about this endpoint will be answered the same way.
			//
			// raw_delete_files is kept ALONGSIDE the parsed bool deliberately: an
			// absent parameter and an explicit "false" are indistinguishable once
			// parsed, and they implicate completely different code on the caller's
			// side.
			ev := q.logger.Info().
				Str("infohash", hash).
				Bool("delete_files", deleteFiles).
				Str("raw_delete_files", rawDeleteFiles).
				Str("caller", caller.agent).
				Str("caller_addr", caller.addr).
				Str("auth", caller.auth)
			if category := r.FormValue("category"); category != "" {
				ev = ev.Str("category", category)
			}
			ev.Msg("Queue entry deleted via the qBittorrent API. The provider copy is KEPT regardless of " +
				"delete_files — releasing it here would delete content the library has already imported")
		case errors.Is(err, storage.ErrEntryNotFound):
			// An entry that is already absent is a satisfied delete, not a
			// failure. This previously matched on the message text containing
			// "not found", which happened to work only because both the storage
			// and store-level sentinels are worded that way — rewording either
			// would silently turn a tolerated absence back into a 500. Match the
			// sentinel instead.
			q.logger.Debug().Str("infohash", hash).Msg("Delete requested for an entry that is already absent")
		default:
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

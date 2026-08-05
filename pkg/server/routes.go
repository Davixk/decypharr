package server

import (
	"io/fs"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (s *Server) WebRoutes() http.Handler {
	r := chi.NewRouter()

	// Apply setup redirect middleware globally
	r.Use(s.setupRedirectMiddleware)

	// Static assets - always public
	staticFS, _ := fs.Sub(assetsEmbed, "assets/build")
	imagesFS, _ := fs.Sub(imagesEmbed, "assets/images")
	r.Handle("/assets/*", http.StripPrefix(s.urlBase+"assets/", http.FileServer(http.FS(staticFS))))
	r.Handle("/images/*", http.StripPrefix(s.urlBase+"images/", http.FileServer(http.FS(imagesFS))))

	// Public routes - no auth needed
	r.Get("/version", s.handleGetVersion)
	r.Get("/login", s.LoginHandler)
	r.Post("/login", s.LoginHandler)
	r.Get("/register", s.RegisterHandler)
	r.Post("/register", s.RegisterHandler)
	r.Post("/skip-auth", s.skipAuthHandler)

	// Setup wizard - public, no auth required
	r.Get("/setup", s.SetupHandler)
	r.Post("/api/setup/complete", s.setupCompleteHandler)

	// Protected routes - require auth
	r.Group(func(r chi.Router) {
		r.Use(s.authMiddleware)
		// Web pages
		r.Get("/", s.IndexHandler)
		r.Get("/browse", s.BrowseHandler)
		r.Get("/download", s.DownloadHandler)
		r.Get("/repair", s.RepairHandler)
		r.Get("/stats", s.StatsHandler)
		r.Get("/settings", s.ConfigHandler)

		// API routes
		r.Route("/api", func(r chi.Router) {
			// Arr management
			r.Get("/arrs", s.handleGetArrs)
			r.Post("/add", s.handleAddContent)

			// Repair / health-checker operations
			r.Get("/repair/config", s.handleGetRepairConfig)
			// PATCH = partial update (absent keys preserved).
			// PUT   = full replacement (absent keys revert to their zero value).
			r.Patch("/repair/config", s.handlePatchRepairConfig)
			r.Put("/repair/config", s.handleReplaceRepairConfig)
			r.Get("/repair/status", s.handleRepairStatus)
			r.Post("/repair/run", s.handleRunRepair)
			r.Post("/repair/stop", s.handleStopRepair)
			r.Post("/repair/recheck/media", s.handleRecheckMedia)
			r.Post("/repair/fix", s.handleFixBroken)
			r.Post("/repair/clear-state", s.handleClearRepairState)
			r.Get("/repair/runs", s.handleListRepairRuns)
			r.Get("/repair/runs/{id}", s.handleGetRepairRun)
			r.Delete("/repair/runs", s.handleClearRepairRuns)
			r.Get("/repair/health", s.handleListEntryHealth)
			r.Get("/repair/health/{name}", s.handleGetEntryHealth)
			r.Post("/repair/health/{name}/check", s.handleRecheckEntry)

			// Queue diagnostics -- read-only. Answers index membership without
			// going through debrid submission, which otherwise confounds the
			// answer with provider cache state.
			r.Get("/queue/consistency", s.handleQueueConsistency)
			r.Get("/queue/consistency/{infohash}", s.handleQueueKeyState)

			// Debrid diagnostics -- read-only. Distinguishes "configured" from
			// "registered", and shows the provider chain an add would really
			// walk, resolved by the same code the add path uses.
			r.Get("/debrids", s.handleDebridClients)
			r.Get("/debrids/chain/{arr}", s.handleDebridChain)

			// Full provider-vs-local reconcile dump -- READ-ONLY, and the
			// complete list rather than the 50-item sample in
			// /api/stats' provider_divergence. Expensive (one full enumeration
			// per provider plus a scan of both stores), so it is triggered by an
			// operator, never by a sweep. It changes NO state: decypharr must
			// never auto-prune unclaimed provider items.
			r.Get("/debrids/unclaimed", s.handleProviderUnclaimedDump)

			// Torrent management
			r.Get("/torrents", s.handleGetTorrents)
			r.Delete("/torrents/{category}/{hash}", s.handleDeleteTorrent)
			r.Delete("/torrents", s.handleDeleteTorrents) // Fixed trailing slash

			// PRUNE IS NOT DELETE, and they are separate routes so that stays
			// obvious. DELETE removes the entry (optionally from the provider
			// too) and tells the *arr nothing, leaving it holding a queue row
			// for a download that no longer exists. PRUNE releases the provider
			// slot and then FAILS the entry, so the arr sees a failed download
			// and re-searches — the same pipeline the automatic stall prune
			// uses, not a parallel implementation of it.
			r.Post("/torrents/{hash}/prune", s.handlePruneTorrent)
			r.Post("/torrents/prune", s.handlePruneTorrents)

			// Browse - WebDAV-style hierarchical file browser
			r.Route("/browse", func(r chi.Router) {
				// Hierarchical browse endpoints
				r.Get("/", s.handleBrowseMount)                                    // Mount: groups (__all__, __bad__, etc.)
				r.Get("/{group}", s.handleBrowseGroup)                             // Group: torrents
				r.Get("/{group}/{subgroup}/{torrent}", s.handleBrowseTorrentFiles) // Torrent files (with subgroup)
				r.Get("/{group}/{torrent}", s.handleBrowseTorrentFiles)            // Torrent files (without subgroup) - This route needs to come after the subgroup route

				// Torrent operations
				r.Delete("/torrents/{id}", s.handleDeleteBrowseTorrent)
				r.Delete("/torrents/batch", s.handleBatchDeleteBrowseTorrents)

				// File download
				r.Get("/download/{torrent}/{file}", s.handleDownloadFile)
			})

			// Config/Auth
			r.Get("/config", s.handleGetConfig)
			// PATCH = partial update (absent keys preserved).
			// PUT   = full replacement (absent keys revert to their zero value).
			r.Patch("/config", s.handlePatchConfig)
			r.Put("/config", s.handleReplaceConfig)
			r.Post("/mount/cache/cleanup", s.handleRunMountCacheCleanup)
			r.Post("/mount/cache/purge", s.handlePurgeMountCache)
			r.Post("/refresh-token", s.handleRefreshAPIToken)
			r.Post("/update-auth", s.handleUpdateAuth)
		})
	})

	return r
}

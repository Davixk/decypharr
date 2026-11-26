package server

import (
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Provider placement endpoints.
//
// These exist for moving a library off a provider without deleting anything the
// library reads. Lookups resolve the infohash case-tolerantly (the exact key
// first, then lowercase, then a legacy variant), so Sonarr/Radarr's uppercase
// downloadId finds the row. Until the case-twin merge has run, one hash can
// still have two rows; the report's `infohash` names the row it describes and
// `case_twins` names the others, which are addressable by their exact key.

// handleGetPlacements serves GET /api/torrents/{hash}/placements.
func (s *Server) handleGetPlacements(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	if hash == "" {
		http.Error(w, "No hash provided", http.StatusBadRequest)
		return
	}
	report, twins, err := s.manager.EntryPlacements(hash)
	if err != nil {
		writePlacementLookupError(w, hash, twins, err)
		return
	}
	utils.JSONResponse(w, report, http.StatusOK)
}

// handleDropPlacement serves DELETE /api/torrents/{hash}/placements/{provider}.
// On success it answers with the entry's placements as they now stand.
func (s *Server) handleDropPlacement(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	provider := strings.TrimSpace(utils.PathUnescape(chi.URLParam(r, "provider")))
	if hash == "" || provider == "" {
		http.Error(w, "Hash and provider are required", http.StatusBadRequest)
		return
	}
	if err := s.manager.DropPlacement(hash, provider); err != nil {
		switch {
		case errors.Is(err, storage.ErrEntryNotFound):
			_, twins, _ := s.manager.EntryPlacements(hash)
			writePlacementLookupError(w, hash, twins, err)
		case errors.Is(err, storage.ErrPlacementNotFound):
			http.Error(w, err.Error(), http.StatusNotFound)
		case errors.Is(err, manager.ErrPlacementIsActive),
			errors.Is(err, manager.ErrLastPlacement),
			errors.Is(err, storage.ErrPlacementNotCompleted),
			errors.Is(err, storage.ErrStaleEntryGeneration):
			http.Error(w, err.Error(), http.StatusConflict)
		default:
			s.logger.Error().Err(err).Str("infohash", hash).Str("provider", provider).Msg("Failed to drop placement")
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	s.logger.Info().Str("infohash", hash).Str("provider", provider).Msg("Dropped a non-active provider placement via the API")
	report, _, err := s.manager.EntryPlacements(hash)
	if err != nil {
		utils.JSONResponse(w, map[string]any{"success": true}, http.StatusOK)
		return
	}
	utils.JSONResponse(w, report, http.StatusOK)
}

// handleSwitchProvider serves POST /api/torrents/{hash}/switch.
//
// Body: {"target": "<configured provider name>", "keep_old": false}.
// Answers 202 with the job; poll GET /api/migrations/{id}. The switch completes
// only for content the target already has cached — see Manager.SwitchTorrent.
func (s *Server) handleSwitchProvider(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	if hash == "" {
		http.Error(w, "No hash provided", http.StatusBadRequest)
		return
	}
	var req struct {
		Target  string `json:"target"`
		KeepOld bool   `json:"keep_old"`
	}
	if err := decodeOptionalJSONBody(r, &req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Target = strings.TrimSpace(req.Target)
	if req.Target == "" {
		http.Error(w, "target is required", http.StatusBadRequest)
		return
	}
	// The manager's own context, not the request's: the move outlives this
	// request by design, and a client hanging up must not abandon it halfway.
	job, err := s.manager.SwitchTorrent(s.manager.Context(), hash, req.Target, req.KeepOld, false)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrEntryNotFound):
			_, twins, _ := s.manager.EntryPlacements(hash)
			writePlacementLookupError(w, hash, twins, err)
		case errors.Is(err, storage.ErrAlreadyOnDebrid):
			http.Error(w, err.Error(), http.StatusConflict)
		case errors.Is(err, manager.ErrSwitchTargetUnknown), errors.Is(err, manager.ErrSwitchUnsupported):
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	utils.JSONResponse(w, job, http.StatusAccepted)
}

// handleGetMigration serves GET /api/migrations/{id}.
func (s *Server) handleGetMigration(w http.ResponseWriter, r *http.Request) {
	job, err := s.manager.GetMigrationJob(chi.URLParam(r, "id"))
	if err != nil {
		if errors.Is(err, manager.ErrMigrationJobNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, job, http.StatusOK)
}

// handleMergeCaseTwins serves POST /api/maintenance/merge-case-twins.
//
// dry_run DEFAULTS TO TRUE: a bare POST only reports. A real run requires an
// explicit dry_run=false (also 0/no/off). Anything unrecognised is refused
// rather than read as either, so a typo cannot start a real run. The real
// run's report names its journal (journal_name); POST
// /api/maintenance/merge-case-twins/restore undoes it from there, fenced.
// It never contacts a provider.
func (s *Server) handleMergeCaseTwins(w http.ResponseWriter, r *http.Request) {
	dryRun, ok := parseMergeDryRun(r.URL.Query().Get("dry_run"))
	if !ok {
		http.Error(w, "dry_run must be true or false", http.StatusBadRequest)
		return
	}
	if !dryRun {
		s.logger.Warn().Msg("Case-twin merge started via the API (real run)")
	}
	report, err := s.manager.MergeCaseTwins(dryRun)
	if err != nil {
		if errors.Is(err, storage.ErrCaseTwinMergeRunning) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		s.logger.Error().Err(err).Bool("dry_run", dryRun).Msg("Case-twin merge failed")
		utils.JSONResponse(w, map[string]any{"error": err.Error(), "report": report}, http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, report, http.StatusOK)
}

// caseTwinRestorer is what the restore route needs. *manager.Manager satisfies
// it in production; tests drive the same code path with a *storage.Storage.
type caseTwinRestorer interface {
	RestoreCaseTwinMergeJournal(journal string, opts storage.CaseTwinRestoreOptions) (*storage.CaseTwinRestoreReport, error)
}

// handleRestoreCaseTwins serves POST /api/maintenance/merge-case-twins/restore.
func (s *Server) handleRestoreCaseTwins(w http.ResponseWriter, r *http.Request) {
	serveCaseTwinRestore(w, r, s.manager, s.logger)
}

// serveCaseTwinRestore undoes a case-twin merge from its journal.
//
// Body: {"journal": "<file name>", "infohash": "<optional>", "dry_run": true, "force": false}.
//   - journal is the bare FILE NAME the merge reported as journal_name
//     (case-twin-merge-*.jsonl, inside the database directory). A path,
//     separator or ".." is refused with 400.
//   - dry_run DEFAULTS TO TRUE, as the merge does: it reports would_restore /
//     would_refuse per group and writes nothing. A real restore needs an
//     explicit false.
//   - a group whose merged row has been written since the merge, or whose
//     journal predates the version fence, is refused; force=true overrides.
//
// It never contacts a provider.
func serveCaseTwinRestore(w http.ResponseWriter, r *http.Request, restorer caseTwinRestorer, logger zerolog.Logger) {
	var req struct {
		Journal  string `json:"journal"`
		InfoHash string `json:"infohash"`
		DryRun   *bool  `json:"dry_run"`
		Force    bool   `json:"force"`
	}
	if err := decodeOptionalJSONBody(r, &req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	opts := storage.CaseTwinRestoreOptions{
		InfoHash: strings.TrimSpace(req.InfoHash),
		DryRun:   req.DryRun == nil || *req.DryRun,
		Force:    req.Force,
	}
	if !opts.DryRun {
		logger.Warn().Str("journal", req.Journal).Str("infohash", opts.InfoHash).Bool("force", opts.Force).
			Msg("Case-twin journal restore started via the API (real run)")
	}
	report, err := restorer.RestoreCaseTwinMergeJournal(req.Journal, opts)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrInvalidCaseTwinJournal):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case errors.Is(err, os.ErrNotExist):
			http.Error(w, err.Error(), http.StatusNotFound)
		case errors.Is(err, storage.ErrCaseTwinMergeRunning):
			http.Error(w, err.Error(), http.StatusConflict)
		case errors.Is(err, storage.ErrEntryNotFound):
			utils.JSONResponse(w, map[string]any{"error": err.Error(), "report": report}, http.StatusNotFound)
		default:
			logger.Error().Err(err).Str("journal", req.Journal).Msg("Case-twin journal restore failed")
			utils.JSONResponse(w, map[string]any{"error": err.Error(), "report": report}, http.StatusInternalServerError)
		}
		return
	}
	utils.JSONResponse(w, report, http.StatusOK)
}

// parseMergeDryRun reads dry_run, defaulting to TRUE. Only an explicit false
// selects a real run; an unrecognised value is rejected (ok=false).
func parseMergeDryRun(raw string) (dryRun, ok bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "1", "true", "yes", "on":
		return true, true
	case "0", "false", "no", "off":
		return false, true
	default:
		return true, false
	}
}

// writePlacementLookupError answers a lookup that resolved to no row, naming any
// row a case-insensitive scan still found so the caller can retry with its key.
func writePlacementLookupError(w http.ResponseWriter, hash string, twins []manager.CaseTwin, err error) {
	if errors.Is(err, storage.ErrEntryNotFound) {
		utils.JSONResponse(w, map[string]any{
			"error":      "no entry has this infohash in any case",
			"infohash":   hash,
			"case_twins": twins,
		}, http.StatusNotFound)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

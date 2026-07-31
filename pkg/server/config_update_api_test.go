package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	json "github.com/bytedance/sonic"

	"github.com/sirrobot01/decypharr/internal/config"
)

// apiTestConfigJSON mirrors the production shape that was lost: two configured
// providers with api keys, one with an explicit download_uncached=false.
const apiTestConfigJSON = `{
  "log_level": "info",
  "download_folder": "/downloads",
  "debrids": [
    {"name": "rd", "provider": "realdebrid", "api_key": "rd-key", "download_uncached": false},
    {"name": "tb", "provider": "torbox", "api_key": "tb-key", "download_uncached": true}
  ],
  "arrs": [{"name": "radarr", "host": "http://radarr:7878", "token": "tok"}]
}`

// apiTestNestedConfigJSON adds a repair block with every destructive-action
// safeguard set, for the nested-merge tests below.
const apiTestNestedConfigJSON = `{
  "log_level": "info",
  "download_folder": "/downloads",
  "debrids": [{"name": "rd", "provider": "realdebrid", "api_key": "rd-key"}],
  "repair": {
    "enabled": true,
    "source": "managed",
    "schedule": "0 3 * * *",
    "stop_schedule": "06:00",
    "workers": 3,
    "max_deletions_per_run": 5,
    "repair": false,
    "prune": true,
    "regrab": true,
    "arrs": ["sonarr", "radarr"]
  }
}`

func setupConfigAPITestWith(t *testing.T, fixture string) string {
	t.Helper()
	config.Reset()
	t.Cleanup(config.Reset)
	dir := t.TempDir()
	config.SetConfigPath(dir)
	cfgFile := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgFile, []byte(fixture), 0644); err != nil {
		t.Fatalf("write initial config: %v", err)
	}
	return cfgFile
}

func setupConfigAPITest(t *testing.T) string {
	t.Helper()
	cfgFile := setupConfigAPITestWith(t, apiTestConfigJSON)
	if cfg := config.Get(); len(cfg.Debrids) != 2 {
		t.Fatalf("fixture did not load: %+v", cfg.Debrids)
	}
	return cfgFile
}

// patchConfigUpdate exercises PATCH /api/config — the partial update. A bare
// Server is enough: the tested paths only need config.Get() and, when a cold
// field changed, the (nil-safe) restart goroutine.
func patchConfigUpdate(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	s := &Server{}
	req := httptest.NewRequest(http.MethodPatch, "/api/config", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.handlePatchConfig(rec, req)
	return rec
}

// putConfigUpdate exercises PUT /api/config — the full replacement.
func putConfigUpdate(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	s := &Server{}
	req := httptest.NewRequest(http.MethodPut, "/api/config", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleReplaceConfig(rec, req)
	return rec
}

func readSavedConfig(t *testing.T, cfgFile string) config.Config {
	t.Helper()
	data, err := os.ReadFile(cfgFile)
	if err != nil {
		t.Fatalf("read saved config: %v", err)
	}
	var saved config.Config
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatalf("unmarshal saved config: %v", err)
	}
	return saved
}

// TestHandleUpdateConfigPartialPatchPreservesSections reproduces the production
// incident: a PATCH without the "debrids" key must not wipe the configured
// providers (api keys included) from disk.
func TestHandleUpdateConfigPartialPatchPreservesSections(t *testing.T) {
	cfgFile := setupConfigAPITest(t)

	rec := patchConfigUpdate(t, `{"log_level":"debug"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	saved := readSavedConfig(t, cfgFile)
	if saved.LogLevel != "debug" {
		t.Fatalf("submitted log_level not applied: %q", saved.LogLevel)
	}
	if len(saved.Debrids) != 2 {
		t.Fatalf("partial PATCH wiped debrid providers: %+v", saved.Debrids)
	}
	if saved.Debrids[0].APIKey != "rd-key" || saved.Debrids[1].APIKey != "tb-key" {
		t.Fatalf("api keys lost: %+v", saved.Debrids)
	}
	if saved.Debrids[0].DownloadUncached == nil || *saved.Debrids[0].DownloadUncached {
		t.Fatalf("explicit download_uncached=false lost: %+v", saved.Debrids[0].DownloadUncached)
	}
	if saved.Debrids[1].DownloadUncached == nil || !*saved.Debrids[1].DownloadUncached {
		t.Fatalf("explicit download_uncached=true lost: %+v", saved.Debrids[1].DownloadUncached)
	}
	if len(saved.Arrs) != 1 || saved.Arrs[0].Token != "tok" {
		t.Fatalf("partial PATCH wiped arrs: %+v", saved.Arrs)
	}
	if saved.DownloadFolder != "/downloads" {
		t.Fatalf("partial PATCH wiped download_folder: %q", saved.DownloadFolder)
	}
}

// TestHandleUpdateConfigExplicitEmptyStillClears: an explicitly submitted empty
// section is a real instruction and must clear, exactly as before the merge
// fix.
func TestHandleUpdateConfigExplicitEmptyStillClears(t *testing.T) {
	cfgFile := setupConfigAPITest(t)

	rec := patchConfigUpdate(t, `{"debrids":[]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	saved := readSavedConfig(t, cfgFile)
	if len(saved.Debrids) != 0 {
		t.Fatalf("explicit empty debrids did not clear: %+v", saved.Debrids)
	}
	if len(saved.Arrs) != 1 {
		t.Fatalf("omitted arrs must survive an explicit debrids clear: %+v", saved.Arrs)
	}
}

// TestHandleUpdateConfigFullPatchOverwrites: a full-config PATCH (every key the
// web UI sends) applies wholesale — the merge has nothing to preserve.
func TestHandleUpdateConfigFullPatchOverwrites(t *testing.T) {
	cfgFile := setupConfigAPITest(t)

	body := `{
		"log_level": "trace",
		"url_base": "/",
		"bind_address": "127.0.0.1",
		"app_url": "",
		"port": "9999",
		"allowed_file_types": ["mkv"],
		"min_file_size": "",
		"max_file_size": "",
		"remove_stalled_after": "10m",
		"nzb_user_agent": "",
		"download_folder": "/new-downloads",
		"refresh_interval": "30s",
		"default_download_action": "symlink",
		"max_active_downloads": 7,
		"skip_pre_cache": false,
		"always_rm_tracker_urls": false,
		"folder_naming": "",
		"disable_webdav": false,
		"refresh_dirs": "",
		"custom_folders": {},
		"debrids": [{"name": "new", "provider": "realdebrid", "api_key": "new-key", "download_uncached": true}],
		"arrs": [],
		"queue_cleanup": {"rules": []},
		"mount": {"type": "none", "mount_path": "/mnt"},
		"usenet": {"providers": []},
		"notifications": {"enabled": false},
		"repair": {"enabled": false}
	}`
	rec := patchConfigUpdate(t, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	saved := readSavedConfig(t, cfgFile)
	if len(saved.Debrids) != 1 || saved.Debrids[0].APIKey != "new-key" {
		t.Fatalf("full PATCH did not replace debrids wholesale: %+v", saved.Debrids)
	}
	if saved.Debrids[0].DownloadUncached == nil || !*saved.Debrids[0].DownloadUncached {
		t.Fatalf("submitted download_uncached=true lost: %+v", saved.Debrids[0].DownloadUncached)
	}
	if len(saved.Arrs) != 0 {
		t.Fatalf("full PATCH with empty arrs did not clear them: %+v", saved.Arrs)
	}
	if saved.DownloadFolder != "/new-downloads" || saved.LogLevel != "trace" {
		t.Fatalf("full PATCH fields not applied: folder=%q level=%q", saved.DownloadFolder, saved.LogLevel)
	}
}

// TestHandleUpdateConfigNestedPartialPatchPreservesSafetyKnobs is the
// end-to-end regression for the nested half of the wipe. The merge used to run
// at the TOP LEVEL only, so an update carrying {"repair":{"enabled":true}}
// replaced the whole repair block on disk — zeroing max_deletions_per_run (the
// destructive-action cap), stop_schedule, prune and regrab — and answered 200.
//
// The body also changes log_level, a cold field, so the handler takes the
// restart branch and never touches the (nil) manager.
func TestHandleUpdateConfigNestedPartialPatchPreservesSafetyKnobs(t *testing.T) {
	cfgFile := setupConfigAPITestWith(t, apiTestNestedConfigJSON)
	if got := config.Get().Repair.MaxDeletionsPerRun; got != 5 {
		t.Fatalf("fixture did not load: max_deletions_per_run = %d", got)
	}

	rec := patchConfigUpdate(t, `{"log_level":"debug","repair":{"enabled":true}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	saved := readSavedConfig(t, cfgFile)
	if saved.LogLevel != "debug" || !saved.Repair.Enabled {
		t.Fatalf("submitted values not applied: %+v", saved.Repair)
	}
	if saved.Repair.MaxDeletionsPerRun != 5 {
		t.Fatalf("nested partial PATCH wiped max_deletions_per_run: %d", saved.Repair.MaxDeletionsPerRun)
	}
	if saved.Repair.StopSchedule != "06:00" {
		t.Fatalf("nested partial PATCH wiped stop_schedule: %q", saved.Repair.StopSchedule)
	}
	if !saved.Repair.Prune || !saved.Repair.ArrDeleteEnabled() {
		t.Fatalf("nested partial PATCH wiped prune/arr_delete: %+v", saved.Repair)
	}
	if saved.Repair.Schedule != "0 3 * * *" || saved.Repair.Source != config.RepairSourceManaged || saved.Repair.Workers != 3 {
		t.Fatalf("nested partial PATCH wiped scheduling fields: %+v", saved.Repair)
	}
	if saved.Repair.Repair == nil || *saved.Repair.Repair {
		t.Fatalf("nested partial PATCH lost the repair:false tri-state: %v", saved.Repair.Repair)
	}
	if len(saved.Repair.Arrs) != 2 {
		t.Fatalf("nested partial PATCH wiped repair.arrs: %+v", saved.Repair.Arrs)
	}
	if len(saved.Debrids) != 1 || saved.Debrids[0].APIKey != "rd-key" {
		t.Fatalf("omitted top-level sections were not preserved: %+v", saved.Debrids)
	}
}

// TestHandleUpdateConfigNestedExplicitValuesStillApply: within a submitted
// section, an explicit zero/false/empty is a real instruction — otherwise the
// operator could never clear the cap or turn a knob off through this endpoint.
// An explicitly submitted array still replaces the stored one wholesale.
func TestHandleUpdateConfigNestedExplicitValuesStillApply(t *testing.T) {
	cfgFile := setupConfigAPITestWith(t, apiTestNestedConfigJSON)

	body := `{"log_level":"debug","repair":{"max_deletions_per_run":-1,"stop_schedule":"","prune":false,"regrab":false,"repair":true,"arrs":["radarr"]}}`
	rec := patchConfigUpdate(t, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	saved := readSavedConfig(t, cfgFile)
	if saved.Repair.MaxDeletionsPerRun != -1 {
		t.Fatalf("explicit max_deletions_per_run:-1 not applied: %d", saved.Repair.MaxDeletionsPerRun)
	}
	if saved.Repair.StopSchedule != "" {
		t.Fatalf("explicit stop_schedule:\"\" not applied: %q", saved.Repair.StopSchedule)
	}
	if saved.Repair.Prune || saved.Repair.ArrDeleteEnabled() {
		t.Fatalf("explicit prune/arr_delete:false not applied: %+v", saved.Repair)
	}
	if saved.Repair.Repair == nil || !*saved.Repair.Repair {
		t.Fatalf("explicit repair:true not applied: %v", saved.Repair.Repair)
	}
	if len(saved.Repair.Arrs) != 1 || saved.Repair.Arrs[0] != "radarr" {
		t.Fatalf("explicitly submitted repair.arrs did not replace wholesale: %+v", saved.Repair.Arrs)
	}
	if !saved.Repair.Enabled || saved.Repair.Schedule != "0 3 * * *" {
		t.Fatalf("unmentioned repair fields were not preserved: %+v", saved.Repair)
	}
}

// TestHandleReplaceConfigPartialPutClearsOmittedFields is PUT's honest
// contract, stated as a test: the submitted document IS the config, so every
// key the caller left out reverts to its zero value — and then, exactly as on
// any other save, Save/setDefaults fills in the documented defaults for the
// fields that have one (download_folder, repair.source/workers/…). This is the
// behaviour a PATCH must NOT have, and the reason the merge was moved off the
// verb that promises replacement.
//
// Note what that means for the repair safety knobs — max_deletions_per_run,
// stop_schedule, prune and regrab have NO default, so they land on their zero
// value here. That is not a silent downgrade of safety: 0 resolves to the
// default cap of 100 (unlimited is -1), and prune/regrab false means "delete
// nothing". A replace cannot produce a MORE destructive config than the caller
// asked for — but it CAN drop a cap the operator had tightened, which is why
// the UI patches instead.
func TestHandleReplaceConfigPartialPutClearsOmittedFields(t *testing.T) {
	cfgFile := setupConfigAPITestWith(t, apiTestNestedConfigJSON)

	rec := putConfigUpdate(t, `{"log_level":"debug"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	saved := readSavedConfig(t, cfgFile)
	if saved.LogLevel != "debug" {
		t.Fatalf("submitted log_level not applied: %q", saved.LogLevel)
	}
	if len(saved.Debrids) != 0 {
		t.Fatalf("PUT must clear the omitted debrids section: %+v", saved.Debrids)
	}
	// download_folder HAS a default, so "cleared" means "back to the default",
	// not "back to the stored /downloads".
	if wantFolder := filepath.Join(filepath.Dir(cfgFile), "downloads"); saved.DownloadFolder != wantFolder {
		t.Fatalf("PUT must clear the omitted download_folder back to its default: got %q, want %q",
			saved.DownloadFolder, wantFolder)
	}
	// The whole repair block was omitted, so it is back to its zero value.
	if saved.Repair.Enabled {
		t.Fatalf("PUT must clear repair.enabled: %+v", saved.Repair)
	}
	if saved.Repair.MaxDeletionsPerRun != 0 {
		t.Fatalf("PUT must clear max_deletions_per_run (0 ⇒ default cap 100): %d", saved.Repair.MaxDeletionsPerRun)
	}
	if saved.Repair.StopSchedule != "" {
		t.Fatalf("PUT must clear stop_schedule: %q", saved.Repair.StopSchedule)
	}
	if saved.Repair.Prune || saved.Repair.ArrDeleteEnabled() {
		t.Fatalf("PUT must clear prune/regrab (⇒ delete nothing): %+v", saved.Repair)
	}
	if saved.Repair.Repair != nil {
		t.Fatalf("PUT must clear the repair tri-state to nil (⇒ defaults true): %v", *saved.Repair.Repair)
	}
	if len(saved.Repair.Arrs) != 0 {
		t.Fatalf("PUT must clear the omitted repair.arrs list: %+v", saved.Repair.Arrs)
	}
	// ... and the result is still a VALID config: Save applies defaults, and
	// the cleared safety knobs resolve to their conservative values.
	if saved.BindAddress != "0.0.0.0" || saved.Port != "8282" {
		t.Fatalf("PUT did not produce a serviceable config: bind=%q port=%q", saved.BindAddress, saved.Port)
	}
	if !saved.Repair.RepairEnabled() {
		t.Fatal("a cleared repair tri-state must resolve to true (re-acquire, non-destructive)")
	}
}

// TestHandleReplaceConfigExplicitValuesApply: PUT is not merely destructive —
// what the caller DOES submit is applied, nested sections included, and a
// submitted list replaces wholesale just as it does under PATCH.
func TestHandleReplaceConfigExplicitValuesApply(t *testing.T) {
	cfgFile := setupConfigAPITestWith(t, apiTestNestedConfigJSON)

	body := `{"log_level":"debug","download_folder":"/new","repair":{"enabled":true,"schedule":"@daily","max_deletions_per_run":25,"prune":true,"repair":false,"arrs":["radarr"]}}`
	rec := putConfigUpdate(t, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	saved := readSavedConfig(t, cfgFile)
	if saved.DownloadFolder != "/new" {
		t.Fatalf("submitted download_folder not applied: %q", saved.DownloadFolder)
	}
	if !saved.Repair.Enabled || saved.Repair.Schedule != "@daily" || saved.Repair.MaxDeletionsPerRun != 25 || !saved.Repair.Prune {
		t.Fatalf("submitted repair fields not applied: %+v", saved.Repair)
	}
	if saved.Repair.Repair == nil || *saved.Repair.Repair {
		t.Fatalf("submitted repair:false tri-state not applied: %v", saved.Repair.Repair)
	}
	if len(saved.Repair.Arrs) != 1 || saved.Repair.Arrs[0] != "radarr" {
		t.Fatalf("submitted repair.arrs did not replace wholesale: %+v", saved.Repair.Arrs)
	}
	// Keys omitted INSIDE the submitted repair section are cleared too — the
	// replacement is not "top level only". stop_schedule and regrab have no
	// default so they land on zero; source and workers fall back to the
	// documented defaults rather than keeping the stored managed/3.
	if saved.Repair.StopSchedule != "" || saved.Repair.ArrDeleteEnabled() {
		t.Fatalf("PUT preserved fields omitted inside a submitted section: %+v", saved.Repair)
	}
	if saved.Repair.Source != config.RepairSourceArr || saved.Repair.Workers != 5 {
		t.Fatalf("PUT kept the stored source/workers instead of reverting to defaults: %+v", saved.Repair)
	}
	if len(saved.Debrids) != 0 {
		t.Fatalf("PUT must clear the omitted debrids section: %+v", saved.Debrids)
	}
}

// TestHandleUpdateConfigPreservesAuthFieldsOnBothVerbs: auth lives outside the
// config form (`json:"-"` plus two flags the UI never sends) and is restored
// from the live config by the shared handler. A PUT must not be able to disable
// authentication by omission.
func TestHandleUpdateConfigPreservesAuthFieldsOnBothVerbs(t *testing.T) {
	for _, tc := range []struct {
		name string
		do   func(*testing.T, string) *httptest.ResponseRecorder
	}{
		{"patch", patchConfigUpdate},
		{"put", putConfigUpdate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfgFile := setupConfigAPITestWith(t,
				`{"log_level":"info","use_auth":true,"enable_webdav_auth":true,"download_folder":"/downloads"}`)
			if !config.Get().UseAuth {
				t.Fatal("fixture did not load use_auth")
			}

			if rec := tc.do(t, `{"log_level":"debug"}`); rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}

			saved := readSavedConfig(t, cfgFile)
			if !saved.UseAuth || !saved.EnableWebdavAuth {
				t.Fatalf("%s disabled auth by omission: use_auth=%v enable_webdav_auth=%v",
					tc.name, saved.UseAuth, saved.EnableWebdavAuth)
			}
		})
	}
}

// TestHandleUpdateConfigRejectsEmptyAndMalformedBody: neither verb may turn a
// missing or broken body into a silent 200. For PUT especially, a zero-value
// document is not "do nothing" — it would replace the entire configuration with
// zeros.
func TestHandleUpdateConfigRejectsEmptyAndMalformedBody(t *testing.T) {
	for _, tc := range []struct {
		name string
		do   func(*testing.T, string) *httptest.ResponseRecorder
	}{
		{"patch", patchConfigUpdate},
		{"put", putConfigUpdate},
	} {
		for _, body := range []string{``, `   `, `{`, `{"debrids":[`, `not json`} {
			t.Run(tc.name+" body="+body, func(t *testing.T) {
				cfgFile := setupConfigAPITest(t)
				if rec := tc.do(t, body); rec.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want 400 for body %q", rec.Code, body)
				}
				if saved := readSavedConfig(t, cfgFile); len(saved.Debrids) != 2 {
					t.Fatalf("a rejected body still rewrote the config: %+v", saved.Debrids)
				}
			})
		}
	}
}

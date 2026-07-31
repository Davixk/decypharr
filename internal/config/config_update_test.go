package config

import (
	"os"
	"reflect"
	"strings"
	"testing"

	json "github.com/bytedance/sonic"
)

func boolPtr(value bool) *bool { return &value }

// TestDebridDownloadUncachedSaveRoundTrip reproduces the production bug where
// a config update carrying debrids[].download_uncached=false returned 200 but
// the key was stripped from disk (bool + omitempty), and any later save
// re-stripped a hand-edited false. With *bool, explicit values survive save
// round-trips while an absent key stays absent and resolves to the historical
// default (false).
func TestDebridDownloadUncachedSaveRoundTrip(t *testing.T) {
	tests := []struct {
		name       string
		fragment   string // JSON inside the debrid object, "" = key absent
		wantOnDisk bool
		want       *bool
	}{
		{name: "explicit false persists", fragment: `"download_uncached":false,`, wantOnDisk: true, want: boolPtr(false)},
		{name: "explicit true persists", fragment: `"download_uncached":true,`, wantOnDisk: true, want: boolPtr(true)},
		{name: "absent stays absent", fragment: ``, wantOnDisk: false, want: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			SetConfigPath(t.TempDir())

			// Simulate the API handler's decode of an update body.
			body := `{"debrids":[{"name":"rd","provider":"realdebrid","api_key":"secret",` +
				test.fragment + `"rate_limit":"250/minute"}]}`
			var cfg Config
			if err := json.Unmarshal([]byte(body), &cfg); err != nil {
				t.Fatalf("unmarshal update body: %v", err)
			}
			if err := cfg.Save(); err != nil {
				t.Fatalf("save config: %v", err)
			}

			verify := func(step string) {
				data, err := os.ReadFile(cfg.JsonFile())
				if err != nil {
					t.Fatalf("%s: read config file: %v", step, err)
				}
				if got := strings.Contains(string(data), `"download_uncached"`); got != test.wantOnDisk {
					t.Fatalf("%s: download_uncached key on disk = %v, want %v: %s", step, got, test.wantOnDisk, data)
				}
			}
			verify("first save")

			// Reload the persisted file the way loadConfig does (decode +
			// defaults) and confirm the tri-state value and its resolution.
			data, err := os.ReadFile(cfg.JsonFile())
			if err != nil {
				t.Fatalf("read config file: %v", err)
			}
			var loaded Config
			if err := json.Unmarshal(data, &loaded); err != nil {
				t.Fatalf("unmarshal persisted config: %v", err)
			}
			loaded.setDefaults()
			if len(loaded.Debrids) != 1 {
				t.Fatalf("expected 1 debrid after reload, got %d", len(loaded.Debrids))
			}
			got := loaded.Debrids[0].DownloadUncached
			switch {
			case test.want == nil && got != nil:
				t.Fatalf("expected nil DownloadUncached after reload, got %v", *got)
			case test.want != nil && (got == nil || *got != *test.want):
				t.Fatalf("DownloadUncached after reload = %v, want %v", got, *test.want)
			}
			wantEffective := test.want != nil && *test.want
			if loaded.Debrids[0].DownloadsUncached() != wantEffective {
				t.Fatalf("DownloadsUncached() = %v, want %v", loaded.Debrids[0].DownloadsUncached(), wantEffective)
			}

			// A later save (the production re-strip path) must not change the
			// on-disk representation of the key.
			if err := loaded.Save(); err != nil {
				t.Fatalf("re-save config: %v", err)
			}
			verify("re-save")
		})
	}
}

func preserveBaseConfig() *Config {
	return &Config{
		LogLevel:       "info",
		Port:           "8282",
		DownloadFolder: "/downloads",
		Debrids: []Debrid{
			{Name: "rd", Provider: "realdebrid", APIKey: "rd-key", DownloadUncached: boolPtr(false)},
			{Name: "tb", Provider: "torbox", APIKey: "tb-key", DownloadUncached: boolPtr(true)},
		},
		Arrs:   []Arr{{Name: "radarr", Host: "http://radarr:7878", Token: "tok"}},
		Usenet: Usenet{Providers: []UsenetProvider{{Host: "news.example", Username: "u", Password: "p"}}},
	}
}

// TestPreserveMissingSectionsKeepsOmittedSections covers the production data
// loss: an update without the "debrids" key must not wipe configured providers
// (api keys included) or any other omitted section.
func TestPreserveMissingSectionsKeepsOmittedSections(t *testing.T) {
	current := preserveBaseConfig()
	body := []byte(`{"log_level":"debug"}`)

	var updated Config
	if err := json.Unmarshal(body, &updated); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if err := updated.PreserveMissingSections(current, body); err != nil {
		t.Fatalf("PreserveMissingSections: %v", err)
	}

	if updated.LogLevel != "debug" {
		t.Fatalf("posted log_level lost: %q", updated.LogLevel)
	}
	if len(updated.Debrids) != 2 || updated.Debrids[0].APIKey != "rd-key" || updated.Debrids[1].APIKey != "tb-key" {
		t.Fatalf("omitted debrids section was not preserved: %+v", updated.Debrids)
	}
	if updated.Debrids[0].DownloadUncached == nil || *updated.Debrids[0].DownloadUncached {
		t.Fatalf("explicit download_uncached=false lost during merge: %+v", updated.Debrids[0].DownloadUncached)
	}
	if updated.Debrids[1].DownloadUncached == nil || !*updated.Debrids[1].DownloadUncached {
		t.Fatalf("explicit download_uncached=true lost during merge: %+v", updated.Debrids[1].DownloadUncached)
	}
	if len(updated.Arrs) != 1 || updated.Arrs[0].Token != "tok" {
		t.Fatalf("omitted arrs section was not preserved: %+v", updated.Arrs)
	}
	if len(updated.Usenet.Providers) != 1 {
		t.Fatalf("omitted usenet section was not preserved: %+v", updated.Usenet)
	}
	if updated.DownloadFolder != "/downloads" || updated.Port != "8282" {
		t.Fatalf("omitted scalar fields were not preserved: folder=%q port=%q", updated.DownloadFolder, updated.Port)
	}
}

// TestPreserveMissingSectionsExplicitEmptyStillClears: presence of the key,
// even with an empty value, means the caller wants that value.
func TestPreserveMissingSectionsExplicitEmptyStillClears(t *testing.T) {
	current := preserveBaseConfig()
	body := []byte(`{"debrids":[],"download_folder":""}`)

	var updated Config
	if err := json.Unmarshal(body, &updated); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if err := updated.PreserveMissingSections(current, body); err != nil {
		t.Fatalf("PreserveMissingSections: %v", err)
	}

	if len(updated.Debrids) != 0 {
		t.Fatalf("explicitly posted empty debrids must clear, got %+v", updated.Debrids)
	}
	if updated.DownloadFolder != "" {
		t.Fatalf("explicitly posted empty download_folder must clear, got %q", updated.DownloadFolder)
	}
	if len(updated.Arrs) != 1 {
		t.Fatalf("omitted arrs must be preserved, got %+v", updated.Arrs)
	}
}

// TestPreserveMissingSectionsFullPatchUnchanged: a body that carries every
// top-level key (the web UI's full-config save) must decode identically with
// and without the merge step.
func TestPreserveMissingSectionsFullPatchUnchanged(t *testing.T) {
	current := preserveBaseConfig()
	body := []byte(`{
		"log_level": "trace",
		"url_base": "/",
		"bind_address": "127.0.0.1",
		"app_url": "http://app.example",
		"port": "9999",
		"allowed_file_types": ["mkv"],
		"min_file_size": "1MB",
		"max_file_size": "10GB",
		"remove_stalled_after": "10m",
		"nzb_user_agent": "agent",
		"download_folder": "/new-downloads",
		"refresh_interval": "30s",
		"default_download_action": "symlink",
		"max_active_downloads": 7,
		"skip_pre_cache": true,
		"always_rm_tracker_urls": true,
		"folder_naming": "original",
		"disable_webdav": true,
		"refresh_dirs": "/dirs",
		"custom_folders": {},
		"debrids": [{"name": "new", "provider": "realdebrid", "api_key": "new-key", "download_uncached": false}],
		"arrs": [],
		"queue_cleanup": {"rules": []},
		"mount": {"type": "none", "mount_path": "/mnt"},
		"usenet": {"providers": []},
		"notifications": {"enabled": false},
		"repair": {"enabled": false}
	}`)

	var plain, merged Config
	if err := json.Unmarshal(body, &plain); err != nil {
		t.Fatalf("unmarshal body (plain): %v", err)
	}
	if err := json.Unmarshal(body, &merged); err != nil {
		t.Fatalf("unmarshal body (merged): %v", err)
	}
	if err := merged.PreserveMissingSections(current, body); err != nil {
		t.Fatalf("PreserveMissingSections: %v", err)
	}
	if !reflect.DeepEqual(plain, merged) {
		t.Fatalf("full-config PATCH changed by merge:\nplain:  %+v\nmerged: %+v", plain, merged)
	}
	if len(merged.Debrids) != 1 || merged.Debrids[0].APIKey != "new-key" {
		t.Fatalf("posted debrids did not win wholesale: %+v", merged.Debrids)
	}
	if merged.Debrids[0].DownloadUncached == nil || *merged.Debrids[0].DownloadUncached {
		t.Fatalf("posted download_uncached=false lost: %+v", merged.Debrids[0].DownloadUncached)
	}
	if len(merged.Arrs) != 0 {
		t.Fatalf("posted empty arrs did not clear: %+v", merged.Arrs)
	}
}

// preserveNestedConfig is an operator config with every destructive-action
// safeguard deliberately set, plus a *bool two levels deep
// (mount.rclone.async_read) and slices/maps at both levels.
func preserveNestedConfig() *Config {
	return &Config{
		LogLevel:       "info",
		DownloadFolder: "/downloads",
		Debrids:        []Debrid{{Name: "rd", Provider: "realdebrid", APIKey: "rd-key"}},
		CustomFolders:  map[string]CustomFolders{"movies": {Filters: map[string]string{"a": "b"}}},
		Repair: RepairConfig{
			Enabled:            true,
			Source:             RepairSourceManaged,
			Schedule:           "0 3 * * *",
			StopSchedule:       "06:00",
			Workers:            3,
			RecheckInterval:    "168h",
			MaxDeletionsPerRun: 5,
			Prune:              true,
			ArrDelete:          boolPtr(true),
			SkipNZBRepair:      true,
			Repair:             boolPtr(false),
			Arrs:               []string{"sonarr", "radarr"},
		},
		Mount: Mount{
			Type:      MountTypeRclone,
			MountPath: "/mnt",
			Rclone: Rclone{
				Enabled:      true,
				Port:         "5572",
				VfsCacheMode: "full",
				Transfers:    8,
				AsyncRead:    boolPtr(false),
			},
			DFS: DFS{ChunkSize: "10MB", CacheDir: "/cache"},
		},
	}
}

// mergePartial decodes body the way applyConfigUpdate does and applies the
// merge against current, returning what would be saved.
func mergePartial(t *testing.T, current *Config, body string) *Config {
	t.Helper()
	var updated Config
	if err := json.Unmarshal([]byte(body), &updated); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if err := updated.PreserveMissingSections(current, []byte(body)); err != nil {
		t.Fatalf("PreserveMissingSections: %v", err)
	}
	return &updated
}

// TestPreserveMissingSectionsNestedPartialKeepsSafetyKnobs is the regression
// test for the nested half of the wipe: the merge used to run at the TOP LEVEL
// only, so posting {"repair":{"enabled":true}} replaced the whole repair block
// and silently zeroed max_deletions_per_run (the destructive-action cap),
// stop_schedule, prune and regrab.
func TestPreserveMissingSectionsNestedPartialKeepsSafetyKnobs(t *testing.T) {
	current := preserveNestedConfig()
	got := mergePartial(t, current, `{"repair":{"enabled":true}}`)

	if !got.Repair.Enabled {
		t.Fatalf("posted repair.enabled not applied: %+v", got.Repair)
	}
	if got.Repair.MaxDeletionsPerRun != 5 {
		t.Fatalf("nested partial PATCH wiped max_deletions_per_run: %d", got.Repair.MaxDeletionsPerRun)
	}
	if got.Repair.StopSchedule != "06:00" {
		t.Fatalf("nested partial PATCH wiped stop_schedule: %q", got.Repair.StopSchedule)
	}
	if !got.Repair.Prune {
		t.Fatalf("nested partial PATCH wiped prune")
	}
	if !got.Repair.ArrDeleteEnabled() {
		t.Fatalf("nested partial PATCH wiped arr_delete")
	}
	if got.Repair.Schedule != "0 3 * * *" || got.Repair.Source != RepairSourceManaged ||
		got.Repair.Workers != 3 || got.Repair.RecheckInterval != "168h" || !got.Repair.SkipNZBRepair {
		t.Fatalf("nested partial PATCH wiped the remaining repair fields: %+v", got.Repair)
	}
	if got.Repair.Repair == nil || *got.Repair.Repair {
		t.Fatalf("nested partial PATCH lost the repair:false tri-state: %v", got.Repair.Repair)
	}
	if len(got.Repair.Arrs) != 2 {
		t.Fatalf("nested partial PATCH wiped repair.arrs: %+v", got.Repair.Arrs)
	}
	// Sibling top-level sections still behave as before.
	if len(got.Debrids) != 1 || got.Debrids[0].APIKey != "rd-key" || got.DownloadFolder != "/downloads" {
		t.Fatalf("omitted top-level sections were not preserved: %+v", got)
	}
}

// TestPreserveMissingSectionsNestedExplicitValuesStillApply: inside a posted
// section, presence of a key — even with a zero/false value — is a real
// instruction and must overwrite. Losing this would make the cap unclearable.
func TestPreserveMissingSectionsNestedExplicitValuesStillApply(t *testing.T) {
	current := preserveNestedConfig()
	got := mergePartial(t, current,
		`{"repair":{"max_deletions_per_run":0,"stop_schedule":"","prune":false,"regrab":false,"workers":0}}`)

	if got.Repair.MaxDeletionsPerRun != 0 {
		t.Fatalf("explicit max_deletions_per_run:0 not applied: %d", got.Repair.MaxDeletionsPerRun)
	}
	if got.Repair.StopSchedule != "" {
		t.Fatalf("explicit stop_schedule:\"\" not applied: %q", got.Repair.StopSchedule)
	}
	if got.Repair.Prune || got.Repair.ArrDeleteEnabled() {
		t.Fatalf("explicit prune/regrab:false not applied: %+v", got.Repair)
	}
	if got.Repair.Workers != 0 {
		t.Fatalf("explicit workers:0 not applied: %d", got.Repair.Workers)
	}
	if got.Repair.Schedule != "0 3 * * *" || !got.Repair.Enabled {
		t.Fatalf("unmentioned repair fields were not preserved: %+v", got.Repair)
	}
}

// TestPreserveMissingSectionsNestedBoolPointerTriState pins the *bool
// tri-states reachable through a nested merge: repair.repair (one level deep)
// and mount.rclone.async_read (two). Absent ⇒ keep the current pointer, nil
// included; explicitly posted true/false ⇒ overwrite.
func TestPreserveMissingSectionsNestedBoolPointerTriState(t *testing.T) {
	tests := []struct {
		name          string
		currentRepair *bool
		currentAsync  *bool
		body          string
		wantRepair    *bool
		wantAsync     *bool
	}{
		{
			name: "absent keeps explicit false", currentRepair: boolPtr(false), currentAsync: boolPtr(false),
			body:       `{"repair":{"enabled":true},"mount":{"rclone":{"vfs_cache_mode":"off"}}}`,
			wantRepair: boolPtr(false), wantAsync: boolPtr(false),
		},
		{
			name: "absent keeps explicit true", currentRepair: boolPtr(true), currentAsync: boolPtr(true),
			body:       `{"repair":{"enabled":true},"mount":{"rclone":{"vfs_cache_mode":"off"}}}`,
			wantRepair: boolPtr(true), wantAsync: boolPtr(true),
		},
		{
			name: "absent keeps nil (unset)", currentRepair: nil, currentAsync: nil,
			body:       `{"repair":{"enabled":true},"mount":{"rclone":{"vfs_cache_mode":"off"}}}`,
			wantRepair: nil, wantAsync: nil,
		},
		{
			name: "explicit false overwrites unset", currentRepair: nil, currentAsync: nil,
			body:       `{"repair":{"repair":false},"mount":{"rclone":{"async_read":false}}}`,
			wantRepair: boolPtr(false), wantAsync: boolPtr(false),
		},
		{
			name: "explicit true overwrites false", currentRepair: boolPtr(false), currentAsync: boolPtr(false),
			body:       `{"repair":{"repair":true},"mount":{"rclone":{"async_read":true}}}`,
			wantRepair: boolPtr(true), wantAsync: boolPtr(true),
		},
	}

	check := func(t *testing.T, label string, got, want *bool) {
		t.Helper()
		switch {
		case want == nil && got != nil:
			t.Fatalf("%s = %v, want unset (nil)", label, *got)
		case want != nil && (got == nil || *got != *want):
			t.Fatalf("%s = %v, want %v", label, got, *want)
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := preserveNestedConfig()
			current.Repair.Repair = test.currentRepair
			current.Mount.Rclone.AsyncRead = test.currentAsync

			got := mergePartial(t, current, test.body)
			check(t, "repair.repair", got.Repair.Repair, test.wantRepair)
			check(t, "mount.rclone.async_read", got.Mount.Rclone.AsyncRead, test.wantAsync)
			if got.Repair.RepairEnabled() != (test.wantRepair == nil || *test.wantRepair) {
				t.Fatalf("RepairEnabled() = %v for %v", got.Repair.RepairEnabled(), test.wantRepair)
			}
		})
	}
}

// TestPreserveMissingSectionsNestedListsReplaceWholesale pins the chosen
// slice/map semantics: an explicitly posted list REPLACES (index is not
// identity, so element-merging would graft one entry's fields onto another);
// an omitted one is preserved. Maps behave identically — an explicitly posted
// object replaces key-for-key, it is not merged into the stored one.
func TestPreserveMissingSectionsNestedListsReplaceWholesale(t *testing.T) {
	current := preserveNestedConfig()

	got := mergePartial(t, current, `{"repair":{"arrs":["radarr"]},"custom_folders":{"shows":{}}}`)
	if len(got.Repair.Arrs) != 1 || got.Repair.Arrs[0] != "radarr" {
		t.Fatalf("posted repair.arrs did not replace wholesale: %+v", got.Repair.Arrs)
	}
	if len(got.CustomFolders) != 1 {
		t.Fatalf("posted custom_folders did not replace wholesale: %+v", got.CustomFolders)
	}
	if _, ok := got.CustomFolders["movies"]; ok {
		t.Fatalf("posted custom_folders was key-merged instead of replaced: %+v", got.CustomFolders)
	}
	if got.Repair.MaxDeletionsPerRun != 5 {
		t.Fatalf("a posted list must not disturb its siblings: %d", got.Repair.MaxDeletionsPerRun)
	}

	// Explicitly empty still clears; omitted still preserves.
	cleared := mergePartial(t, current, `{"repair":{"arrs":[]},"custom_folders":{}}`)
	if len(cleared.Repair.Arrs) != 0 || len(cleared.CustomFolders) != 0 {
		t.Fatalf("explicitly empty lists did not clear: %+v / %+v", cleared.Repair.Arrs, cleared.CustomFolders)
	}
	kept := mergePartial(t, current, `{"repair":{"enabled":true}}`)
	if len(kept.Repair.Arrs) != 2 || len(kept.CustomFolders) != 1 {
		t.Fatalf("omitted lists were not preserved: %+v / %+v", kept.Repair.Arrs, kept.CustomFolders)
	}
}

// TestPreserveMissingSectionsMergesArbitraryDepth: the merge is not special-cased
// to "repair" — it applies at every struct level, e.g. mount.rclone.
func TestPreserveMissingSectionsMergesArbitraryDepth(t *testing.T) {
	current := preserveNestedConfig()
	got := mergePartial(t, current, `{"mount":{"rclone":{"vfs_cache_mode":"off"}}}`)

	if got.Mount.Rclone.VfsCacheMode != "off" {
		t.Fatalf("posted mount.rclone.vfs_cache_mode not applied: %q", got.Mount.Rclone.VfsCacheMode)
	}
	if got.Mount.Rclone.Port != "5572" || got.Mount.Rclone.Transfers != 8 || !got.Mount.Rclone.Enabled {
		t.Fatalf("sibling rclone fields were not preserved: %+v", got.Mount.Rclone)
	}
	if got.Mount.Type != MountTypeRclone || got.Mount.MountPath != "/mnt" {
		t.Fatalf("parent mount fields were not preserved: %+v", got.Mount)
	}
	if got.Mount.DFS.ChunkSize != "10MB" || got.Mount.DFS.CacheDir != "/cache" {
		t.Fatalf("sibling mount.dfs section was not preserved: %+v", got.Mount.DFS)
	}
	if got.Repair.MaxDeletionsPerRun != 5 {
		t.Fatalf("an unrelated posted section wiped the deletion cap: %d", got.Repair.MaxDeletionsPerRun)
	}
}

// TestPreserveMissingSectionsFullNestedPatchUnchanged: a body that carries every
// key of a section behaves exactly like a plain decode, i.e. the recursive merge
// is a no-op for the web UI's full-config save.
func TestPreserveMissingSectionsFullNestedPatchUnchanged(t *testing.T) {
	current := preserveNestedConfig()
	body := `{"repair":{
		"enabled": false, "source": "arr", "schedule": "@daily", "workers": 1,
		"nntp_connection_percent": 30, "strategy": "per_entry", "recheck_interval": "24h",
		"arrs": [], "auto_repair": false, "skip_nzb_repair": false,
		"repair": true, "prune": false, "regrab": false,
		"max_deletions_per_run": 10, "stop_schedule": ""
	}}`

	var plain Config
	if err := json.Unmarshal([]byte(body), &plain); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	got := mergePartial(t, current, body)
	if !reflect.DeepEqual(plain.Repair, got.Repair) {
		t.Fatalf("fully posted section changed by merge:\nplain:  %+v\nmerged: %+v", plain.Repair, got.Repair)
	}
}

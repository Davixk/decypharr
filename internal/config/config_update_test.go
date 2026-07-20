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
// POST /api/config with debrids[].download_uncached=false returned 200 but the
// key was stripped from disk (bool + omitempty), and any later save re-stripped
// a hand-edited false. With *bool, explicit values survive save round-trips
// while an absent key stays absent and resolves to the historical default
// (false).
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

			// Simulate the API handler's decode of a POST body.
			body := `{"debrids":[{"name":"rd","provider":"realdebrid","api_key":"secret",` +
				test.fragment + `"rate_limit":"250/minute"}]}`
			var cfg Config
			if err := json.Unmarshal([]byte(body), &cfg); err != nil {
				t.Fatalf("unmarshal POST body: %v", err)
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
// loss: a POST without the "debrids" key must not wipe configured providers
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

// TestPreserveMissingSectionsFullPostUnchanged: a body that posts every
// top-level key (the web UI's full-config save) must decode identically with
// and without the merge step.
func TestPreserveMissingSectionsFullPostUnchanged(t *testing.T) {
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
		t.Fatalf("full-config POST changed by merge:\nplain:  %+v\nmerged: %+v", plain, merged)
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

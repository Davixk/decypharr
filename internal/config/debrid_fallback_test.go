package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestUpdateDebridKeepsUnsetPriorityUnmaterialized(t *testing.T) {
	cfg := &Config{Debrids: []Debrid{
		{Name: "first"},
		{Name: "explicit", Priority: 20},
		{Name: "third"},
	}}

	for index, provider := range cfg.Debrids {
		cfg.Debrids[index] = cfg.updateDebrid(index, provider)
	}

	tests := []struct {
		index         int
		wantPriority  int
		wantConfigPos int
	}{
		{index: 0, wantPriority: 0, wantConfigPos: 0},
		{index: 1, wantPriority: 20, wantConfigPos: 1},
		{index: 2, wantPriority: 0, wantConfigPos: 2},
	}
	for _, test := range tests {
		provider := cfg.Debrids[test.index]
		if provider.Priority != test.wantPriority || provider.ConfigOrder != test.wantConfigPos {
			t.Fatalf("provider %d normalized to priority=%d order=%d, want priority=%d order=%d",
				test.index, provider.Priority, provider.ConfigOrder, test.wantPriority, test.wantConfigPos)
		}
	}
}

func TestUpdateDebridSaveRoundTripLeavesUnsetPriorityAbsent(t *testing.T) {
	cfg := &Config{Debrids: []Debrid{
		{Provider: "realdebrid", Name: "rd", APIKey: "secret"},
		{Provider: "alldebrid", Name: "ad", APIKey: "secret"},
	}}

	for index, provider := range cfg.Debrids {
		cfg.Debrids[index] = cfg.updateDebrid(index, provider)
	}

	data, err := json.Marshal(cfg.Debrids)
	if err != nil {
		t.Fatalf("marshal debrids: %v", err)
	}
	if strings.Contains(string(data), `"priority"`) {
		t.Fatalf("unset priority was materialized into persisted config: %s", data)
	}
}

func TestDebridFallbackConfigurationJSONRoundTrip(t *testing.T) {
	downloadUncached := true
	original := Config{
		Debrids: []Debrid{{
			Provider:         "alldebrid",
			Name:             "primary",
			APIKey:           "secret",
			DownloadUncached: &downloadUncached,
			Priority:         7,
			ConfigOrder:      42,
		}},
		Arrs: []Arr{{
			Name:              "radarr",
			DownloadUncached:  &downloadUncached,
			SelectedDebrid:    "primary",
			FallbackOnFailure: true,
		}},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	jsonText := string(data)
	if !strings.Contains(jsonText, `"priority":7`) || !strings.Contains(jsonText, `"fallback_on_failure":true`) {
		t.Fatalf("new routing fields missing from JSON: %s", jsonText)
	}
	if strings.Contains(jsonText, "ConfigOrder") || strings.Contains(jsonText, "config_order") {
		t.Fatalf("internal config order leaked into JSON: %s", jsonText)
	}

	var decoded Config
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	if len(decoded.Debrids) != 1 || decoded.Debrids[0].Priority != 7 {
		t.Fatalf("priority did not round-trip: %+v", decoded.Debrids)
	}
	if len(decoded.Arrs) != 1 || !decoded.Arrs[0].FallbackOnFailure || decoded.Arrs[0].SelectedDebrid != "primary" {
		t.Fatalf("fallback routing did not round-trip: %+v", decoded.Arrs)
	}
}

func TestFallbackOnFailureDefaultsOff(t *testing.T) {
	var configured Arr
	if err := json.Unmarshal([]byte(`{"name":"radarr","selected_debrid":"primary"}`), &configured); err != nil {
		t.Fatalf("unmarshal legacy Arr config: %v", err)
	}
	if configured.FallbackOnFailure {
		t.Fatal("legacy Arr config unexpectedly enabled provider fallback")
	}
}

func TestValidateDebridsRejectsNegativePriority(t *testing.T) {
	err := validateDebrids([]Debrid{{APIKey: "secret", Priority: -1}})
	if err == nil || !strings.Contains(err.Error(), "priority") {
		t.Fatalf("expected priority validation error, got %v", err)
	}
}

package config

import (
	"strings"
	"testing"
)

// The guardrail must fire when max_active_downloads x
// usenet.processing_max_connections exceeds twice the total provider
// connection budget, name the numbers, and recommend a bound.
func TestUsenetConnectionBudgetWarningFires(t *testing.T) {
	cfg := &Config{
		MaxActiveDownloads: 10,
		Usenet: Usenet{
			ProcessingMaxConnections: 20,
			Providers: []UsenetProvider{
				{Host: "a.example", MaxConnections: 30},
				{Host: "b.example", MaxConnections: 20},
			},
		},
	}
	// demand = 10 x 20 = 200 > 2 x (30 + 20) = 100
	warning := cfg.UsenetConnectionBudgetWarning()
	if warning == "" {
		t.Fatal("expected a warning for an over-budget configuration")
	}
	for _, fragment := range []string{
		"max_active_downloads (10)",
		"processing_max_connections (20)",
		"200",
		"2 x 50 = 100",
		"max_active_downloads <= 5",
	} {
		if !strings.Contains(warning, fragment) {
			t.Fatalf("warning %q missing fragment %q", warning, fragment)
		}
	}
}

func TestUsenetConnectionBudgetWarningSilentCases(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{
			name: "within budget",
			cfg: Config{
				MaxActiveDownloads: 5,
				Usenet: Usenet{
					ProcessingMaxConnections: 15,
					Providers:                []UsenetProvider{{Host: "a.example", MaxConnections: 50}},
				},
			},
		},
		{
			name: "exactly at the 2x boundary",
			cfg: Config{
				MaxActiveDownloads: 5,
				Usenet: Usenet{
					ProcessingMaxConnections: 20,
					Providers:                []UsenetProvider{{Host: "a.example", MaxConnections: 50}},
				},
			},
		},
		{
			name: "no usenet providers",
			cfg: Config{
				MaxActiveDownloads: 100,
				Usenet:             Usenet{ProcessingMaxConnections: 100},
			},
		},
		{
			name: "zero provider connections",
			cfg: Config{
				MaxActiveDownloads: 100,
				Usenet: Usenet{
					ProcessingMaxConnections: 100,
					Providers:                []UsenetProvider{{Host: "a.example", MaxConnections: 0}},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if warning := tc.cfg.UsenetConnectionBudgetWarning(); warning != "" {
				t.Fatalf("unexpected warning: %q", warning)
			}
		})
	}
}

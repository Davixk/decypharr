package config

import (
	"strings"
	"testing"
)

// This guardrail used to warn when max_active_downloads x
// processing_max_connections exceeded twice the provider pool. That formula
// described a demand that can no longer occur: NNTP passes are gated by
// processSem = floor(pool / processing_max_connections), so gate x pmc <= pool
// holds by construction. max_active_downloads no longer sizes the job pool
// either — it bounds post-download local I/O — so both of its inputs had
// stopped meaning what the formula assumed.
//
// What remains possible, and is otherwise invisible, is a per-pass connection
// budget larger than the whole pool: the gate clamps to 1 and every NZB import
// serialises. The symptom is slowness with nothing in the logs, which is
// exactly the class of problem a startup warning should catch.

func TestWarnsWhenPerPassBudgetExceedsThePool(t *testing.T) {
	cfg := &Config{
		Usenet: Usenet{
			ProcessingMaxConnections: 60,
			Providers: []UsenetProvider{
				{Host: "a.example", MaxConnections: 30},
				{Host: "b.example", MaxConnections: 20},
			},
		},
	}
	// pool = 50, pmc = 60 -> gate clamps to 1, everything serialises.
	if got := cfg.UsenetProcessConcurrency(); got != 1 {
		t.Fatalf("gate = %d, want 1 — the premise of this warning", got)
	}

	warning := cfg.UsenetConnectionBudgetWarning()
	if warning == "" {
		t.Fatal("expected a warning: NZB processing is silently serialised in this configuration")
	}
	for _, fragment := range []string{
		"processing_max_connections (60)",
		"(50)",
		"serialised",
		"at most 50",
	} {
		if !strings.Contains(warning, fragment) {
			t.Fatalf("warning %q missing fragment %q", warning, fragment)
		}
	}
}

func TestBudgetWarningStaysSilentWhenTheGateCanParallelise(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		gate int
	}{
		{
			name: "comfortable headroom",
			cfg: Config{Usenet: Usenet{
				ProcessingMaxConnections: 8,
				Providers:                []UsenetProvider{{Host: "a.example", MaxConnections: 100}},
			}},
			gate: 12,
		},
		{
			// The old formula fired loudly here (10 x 20 = 200 > 2 x 50). The
			// gate makes it a non-event: 2 concurrent passes of 20 connections
			// against a 50-connection pool is not oversubscription.
			name: "old formula would have warned",
			cfg: Config{MaxActiveDownloads: 10, Usenet: Usenet{
				ProcessingMaxConnections: 20,
				Providers: []UsenetProvider{
					{Host: "a.example", MaxConnections: 30},
					{Host: "b.example", MaxConnections: 20},
				},
			}},
			gate: 2,
		},
		{
			name: "pmc exactly equals the pool",
			cfg: Config{Usenet: Usenet{
				ProcessingMaxConnections: 50,
				Providers:                []UsenetProvider{{Host: "a.example", MaxConnections: 50}},
			}},
			gate: 1,
		},
		{
			name: "no usenet providers",
			cfg:  Config{Usenet: Usenet{ProcessingMaxConnections: 100}},
		},
		{
			name: "zero provider connections",
			cfg: Config{Usenet: Usenet{
				ProcessingMaxConnections: 100,
				Providers:                []UsenetProvider{{Host: "a.example", MaxConnections: 0}},
			}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if warning := tc.cfg.UsenetConnectionBudgetWarning(); warning != "" {
				t.Fatalf("unexpected warning: %q", warning)
			}
			if tc.gate > 0 {
				if got := tc.cfg.UsenetProcessConcurrency(); got != tc.gate {
					t.Fatalf("gate = %d, want %d", got, tc.gate)
				}
			}
		})
	}
}

// MaxConcurrentJobs must NOT inherit max_active_downloads. Aliasing would hand
// every existing config its old 5-or-14 as a job-slot ceiling and deliver none
// of the fix while looking migrated.
func TestMaxConcurrentJobsDoesNotInheritMaxActiveDownloads(t *testing.T) {
	cfg := &Config{MaxActiveDownloads: 14}
	cfg.setDefaults()

	if cfg.MaxConcurrentJobs != DefaultMaxConcurrentJobs {
		t.Fatalf("MaxConcurrentJobs = %d, want the machine-ceiling default %d: it must not be aliased to "+
			"max_active_downloads, or upgrading configs silently keep the old bottleneck",
			cfg.MaxConcurrentJobs, DefaultMaxConcurrentJobs)
	}
	if cfg.MaxActiveDownloads != 14 {
		t.Fatalf("MaxActiveDownloads = %d, want 14 preserved: it still sizes the post-download action gate",
			cfg.MaxActiveDownloads)
	}
	if DefaultMaxConcurrentJobs <= 14 {
		t.Fatalf("DefaultMaxConcurrentJobs = %d: a machine ceiling in the same range as the old download "+
			"count would leave short jobs queued behind long ones", DefaultMaxConcurrentJobs)
	}
}

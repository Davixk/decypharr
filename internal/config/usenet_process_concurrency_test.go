package config

import "testing"

// UsenetProcessConcurrency must fit heavy Process passes to the provider pool:
// floor(sum(provider max_connections) / processing_max_connections), clamped to
// at least 1. concurrentProcess x processing_max_connections must never exceed
// the pool.
func TestUsenetProcessConcurrency(t *testing.T) {
	cases := []struct {
		name      string
		providers []UsenetProvider
		pmc       int
		want      int
	}{
		{
			// The production incident shape: 100-connection pool, pmc 8 -> 12
			// concurrent passes (12 x 8 = 96 <= 100), so MAD=14 no longer
			// oversubscribes.
			name:      "incident shape 100 pool pmc 8",
			providers: []UsenetProvider{{Host: "a", MaxConnections: 100}},
			pmc:       8,
			want:      12,
		},
		{
			name:      "sums across providers",
			providers: []UsenetProvider{{Host: "a", MaxConnections: 60}, {Host: "b", MaxConnections: 40}},
			pmc:       10,
			want:      10,
		},
		{
			name:      "clamps to at least 1 when pmc exceeds pool",
			providers: []UsenetProvider{{Host: "a", MaxConnections: 20}},
			pmc:       50,
			want:      1,
		},
		{
			name:      "exact multiple",
			providers: []UsenetProvider{{Host: "a", MaxConnections: 32}},
			pmc:       8,
			want:      4,
		},
		{
			name:      "floors the division",
			providers: []UsenetProvider{{Host: "a", MaxConnections: 31}},
			pmc:       8,
			want:      3,
		},
		{
			name:      "no providers defaults to 1",
			providers: nil,
			pmc:       8,
			want:      1,
		},
		{
			name:      "zero pmc defaults to 1",
			providers: []UsenetProvider{{Host: "a", MaxConnections: 100}},
			pmc:       0,
			want:      1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{Usenet: Usenet{Providers: tc.providers, ProcessingMaxConnections: tc.pmc}}
			if got := cfg.UsenetProcessConcurrency(); got != tc.want {
				t.Fatalf("UsenetProcessConcurrency() = %d, want %d", got, tc.want)
			}
			// Invariant: gate x pmc must never exceed the pool (unless clamped
			// up to 1 because pmc alone already exceeds the pool).
			if tc.pmc > 0 {
				total := cfg.UsenetProviderConnectionTotal()
				if got := cfg.UsenetProcessConcurrency(); got > 1 && got*tc.pmc > total {
					t.Fatalf("gate %d x pmc %d = %d oversubscribes pool %d", got, tc.pmc, got*tc.pmc, total)
				}
			}
		})
	}
}

package manager

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// THE GATE'S ONE JOB IS TO BE CHEAP AND TIMID.
//
// It runs inside a request an arr is blocked on, so a refusal is free and a
// wrong refusal is expensive: coverage of the only usable source is ~53%, and
// refusing on ignorance would silently reject half of every grab the arrs make.
//
// So most of these tests assert that it ALLOWS. The single test that asserts a
// refusal is there so an implementation that always allows cannot pass.

const testHash = "0123456789abcdef0123456789abcdef01234567"

func intPointer(v int) *int { return &v }

// bitmagnetStub serves the GraphQL shape bitmagnet returns, and counts calls so
// the short-circuit paths can be proven to skip the network entirely.
func bitmagnetStub(t *testing.T, body string, status int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func seedersBody(hash string, seeders string) string {
	return fmt.Sprintf(
		`{"data":{"torrentContent":{"search":{"items":[{"infoHash":"%s","torrent":{"seeders":%s}}]}}}}`,
		hash, seeders)
}

func gateFixture(t *testing.T, cfg config.SeederGateConfig) *Manager {
	t.Helper()
	m, _ := newStallPruneFixture(t)
	config.Get().SeederGate = cfg
	return m
}

// TestGateRefusesAThinSwarm is the feature. Everything else guards it.
func TestGateRefusesAThinSwarm(t *testing.T) {
	srv, calls := bitmagnetStub(t, seedersBody(testHash, "0"), http.StatusOK)
	m := gateFixture(t, config.SeederGateConfig{MinSeeders: intPointer(1), BitmagnetURL: srv.URL})

	reason := m.seederGateRefusal(context.Background(), testHash, 0)
	if reason == "" {
		t.Fatal("a bitmagnet-confirmed 0-seeder uncached release must be refused")
	}
	if !strings.Contains(reason, "0 seeders") {
		t.Fatalf("reason = %q; it must name the count that caused the refusal", reason)
	}
	if calls.Load() != 1 {
		t.Fatalf("bitmagnet calls = %d, want 1", calls.Load())
	}
}

// TestGateAllowsWhenItCannotKnow is the fail-open contract, across every way the
// answer can be unavailable. ~47% of real grabs land in one of these.
func TestGateAllowsWhenItCannotKnow(t *testing.T) {
	cases := []struct {
		name   string
		body   string
		status int
		hash   string
	}{
		{"hash not indexed", `{"data":{"torrentContent":{"search":{"items":[]}}}}`, http.StatusOK, testHash},
		{"indexed but seeders null", seedersBody(testHash, "null"), http.StatusOK, testHash},
		{"server error", `{}`, http.StatusInternalServerError, testHash},
		{"malformed response", `not json at all`, http.StatusOK, testHash},
		{"empty response", ``, http.StatusOK, testHash},
		{"a different hash came back", seedersBody("ffffffffffffffffffffffffffffffffffffffff", "0"), http.StatusOK, testHash},
		{"not an infohash", seedersBody(testHash, "0"), http.StatusOK, "not-a-hash"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := bitmagnetStub(t, tc.body, tc.status)
			m := gateFixture(t, config.SeederGateConfig{MinSeeders: intPointer(1), BitmagnetURL: srv.URL})

			if got := m.seederGateRefusal(context.Background(), tc.hash, 0); got != "" {
				t.Fatalf("reason = %q, want allow: no answer must never mean refuse", got)
			}
		})
	}
}

// TestGateAllowsWhenTheLookupIsSlow: the caller is an arr blocked on an add, so
// a hanging index must cost a bounded wait and then get out of the way.
func TestGateAllowsWhenTheLookupIsSlow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
	}))
	t.Cleanup(srv.Close)
	m := gateFixture(t, config.SeederGateConfig{
		MinSeeders:   intPointer(1),
		BitmagnetURL: srv.URL,
		Timeout:      "100ms",
	})

	start := time.Now()
	if got := m.seederGateRefusal(context.Background(), testHash, 0); got != "" {
		t.Fatalf("reason = %q, want allow on timeout", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("gate took %s; the timeout must bound what the arr waits for", elapsed)
	}
}

// TestProviderCountConfirmsButNeverCondemns. A provider has not had time to
// discover peers on a transfer seconds old, so its zero is ignorance — but a
// non-zero reading is real evidence and must skip the lookup entirely.
func TestProviderCountConfirmsButNeverCondemns(t *testing.T) {
	// Non-zero from the provider: allow without asking bitmagnet at all.
	srv, calls := bitmagnetStub(t, seedersBody(testHash, "0"), http.StatusOK)
	m := gateFixture(t, config.SeederGateConfig{MinSeeders: intPointer(1), BitmagnetURL: srv.URL})

	if got := m.seederGateRefusal(context.Background(), testHash, 5); got != "" {
		t.Fatalf("reason = %q; a provider reporting 5 seeders is positive evidence", got)
	}
	if calls.Load() != 0 {
		t.Fatalf("bitmagnet calls = %d, want 0: a confirmed swarm needs no second opinion", calls.Load())
	}

	// Zero from the provider must not itself refuse — only bitmagnet can, and
	// here bitmagnet says the swarm is fine.
	srv2, _ := bitmagnetStub(t, seedersBody(testHash, "12"), http.StatusOK)
	m2 := gateFixture(t, config.SeederGateConfig{MinSeeders: intPointer(1), BitmagnetURL: srv2.URL})
	if got := m2.seederGateRefusal(context.Background(), testHash, 0); got != "" {
		t.Fatalf("reason = %q; a zero from the provider at t=0 is not a verdict", got)
	}
}

// TestGateIsOffUnlessSwitchedOn. Absent MUST mean off — a previous version of
// this feature defaulted absent to 1 and ran for operators who had never heard
// of it.
func TestGateIsOffUnlessSwitchedOn(t *testing.T) {
	srv, calls := bitmagnetStub(t, seedersBody(testHash, "0"), http.StatusOK)
	zero := 0

	cases := []struct {
		name string
		cfg  config.SeederGateConfig
	}{
		{"entirely absent", config.SeederGateConfig{}},
		{"min_seeders absent, endpoint set", config.SeederGateConfig{BitmagnetURL: srv.URL}},
		{"min_seeders explicitly 0", config.SeederGateConfig{MinSeeders: &zero, BitmagnetURL: srv.URL}},
		{"threshold set but no endpoint", config.SeederGateConfig{MinSeeders: intPointer(1)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls.Store(0)
			m := gateFixture(t, tc.cfg)
			if got := m.seederGateRefusal(context.Background(), testHash, 0); got != "" {
				t.Fatalf("reason = %q, want allow: the gate must be off", got)
			}
			if calls.Load() != 0 {
				t.Fatalf("a disabled gate made %d lookups", calls.Load())
			}
		})
	}
}

// TestGateHasNoSettleWindow pins the property the previous attempt violated.
// There is no waiting of any duration inside a live blocking request: a gate
// that waits has already answered, and its verdict then costs a full re-search
// instead of nothing.
func TestGateHasNoSettleWindow(t *testing.T) {
	srv, _ := bitmagnetStub(t, seedersBody(testHash, "0"), http.StatusOK)
	m := gateFixture(t, config.SeederGateConfig{MinSeeders: intPointer(1), BitmagnetURL: srv.URL})

	start := time.Now()
	if got := m.seederGateRefusal(context.Background(), testHash, 0); got == "" {
		t.Fatal("expected a refusal for this fixture")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("gate took %s; it must answer at lookup speed and never wait for a swarm to settle", elapsed)
	}
}

// TestThresholdIsHonoured: the measurement says the cliff is 0->1, but the knob
// is the operator's.
func TestThresholdIsHonoured(t *testing.T) {
	srv, _ := bitmagnetStub(t, seedersBody(testHash, "2"), http.StatusOK)

	lenient := gateFixture(t, config.SeederGateConfig{MinSeeders: intPointer(1), BitmagnetURL: srv.URL})
	if got := lenient.seederGateRefusal(context.Background(), testHash, 0); got != "" {
		t.Fatalf("reason = %q; 2 seeders meets a threshold of 1", got)
	}

	strict := gateFixture(t, config.SeederGateConfig{MinSeeders: intPointer(5), BitmagnetURL: srv.URL})
	if got := strict.seederGateRefusal(context.Background(), testHash, 0); got == "" {
		t.Fatal("2 seeders is below a threshold of 5 and must be refused")
	}
}

// TestInfoHashValidationIsTheInjectionGuard. The hash is interpolated into a
// GraphQL document and arrives from a magnet written by somebody else, so
// anything non-hex must be rejected before it reaches the query — and rejection
// allows, so a strange hash costs nothing.
func TestInfoHashValidationIsTheInjectionGuard(t *testing.T) {
	for _, bad := range []string{
		``,
		`short`,
		`"]}) { evil }`,
		strings.Repeat("z", 40),
		testHash + "0",
		testHash[:39],
	} {
		if isInfoHash(bad) {
			t.Errorf("isInfoHash(%q) = true; only a bare 40-char hex string may reach the query", bad)
		}
	}
	for _, good := range []string{testHash, strings.ToUpper(testHash)} {
		if !isInfoHash(good) {
			t.Errorf("isInfoHash(%q) = false; this is a valid infohash", good)
		}
	}
}

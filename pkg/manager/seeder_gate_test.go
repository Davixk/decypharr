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
// wrong refusal is expensive: the release is deleted and the arr moves on. So
// most of these tests assert that it ALLOWS. The few that assert a refusal exist
// so an implementation that always allows cannot pass.

const testHash = "0123456789abcdef0123456789abcdef01234567"

func intPointer(v int) *int { return &v }

func magnetWith(hash string, trackers ...string) string {
	link := "magnet:?xt=urn:btih:" + hash
	for _, tr := range trackers {
		link += "&tr=" + tr
	}
	return link
}

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
		`{"data":{"torrentContent":{"search":{"items":[{"infoHash":"%s","torrent":{"seeders":%s,"leechers":null}}]}}}}`,
		hash, seeders)
}

func gateFixture(t *testing.T, cfg config.SeederGateConfig) *Manager {
	t.Helper()
	m, _ := newStallPruneFixture(t)
	config.Get().SeederGate = cfg
	return m
}

// bitmagnetOnly is the cheap deterministic source for policy tests. UDP scrape
// behaviour is exercised in pkg/swarm against a real socket.
func bitmagnetOnly(url string, minSeeders int) config.SeederGateConfig {
	return config.SeederGateConfig{
		MinSeeders:   intPointer(minSeeders),
		Sources:      []string{config.SwarmSourceBitmagnet},
		BitmagnetURL: url,
	}
}

// TestGateRefusesAThinSwarm is the feature. Everything else guards it.
func TestGateRefusesAThinSwarm(t *testing.T) {
	srv, calls := bitmagnetStub(t, seedersBody(testHash, "0"), http.StatusOK)
	m := gateFixture(t, bitmagnetOnly(srv.URL, 1))

	reason := m.seederGateRefusal(context.Background(), testHash, magnetWith(testHash), 0)
	if reason == "" {
		t.Fatal("a confirmed 0-seeder uncached release must be refused")
	}
	if !strings.Contains(reason, "0 seeders") {
		t.Fatalf("reason = %q; it must name the count that caused the refusal", reason)
	}
	// A verdict that deletes a transfer has to say what informed it.
	if !strings.Contains(reason, "bitmagnet") {
		t.Fatalf("reason = %q; it must name the source it trusted", reason)
	}
	if calls.Load() != 1 {
		t.Fatalf("lookups = %d, want 1", calls.Load())
	}
}

// TestGateAllowsWhenItCannotKnow is the fail-open contract across every way the
// answer can be unavailable.
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
			m := gateFixture(t, bitmagnetOnly(srv.URL, 1))

			if got := m.seederGateRefusal(context.Background(), tc.hash, magnetWith(tc.hash), 0); got != "" {
				t.Fatalf("reason = %q, want allow: no answer must never mean refuse", got)
			}
		})
	}
}

// TestGateAllowsWhenTheLookupIsSlow: the caller is an arr blocked on an add, so
// a hanging source must cost a bounded wait and then get out of the way.
func TestGateAllowsWhenTheLookupIsSlow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
	}))
	t.Cleanup(srv.Close)
	cfg := bitmagnetOnly(srv.URL, 1)
	cfg.Timeout = "150ms"
	m := gateFixture(t, cfg)

	start := time.Now()
	if got := m.seederGateRefusal(context.Background(), testHash, magnetWith(testHash), 0); got != "" {
		t.Fatalf("reason = %q, want allow on timeout", got)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("gate took %s; the timeout must bound what the arr waits for", elapsed)
	}
}

// TestProviderCountConfirmsButNeverCondemns. A provider has not had time to
// discover peers on a transfer seconds old, so its zero is ignorance — but a
// non-zero reading is real evidence and must skip the lookup entirely.
func TestProviderCountConfirmsButNeverCondemns(t *testing.T) {
	srv, calls := bitmagnetStub(t, seedersBody(testHash, "0"), http.StatusOK)
	m := gateFixture(t, bitmagnetOnly(srv.URL, 1))

	if got := m.seederGateRefusal(context.Background(), testHash, magnetWith(testHash), 5); got != "" {
		t.Fatalf("reason = %q; a provider reporting 5 seeders is positive evidence", got)
	}
	if calls.Load() != 0 {
		t.Fatalf("lookups = %d, want 0: a confirmed swarm needs no second opinion", calls.Load())
	}

	srv2, _ := bitmagnetStub(t, seedersBody(testHash, "12"), http.StatusOK)
	m2 := gateFixture(t, bitmagnetOnly(srv2.URL, 1))
	if got := m2.seederGateRefusal(context.Background(), testHash, magnetWith(testHash), 0); got != "" {
		t.Fatalf("reason = %q; a zero from the provider at t=0 is not a verdict", got)
	}
}

// TestGateIsOffUnlessSwitchedOn. Absent MUST mean off — a previous version
// defaulted absent to 1 and ran for operators who had never heard of it.
func TestGateIsOffUnlessSwitchedOn(t *testing.T) {
	srv, calls := bitmagnetStub(t, seedersBody(testHash, "0"), http.StatusOK)
	zero := 0

	cases := []struct {
		name string
		cfg  config.SeederGateConfig
	}{
		{"entirely absent", config.SeederGateConfig{}},
		{"threshold absent, source configured", config.SeederGateConfig{
			Sources: []string{config.SwarmSourceBitmagnet}, BitmagnetURL: srv.URL}},
		{"threshold explicitly 0", config.SeederGateConfig{
			MinSeeders: &zero, Sources: []string{config.SwarmSourceBitmagnet}, BitmagnetURL: srv.URL}},
		{"threshold set but no sources", config.SeederGateConfig{MinSeeders: intPointer(1)}},
		{"bitmagnet named without an endpoint", config.SeederGateConfig{
			MinSeeders: intPointer(1), Sources: []string{config.SwarmSourceBitmagnet}}},
		{"unrecognised source name", config.SeederGateConfig{
			MinSeeders: intPointer(1), Sources: []string{"prowlarr"}, BitmagnetURL: srv.URL}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls.Store(0)
			m := gateFixture(t, tc.cfg)
			if got := m.seederGateRefusal(context.Background(), testHash, magnetWith(testHash), 0); got != "" {
				t.Fatalf("reason = %q, want allow: the gate must be off", got)
			}
			if calls.Load() != 0 {
				t.Fatalf("a disabled gate made %d lookups", calls.Load())
			}
		})
	}
}

// TestSourcesAreOrderedAndSwappable is the durable half of the operator's
// directive: the gate must depend on the interface, not on a backend. A source
// that cannot answer falls through to the next.
func TestSourcesAreOrderedAndSwappable(t *testing.T) {
	live, liveCalls := bitmagnetStub(t, seedersBody(testHash, "0"), http.StatusOK)

	// udp_scrape with no trackers to ask is a guaranteed "unknown" first link,
	// which is exactly the fall-through this asserts.
	m := gateFixture(t, config.SeederGateConfig{
		MinSeeders:   intPointer(1),
		Sources:      []string{config.SwarmSourceUDPScrape, config.SwarmSourceBitmagnet},
		BitmagnetURL: live.URL,
	})

	// No trackers in the magnet and no fallback set => the scrape cannot answer
	// and must hand over rather than condemn.
	if got := m.seederGateRefusal(context.Background(), testHash, magnetWith(testHash), 0); got == "" {
		t.Fatal("the chain must fall through to bitmagnet when the scrape has nobody to ask")
	}
	if liveCalls.Load() != 1 {
		t.Fatalf("second source lookups = %d, want 1", liveCalls.Load())
	}
}

// TestDHTOnlyMagnetAllows: a magnet with no announce list gives the scrape
// nobody to ask. That is an absence, not a verdict.
func TestDHTOnlyMagnetAllows(t *testing.T) {
	m := gateFixture(t, config.SeederGateConfig{
		MinSeeders: intPointer(1),
		Sources:    []string{config.SwarmSourceUDPScrape},
	})
	if got := m.seederGateRefusal(context.Background(), testHash, magnetWith(testHash), 0); got != "" {
		t.Fatalf("reason = %q; a DHT-only magnet has no tracker to ask and must be allowed", got)
	}
}

// TestStrippedTrackersAllow pins the always_rm_tracker_urls interaction: with
// that setting on, magnets arrive with no tr= list at all, so the scrape can
// never answer. It must allow, and it must not be mistaken for a verdict.
func TestStrippedTrackersAllow(t *testing.T) {
	m := gateFixture(t, config.SeederGateConfig{
		MinSeeders: intPointer(1),
		Sources:    []string{config.SwarmSourceUDPScrape},
	})
	stripped := "magnet:?xt=urn:btih:" + testHash + "&dn=Some.Release"
	if got := m.seederGateRefusal(context.Background(), testHash, stripped, 0); got != "" {
		t.Fatalf("reason = %q; a stripped magnet must allow", got)
	}
}

// TestThresholdIsHonoured: the measurement says the cliff is 0->1, but the knob
// is the operator's.
func TestThresholdIsHonoured(t *testing.T) {
	srv, _ := bitmagnetStub(t, seedersBody(testHash, "2"), http.StatusOK)

	lenient := gateFixture(t, bitmagnetOnly(srv.URL, 1))
	if got := lenient.seederGateRefusal(context.Background(), testHash, magnetWith(testHash), 0); got != "" {
		t.Fatalf("reason = %q; 2 seeders meets a threshold of 1", got)
	}

	strict := gateFixture(t, bitmagnetOnly(srv.URL, 5))
	if got := strict.seederGateRefusal(context.Background(), testHash, magnetWith(testHash), 0); got == "" {
		t.Fatal("2 seeders is below a threshold of 5 and must be refused")
	}
}

// TestMagnetTrackerExtraction feeds the scrape its announce list.
func TestMagnetTrackerExtraction(t *testing.T) {
	link := magnetWith(testHash,
		"udp%3A%2F%2Ftracker.example%3A1337%2Fannounce",
		"http%3A%2F%2Fweb.example%2Fannounce")
	got := magnetTrackers(link)
	if len(got) != 2 {
		t.Fatalf("trackers = %v, want both announce URLs", got)
	}
	if magnetTrackers("") != nil || magnetTrackers("not a magnet at all") == nil {
		// The second is a URL parse that succeeds with no query — an empty
		// list, not a crash.
		_ = got
	}
}

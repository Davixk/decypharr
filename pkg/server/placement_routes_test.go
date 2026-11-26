package server

import (
	"testing"

	"github.com/go-chi/chi/v5"
)

// The placement routes are checked against the routing table itself rather than
// by executing handlers: no server test can build a real manager, and what needs
// pinning here is the registration and the shape of each match.
//
// 🔴 THE ONE THAT MATTERS: `DELETE /api/torrents/{category}/{hash}` deletes a
// whole entry and releases EVERY placement. `DELETE …/placements/{provider}`
// drops exactly one. Those two must never resolve to each other, or a request
// meant to free one stale copy would wipe the copy the library reads.
func TestPlacementRoutesResolveToTheirOwnPatterns(t *testing.T) {
	mux, ok := (&Server{}).WebRoutes().(*chi.Mux)
	if !ok {
		t.Fatal("WebRoutes no longer returns a *chi.Mux; this test needs another way to read the table")
	}

	cases := []struct {
		method, path, wantPattern string
	}{
		{"GET", "/api/torrents/abc123/placements", "/api/torrents/{hash}/placements"},
		{"DELETE", "/api/torrents/abc123/placements/AllDebrid", "/api/torrents/{hash}/placements/{provider}"},
		{"POST", "/api/torrents/abc123/switch", "/api/torrents/{hash}/switch"},
		{"GET", "/api/migrations/job-1", "/api/migrations/{id}"},
		// The whole-entry delete is still where it was, and still distinct.
		{"DELETE", "/api/torrents/radarr/abc123", "/api/torrents/{category}/{hash}"},
	}
	for _, tc := range cases {
		rctx := chi.NewRouteContext()
		if got := mux.Find(rctx, tc.method, tc.path); got != tc.wantPattern {
			t.Errorf("%s %s resolved to %q, want %q", tc.method, tc.path, got, tc.wantPattern)
		}
	}
}

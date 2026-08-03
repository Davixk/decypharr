package realdebrid

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// TestIsDeadRealDebridStatusIsAnAllowlist pins the distinction that matters:
// getStatus reaches TorrentStatusError through a catch-all `default`, so an
// unrecognised status reads as an error there. isDeadRealDebridStatus must NOT
// inherit that, because its verdict feeds destructive components.
func TestIsDeadRealDebridStatusIsAnAllowlist(t *testing.T) {
	dead := []string{"magnet_error", "error", "virus", "dead"}
	for _, s := range dead {
		if !isDeadRealDebridStatus(s) {
			t.Errorf("isDeadRealDebridStatus(%q) = false, want true", s)
		}
	}

	alive := []string{
		"downloaded", "downloading", "queued", "magnet_conversion",
		"waiting_files_selection", "compressing", "uploading",
	}
	for _, s := range alive {
		if isDeadRealDebridStatus(s) {
			t.Errorf("isDeadRealDebridStatus(%q) = true, want false", s)
		}
	}

	// The load-bearing case: statuses RealDebrid has not documented, or adds
	// later, must fall through as NOT dead and be resolved by the payload
	// probe. getStatus classifies every one of these as an error.
	unknown := []string{"", "seeding", "paused", "some_future_status"}
	for _, s := range unknown {
		if isDeadRealDebridStatus(s) {
			t.Errorf("isDeadRealDebridStatus(%q) = true; an unrecognised status must never be a dead verdict", s)
		}
		if got := getStatus(s); got != types.TorrentStatusError {
			t.Errorf("premise check: getStatus(%q) = %q, expected the catch-all error this test exists to avoid inheriting", s, got)
		}
	}
}

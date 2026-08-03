package alldebrid

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// TestIsDeadAlldebridStatusIsBoundedBelow pins the difference from
// getAlldebridStatus: that mapper's `default` arm captures NEGATIVE and
// otherwise-nonsensical codes as errors, which is harmless for "is this usable
// yet" and unacceptable for a verdict that feeds destructive components.
func TestIsDeadAlldebridStatusIsBoundedBelow(t *testing.T) {
	// AllDebrid's documented failure band: 5 Upload fail .. 11 Deleted on the
	// hoster website. Codes above the documented top still belong to the band —
	// the live API returns labels outside the published set ("Expired - Files
	// removed", "No peer after 30 minutes").
	for code := 5; code <= 20; code++ {
		if !isDeadAlldebridStatus(code) {
			t.Errorf("isDeadAlldebridStatus(%d) = false, want true", code)
		}
	}

	// 0-3 in progress, 4 Ready.
	for code := 0; code <= 4; code++ {
		if isDeadAlldebridStatus(code) {
			t.Errorf("isDeadAlldebridStatus(%d) = true, want false", code)
		}
	}

	// The load-bearing case. A malformed or absent statusCode decoding to a
	// negative value must NOT be a dead verdict — but getAlldebridStatus does
	// call it an error, which is exactly the inheritance being avoided.
	for _, code := range []int{-1, -7, -1000} {
		if isDeadAlldebridStatus(code) {
			t.Errorf("isDeadAlldebridStatus(%d) = true; an out-of-band code must never be a dead verdict", code)
		}
		if got := getAlldebridStatus(code); got != types.TorrentStatusError {
			t.Errorf("premise check: getAlldebridStatus(%d) = %q, expected the catch-all error this test exists to avoid inheriting", code, got)
		}
	}
}

package manager

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/customerror"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// TestUnrestrictProbeSplitsTakedownFromOutage covers the sweep side of the same
// split, on the probe that resolves a real link.
//
// `broken` is a DESTRUCTIVE-ELIGIBLE verdict: a fully-broken entry is what PRUNE
// deletes. Before the split this probe reached it on HosterUnavailableError,
// which on THIS path means RealDebrid code 19 or 24 — the hoster is having a bad
// day. A provider-wide outage therefore made an entire library destructive-eligible
// in one sweep, held back only by the per-run deletion cap that exists precisely
// because of this ("a debrid outage returning HosterUnavailable for everything").
// Meanwhile a genuine takedown fell into `unrestrict_link_error`, which is never
// actionable, so the one refusal that IS a content verdict was the one the sweep
// could not act on. Exactly backwards, in both directions.
func TestUnrestrictProbeSplitsTakedownFromOutage(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("d", 40)
	entry := persistLifecycleEntry(t, m, lifecycleEntry(hash, "provider", "provider-id"))
	repair := &Repair{manager: m, logger: zerolog.Nop(), parentCtx: context.Background()}

	for _, tc := range []struct {
		name       string
		err        error
		wantBroken bool
		wantReason string
	}{
		{
			name:       "legal takedown is a content verdict",
			err:        customerror.NewContentTakedownError(fmt.Errorf("infringing_file (code 35)")),
			wantBroken: true,
			wantReason: "debrid_takedown",
		},
		{
			name:       "hoster unavailable is not a content verdict here",
			err:        customerror.HosterUnavailableError,
			wantBroken: false,
			wantReason: "provider_probe_indeterminate",
		},
		{
			name:       "empty link stays broken",
			err:        debridTypes.EmptyDownloadLinkError,
			wantBroken: true,
			wantReason: "empty_download_link",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &lifecycleDebridClient{name: "provider"}
			client.getLink = func(string, *debridTypes.File) (debridTypes.DownloadLink, error) {
				return debridTypes.DownloadLink{}, tc.err
			}
			got := repair.probeTorrentFileByUnrestrict(
				context.Background(), entry, entry.Files["Movie.mkv"], "Movie.mkv",
				fileResult{name: "Movie.mkv"}, client, false,
			)
			if got.broken != tc.wantBroken || got.reason != tc.wantReason {
				t.Fatalf("probe = (broken=%t reason=%q), want (broken=%t reason=%q)",
					got.broken, got.reason, tc.wantBroken, tc.wantReason)
			}
			if got.healthy {
				t.Fatal("a refused probe scored healthy")
			}
		})
	}
}

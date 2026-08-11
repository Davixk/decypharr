package realdebrid

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// newUnrestrictErrorRealDebrid builds a client whose /unrestrict/link/ endpoint
// answers with one of RealDebrid's documented error codes.
func newUnrestrictErrorRealDebrid(t *testing.T, status, errorCode int) *RealDebrid {
	t.Helper()
	rd := newTestRealDebrid(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = fmt.Fprintf(w, `{"error":"test","error_code":%d}`, errorCode)
	})
	rd.accountsManager = account.NewManager(
		config.Debrid{Name: "test-realdebrid", DownloadAPIKeys: []string{"token"}},
		nil,
		zerolog.Nop(),
	)
	return rd
}

// TestUnrestrictErrorCodesSplitOutageFromTakedown is the regression test for the
// lumping this change exists to undo.
//
// Codes 19 (hoster temporarily unavailable), 24 (file unavailable) and 35
// (infringing file) all returned the SAME HosterUnavailableError, so "the hoster
// is having a bad day" and "this release is legally dead" were one value to every
// caller downstream. That is wrong in both directions at once: an outage could
// drive an entry to a permanent Bad verdict, and a real takedown could only be
// discovered the way an outage was.
//
// The two properties asserted per code are the ones every downstream consumer
// routes on: whether the error is a durable CONTENT verdict, and whether it is
// worth retrying.
func TestUnrestrictErrorCodesSplitOutageFromTakedown(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		errorCode int
		takedown  bool
		permanent bool
	}{
		// 4xx deliberately: the shared request client retries every 5xx to
		// exhaustion and then reports "gave up after N attempts" with the body
		// discarded, so a 5xx-delivered error code never reaches the switch under
		// test at all. That is a separate, pre-existing property of the transport
		// and not what this test is pinning.
		{name: "19 hoster temporarily unavailable", status: http.StatusForbidden, errorCode: 19},
		{name: "24 file unavailable", status: http.StatusForbidden, errorCode: 24},
		{name: "35 infringing file", status: http.StatusUnavailableForLegalReasons, errorCode: 35, takedown: true, permanent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rd := newUnrestrictErrorRealDebrid(t, tc.status, tc.errorCode)
			_, err := rd.GetDownloadLink("torrent-id", &types.File{
				Id:   "file-1",
				Name: "Movie.mkv",
				Link: "https://real-debrid.com/d/ABCDEFGHIJKLMNOPQ",
			})
			if err == nil {
				t.Fatalf("code %d resolved a download link successfully", tc.errorCode)
			}

			if got := customerror.IsContentTakedown(err); got != tc.takedown {
				t.Fatalf("code %d: IsContentTakedown = %t, want %t (err=%v)", tc.errorCode, got, tc.takedown, err)
			}
			if got := customerror.IsContentPermanentlyGone(err); got != tc.permanent {
				t.Fatalf("code %d: IsContentPermanentlyGone = %t, want %t (err=%v)", tc.errorCode, got, tc.permanent, err)
			}
			if tc.permanent {
				// A takedown must not be reachable through the outage sentinel any
				// more, or every caller that routes on it — the re-insertion
				// trigger above all — keeps treating it as recoverable.
				if errors.Is(err, customerror.HosterUnavailableError) {
					t.Fatalf("code %d still surfaces as HosterUnavailableError; the split did not take", tc.errorCode)
				}
				if customerror.IsRetriableError(err) {
					t.Fatalf("code %d is retryable; a legal takedown never becomes bytes", tc.errorCode)
				}
				return
			}
			if !errors.Is(err, customerror.HosterUnavailableError) {
				t.Fatalf("code %d lost its transient hoster-unavailable classification: %v", tc.errorCode, err)
			}
			if !customerror.IsRetriableError(err) {
				t.Fatalf("code %d is not retryable; an outage clears on its own and must be retried", tc.errorCode)
			}
		})
	}
}

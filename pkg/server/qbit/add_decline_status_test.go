package qbit

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// A per-release decline — RealDebrid answering 451 for one release, AllDebrid
// answering MAGNET_TOO_MANY, nothing cached, every file filtered out — MUST NOT
// be surfaced in a way Sonarr/Radarr read as "the download client is down".
//
// Which branch the arr takes is decided entirely by the exception its
// qBittorrent client raises from the add:
//
//	rejection / generic add failure  -> Skipped -> the arr tries the NEXT ranked
//	                                    candidate in the SAME search cycle
//	DownloadClientUnavailableException / DownloadClientAuthenticationException
//	                                 -> Failed  -> the arr marks the client down
//	                                    and defers every remaining candidate of
//	                                    that protocol to a LATER cycle
//
// This matters more since ARR-DELETE stopped blocklisting: same-cycle iteration
// down the ranked list is now the ONLY thing producing forward progress on a
// declined release. A regression that surfaced a per-release decline as a
// client-unavailable error would silently convert "try the next release" into
// "give up on this protocol until the next scheduled search" — with no
// blocklist written and no error an operator would notice.
//
// The arr's HTTP client raises the unavailable/auth exceptions for connection
// failures, timeouts and server-error responses. A 4xx WITH a body is read as an
// ordinary rejection. So the guarantee to hold is: a decline answers 4xx, never
// 5xx, and never by dropping the connection.
//
// 5xx is reserved for decypharr actually being broken, which is the one case
// where "client is down" is the true statement.
func TestAddDeclineAnswers4xxSoTheArrTriesTheNextCandidate(t *testing.T) {
	m := newQBitTestManager(t)
	q := New(m)

	// No debrid client is registered on the test manager, so the add cannot be
	// satisfied and takes the same decline path a provider refusal takes.
	form := url.Values{
		"urls":     {"magnet:?xt=urn:btih:ffffffffffffffffffffffffffffffffffffffff"},
		"category": {"radarr"},
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v2/torrents/add", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	q.handleTorrentsAdd(rec, req)

	code := rec.Code
	if code >= 500 {
		t.Fatalf("add decline answered %d — a 5xx is read by the arr as DownloadClientUnavailable, "+
			"which defers every remaining candidate of this protocol to a later search cycle "+
			"instead of trying the next ranked release now", code)
	}
	if code < 400 {
		t.Fatalf("add decline answered %d — a decline must not report success", code)
	}
	if body := strings.TrimSpace(rec.Body.String()); body == "" {
		t.Fatal("add decline answered with an empty body — the arr logs the response body as the " +
			"rejection reason, and an empty one leaves the operator with no way to tell which " +
			"provider declined or why")
	}
}

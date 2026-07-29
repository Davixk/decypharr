package sabnzbd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Sonarr's "Remove from queue" with removeFromClient=true calls
// mode=history&name=delete. Deleting an id decypharr does not have is the
// NORMAL case there: an arr routinely tracks client items it has no grab
// history for, and asks for them to be removed.
//
// Returning 500 strands those rows permanently — Sonarr treats a 5xx as a hard
// failure and keeps the row, surfacing no error, so the operator sees a button
// that does nothing. Observed on a production box with 50 rows that could not
// be cleared through the UI at all.
//
// A delete is idempotent: the caller asked for the entry to be gone, and it is
// gone. That is success.
func TestHistoryDeleteOfAbsentIDSucceeds(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"single unknown id", "3b5c5100-2d97-403b-a0c1-c4e1082f281d"},
		{"unknown id with del_files=1", "3b5c5100-2d97-403b-a0c1-c4e1082f281d&del_files=1"},
		{"several unknown ids", "unknown-a,unknown-b,unknown-c"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newFakeNNTPServer(t, true)
			s, _ := newSABTestHarness(t, server)

			req := httptest.NewRequest(http.MethodGet,
				"/api?mode=history&name=delete&archive=0&value="+tc.value, nil)
			recorder := httptest.NewRecorder()
			s.Routes().ServeHTTP(recorder, req)

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%q\n"+
					"a 5xx here makes the row unremovable from the arr's UI",
					recorder.Code, recorder.Body.String())
			}

			var response StatusResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatalf("decode response: %v (body=%q)", err, recorder.Body.String())
			}
			if !response.Status {
				t.Fatalf("response = %+v, want status true", response)
			}
		})
	}
}

// A delete that names a real entry must still actually delete it — the
// tolerance above must not degrade into "report success and do nothing".
func TestHistoryDeleteOfPresentIDStillDeletes(t *testing.T) {
	server := newFakeNNTPServer(t, true)
	s, m := newSABTestHarness(t, server)

	entry := addFailedNZBEntry(t, m, "delete-me-1", "articles missing on provider", 1)

	if _, err := m.Queue().GetTorrent(entry.InfoHash); err != nil {
		t.Fatalf("seeded entry not in queue: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet,
		"/api?mode=history&name=delete&archive=0&value="+entry.InfoHash, nil)
	recorder := httptest.NewRecorder()
	s.Routes().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", recorder.Code, recorder.Body.String())
	}
	if _, err := m.Queue().GetTorrent(entry.InfoHash); err == nil {
		t.Fatal("entry still present after a successful delete")
	}
}

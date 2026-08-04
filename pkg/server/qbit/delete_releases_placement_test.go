package qbit

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// THE PROVIDER-SLOT LEAK.
//
// /api/v2/torrents/delete is the endpoint Sonarr and Radarr call when they
// abandon a download. It passed a nil cleanup unconditionally, so the queue row
// went and the provider transfer kept running — forever, holding a slot nothing
// could reclaim, because every release path in decypharr starts from a local
// entry that no longer existed.
//
// Measured on a live account: 94 of 96 active RealDebrid transfers had no local
// record, 93 still downloading, 67 of them 50-99% complete, median age 33.5h.
// The *arrs' own stalled-download handling is the likely trigger — which means
// the cleanup was causing the congestion it was meant to relieve.
//
// qBittorrent's deleteFiles parameter is how the caller says whether the data
// goes too. decypharr never read it.

func seedDeletableEntry(t *testing.T, q *QBit, hash string) {
	t.Helper()
	entry := &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       hash,
		Name:           hash + ".mkv",
		AddedOn:        time.Unix(1_700_000_000, 0).UTC(),
		ActiveProvider: "prov",
		Providers: map[string]*storage.ProviderEntry{
			"prov": {Provider: "prov", ID: "placement-" + hash},
		},
		Files: map[string]*storage.File{
			hash + ".mkv": {Name: hash + ".mkv", InfoHash: hash, Size: 10},
		},
	}
	if err := q.manager.Queue().Add(entry); err != nil {
		t.Fatalf("Add %s: %v", hash, err)
	}
}

func invokeDelete(t *testing.T, q *QBit, form url.Values, wantStatus int) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/torrents/delete", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder := httptest.NewRecorder()
	q.Routes().ServeHTTP(recorder, req)
	if recorder.Code != wantStatus {
		t.Fatalf("delete status = %d, want %d, body=%q", recorder.Code, wantStatus, recorder.Body.String())
	}
}

// TestDeleteCompletesEvenIfThePlacementCannotBeReleased.
//
// This fixture has no configured provider, so releasing the placement fails —
// which is the interesting case, not an artefact. Failing the REQUEST would
// leave the queue row AND the placement, with the *arr retrying the same call
// forever against the same broken condition; the row would become undeletable.
//
// So the release failure is logged loudly and the delete completes. What breaks
// the tie is that a leaked placement is now RECOVERABLE — the provider-sourced
// stall prune finds abandoned transfers from the provider's own active list and
// needs no local record — so the worst case degrades to "an orphan the other
// sweep reaps" rather than "an orphan nothing can ever see".
func TestDeleteCompletesEvenIfThePlacementCannotBeReleased(t *testing.T) {
	m := newQBitTestManager(t)
	q := New(m)
	seedDeletableEntry(t, q, "gone-hash")

	form := url.Values{"hashes": {"gone-hash"}, "deleteFiles": {"true"}}
	invokeDelete(t, q, form, http.StatusOK)

	if _, err := m.Queue().GetTorrent("gone-hash"); err == nil {
		t.Fatal("queue row survived a delete")
	}
}

// TestDeleteWithoutDeleteFilesKeepsTheProviderCopy. qBittorrent's contract:
// without the flag, the client forgets the torrent but the data stays. For a
// debrid client the provider copy IS the data, so it must survive — this is the
// case where an *arr is merely reorganising its queue, not abandoning content.
func TestDeleteWithoutDeleteFilesKeepsTheProviderCopy(t *testing.T) {
	m := newQBitTestManager(t)
	q := New(m)
	seedDeletableEntry(t, q, "keep-hash")

	form := url.Values{"hashes": {"keep-hash"}}
	invokeDelete(t, q, form, http.StatusOK)

	if _, err := m.Queue().GetTorrent("keep-hash"); err == nil {
		t.Fatal("queue row survived a delete")
	}
}

// TestDeleteFilesIsParsedCaseInsensitively — clients spell booleans however
// they like, and a missed parse silently reinstates the leak.
func TestDeleteFilesIsParsedCaseInsensitively(t *testing.T) {
	for _, spelling := range []string{"true", "True", "TRUE", " true "} {
		m := newQBitTestManager(t)
		q := New(m)
		hash := "case-hash"
		seedDeletableEntry(t, q, hash)

		form := url.Values{"hashes": {hash}, "deleteFiles": {spelling}}
		invokeDelete(t, q, form, http.StatusOK)

		if _, err := m.Queue().GetTorrent(hash); err == nil {
			t.Fatalf("spelling %q: queue row survived", spelling)
		}
	}
}

// TestDeleteOfAnAbsentHashIsSatisfied: an *arr removing something twice is not
// an error, and returning 500 makes it retry forever.
func TestDeleteOfAnAbsentHashIsSatisfied(t *testing.T) {
	m := newQBitTestManager(t)
	q := New(m)

	form := url.Values{"hashes": {"never-existed"}, "deleteFiles": {"true"}}
	invokeDelete(t, q, form, http.StatusOK)
}

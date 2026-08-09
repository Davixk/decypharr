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

// ⚠️ THE DOCTRINE THIS FILE ONCE ASSERTED WAS REVERSED. Read this before
// "restoring" anything.
//
// /api/v2/torrents/delete is the endpoint Sonarr and Radarr call. This file used
// to argue that honouring qBittorrent's deleteFiles parameter against the
// PROVIDER was the fix for a slot leak — 94 of 96 active RealDebrid transfers
// with no local record. That leak was real. Honouring the flag here was not the
// fix, and it destroyed the library.
//
// An *arr's routine POST-IMPORT cleanup sends deleteFiles=true. By then it has
// already imported the release as a symlink pointing INTO the mount, so the
// "downloaded data" it means and the bytes the library now depends on are the
// same bytes. Measured: 2,592 provider-copy releases in 24h; MissingFromDisk
// reaps climbing 56/day to 8,302/day; each one re-searched and re-grabbed.
//
// The endpoint now passes a nil cleanup unconditionally, matching upstream. The
// slot leak belongs to the provider-sourced stall prune, which reaps abandoned
// transfers from the provider's OWN active list with no local record needed —
// machinery that did not exist when the original change was made.
//
// ⚠️ LIMIT OF THESE TESTS, STATED HONESTLY: this fixture has no configured
// provider, so "the provider copy was not released" is NOT mechanically
// observable here — these tests would pass under either behaviour. The real
// guard is at the manager layer, where a fake client can record deletes. Do not
// read a green run in this package as proof the invariant holds.

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

// A delete carrying deleteFiles=true still drops the queue row — the caller is
// entitled to have its queue item removed. What it must NOT do is take the
// provider copy with it; see the file header.
func TestDeleteWithDeleteFilesStillDropsTheQueueRow(t *testing.T) {
	m := newQBitTestManager(t)
	q := New(m)
	seedDeletableEntry(t, q, "gone-hash")

	form := url.Values{"hashes": {"gone-hash"}, "deleteFiles": {"true"}}
	invokeDelete(t, q, form, http.StatusOK)

	if _, err := m.Queue().GetTorrent("gone-hash"); err == nil {
		t.Fatal("queue row survived a delete")
	}
}

// The same outcome without the flag. Both spellings now behave identically at
// this endpoint, which is the point: the flag no longer selects between two
// behaviours, it is only recorded.
func TestDeleteWithoutDeleteFilesDropsTheQueueRow(t *testing.T) {
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

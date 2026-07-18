package qbit

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestHandleSetCategoryTargetsOnlyRequestedTorrentAndSupportsAll(t *testing.T) {
	m := newQBitTestManager(t)
	for _, hash := range []string{"target-hash", "second-hash", "other-hash"} {
		entry := &storage.Entry{
			Protocol: config.ProtocolTorrent,
			InfoHash: hash,
			Name:     hash + ".mkv",
			AddedOn:  time.Unix(1_700_000_000, 0).UTC(),
			Files: map[string]*storage.File{
				hash + ".mkv": {Name: hash + ".mkv", InfoHash: hash, Size: 10},
			},
		}
		if err := m.Queue().Add(entry); err != nil {
			t.Fatalf("Add %s: %v", hash, err)
		}
	}
	q := New(m)

	form := url.Values{"category": {"selected"}}
	form.Add("hashes", " TARGET-HASH | second-hash ")
	form.Add("hashes", "  |  ")
	invokeRoutedSetCategory(t, q, form, http.StatusOK)
	target, err := m.Queue().GetTorrent("target-hash")
	if err != nil {
		t.Fatalf("Get target: %v", err)
	}
	other, err := m.Queue().GetTorrent("other-hash")
	if err != nil {
		t.Fatalf("Get other: %v", err)
	}
	if target.Category != "selected" {
		t.Fatalf("target category = %q, want selected", target.Category)
	}
	second, err := m.Queue().GetTorrent("second-hash")
	if err != nil {
		t.Fatalf("Get second: %v", err)
	}
	if second.Category != "selected" {
		t.Fatalf("second category = %q, want selected", second.Category)
	}
	if other.Category != "" {
		t.Fatalf("untargeted category changed to %q", other.Category)
	}

	invokeRoutedSetCategory(t, q, url.Values{
		"hashes":   {"  AlL  "},
		"category": {"everyone"},
	}, http.StatusOK)
	for _, hash := range []string{"target-hash", "second-hash", "other-hash"} {
		entry, err := m.Queue().GetTorrent(hash)
		if err != nil {
			t.Fatalf("Get %s after all: %v", hash, err)
		}
		if entry.Category != "everyone" {
			t.Fatalf("%s category = %q, want everyone", hash, entry.Category)
		}
	}
}

func TestHandleSetCategoryRequiresHashes(t *testing.T) {
	m := newQBitTestManager(t)
	q := New(m)
	invokeRoutedSetCategory(t, q, url.Values{
		"hashes":   {"  |   |  "},
		"category": {"selected"},
	}, http.StatusBadRequest)
}

func newQBitTestManager(t *testing.T) *manager.Manager {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	config.Get().UseAuth = false
	m := manager.New()
	t.Cleanup(func() {
		if err := m.Stop(); err != nil {
			t.Errorf("Stop manager: %v", err)
		}
	})
	return m
}

func invokeRoutedSetCategory(t *testing.T, q *QBit, form url.Values, wantStatus int) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/torrents/setCategory", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder := httptest.NewRecorder()
	q.Routes().ServeHTTP(recorder, req)
	if recorder.Code != wantStatus {
		t.Fatalf("setCategory status = %d, want %d, body=%q", recorder.Code, wantStatus, recorder.Body.String())
	}
}

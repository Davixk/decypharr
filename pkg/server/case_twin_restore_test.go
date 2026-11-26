package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/appendstore"
	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"google.golang.org/protobuf/proto"
)

// The restore route is driven through serveCaseTwinRestore — the exact code
// the handler runs — with a real *storage.Storage as the restorer, since no
// server test can build a manager. Stored bytes are read straight from the
// entries database file while the store is closed, so "byte for byte" means
// the bytes on disk, not a re-encoding.

var (
	routeLower = strings.Repeat("d7", 20)
	routeUpper = strings.ToUpper(routeLower)
)

func restoreTwinEntry(hash, name, active string) *storage.Entry {
	added := time.Unix(1_700_000_000, 0).UTC()
	return &storage.Entry{
		Protocol:         config.ProtocolTorrent,
		InfoHash:         hash,
		Name:             name,
		OriginalFilename: name,
		ActiveProvider:   active,
		Status:           debridTypes.TorrentStatusDownloaded,
		AddedOn:          added,
		Providers: map[string]*storage.ProviderEntry{
			"ad": {Provider: "ad", ID: "ad-1", Status: debridTypes.TorrentStatusDownloaded},
		},
		Files: map[string]*storage.File{
			"movie.mkv": {Name: "movie.mkv", Size: 100, InfoHash: hash, AddedOn: added},
		},
	}
}

// rawEntries opens the closed store's entries database directly.
func rawEntries(t *testing.T, dir string) *appendstore.Store {
	t.Helper()
	raw, err := appendstore.Open(filepath.Join(dir, "entries.db"), appendstore.Options{})
	if err != nil {
		t.Fatalf("open raw entries store: %v", err)
	}
	return raw
}

// rawBytes returns the stored bytes of keys from the closed store at dir;
// absent keys map to nil.
func rawBytes(t *testing.T, dir string, keys ...string) map[string][]byte {
	t.Helper()
	raw := rawEntries(t, dir)
	defer func() { _ = raw.Close() }()
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if !raw.Exists(key) {
			out[key] = nil
			continue
		}
		value, err := raw.Get(key)
		if err != nil {
			t.Fatalf("raw read %s: %v", key, err)
		}
		out[key] = append([]byte(nil), value...)
	}
	return out
}

func openRouteStore(t *testing.T, dir string) *storage.Storage {
	t.Helper()
	store, err := storage.NewStorage(dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	return store
}

// mergedRouteStore seeds a legacy twin pair the way old builds stored it and
// records the pre-merge bytes, then merges. The caller owns the open store.
func mergedRouteStore(t *testing.T) (*storage.Storage, string, string, map[string][]byte) {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	dir := t.TempDir()
	raw := rawEntries(t, dir)
	for _, row := range []*storage.Entry{restoreTwinEntry(routeUpper, "Upper Name", "ad"), restoreTwinEntry(routeLower, "Lower Name", "")} {
		data, err := proto.Marshal(storage.EntryToProto(row))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if err := raw.Put(row.InfoHash, data, nil); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw store: %v", err)
	}
	// One open/close lets startup migrations version and re-index the legacy
	// rows; the bytes captured after it are the pre-merge state the journal
	// records, and reopening does not change them again.
	if err := openRouteStore(t, dir).Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	original := rawBytes(t, dir, routeUpper, routeLower)

	store := openRouteStore(t, dir)
	report, err := store.MergeCaseTwins(false)
	if err != nil || report.Merged != 1 || report.JournalName == "" {
		_ = store.Close()
		t.Fatalf("merge = %+v, %v", report, err)
	}
	return store, dir, report.JournalName, original
}

func postRestore(t *testing.T, store *storage.Storage, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/maintenance/merge-case-twins/restore", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	serveCaseTwinRestore(rec, req, store, zerolog.Nop())
	return rec
}

func decodeRestoreReport(t *testing.T, rec *httptest.ResponseRecorder) storage.CaseTwinRestoreReport {
	t.Helper()
	var report storage.CaseTwinRestoreReport
	if err := json.Unmarshal(rec.Body.Bytes(), &report); err != nil {
		t.Fatalf("decode report (%d %s): %v", rec.Code, rec.Body.String(), err)
	}
	return report
}

// dry_run omitted means a dry run; an explicit false restores the exact bytes.
func TestCaseTwinRestoreRouteDefaultsToDryRunThenRestoresByteForByte(t *testing.T) {
	store, dir, journal, original := mergedRouteStore(t)
	closed := false
	defer func() {
		if !closed {
			_ = store.Close()
		}
	}()

	rec := postRestore(t, store, `{"journal":"`+journal+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("dry restore status %d: %s", rec.Code, rec.Body.String())
	}
	if report := decodeRestoreReport(t, rec); !report.DryRun || report.RowsRestored != 0 || report.Results[0].Action != storage.CaseTwinRestoreWouldRestore {
		t.Fatalf("default restore = %+v; want a dry run that would restore", report)
	}
	if entry, err := store.Get(routeUpper); err != nil || entry.InfoHash != routeLower {
		t.Fatalf("after a dry restore Get(upper) = %+v, %v; want the merged lowercase row still in place", entry, err)
	}

	rec = postRestore(t, store, `{"journal":"`+journal+`","dry_run":false}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("restore status %d: %s", rec.Code, rec.Body.String())
	}
	if report := decodeRestoreReport(t, rec); report.DryRun || report.RowsRestored != 2 || report.Results[0].Action != storage.CaseTwinRestoreRestored {
		t.Fatalf("restore = %+v; want 2 rows restored", report)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	closed = true
	restored := rawBytes(t, dir, routeUpper, routeLower)
	for key, want := range original {
		if want == nil || !bytes.Equal(restored[key], want) {
			t.Fatalf("row %s on disk after the restore differs from its pre-merge bytes", key)
		}
	}
}

// Paths are refused with 400, a missing journal is 404, and a row written
// since the merge is refused and keeps its write.
func TestCaseTwinRestoreRouteRefusesPathsAndChangedRows(t *testing.T) {
	store, _, journal, _ := mergedRouteStore(t)
	defer func() { _ = store.Close() }()

	for _, name := range []string{"../" + journal, "/etc/passwd", "sub/" + journal, "..", ""} {
		body, _ := json.Marshal(map[string]any{"journal": name, "dry_run": false, "force": true})
		if rec := postRestore(t, store, string(body)); rec.Code != http.StatusBadRequest {
			t.Errorf("journal %q answered %d, want 400: %s", name, rec.Code, rec.Body.String())
		}
	}
	if rec := postRestore(t, store, `{"journal":"case-twin-merge-missing.jsonl"}`); rec.Code != http.StatusNotFound {
		t.Errorf("a missing journal answered %d, want 404", rec.Code)
	}

	if _, _, err := store.MutateEntryIfPresent(routeLower, func(entry *storage.Entry) (bool, error) {
		entry.Category = "after-merge"
		return true, nil
	}); err != nil {
		t.Fatalf("post-merge write: %v", err)
	}
	rec := postRestore(t, store, `{"journal":"`+journal+`","dry_run":false}`)
	report := decodeRestoreReport(t, rec)
	if rec.Code != http.StatusOK || report.Refused != 1 || report.Results[0].Action != storage.CaseTwinRestoreRefused {
		t.Fatalf("restore over a changed row = %d %+v; want refused", rec.Code, report)
	}
	if entry, err := store.Get(routeLower); err != nil || entry.Category != "after-merge" {
		t.Fatalf("a refused restore rolled back the post-merge write: %+v, %v", entry, err)
	}
}

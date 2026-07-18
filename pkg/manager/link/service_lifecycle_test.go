package link

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestPlacementRefreshPreservesQueueWorkflowFields(t *testing.T) {
	hash := strings.Repeat("a", 40)
	original := linkLifecycleEntry(hash, "old", "old-id")
	original.Action = config.DownloadActionDownload
	original.CallbackURL = "https://callback.invalid/keep"
	original.Category = "radarr"
	original.SavePath = "/downloads/keep"
	original.LastError = "terminal workflow state"
	original.Status = debridTypes.TorrentStatusError
	original.Progress = 0.42
	original.Bad = true
	original.Providers["old"].Files = map[string]*storage.ProviderFile{}

	refreshed := linkLifecycleEntry(hash, "new", "new-id")
	service := New(
		xsync.NewMap[string, debrid.Client](),
		func(expected *storage.Entry) (*storage.Entry, error) {
			if expected != original {
				t.Fatalf("refresher received a different workflow snapshot")
			}
			return refreshed, nil
		},
		nil,
		nil,
		http.DefaultClient,
		0,
		zerolog.Nop(),
	)

	file, err := service.getPlacementFile(original, "Movie.mkv")
	if err != nil {
		t.Fatalf("getPlacementFile: %v", err)
	}
	if file.Id != "file-new-id" || original.ActiveProvider != "new" {
		t.Fatalf("provider refresh did not update placement: file=%+v active=%q", file, original.ActiveProvider)
	}
	if original.Action != config.DownloadActionDownload || original.CallbackURL != "https://callback.invalid/keep" ||
		original.Category != "radarr" || original.SavePath != "/downloads/keep" || original.LastError != "terminal workflow state" ||
		original.Status != debridTypes.TorrentStatusError || original.Progress != 0.42 || !original.Bad {
		t.Fatalf("provider refresh overwrote queue workflow fields: %+v", original)
	}
	refreshed.Providers["new"].Files["Movie.mkv"].Id = "mutated-after-refresh"
	if original.Providers["new"].Files["Movie.mkv"].Id != "file-new-id" {
		t.Fatal("provider refresh retained mutable aliases to the main snapshot")
	}
}

func TestPlacementRefreshErrorDoesNotMutateCaller(t *testing.T) {
	hash := strings.Repeat("b", 40)
	original := linkLifecycleEntry(hash, "old", "old-id")
	original.Providers["old"].Files = map[string]*storage.ProviderFile{}
	original.Category = "replacement-sensitive"
	service := New(
		xsync.NewMap[string, debrid.Client](),
		func(*storage.Entry) (*storage.Entry, error) {
			return nil, fmt.Errorf("%w for main entry %s", storage.ErrStaleEntryGeneration, hash)
		},
		nil,
		nil,
		http.DefaultClient,
		0,
		zerolog.Nop(),
	)

	if _, err := service.getPlacementFile(original, "Movie.mkv"); err == nil {
		t.Fatal("stale refresh unexpectedly succeeded")
	}
	if original.ActiveProvider != "old" || original.Category != "replacement-sensitive" || original.Providers["old"].ID != "old-id" {
		t.Fatalf("failed stale refresh mutated caller: %+v", original)
	}
}

func TestSingleflightKeyIncludesMainRevisionAndGeneration(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	hash := strings.Repeat("c", 40)
	entry := linkLifecycleEntry(hash, "provider", "first-id")
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate first: %v", err)
	}
	first, _ := store.Get(hash)
	firstKey := entrySingleflightKey(first, "Movie.mkv")

	first.Category = "revision-two"
	if err := store.AddOrUpdate(first); err != nil {
		t.Fatalf("advance revision: %v", err)
	}
	second, _ := store.Get(hash)
	secondKey := entrySingleflightKey(second, "Movie.mkv")
	if firstKey == secondKey {
		t.Fatal("singleflight key did not change with the main-store revision")
	}

	deleted, err := store.DeleteIfCurrent(second)
	if err != nil || !deleted {
		t.Fatalf("DeleteIfCurrent = (%v, %v)", deleted, err)
	}
	replacement := linkLifecycleEntry(hash, "provider", "replacement-id")
	if err := store.AddOrUpdate(replacement); err != nil {
		t.Fatalf("AddOrUpdate replacement: %v", err)
	}
	third, _ := store.Get(hash)
	thirdKey := entrySingleflightKey(third, "Movie.mkv")
	if secondKey == thirdKey {
		t.Fatal("singleflight key conflated delete/re-add generations")
	}
}

func linkLifecycleEntry(hash, provider, id string) *storage.Entry {
	added := time.Unix(1_700_000_000, 0).UTC()
	return &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       hash,
		Name:           "Movie.mkv",
		Size:           100,
		Bytes:          100,
		Magnet:         "magnet:?xt=urn:btih:" + hash,
		ActiveProvider: provider,
		Providers: map[string]*storage.ProviderEntry{
			provider: {
				Provider: provider,
				ID:       id,
				Status:   debridTypes.TorrentStatusDownloaded,
				Files: map[string]*storage.ProviderFile{
					"Movie.mkv": {Id: "file-" + id, Link: "https://example.invalid/" + id},
				},
			},
		},
		Files: map[string]*storage.File{
			"Movie.mkv": {Name: "Movie.mkv", Size: 100, InfoHash: hash, AddedOn: added},
		},
		Status:     debridTypes.TorrentStatusDownloaded,
		IsComplete: true,
		AddedOn:    added,
		CreatedAt:  added,
		UpdatedAt:  added,
	}
}

package manager

import (
	"path/filepath"
	"testing"

	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// TestTorrentQueueEntryNeverCollapsesToSavePath pins the invariant that a
// torrent queue entry's download path is always strictly below its SavePath.
//
// A magnet carries its display name in the optional "dn" parameter, so a bare
// `magnet:?xt=urn:btih:<hash>` — which is what a re-add probe and some grabs
// produce — parses with an empty Name. filepath.Join(SavePath, "") cleans back
// to SavePath, and for an *arr entry SavePath is the shared category directory,
// so every path derived from the entry would point at a directory owned by all
// of its siblings.
func TestTorrentQueueEntryNeverCollapsesToSavePath(t *testing.T) {
	const infoHash = "c0e8694ca9b09d0117eb57b03ed4395ccb7ae9c8"

	cases := []struct {
		name       string
		magnetName string
	}{
		{"bare magnet with no dn", ""},
		{"dn present but only whitespace", "   "},
		{"dn present but only dots", "..."},
		{"usable dn", "Asteroid.City.2023.2160p.WEBDL-NAHOM"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := &ImportRequest{
				Magnet: &utils.Magnet{
					InfoHash: infoHash,
					Name:     tc.magnetName,
				},
				Arr:            &arr.Arr{Name: "radarr"},
				DownloadFolder: filepath.Join("mnt", "downloads"),
			}

			entry := newTorrentQueueEntry(req, debridTypes.TorrentStatusQueued)

			savePath := filepath.Clean(entry.SavePath)
			downloadPath := filepath.Clean(entry.DownloadPath())

			if downloadPath == savePath {
				t.Fatalf("download path collapsed onto the category directory %q; "+
					"deleting this entry would target every sibling entry's files", savePath)
			}
			if !utils.IsUsableName(entry.Name) {
				t.Fatalf("entry name %q is not a usable path component", entry.Name)
			}

			// The entry must stay addressable by its own identity.
			if entry.InfoHash != infoHash {
				t.Fatalf("infohash = %q, want %q", entry.InfoHash, infoHash)
			}
			// A usable dn must be preserved verbatim, not replaced.
			if utils.IsUsableName(tc.magnetName) && entry.Name != tc.magnetName {
				t.Fatalf("usable magnet name %q was replaced with %q", tc.magnetName, entry.Name)
			}
		})
	}
}

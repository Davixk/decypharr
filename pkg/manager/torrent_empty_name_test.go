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
			// 🔴 THE FIELD THIS TEST DID NOT CHECK, AND WHAT THAT COST.
			//
			// Asserting Name alone read as full coverage of "an entry is never
			// nameless". It covered one of two fields: two of the five
			// folder-naming modes read OriginalFilename, which had no fallback,
			// so under those modes a correctly-named entry still resolved to
			// path.Clean("") — ".". Measured on a live deployment: 3,864 of
			// 3,870 downloads from one indexer, every one stored with "." as
			// its name attribute and its folder.
			if !utils.IsUsableName(entry.OriginalFilename) {
				t.Fatalf("original filename %q is not a usable path component; the folder-naming modes that "+
					"read this field resolve it to %q, which is the shared parent directory",
					entry.OriginalFilename, ".")
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

// 🛑 THE PROVIDER'S ANSWER CANNOT UN-NAME AN ENTRY.
//
// The placeholder is applied at creation, and then the provider's response is
// copied over it. That copy was unconditional, so a provider returning an empty
// name reached straight past the fallback and left the entry nameless — the
// same "." outcome, arriving from the opposite direction and later in time.
//
// A nameless entry has to be a TRANSIENT state. Nothing is allowed to make it
// permanent, least of all the step whose job is to resolve it.
func TestProviderResponseNeverBlanksAName(t *testing.T) {
	const infoHash = "c0e8694ca9b09d0117eb57b03ed4395ccb7ae9c8"

	for _, tc := range []struct {
		name         string
		providerName string
		providerOrig string
	}{
		{"provider returns nothing", "", ""},
		{"provider returns whitespace", "   ", "   "},
		{"provider returns dots", "...", "..."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := newTorrentQueueEntry(&ImportRequest{
				Magnet:         &utils.Magnet{InfoHash: infoHash},
				Arr:            &arr.Arr{Name: "radarr"},
				DownloadFolder: filepath.Join("mnt", "downloads"),
			}, debridTypes.TorrentStatusQueued)

			applyDebridTorrentToEntry(entry, &debridTypes.Torrent{
				InfoHash:         infoHash,
				Name:             tc.providerName,
				OriginalFilename: tc.providerOrig,
				Debrid:           "rd",
			})

			if !utils.IsUsableName(entry.Name) {
				t.Fatalf("the provider's empty name overwrote the placeholder; entry name is now %q", entry.Name)
			}
			if !utils.IsUsableName(entry.OriginalFilename) {
				t.Fatalf("the provider's empty original filename overwrote the placeholder; it is now %q",
					entry.OriginalFilename)
			}
		})
	}

	// And a real name from the provider MUST replace the placeholder — that is
	// the whole point of the placeholder being temporary.
	entry := newTorrentQueueEntry(&ImportRequest{
		Magnet:         &utils.Magnet{InfoHash: infoHash},
		Arr:            &arr.Arr{Name: "radarr"},
		DownloadFolder: filepath.Join("mnt", "downloads"),
	}, debridTypes.TorrentStatusQueued)

	applyDebridTorrentToEntry(entry, &debridTypes.Torrent{
		InfoHash:         infoHash,
		Name:             "Asteroid.City.2023.2160p.WEBDL-NAHOM",
		OriginalFilename: "asteroid.city.2023.original",
		Debrid:           "rd",
	})

	if entry.Name != "Asteroid.City.2023.2160p.WEBDL-NAHOM" {
		t.Fatalf("the provider resolved a real name and it was not adopted; entry name is %q", entry.Name)
	}
	if entry.OriginalFilename != "asteroid.city.2023.original" {
		t.Fatalf("the provider's original filename was not adopted; it is %q", entry.OriginalFilename)
	}
}

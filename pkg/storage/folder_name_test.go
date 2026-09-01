package storage

import (
	"path/filepath"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

// A FOLDER NAME IS NEVER "." — UNDER ANY NAMING MODE, FOR ANY ENTRY.
//
// path.Clean("") returns ".", and "." is not a name: it is THIS DIRECTORY.
// filepath.Join(mount, "__all__", ".") is "__all__" itself, so every nameless
// entry aliases onto the directory all of its siblings share — the same shape
// as the category-directory data-loss incident, reached by a different route.
//
// This is a funnel test on purpose. The naming MODE decides which field is
// read, so a fallback applied to one field is not a fallback at all under a
// different config — which is exactly how 3,864 of 3,870 downloads from one
// indexer came to be stored with "." as their name attribute while their Name
// field held a perfectly good infohash placeholder.
func TestGetTorrentFolderNeverReturnsCurrentDirectory(t *testing.T) {
	const infoHash = "60e1c97e2b9a2ba0f4f66b6d0b1b1c8e5a4d3c2b"

	modes := []struct {
		name string
		mode config.WebDavFolderNaming
	}{
		{"filename", config.WebDavUseFileName},
		{"original name", config.WebDavUseOriginalName},
		{"filename no ext", config.WebDavUseFileNameNoExt},
		{"original name no ext", config.WebDavUseOriginalNameNoExt},
		{"hash", config.WebdavUseHash},
		{"default", config.WebDavFolderNaming("something-unrecognised")},
	}

	// Every combination of unusable name material a magnet with no "dn" can
	// produce. The entry still has an infohash — it always does.
	entries := []struct {
		name  string
		entry *Entry
	}{
		{"both empty", &Entry{InfoHash: infoHash}},
		{"both whitespace", &Entry{InfoHash: infoHash, Name: "  ", OriginalFilename: "  "}},
		{"both dots", &Entry{InfoHash: infoHash, Name: "...", OriginalFilename: "..."}},
		{"name set, original empty", &Entry{InfoHash: infoHash, Name: infoHash}},
		{"original set, name empty", &Entry{InfoHash: infoHash, OriginalFilename: infoHash}},
	}

	for _, m := range modes {
		for _, e := range entries {
			t.Run(m.name+"/"+e.name, func(t *testing.T) {
				folder := GetTorrentFolder(m.mode, e.entry)

				if folder == "." || folder == "" {
					t.Fatalf("folder = %q. Every path derived from this entry now points at the directory "+
						"its siblings share, and the entry is indistinguishable from every other nameless one", folder)
				}
				// The stated fallback is the identity no two entries share.
				if folder != infoHash {
					t.Fatalf("folder = %q, want the infohash %q as the fallback", folder, infoHash)
				}
				// And the joined path must stay strictly below its parent.
				parent := filepath.Join("mnt", "__all__")
				if filepath.Clean(filepath.Join(parent, folder)) == filepath.Clean(parent) {
					t.Fatalf("joining folder %q onto %q collapsed back onto the parent", folder, parent)
				}
			})
		}
	}
}

// A REAL NAME IS NEVER REPLACED. The fallback must be reachable only by entries
// that have nothing usable — otherwise it would quietly rename the library.
func TestGetTorrentFolderPreservesUsableNames(t *testing.T) {
	entry := &Entry{
		InfoHash:         "60e1c97e2b9a2ba0f4f66b6d0b1b1c8e5a4d3c2b",
		Name:             "Asteroid.City.2023.2160p.WEBDL-NAHOM.mkv",
		OriginalFilename: "asteroid.city.2023.original.mkv",
	}

	if got := GetTorrentFolder(config.WebDavUseFileName, entry); got != entry.Name {
		t.Errorf("filename mode = %q, want %q", got, entry.Name)
	}
	if got := GetTorrentFolder(config.WebDavUseOriginalName, entry); got != entry.OriginalFilename {
		t.Errorf("original-name mode = %q, want %q", got, entry.OriginalFilename)
	}
	if got := GetTorrentFolder(config.WebDavUseFileNameNoExt, entry); got != "Asteroid.City.2023.2160p.WEBDL-NAHOM" {
		t.Errorf("filename-no-ext mode = %q", got)
	}
	if got := GetTorrentFolder(config.WebdavUseHash, entry); got != entry.InfoHash {
		t.Errorf("hash mode = %q, want the infohash", got)
	}
}

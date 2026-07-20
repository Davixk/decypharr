package parser

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// determineNZBName must never return an unusable (collapsing) name when a
// usable source is available, and must return "" only when every source
// collapses — so the caller can substitute the unique NZB ID.
func TestDetermineNZBName(t *testing.T) {
	cases := []struct {
		name     string
		filename string
		meta     map[string]string
		want     string
	}{
		{"valid filename strips ext", "Great.Movie.2024.nzb", nil, "Great.Movie.2024"},
		{"empty filename, no meta", "", nil, ""},
		{"bare extension collapses", ".nzb", nil, ""},
		{"all invalid chars collapse", "???.nzb", nil, ""},
		{"stars collapse", "***.nzb", nil, ""},
		{"collapsing filename falls back to meta Name", "???.nzb", map[string]string{"Name": "Fallback Movie"}, "Fallback Movie"},
		{"collapsing filename falls back to title", "***.nzb", map[string]string{"title": "Title Movie"}, "Title Movie"},
		{"meta Name preferred over title", "", map[string]string{"Name": "By Name", "title": "By Title"}, "By Name"},
		{"multi-space release name preserved", "www.UIndex.org    Some.Movie.mkv", nil, "www.UIndex.org    Some.Movie"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := determineNZBName(tc.filename, tc.meta); got != tc.want {
				t.Fatalf("determineNZBName(%q, %v) = %q, want %q", tc.filename, tc.meta, got, tc.want)
			}
		})
	}
}

// Parse must substitute the unique NZB ID whenever the derived name is unusable
// (".nzb"/"???.nzb"/"***.nzb"/""), so the resulting DownloadPath() can never
// collapse onto the category SavePath — the confirmed data-loss trigger.
func TestParseSubstitutesUnusableNameWithID(t *testing.T) {
	server := newFakeStatServer(t, "223 0 %s")
	host, port := server.hostPort(t)
	parser := newGatingParser(t, host, port)

	savePath := filepath.Join("downloads", "radarr")
	for _, filename := range []string{"", ".nzb", "???.nzb", "***.nzb"} {
		nzb, _, err := parser.Parse(context.Background(), filename, []byte(gatingTestNZB))
		if err != nil {
			t.Fatalf("Parse(%q): %v", filename, err)
		}
		if !utils.IsUsableName(nzb.Name) {
			t.Fatalf("Parse(%q) produced unusable name %q", filename, nzb.Name)
		}
		if nzb.Name != nzb.ID {
			t.Fatalf("Parse(%q) name = %q, want the substituted ID %q", filename, nzb.Name, nzb.ID)
		}
		entry := &storage.Entry{SavePath: savePath, Name: nzb.Name}
		if entry.DownloadPath() == filepath.Clean(savePath) {
			t.Fatalf("Parse(%q): DownloadPath %q collapsed onto SavePath %q", filename, entry.DownloadPath(), savePath)
		}
	}
}

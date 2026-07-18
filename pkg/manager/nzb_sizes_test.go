package manager

import (
	"errors"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

func TestNormalizedNZBEntrySizesRequiresExactGeneration(t *testing.T) {
	entry := &storage.Entry{
		InfoHash:      "same-id",
		NZBGeneration: "current-generation",
		Files: map[string]*storage.File{
			"video.mkv": {Name: "video.mkv", Size: 100},
		},
	}
	metadata := &storage.NZB{
		ID:         entry.InfoHash,
		Generation: "stale-generation",
		Files:      []storage.NZBFile{{Name: "video.mkv", Size: 90}},
	}
	if _, err := normalizedNZBEntrySizes(entry, metadata); !errors.Is(err, usenet.ErrStaleNZBGeneration) {
		t.Fatalf("generation mismatch error = %v, want ErrStaleNZBGeneration", err)
	}

	metadata.Generation = entry.NZBGeneration
	sizes, err := normalizedNZBEntrySizes(entry, metadata)
	if err != nil {
		t.Fatalf("matching generations: %v", err)
	}
	if got := sizes["video.mkv"]; got != 90 {
		t.Fatalf("normalized size = %d, want 90", got)
	}
}

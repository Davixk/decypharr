package qbit

import (
	"testing"

	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// qBittorrent's own vocabulary already carries this: queuedDL means "admitted
// but not started". decypharr emitted it correctly and then never put entries
// into the state that produced it, because the status was advanced to
// Downloading during the synchronous add — before the job was even submitted.
//
// The operator flagged queuedDL as a state he had never seen. It was right all
// along; nothing ever reached it.
func TestQueuedEntryReportsQueuedDL(t *testing.T) {
	waiting := &storage.Entry{
		InfoHash: "waiting-for-a-worker",
		Name:     "Something.S01E01",
		Status:   debridTypes.TorrentStatusQueued,
		State:    storage.EntryStateDownloading,
	}

	if got := convertToQBitTorrentTorrent(waiting).State; got != storage.TorrentState("queuedDL") {
		t.Fatalf("state = %q, want queuedDL: an entry waiting for a worker is not downloading", got)
	}
}

// The mirror, so a fix cannot report queuedDL for everything and still pass.
func TestWorkedEntryDoesNotReportQueuedDL(t *testing.T) {
	working := &storage.Entry{
		InfoHash: "picked-up",
		Name:     "Something.S01E02",
		Status:   debridTypes.TorrentStatusDownloading,
		State:    storage.EntryStateDownloading,
	}

	if got := convertToQBitTorrentTorrent(working).State; got == storage.TorrentState("queuedDL") {
		t.Fatal("an entry a worker has picked up must not report queuedDL")
	}
}

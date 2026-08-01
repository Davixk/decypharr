package sabnzbd

import (
	"testing"

	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// The operator's acceptance criterion, in his words:
//
//	"so now DOWNLOADING will mean DOWNLOADING. queued will mean QUEUED"
//
// SABnzbd already has "Queued" and the arrs already understand it. Nothing here
// needed inventing — the entry simply arrived claiming to be Downloading while
// it sat in a slice waiting for a worker, so 310 items advertised
// "Downloading, 0 B, 0%" and a 17-minute admission wait was indistinguishable
// from a slow transfer.
//
// This pins the mapping so a future early-Downloading transition is caught at
// the surface the operator actually reads.
func TestQueuedEntryReportsQueued(t *testing.T) {
	waiting := &storage.Entry{
		InfoHash: "waiting-for-a-worker",
		Name:     "Something.S01E01",
		Status:   debridTypes.TorrentStatusQueued,
		State:    storage.EntryStateDownloading,
	}

	if got := convertToSABnzbdNZB(waiting).Status; got != StatusQueued {
		t.Fatalf("status = %q, want %q: an entry waiting for a worker is not downloading", got, StatusQueued)
	}
}

// The mirror. Without this, a fix that reported Queued for everything would
// pass the test above while making nothing ever look active — trading one lie
// for another.
func TestWorkedEntryDoesNotReportQueued(t *testing.T) {
	working := &storage.Entry{
		InfoHash: "picked-up",
		Name:     "Something.S01E02",
		Status:   debridTypes.TorrentStatusDownloading,
		State:    storage.EntryStateDownloading,
	}

	if got := convertToSABnzbdNZB(working).Status; got == StatusQueued {
		t.Fatal("an entry a worker has picked up must not report Queued")
	}
}

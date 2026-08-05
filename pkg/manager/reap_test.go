package manager

import (
	"testing"

	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// THE INVARIANT: REAPED ⇒ THE ARR SEES A FAILED DOWNLOAD.
//
// It was violated wholesale — 15,004 queue rows removed in 24h against 91
// downloadFailed events at the *arrs.
//
// 🔑 decypharr IS THE DOWNLOAD CLIENT. It marks a download failed through the
// shim and PARKS it there; the *arr polls, sees the failure, and does whatever
// it is configured to do. decypharr never calls an *arr API and never deletes
// its own rows — the *arr collects them through the shim's delete API.
//
// So "reap" here means MARK FAILED AND LEAVE IT, and the branch that matters is
// whether the error is a verdict on the RELEASE ("this will not deliver") or
// merely decypharr's own state. Capacity, rate limits and transport failures are
// ours; presenting them as failed downloads would invite the *arr to blocklist
// releases that were never bad — on this box, ~15,000 of them.

func reapEntryFixture(errText string) *storage.Entry {
	e := &storage.Entry{
		InfoHash:  "0123456789abcdef0123456789abcdef01234567",
		Category:  "sonarr",
		State:     storage.EntryStateError,
		LastError: errText,
		Status:    debridTypes.TorrentStatusDownloading,
	}
	return e
}

// ⚠️ OUR STATE IS NOT A RELEASE VERDICT. Every one of these must park.
//
// This is the case that would have blocklisted ~15,000 releases on the first
// sweep after deploy, on a box where the failures were decypharr's own storm.
func TestOurOwnFailuresNeverReap(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  string
	}{
		{"provider at capacity", "too many active downloads"},
		{"add quota", "provider add quota exhausted"},
		{"rate limited", "POST /torrents/addMagnet gave up after 4 attempt(s): status 429"},
		{"service unavailable", "alldebrid API error: status 503"},
		{"slow down", "realdebrid error 5: slow down"},
		{"timeout", "context deadline exceeded"},
		{"connection refused", "dial tcp 1.2.3.4:443: connect: connection refused"},
		{"451 is provider-scoped", "realdebrid API error: Status: 451"},
		{"unclassified", "something nobody taught classifyReap about"},
		{"no reason recorded", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verdict, why := classifyReap(reapEntryFixture(tc.err))
			if verdict != reapPark {
				t.Fatalf("verdict = %s (%s); this is decypharr's or the provider's state, not a "+
					"statement about the release — reaping it blocklists a release that was never bad",
					verdict, why)
			}
		})
	}
}

// A verdict on the release must be presented to the *arr as a failed download,
// which is the whole point of parking it rather than deleting it.
func TestReleaseVerdictsReap(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  string
	}{
		{"eta over ceiling", "failing a torrent that will not finish: eta 41h exceeds max_eta 16h"},
		{"stalled", "stalled with no progress across the sampling window"},
		{"dead swarm", "dead swarm: no seeders"},
		{"content unavailable", "articles missing on provider"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verdict, why := classifyReap(reapEntryFixture(tc.err))
			if verdict != reapFail {
				t.Fatalf("verdict = %s (%s); this says the release will not deliver, which is exactly "+
					"what the arr must be told so it can re-search", verdict, why)
			}
		})
	}
}

// ⚠️ NEVER PLACED = QUEUED = UNTOUCHABLE.
//
// The old reaper skipped provider status "queued", which reads like it honoured
// the operator's doctrine. It did not: a row that never received a placement has
// an EMPTY provider status, so it fell through to the progress-0 branch and was
// deleted. During a provider storm "never got a placement" is the NORMAL
// condition — which is how a sweep that appeared to exclude queued rows reaped
// thousands of never-dispatched ones.
func TestNeverPlacedRowsAreUntouchable(t *testing.T) {
	entry := reapEntryFixture("failing a torrent that will not finish: eta 41h")
	entry.Status = "" // never placed: empty provider status, NOT "queued"

	verdict, why := classifyReap(entry)
	if verdict != reapPark {
		t.Fatalf("verdict = %s (%s); a row that never reached a provider is QUEUED in the operator's "+
			"sense and no reaper may touch it", verdict, why)
	}
}

// The bias on uncertainty is deliberate and asymmetric: wrongly parked costs one
// retry cycle; wrongly presenting a good release as failed invites the *arr to
// blocklist it and spend a full indexer search replacing something that was fine.
func TestUnclassifiableParksRatherThanReaps(t *testing.T) {
	verdict, _ := classifyReap(reapEntryFixture("¡qué?"))
	if verdict != reapPark {
		t.Fatal("an error this code does not understand must park; guessing costs a release its place")
	}
	if v, _ := classifyReap(nil); v != reapPark {
		t.Fatal("a nil entry must park")
	}
}

// ⚠️ A REAP MARKS AND PARKS. IT MUST NOT DELETE.
//
// decypharr is the download client: it presents a failed download and waits. The
// row leaves only when the *arr removes it through the shim. Deleting it here is
// exactly what lost 15,000 rows, so this asserts the row SURVIVES.
func TestMarkingFailedNeverRemovesTheRow(t *testing.T) {
	m := newActionLifecycleFixture(t, 1)
	entry := reapEntryFixture("failing a torrent that will not finish: eta 41h")
	entry.State = storage.EntryStateDownloading
	entry.LastError = "failing a torrent that will not finish: eta 41h"
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("seed: %v", err)
	}

	marked, err := m.markFailedAndPark(entry, "test")
	if err != nil {
		t.Fatalf("markFailedAndPark: %v", err)
	}
	if !marked {
		t.Fatal("a release verdict must be presented to the arr as a failed download")
	}

	got, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil || got == nil {
		t.Fatalf("THE ROW WAS REMOVED (err=%v). decypharr does not delete its own rows — the arr "+
			"collects them through the shim. Removing it here is the defect that lost 15,004 rows.", err)
	}
	if got.State != storage.EntryStateError {
		t.Fatalf("state = %q, want error so the shim presents it as a failed download", got.State)
	}
}

// A parked verdict must leave the row completely untouched — not marked failed,
// because a failed row invites the arr to blocklist a release that was never bad.
func TestParkedRowsAreNotMarkedFailed(t *testing.T) {
	m := newActionLifecycleFixture(t, 1)
	entry := reapEntryFixture("POST /torrents/addMagnet gave up after 4 attempt(s): status 429")
	entry.State = storage.EntryStateDownloading
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("seed: %v", err)
	}

	marked, err := m.markFailedAndPark(entry, "test")
	if err != nil {
		t.Fatalf("markFailedAndPark: %v", err)
	}
	if marked {
		t.Fatal("a rate limit is decypharr's own state; presenting it as a failed download invites " +
			"the arr to blocklist a release that was never bad")
	}

	got, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil || got == nil {
		t.Fatalf("parked row disappeared: %v", err)
	}
	if got.State == storage.EntryStateError {
		t.Fatal("a parked row must not be presented as failed")
	}
}

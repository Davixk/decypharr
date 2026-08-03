package manager

import (
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Measured on a live box: RealDebrid reported 99 active transfers, decypharr
// held a record for TWO, and /api/queue/consistency answered consistent:true at
// that same instant. The 97 pinned slots nothing could release were invisible
// to every health signal we had.
//
// The risk in closing that blind spot is the opposite one: "we have no record"
// is an ABSENCE, and condemning a live download on an absence is far worse than
// failing to notice an abandoned one. Most of these tests pin the directions in
// which the check must refuse to conclude anything.

func orphanRow(id, hash, status string, added time.Time) *debridTypes.Torrent {
	return &debridTypes.Torrent{
		Id:             id,
		InfoHash:       hash,
		Name:           "Remote." + id,
		Debrid:         "prov",
		ProviderStatus: status,
		Added:          added,
	}
}

func longAgo() time.Time { return time.Now().Add(-4 * time.Hour) }

// TestOrphanDetectedWhenNothingClaimsIt is the finding itself.
func TestOrphanDetectedWhenNothingClaimsIt(t *testing.T) {
	m, _ := newStallPruneFixture(t)

	orphans, err := m.findProviderOrphans("prov",
		[]*debridTypes.Torrent{orphanRow("rd-1", strings.Repeat("a", 40), "downloading", longAgo())},
		time.Now())
	if err != nil {
		t.Fatalf("findProviderOrphans: %v", err)
	}
	if len(orphans) != 1 || orphans[0].ID != "rd-1" {
		t.Fatalf("orphans = %+v, want exactly rd-1", orphans)
	}
}

// TestQueueEntriesClaimTheirProviderCopy is the single most important guard. An
// in-flight download lives ONLY in the queue, and in-flight is precisely the
// state an orphan candidate is in — consulting the main store alone would
// report every healthy download in progress as abandoned.
func TestQueueEntriesClaimTheirProviderCopy(t *testing.T) {
	m, _ := newStallPruneFixture(t)
	hash := strings.Repeat("b", 40)
	entry := seedStalledQueueEntry(t, m, hash)

	if _, err := m.GetEntry(hash); err == nil {
		t.Fatal("precondition failed: an in-flight entry must not be in the main store")
	}

	orphans, err := m.findProviderOrphans("prov",
		[]*debridTypes.Torrent{orphanRow(placementIDOf(entry), hash, "downloading", longAgo())},
		time.Now())
	if err != nil {
		t.Fatalf("findProviderOrphans: %v", err)
	}
	if len(orphans) != 0 {
		t.Fatalf("orphans = %+v; a live queue entry claims its provider copy", orphans)
	}
}

// TestPlacementIDClaimsRegardlessOfHash: providers rotate IDs and aliases carry
// synthetic hashes, so an ID match alone is a claim.
func TestPlacementIDClaimsRegardlessOfHash(t *testing.T) {
	m, _ := newStallPruneFixture(t)
	entry := seedStalledQueueEntry(t, m, strings.Repeat("c", 40))

	orphans, err := m.findProviderOrphans("prov",
		[]*debridTypes.Torrent{orphanRow(placementIDOf(entry), "a-completely-different-hash", "queued", longAgo())},
		time.Now())
	if err != nil {
		t.Fatalf("findProviderOrphans: %v", err)
	}
	if len(orphans) != 0 {
		t.Fatalf("orphans = %+v; the placement ID is claimed by a local record", orphans)
	}
}

// TestFolderAliasClaimsItsContentHash: an alias has a synthetic storage key and
// keeps the content magnet, so its real provider hash is only reachable through
// that magnet.
func TestFolderAliasClaimsItsContentHash(t *testing.T) {
	m, _ := newStallPruneFixture(t)
	contentHash := strings.Repeat("d", 40)
	alias := &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       "synthetic-alias-key",
		Name:           "Alias.mkv",
		State:          storage.EntryStateDownloading,
		Status:         debridTypes.TorrentStatusDownloading,
		ActiveProvider: "prov",
		Magnet:         "magnet:?xt=urn:btih:" + contentHash,
		AddedOn:        longAgo(),
		Providers:      map[string]*storage.ProviderEntry{},
		Files:          map[string]*storage.File{},
	}
	if err := m.queue.Add(alias); err != nil {
		t.Fatalf("queue.Add: %v", err)
	}

	orphans, err := m.findProviderOrphans("prov",
		[]*debridTypes.Torrent{orphanRow("rd-alias", contentHash, "downloading", longAgo())},
		time.Now())
	if err != nil {
		t.Fatalf("findProviderOrphans: %v", err)
	}
	if len(orphans) != 0 {
		t.Fatalf("orphans = %+v; the alias claims this content hash through its magnet", orphans)
	}
}

// TestRecentAddsAreExempt: an add that succeeded remotely and has not yet had
// its placement written is the normal state of every torrent for a moment.
func TestRecentAddsAreExempt(t *testing.T) {
	m, _ := newStallPruneFixture(t)

	orphans, err := m.findProviderOrphans("prov",
		[]*debridTypes.Torrent{orphanRow("rd-new", strings.Repeat("e", 40), "magnet_conversion", time.Now().Add(-time.Minute))},
		time.Now())
	if err != nil {
		t.Fatalf("findProviderOrphans: %v", err)
	}
	if len(orphans) != 0 {
		t.Fatalf("orphans = %+v; an add in flight is not an orphan", orphans)
	}
}

// TestMainStoreEntriesClaimTheirCopy keeps the completed library counted too.
func TestMainStoreEntriesClaimTheirCopy(t *testing.T) {
	m, _ := newStallPruneFixture(t)
	entry := probeTorrentEntry(strings.Repeat("f", 40), "Library.Entry")
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}

	orphans, err := m.findProviderOrphans("prov",
		[]*debridTypes.Torrent{orphanRow(placementIDOf(entry), entry.InfoHash, "downloaded", longAgo())},
		time.Now())
	if err != nil {
		t.Fatalf("findProviderOrphans: %v", err)
	}
	if len(orphans) != 0 {
		t.Fatalf("orphans = %+v; a library entry claims its provider copy", orphans)
	}
}

// TestDivergenceUnknownUntilChecked: a zero CheckedAt means the diff has never
// run, and must not read as "no orphans". Same distinction the fill cache draws
// between an absent count and a count of zero.
func TestDivergenceUnknownUntilChecked(t *testing.T) {
	m, _ := newStallPruneFixture(t)
	m.providerOrphans = newProviderOrphanTracker()

	before := m.ProviderDivergence()
	if !before.CheckedAt.IsZero() {
		t.Fatal("an unchecked tracker reported a check time")
	}
	if len(before.Providers) != 0 {
		t.Fatalf("providers = %+v before any check", before.Providers)
	}

	m.reportProviderOrphans("prov", []*debridTypes.Torrent{
		orphanRow("rd-1", strings.Repeat("a", 40), "downloading", longAgo()),
		orphanRow("rd-2", strings.Repeat("b", 40), "queued", longAgo()),
	})

	after := m.ProviderDivergence()
	if after.CheckedAt.IsZero() {
		t.Fatal("a completed check left no timestamp")
	}
	got := after.Providers["prov"]
	if got.Held != 2 || got.Unclaimed != 2 {
		t.Fatalf("divergence = %+v, want held=2 unclaimed=2", got)
	}
	if len(got.Sample) != 2 {
		t.Fatalf("sample = %+v, want both orphans identified", got.Sample)
	}
}

// TestOrphanCheckDeletesNothing. The report is a number for an operator, never
// an action — see the file comment on why absence must not authorise a delete.
func TestOrphanCheckDeletesNothing(t *testing.T) {
	m, client := newStallPruneFixture(t)
	hash := strings.Repeat("9", 40)
	seedStalledQueueEntry(t, m, hash)

	m.reportProviderOrphans("prov", []*debridTypes.Torrent{
		orphanRow("rd-orphan", strings.Repeat("8", 40), "downloading", longAgo()),
	})

	if got := client.released(); len(got) != 0 {
		t.Fatalf("the orphan check deleted %v on the provider; it must only report", got)
	}
	if _, err := m.queue.GetTorrent(hash); err != nil {
		t.Fatalf("the orphan check disturbed a local entry: %v", err)
	}
}

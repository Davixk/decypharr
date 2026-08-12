package manager

import (
	"errors"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// THE USENET CONDEMN→PRUNE POLICY, one assertion per judgement.
//
// Operator's order: NNTP 430 is a confirmed-dead signal, and dead usenet content
// must behave exactly like a confirmed debrid takedown — stop serving, dangling
// symlink notifies, arr replaces. No arr calls anywhere in here.
func newDeadArticleManager(t *testing.T, prune bool) *Manager {
	t.Helper()
	m := newActionLifecycleFixture(t, 1)
	m.livePrunes = newLivePruneBudget()
	cfg := config.Get()
	cfg.Repair.Prune = prune
	t.Cleanup(func() { cfg.Repair.Prune = false })
	return m
}

func seedNZBEntry(t *testing.T, m *Manager, nzoID string) *storage.Entry {
	t.Helper()
	entry := &storage.Entry{
		Protocol: config.ProtocolNZB,
		InfoHash: nzoID,
		Name:     nzoID + ".mkv",
		AddedOn:  time.Unix(1_700_000_000, 0).UTC(),
		Files:    map[string]*storage.File{},
	}
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate %s: %v", nzoID, err)
	}
	return entry
}

func nzbEntryExists(m *Manager, nzoID string) bool {
	entry, err := m.storage.Get(nzoID)
	return err == nil && entry != nil
}

// ALL FILES DEAD → CONDEMNED AND PRUNED. This is the case the operator hit: a
// viewer's stream died mid-file on a 430, and the entry went on being listed
// with a resolving library symlink until the nightly sweep happened to notice.
func TestEveryFileDeadCondemnsAndPrunes(t *testing.T) {
	m := newDeadArticleManager(t, true)
	seedNZBEntry(t, m, "all-dead")

	m.onUsenetDeadContent("all-dead", "movie.mkv", 1, 1, errors.New("430 no such article"))

	if nzbEntryExists(m, "all-dead") {
		t.Fatal("a fully dead NZB was left in place; it stays in the mount listing, its library symlink keeps " +
			"resolving, and the arr never learns it needs a replacement")
	}
}

// 🛑 PARTIAL SETS SURVIVE, AND THIS IS THE CONTROL THAT MATTERS MOST.
//
// The durable NZB record sets IsBad on the FIRST dead file, so an entry with one
// dead episode carries the identical flag as one with nothing left. Condemning
// on that flag would delete a thirteen-file pack because one episode expired —
// destroying twelve good files and buying a full re-search for all of them.
func TestPartiallyDeadPackIsNotPruned(t *testing.T) {
	m := newDeadArticleManager(t, true)
	seedNZBEntry(t, m, "one-episode-gone")

	m.onUsenetDeadContent("one-episode-gone", "S01E07.mkv", 1, 13, errors.New("430 no such article"))

	if !nzbEntryExists(m, "one-episode-gone") {
		t.Fatal("a 13-file pack was deleted because ONE file died. The other twelve were fine and are now gone, " +
			"and every one of them costs a fresh indexer search")
	}
	entry, err := m.storage.Get("one-episode-gone")
	if err != nil {
		t.Fatalf("GetTorrent: %v", err)
	}
	if entry.Bad {
		t.Fatal("a partially dead pack was marked Bad, which stops it serving its TWELVE SURVIVING FILES")
	}
}

// A census we could not take is not a census. total == 0 means the NZB record
// had no file rows — a metadata problem — and answering "yes, delete it" to a
// question whose subject is invisible is how an empty read becomes a deletion.
func TestAnEmptyCensusNeverPrunes(t *testing.T) {
	m := newDeadArticleManager(t, true)
	seedNZBEntry(t, m, "no-census")

	m.onUsenetDeadContent("no-census", "movie.mkv", 0, 0, errors.New("430 no such article"))

	if !nzbEntryExists(m, "no-census") {
		t.Fatal("an entry was pruned on a census of zero files; nothing was proven dead")
	}
}

// WITH repair.prune OFF, THE MARK STILL HAPPENS AND THE DELETION DOES NOT.
//
// Bad stops it being SERVED; prune stops it being LISTED. Only one is
// destructive, so only one is gated — and it is gated by the operator's existing
// destructive consent rather than a second knob defaulting to on.
func TestPruneOffCondemnsButKeepsTheEntry(t *testing.T) {
	m := newDeadArticleManager(t, false)
	seedNZBEntry(t, m, "prune-off")

	m.onUsenetDeadContent("prune-off", "movie.mkv", 1, 1, errors.New("430 no such article"))

	entry, err := m.storage.Get("prune-off")
	if err != nil || entry == nil {
		t.Fatalf("repair.prune=false deleted the entry anyway (entry=%v err=%v)", entry, err)
	}
	if !entry.Bad {
		t.Fatal("the entry was not condemned. With prune off, the Bad mark is the ONLY thing stopping dead " +
			"content being served, and it is not destructive so nothing gates it")
	}
}

// THE CIRCUIT BREAKER. An article-not-found verdict is "missing on every
// configured provider", which sounds like corroboration and, with one provider
// configured, is one server's word about its own index. A provider that changes
// retention answers 430 for a great many articles at once, and every read of
// every affected file would condemn and prune, at read rate.
func TestLivePruneBudgetStopsARunaway(t *testing.T) {
	m := newDeadArticleManager(t, true)
	cfg := config.Get()
	cfg.Repair.MaxLivePrunesPerHour = 2
	t.Cleanup(func() { cfg.Repair.MaxLivePrunesPerHour = 0 })

	ids := []string{"dead-1", "dead-2", "dead-3", "dead-4"}
	for _, id := range ids {
		seedNZBEntry(t, m, id)
	}
	for _, id := range ids {
		m.onUsenetDeadContent(id, "movie.mkv", 1, 1, errors.New("430 no such article"))
	}

	survived := 0
	for _, id := range ids {
		if nzbEntryExists(m, id) {
			survived++
		}
	}
	if survived != 2 {
		t.Fatalf("%d of 4 entries survived a budget of 2 per hour, want 2. Without the cap a provider losing an "+
			"index shelf deletes the library as fast as files are opened", survived)
	}

	// AND THE DEFERRED ONES ARE STILL CONDEMNED. The budget delays a deletion;
	// it must never leave dead content serving.
	for _, id := range ids {
		entry, err := m.storage.Get(id)
		if err != nil || entry == nil {
			continue // pruned
		}
		if !entry.Bad {
			t.Fatalf("%s was skipped by the budget AND left serving; the cap must defer the deletion, not the verdict", id)
		}
	}
}

// The mirror: a negative value means unlimited, so an operator who has decided
// the rate is genuine is not fighting the breaker.
func TestNegativeLivePruneBudgetIsUnlimited(t *testing.T) {
	m := newDeadArticleManager(t, true)
	cfg := config.Get()
	cfg.Repair.MaxLivePrunesPerHour = -1
	t.Cleanup(func() { cfg.Repair.MaxLivePrunesPerHour = 0 })

	ids := []string{"u-1", "u-2", "u-3", "u-4", "u-5"}
	for _, id := range ids {
		seedNZBEntry(t, m, id)
	}
	for _, id := range ids {
		m.onUsenetDeadContent(id, "movie.mkv", 1, 1, errors.New("430 no such article"))
	}
	for _, id := range ids {
		if nzbEntryExists(m, id) {
			t.Fatalf("%s survived an explicitly unlimited budget", id)
		}
	}
}

// An entry decypharr no longer has is not an error — the sweep or a user delete
// may have got there first. It must not panic and must not invent one.
func TestUnknownNZBIsIgnored(t *testing.T) {
	m := newDeadArticleManager(t, true)
	m.onUsenetDeadContent("never-existed", "movie.mkv", 1, 1, errors.New("430 no such article"))
}

func TestLivePruneBudgetWindowRollsOff(t *testing.T) {
	cfg := config.Get()
	cfg.Repair.MaxLivePrunesPerHour = 1
	t.Cleanup(func() { cfg.Repair.MaxLivePrunesPerHour = 0 })

	b := newLivePruneBudget()
	base := time.Unix(1_700_000_000, 0).UTC()

	if ok, _, _ := b.reserve(base); !ok {
		t.Fatal("the first reservation in an empty window was refused")
	}
	if ok, _, _ := b.reserve(base.Add(time.Minute)); ok {
		t.Fatal("a second reservation inside the same hour was allowed against a budget of 1")
	}
	// An hour later the first event has aged out; this is a ROLLING window, not
	// a counter that has to be reset by something.
	if ok, _, _ := b.reserve(base.Add(time.Hour + time.Second)); !ok {
		t.Fatal("the window never rolls off — after the first hour the budget would be permanently exhausted and " +
			"no event-driven prune would ever run again")
	}
}

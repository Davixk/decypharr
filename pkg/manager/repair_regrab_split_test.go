package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
)

// ARR-DELETE used to mean one thing — "delete the arr file record, blocklist the
// grab, search for a replacement" — behind a single checkbox. These tests pin
// the three acts apart.
//
// The subtle one is TestRegrabSearchCoversFilesWithGrabHistory. The old code
// searched ONLY files with no grab-history record, because for the rest it
// relied on MarkHistoryFailed's "Redownload Failed" side effect to trigger the
// arr's own search. That coupling is invisible until you turn blocklisting off:
// the search knob then silently does nothing for every file whose grab record
// still exists, which in a healthy arr is most of them.
//
// A test that seeds an arr with EMPTY history cannot catch that — every file
// falls into the no-history branch and gets searched either way. So these use a
// fake arr that DOES return a grab record.

// grabHistoryArrServer stands in for a Sonarr/Radarr that still holds a grab
// history record for the media it is asked about, which is the case the old
// bundled implementation handled differently (and the reason the split needs
// its own fake rather than reusing fakeArrServer's empty-history one).
type grabHistoryArrServer struct {
	server *httptest.Server

	deletes     atomic.Int64 // DELETE .../moviefile/bulk
	blocklists  atomic.Int64 // POST  /api/v3/history/failed/{id}
	searches    atomic.Int64 // POST  /api/v3/command  (MoviesSearch)
	searchedIDs struct {
		mu  sync.Mutex
		ids []int
	}
}

func newGrabHistoryArrServer(t *testing.T, historyID int) *grabHistoryArrServer {
	t.Helper()
	f := &grabHistoryArrServer{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/bulk"):
			f.deletes.Add(1)

		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/history/failed/"):
			f.blocklists.Add(1)

		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "command"):
			f.searches.Add(1)
			// Record which media ids were asked for, so a test can assert the
			// search covered the file that HAD a grab record rather than only
			// the leftovers.
			var body struct {
				MovieIDs  []int `json:"movieIds"`
				EpisodeID []int `json:"episodeIds"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.searchedIDs.mu.Lock()
			f.searchedIDs.ids = append(f.searchedIDs.ids, body.MovieIDs...)
			f.searchedIDs.ids = append(f.searchedIDs.ids, body.EpisodeID...)
			f.searchedIDs.mu.Unlock()

		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/history"):
			// A live grab record: this is what makes FindGrabHistoryID return
			// non-zero and puts the file on the blocklist-only path in the old
			// implementation.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"records":[{"id":` + itoa(historyID) + `,"downloadId":"abc","eventType":"grabbed"}]}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"records":[]}`))
	}))
	t.Cleanup(f.server.Close)
	return f
}

func itoa(i int) string {
	b, _ := json.Marshal(i)
	return string(b)
}

func (f *grabHistoryArrServer) counts() (deletes, blocklists, searches int) {
	return int(f.deletes.Load()), int(f.blocklists.Load()), int(f.searches.Load())
}

func (f *grabHistoryArrServer) searchedMediaIDs() []int {
	f.searchedIDs.mu.Lock()
	defer f.searchedIDs.mu.Unlock()
	return append([]int(nil), f.searchedIDs.ids...)
}

// newRegrabSplitFixture wires a Manager + Repair against an arr that holds a
// grab record, and seeds one fully-broken entry pointing at it.
func newRegrabSplitFixture(t *testing.T) (*Manager, *Repair, *grabHistoryArrServer) {
	t.Helper()
	m := newActionLifecycleFixture(t, 2)
	m.clients = xsync.NewMap[string, debrid.Client]()
	m.clients.Store("prov", &fakeDebridClient{cfg: config.Debrid{Name: "prov"}})

	arrSrv := newGrabHistoryArrServer(t, 4242)
	m.arr.AddOrUpdate(arr.NewWithOptions("radarr", arrSrv.server.URL, "test-token", arr.Options{}))

	seedBrokenEntry(t, m, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "Broken.Entry.2024")

	r := &Repair{
		manager:   m,
		logger:    zerolog.Nop(),
		parentCtx: context.Background(),
	}
	return m, r, arrSrv
}

func brokenHealthFor(t *testing.T, m *Manager, name string) *storage.EntryHealth {
	t.Helper()
	h, err := m.storage.GetEntryHealth(name)
	if err != nil {
		t.Fatalf("GetEntryHealth(%s): %v", name, err)
	}
	return h
}

// runRegrab drives ARR-DELETE directly with an explicit action set, which is what
// the config/API layers resolve to.
func runRegrab(t *testing.T, r *Repair, m *Manager, actions repairActions) *storage.RepairRun {
	t.Helper()
	run := &storage.RepairRun{ID: "test-run"}
	var statsMu sync.Mutex
	h := brokenHealthFor(t, m, "Broken.Entry.2024")
	r.arrDeleteDeadEntry(context.Background(), run, &statsMu, "Broken.Entry.2024", h, actions)
	return run
}

// TestRegrabDefaultDeletesWithoutBlocklistOrSearch is the operator's step 2:
// "if repair fails, DELETE on the arr side, with NO BLOCKLISTING". With ARR-DELETE
// on and both sub-actions off (the defaults), exactly one thing may happen.
func TestRegrabDefaultDeletesWithoutBlocklistOrSearch(t *testing.T) {
	m, r, arrSrv := newRegrabSplitFixture(t)

	run := runRegrab(t, r, m, repairActions{arrDelete: true})

	deletes, blocklists, searches := arrSrv.counts()
	if deletes != 1 {
		t.Fatalf("arr DeleteFiles calls = %d, want 1", deletes)
	}
	if blocklists != 0 {
		t.Fatalf("blocklist POSTs = %d, want 0 — ARR-DELETE must not blocklist unless asked", blocklists)
	}
	if searches != 0 {
		t.Fatalf("search commands = %d, want 0 — ARR-DELETE must not search unless asked", searches)
	}
	if run.Stats.ArrBlocklisted != 0 || run.Stats.ArrSearched != 0 {
		t.Fatalf("stats claimed work that did not happen: blocklisted=%d searched=%d",
			run.Stats.ArrBlocklisted, run.Stats.ArrSearched)
	}
	if run.Stats.ArrDeleted != 1 {
		t.Fatalf("Stats.ArrDeleted = %d, want 1", run.Stats.ArrDeleted)
	}
}

// TestRegrabSearchCoversFilesWithGrabHistory is THE regression test for the
// split. The seeded file HAS a grab-history record (id 4242), so under the old
// implementation it was routed to MarkHistoryFailed and deliberately excluded
// from SearchMissing — the arr's Redownload-Failed side effect was expected to
// search for it.
//
// With blocklisting off, that side effect never fires. If SearchMissing is not
// called explicitly for this file, enabling "search" does nothing at all for it.
func TestRegrabSearchCoversFilesWithGrabHistory(t *testing.T) {
	m, r, arrSrv := newRegrabSplitFixture(t)

	run := runRegrab(t, r, m, repairActions{arrDelete: true, search: true})

	deletes, blocklists, searches := arrSrv.counts()
	if deletes != 1 {
		t.Fatalf("arr DeleteFiles calls = %d, want 1", deletes)
	}
	if blocklists != 0 {
		t.Fatalf("blocklist POSTs = %d, want 0 — search must not imply blocklist", blocklists)
	}
	if searches != 1 {
		t.Fatalf("search commands = %d, want 1 — a file WITH a grab record must still be searched explicitly", searches)
	}
	if ids := arrSrv.searchedMediaIDs(); len(ids) != 1 || ids[0] != 555 {
		t.Fatalf("searched media ids = %v, want [555] (the broken file's media id)", ids)
	}
	if run.Stats.ArrSearched != 1 {
		t.Fatalf("Stats.ArrSearched = %d, want 1", run.Stats.ArrSearched)
	}
}

// TestRegrabBlocklistOnlyWhenEnabled pins the other direction: blocklisting is
// reachable (so the bad-release case still has a tool) but only on request, and
// turning it on must not drag a search along with it.
func TestRegrabBlocklistOnlyWhenEnabled(t *testing.T) {
	m, r, arrSrv := newRegrabSplitFixture(t)

	run := runRegrab(t, r, m, repairActions{arrDelete: true, blocklist: true})

	deletes, blocklists, searches := arrSrv.counts()
	if deletes != 1 {
		t.Fatalf("arr DeleteFiles calls = %d, want 1", deletes)
	}
	if blocklists != 1 {
		t.Fatalf("blocklist POSTs = %d, want 1", blocklists)
	}
	if searches != 0 {
		t.Fatalf("search commands = %d, want 0 — blocklist must not imply an explicit search", searches)
	}
	if run.Stats.ArrBlocklisted != 1 {
		t.Fatalf("Stats.ArrBlocklisted = %d, want 1", run.Stats.ArrBlocklisted)
	}
}

// TestRegrabSubActionsAreGatedOnRegrab keeps the sub-actions subordinate. With
// ARR-DELETE off, nothing may reach the arr at all — that is the invariant that
// makes "ARR-DELETE is the only arr-coupled component" true.
func TestRegrabSubActionsAreGatedOnRegrab(t *testing.T) {
	acts := repairActions{arrDelete: false, search: true, blocklist: true}
	if acts.wantSearch() {
		t.Fatal("arrSearch() true with regrab off — sub-actions must be gated on ARR-DELETE")
	}
	if acts.wantBlocklist() {
		t.Fatal("arrBlocklist() true with regrab off — sub-actions must be gated on ARR-DELETE")
	}
	if acts.any() {
		t.Fatal("any() true with only sub-actions set — sub-actions are not components")
	}
	if acts.destructive() {
		t.Fatal("destructive() true with only sub-actions set")
	}
}

// TestRegrabLabelNamesTheActs — the run record's Source is what an operator
// reads afterwards to learn what a sweep did. A bare "regrab" no longer answers
// the question the split exists to answer.
func TestRegrabLabelNamesTheActs(t *testing.T) {
	for _, tc := range []struct {
		name string
		acts repairActions
		want string
	}{
		{"delete only", repairActions{arrDelete: true}, "arr(delete)"},
		{"delete+search", repairActions{arrDelete: true, search: true}, "arr(delete+search)"},
		{"delete+blocklist", repairActions{arrDelete: true, blocklist: true}, "arr(delete+blocklist)"},
		{"all three", repairActions{arrDelete: true, search: true, blocklist: true}, "arr(delete+search+blocklist)"},
		{"sub-actions without regrab", repairActions{search: true, blocklist: true}, "check-only"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.acts.label(); got != tc.want {
				t.Fatalf("label() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestManualActionsSubActionsDefaultToConfig — an API caller that names
// components but says nothing about search/blocklist must inherit the
// operator's configured policy, not silently get the zero value.
func TestManualActionsSubActionsDefaultToConfig(t *testing.T) {
	cfg := config.RepairConfig{ArrSearch: true, ArrBlocklist: true}

	sel := &ManualActions{Regrab: true}
	acts := sel.toActions(cfg)
	if !acts.search || !acts.blocklist {
		t.Fatalf("unspecified sub-actions did not inherit config: %+v", acts)
	}

	no := false
	sel = &ManualActions{Regrab: true, Blocklist: &no}
	acts = sel.toActions(cfg)
	if !acts.search {
		t.Fatal("search override leaked from an unrelated blocklist override")
	}
	if acts.blocklist {
		t.Fatal("explicit blocklist:false was ignored")
	}
}

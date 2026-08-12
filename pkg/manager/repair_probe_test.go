package manager

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// probeClient is a fake provider whose CheckFile / GetDownloadLink outcomes are
// configurable, and which counts calls so a test can prove a rejected probe
// spent no provider budget.
type probeClient struct {
	fakeDebridClient
	checkErr   error
	linkURL    string
	linkErr    error
	checkCalls atomic.Int32
	linkCalls  atomic.Int32
}

func (p *probeClient) SupportsCheck() bool { return true }

func (p *probeClient) CheckFile(context.Context, string, string) error {
	p.checkCalls.Add(1)
	return p.checkErr
}

func (p *probeClient) GetDownloadLink(string, *debridTypes.File) (debridTypes.DownloadLink, error) {
	p.linkCalls.Add(1)
	if p.linkErr != nil {
		return debridTypes.DownloadLink{}, p.linkErr
	}
	return debridTypes.DownloadLink{Link: p.linkURL, DownloadLink: p.linkURL, Filename: "file.mkv"}, nil
}

func newProbeFixture(t *testing.T, client debrid.Client) (*Manager, *Repair) {
	t.Helper()
	m := newActionLifecycleFixture(t, 2)
	m.clients = xsync.NewMap[string, debrid.Client]()
	if client != nil {
		m.clients.Store("prov", client)
	}
	m.streamClient = http.DefaultClient
	r := &Repair{manager: m, logger: zerolog.Nop(), parentCtx: context.Background()}
	return m, r
}

func probeTorrentEntry(hash, name string) *storage.Entry {
	now := time.Unix(1_700_000_000, 0).UTC()
	return &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       hash,
		Name:           name,
		ActiveProvider: "prov",
		Status:         debridTypes.TorrentStatusDownloaded,
		IsComplete:     true,
		AddedOn:        now,
		Providers: map[string]*storage.ProviderEntry{
			"prov": {
				Provider: "prov",
				ID:       "placement-" + hash,
				Status:   debridTypes.TorrentStatusDownloaded,
				Files: map[string]*storage.ProviderFile{
					"file.mkv": {Id: "f1", Link: "https://provider.invalid/f1", Path: "/file.mkv"},
				},
			},
		},
		Files: map[string]*storage.File{
			"file.mkv": {Name: "file.mkv", InfoHash: hash, Size: 4096, AddedOn: now},
		},
	}
}

// TestProbeTorrentFileBadEntryIsBroken is the torrent twin of the usenet
// zero-byte bug: decypharr's own read path refuses a bad-marked entry outright,
// so 100% of its reads fail, yet no metadata-level probe ever noticed and the
// entry recorded healthy with broken_count 0.
func TestProbeTorrentFileBadEntryIsBroken(t *testing.T) {
	client := &probeClient{}
	_, r := newProbeFixture(t, client)

	entry := probeTorrentEntry("badhash", "Bad.Entry")
	entry.Bad = true
	res := r.probeTorrentFile(context.Background(), entry, entry.Files["file.mkv"], "file.mkv",
		fileResult{name: "file.mkv"}, RepairRunOptions{}, true, storage.HealthHealthy)

	if !res.broken || res.healthy {
		t.Fatalf("bad-marked entry probed %+v, want broken", res)
	}
	if res.reason != "entry_marked_bad" {
		t.Fatalf("reason = %q, want entry_marked_bad", res.reason)
	}
	// The AVAILABILITY check is still never spent on a bad-marked entry: it
	// cannot change the verdict, and it answers a hoster-support question that
	// says nothing about the content.
	if got := client.checkCalls.Load(); got != 0 {
		t.Fatalf("bad-marked entry cost %d availability checks, want 0", got)
	}
	// ONE unrestrict, though — a deliberate change from the zero-provider-calls
	// rule this test used to assert. A condemned entry whose cause is unknown is
	// exactly the population the takedown work would not act on, and the default
	// probe is structurally incapable of telling a legal takedown from anything
	// else. The escalation is what makes the cause knowable; it costs one link
	// per already-condemned file, bounded by the broken set rather than by the
	// library.
	if got := client.linkCalls.Load(); got != 1 {
		t.Fatalf("bad-marked entry cost %d unrestrict calls, want exactly 1 (the escalation probe)", got)
	}
}

// 🛑 THE ESCALATION MAY REFINE THE REASON. IT MAY NEVER CLEAR broken.
//
// Bad is durable, so decypharr's own read path refuses the entry regardless of
// what the provider says about the link. If a resolving unrestrict were allowed
// to flip this to healthy, the sweep would record a 100%-unreadable entry as
// fine — precisely the bug the bad-entry guard was written to fix, reintroduced
// through the back door of a better probe.
func TestEscalatedProbeNeverResurrectsABadEntry(t *testing.T) {
	// linkURL set and no error: the unrestrict SUCCEEDS.
	client := &probeClient{linkURL: "http://127.0.0.1:1/resolves"}
	_, r := newProbeFixture(t, client)

	entry := probeTorrentEntry("resurrect", "Resolves.But.Bad")
	entry.Bad = true
	res := r.probeTorrentFile(context.Background(), entry, entry.Files["file.mkv"], "file.mkv",
		fileResult{name: "file.mkv"}, RepairRunOptions{}, true, storage.HealthBroken)

	if res.healthy {
		t.Fatal("a resolving unrestrict marked a bad-marked entry HEALTHY. The read path still refuses it, so " +
			"the sweep would be recording a 100%-unreadable entry as fine")
	}
	if !res.broken || res.reason != "entry_marked_bad" {
		t.Fatalf("probed %+v, want broken/entry_marked_bad — a link that resolves is not evidence of anything "+
			"about a durably condemned entry", res)
	}
}

// AND WHAT THE ESCALATION IS FOR: turning "condemned, cause unknown" into a
// confirmed content verdict the takedown pipeline can act on. A legacy Bad and a
// fresh takedown were indistinguishable, which is why nothing acted on legacy
// Bad; a real 451 is what makes the difference.
func TestEscalatedProbeUpgradesAConfirmedTakedown(t *testing.T) {
	client := &probeClient{linkErr: customerror.NewContentTakedownError(
		errors.New("realdebrid: infringing_file (code 35)"))}
	_, r := newProbeFixture(t, client)

	entry := probeTorrentEntry("takendown", "Taken.Down")
	entry.Bad = true
	res := r.probeTorrentFile(context.Background(), entry, entry.Files["file.mkv"], "file.mkv",
		fileResult{name: "file.mkv"}, RepairRunOptions{}, true, storage.HealthHealthy)

	if !res.broken || res.healthy {
		t.Fatalf("a confirmed takedown probed %+v, want broken", res)
	}
	if res.reason != "debrid_takedown" {
		t.Fatalf("reason = %q, want debrid_takedown — without the upgrade this stays an unactionable "+
			"entry_marked_bad and the entry is never recognised as legally dead", res.reason)
	}
}

// A HOSTER OUTAGE MUST NOT BE UPGRADED. The control for the test above: an
// escalation that treated any error as a takedown would convert a provider's bad
// afternoon into a permanent content verdict on every condemned entry at once.
func TestEscalatedProbeDoesNotUpgradeAnOutage(t *testing.T) {
	client := &probeClient{linkErr: customerror.HosterUnavailableError}
	_, r := newProbeFixture(t, client)

	entry := probeTorrentEntry("flapping", "Hoster.Flap")
	entry.Bad = true
	res := r.probeTorrentFile(context.Background(), entry, entry.Files["file.mkv"], "file.mkv",
		fileResult{name: "file.mkv"}, RepairRunOptions{}, true, storage.HealthHealthy)

	if res.reason != "entry_marked_bad" {
		t.Fatalf("reason = %q, want entry_marked_bad — a hoster outage is not a statement about the content", res.reason)
	}
}

// A HEALTHY ENTRY WITH NO HISTORY KEEPS THE CHEAP PROBE. Without this the
// escalation is not selective at all: it would mint a download link for every
// file in the library on every sweep, which is exactly the cost that made
// always-on unrestrict_link opt-in in the first place.
func TestNonSuspectEntryKeepsTheCheapProbe(t *testing.T) {
	client := &probeClient{}
	_, r := newProbeFixture(t, client)

	entry := probeTorrentEntry("ordinary", "Ordinary.Entry")
	res := r.probeTorrentFile(context.Background(), entry, entry.Files["file.mkv"], "file.mkv",
		fileResult{name: "file.mkv"}, RepairRunOptions{}, false, storage.HealthHealthy)

	if !res.healthy {
		t.Fatalf("an ordinary entry probed %+v, want healthy", res)
	}
	if got := client.checkCalls.Load(); got != 1 {
		t.Fatalf("ordinary entry made %d availability checks, want 1", got)
	}
	if got := client.linkCalls.Load(); got != 0 {
		t.Fatalf("an entry with no recorded trouble minted %d download links. Escalating for everything is "+
			"the cost this design exists to avoid", got)
	}
}

// 🛑 AN UPGRADE THAT CANNOT RUN FALLS BACK — IT MUST NOT DOWNGRADE.
//
// This is the regression the first cut shipped, caught by a full sweep run
// rather than by reasoning. An entry with no usable placement is unservable, and
// the cheap path says exactly that: `missing_provider_link`, which is BROKEN and
// therefore prune-eligible. Routed to the unrestrict path it answers
// `placement_not_found` — a NON-VERDICT — so a durably dead entry became
// `unknown`, stopped being actionable, and would have sat in the library
// forever. Eight sweep tests failed on it, every deletion-cap test among them.
func TestEscalationFallsBackWhenItCannotRun(t *testing.T) {
	client := &probeClient{}
	_, r := newProbeFixture(t, client)

	entry := probeTorrentEntry("noplacement", "No.Placement")
	// Suspect by prior status, provider client present — but the placement holds
	// nothing to unrestrict. This is the shape the sweep fixtures have and the
	// one the first cut turned into a non-verdict.
	if placement := entry.GetActiveProvider(); placement != nil {
		placement.Files = map[string]*storage.ProviderFile{}
	}

	res := r.probeTorrentFile(context.Background(), entry, entry.Files["file.mkv"], "file.mkv",
		fileResult{name: "file.mkv"}, RepairRunOptions{}, false, storage.HealthBroken)

	if !res.broken {
		t.Fatalf("an entry with no placement probed %+v, want broken. The escalation cannot run here, so it must "+
			"fall back to the cheap path's verdict — anything else turns a durably dead entry into a "+
			"non-actionable `unknown` that never gets pruned", res)
	}
	if res.reason == "placement_not_found" {
		t.Fatal("the escalation ran anyway and produced a non-verdict; canEscalateProbe did not gate it")
	}
}

// A PREVIOUSLY-BROKEN ENTRY THAT IS NOT Bad DOES get the conclusive probe, and it
// MAY come back healthy — nothing blocks its read path, so a link that resolves
// again means it genuinely recovered.
func TestPreviouslyBrokenEntryEscalatesAndMayRecover(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, 1<<20))
	}))
	defer server.Close()

	client := &probeClient{linkURL: server.URL}
	m, r := newProbeFixture(t, client)
	m.streamClient = server.Client()

	entry := probeTorrentEntry("recovered", "Recovered.Entry")
	res := r.probeTorrentFile(context.Background(), entry, entry.Files["file.mkv"], "file.mkv",
		fileResult{name: "file.mkv"}, RepairRunOptions{}, true, storage.HealthBroken)

	if !res.healthy {
		t.Fatalf("a previously-broken entry whose link now resolves probed %+v, want healthy", res)
	}
	// ⚠️ THE DISCRIMINATOR IS THE *CHECK* COUNT, NOT THE LINK COUNT, and a
	// negative control is what showed it. The payload probe mints a download
	// link on BOTH paths, so `linkCalls > 0` is true whether or not the
	// escalation fired — an assertion on it passed with the escalation removed
	// and proved nothing. CheckFile is the one call only the cheap path makes.
	if got := client.checkCalls.Load(); got != 0 {
		t.Fatalf("a previously-broken entry made %d availability checks; it took the cheap path, which cannot "+
			"see a takedown, so the escalation did not fire", got)
	}
}

// TestProbeTorrentFileIndeterminateIsUnknown pins the three-way discipline on
// the torrent side: a 401/429/5xx tells us nothing about the content.
func TestProbeTorrentFileIndeterminateIsUnknown(t *testing.T) {
	client := &probeClient{checkErr: debridTypes.ErrAvailabilityIndeterminate}
	_, r := newProbeFixture(t, client)

	entry := probeTorrentEntry("indethash", "Indeterminate.Entry")
	res := r.probeTorrentFile(context.Background(), entry, entry.Files["file.mkv"], "file.mkv",
		fileResult{name: "file.mkv"}, RepairRunOptions{}, true, storage.HealthHealthy)

	if res.healthy {
		t.Fatal("an indeterminate provider answer probed HEALTHY")
	}
	if res.broken {
		t.Fatal("an indeterminate provider answer probed BROKEN; a provider outage would mass-delete")
	}
	if res.reason != "provider_probe_indeterminate" {
		t.Fatalf("reason = %q, want provider_probe_indeterminate", res.reason)
	}
	if rollupStatus([]fileResult{res}) != storage.HealthUnknown {
		t.Fatal("indeterminate result did not roll up to unknown")
	}
}

// TestProbeTorrentFileRequiresRealBytes covers the core PART C requirement: an
// availability call that claims the file is fine must not be believed unless
// bytes actually transfer.
func TestProbeTorrentFileRequiresRealBytes(t *testing.T) {
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i)
	}

	cases := []struct {
		name        string
		handler     http.HandlerFunc
		wantBroken  bool
		wantHealthy bool
	}{
		{
			name: "bytes flow",
			handler: func(w http.ResponseWriter, r *http.Request) {
				start, end := rangeBounds(r, len(payload))
				body := payload[start : end+1]
				w.Header().Set("Content-Length", strconv.Itoa(len(body)))
				w.Header().Set("Content-Range", buildContentRange(int64(start), int64(end), int64(len(payload))))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(body)
			},
			wantHealthy: true,
		},
		{
			name: "success status with an empty body",
			handler: func(w http.ResponseWriter, r *http.Request) {
				start, end := rangeBounds(r, len(payload))
				w.Header().Set("Content-Length", "0")
				w.Header().Set("Content-Range", buildContentRange(int64(start), int64(end), int64(len(payload))))
				w.WriteHeader(http.StatusPartialContent)
			},
			wantBroken: true,
		},
		{
			name: "permanent gone",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusGone)
			},
			wantBroken: true,
		},
		{
			name: "transport-level failure is indeterminate",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			t.Cleanup(server.Close)

			client := &probeClient{linkURL: server.URL}
			m, r := newProbeFixture(t, client)
			m.streamClient = server.Client()

			entry := probeTorrentEntry("byteshash", "Bytes.Entry")
			res := r.probeTorrentFile(context.Background(), entry, entry.Files["file.mkv"], "file.mkv",
				fileResult{name: "file.mkv"}, RepairRunOptions{}, true, storage.HealthHealthy)

			if res.healthy != tc.wantHealthy {
				t.Fatalf("healthy = %v, want %v (reason %q)", res.healthy, tc.wantHealthy, res.reason)
			}
			if res.broken != tc.wantBroken {
				t.Fatalf("broken = %v, want %v (reason %q)", res.broken, tc.wantBroken, res.reason)
			}
			if !tc.wantHealthy && !tc.wantBroken && rollupStatus([]fileResult{res}) != storage.HealthUnknown {
				t.Fatal("non-verdict result did not roll up to unknown")
			}
		})
	}
}

// TestProbeTorrentFileSkipsPayloadCheckWhenNotSelected keeps the sweep
// affordable: only one file per infohash pays for the byte transfer.
func TestProbeTorrentFileSkipsPayloadCheckWhenNotSelected(t *testing.T) {
	client := &probeClient{linkURL: "http://127.0.0.1:1/never"}
	_, r := newProbeFixture(t, client)

	entry := probeTorrentEntry("skiphash", "Skip.Entry")
	res := r.probeTorrentFile(context.Background(), entry, entry.Files["file.mkv"], "file.mkv",
		fileResult{name: "file.mkv"}, RepairRunOptions{}, false, storage.HealthHealthy)

	if !res.healthy {
		t.Fatalf("unselected file probed %+v, want the historical healthy verdict", res)
	}
	if got := client.linkCalls.Load(); got != 0 {
		t.Fatalf("unselected file resolved %d download links, want 0", got)
	}
}

func rangeBounds(r *http.Request, size int) (int, int) {
	start, end := 0, size-1
	if hdr := r.Header.Get("Range"); hdr != "" {
		var s, e int64
		if _, err := fmt.Sscanf(hdr, "bytes=%d-%d", &s, &e); err == nil {
			start, end = int(s), int(e)
		}
	}
	if end >= size {
		end = size - 1
	}
	if start > end {
		start = end
	}
	return start, end
}

// TestSelectPayloadProbeFilesPicksOnePerInfohash pins the cost-control rule.
func TestSelectPayloadProbeFilesPicksOnePerInfohash(t *testing.T) {
	item := &storage.EntryItem{
		Name: "Series",
		Files: map[string]*storage.File{
			"a.mkv": {Name: "a.mkv", InfoHash: "h1"},
			"b.mkv": {Name: "b.mkv", InfoHash: "h1"},
			"c.mkv": {Name: "c.mkv", InfoHash: "h2"},
			"d.mkv": {Name: "d.mkv", InfoHash: ""},
		},
	}
	names := []string{"a.mkv", "b.mkv", "c.mkv", "d.mkv"}
	got := selectPayloadProbeFiles(item, names)

	if !got["a.mkv"] || got["b.mkv"] {
		t.Fatalf("infohash h1 selection = %v, want only the first file", got)
	}
	if !got["c.mkv"] {
		t.Fatal("infohash h2 was not selected")
	}
	if got["d.mkv"] {
		t.Fatal("a file with no infohash must not be selected for a byte read")
	}
	// Deterministic: probe order drives selection, so repeated runs agree.
	for range 5 {
		again := selectPayloadProbeFiles(item, names)
		if len(again) != len(got) || !again["a.mkv"] || !again["c.mkv"] {
			t.Fatalf("selection is not deterministic: %v vs %v", again, got)
		}
	}
}

// TestUnresolvableFilesClassifyBroken covers the third mis-classification class:
// content that is not resolvable at all (no infohash, or an infohash whose entry
// row is gone) used to roll up `unknown` and let surviving siblings carry the
// entry to `healthy`.
func TestUnresolvableFilesClassifyBroken(t *testing.T) {
	_, r := newProbeFixture(t, nil)

	item := &storage.EntryItem{
		Name: "Ghost",
		Files: map[string]*storage.File{
			"no-hash.mkv":  {Name: "no-hash.mkv"},
			"no-entry.mkv": {Name: "no-entry.mkv", InfoHash: "not-in-store"},
		},
	}

	noHash := r.probeFile(context.Background(), item, "no-hash.mkv", RepairRunOptions{}, false, storage.HealthHealthy)
	if !noHash.broken || noHash.reason != "missing_infohash" {
		t.Fatalf("file without an infohash probed %+v, want broken/missing_infohash", noHash)
	}
	noEntry := r.probeFile(context.Background(), item, "no-entry.mkv", RepairRunOptions{}, false, storage.HealthHealthy)
	if !noEntry.broken || noEntry.reason != "entry_not_found" {
		t.Fatalf("file whose entry row is gone probed %+v, want broken/entry_not_found", noEntry)
	}
	if rollupStatus([]fileResult{noHash, {name: "ok", healthy: true}}) != storage.HealthBroken {
		t.Fatal("an unresolvable file did not condemn the entry")
	}
}

// TestIndeterminateIsNeverActionable is the safety bar for the whole probe
// rework: an entry that could not be verified must never be pruned or re-grabbed.
func TestIndeterminateIsNeverActionable(t *testing.T) {
	m, r := newProbeFixture(t, nil)
	arrSrv := newFakeArrServer(t)

	unknown := []fileResult{{name: "a.mkv", reason: "provider_probe_indeterminate"}}
	if got := rollupStatus(unknown); got != storage.HealthUnknown {
		t.Fatalf("rollupStatus = %q, want unknown", got)
	}

	h := &storage.EntryHealth{
		EntryName: "Unverified",
		Protocol:  config.ProtocolTorrent,
		Status:    storage.HealthUnknown,
		BrokenFiles: []storage.BrokenFile{{
			EntryName: "Unverified", FileName: "a.mkv", InfoHash: "hash", ArrName: "radarr", MediaID: 1, ArrFileID: 2,
		}},
	}
	run := &storage.RepairRun{ID: "unknown-run"}
	var mu sync.Mutex
	actions := repairActions{repair: true, prune: true, arrDelete: true}
	r.actOnDeadEntry(context.Background(), run, &mu, "Unverified", h, actions, r.newDeletionBudget(run.ID))

	if run.Stats.Pruned != 0 || run.Stats.ArrDeleted != 0 {
		t.Fatalf("unknown entry triggered destructive actions: pruned=%d arr_deleted=%d", run.Stats.Pruned, run.Stats.ArrDeleted)
	}
	if got := arrSrv.totalCalls(); got != 0 {
		t.Fatalf("unknown entry made %d arr calls, want 0", got)
	}
	_ = m
}

// TestVerdictRecheckDelayShortensForIndeterminate keeps `unknown` from becoming
// a permanent resting state.
func TestVerdictRecheckDelayShortensForIndeterminate(t *testing.T) {
	_, r := newProbeFixture(t, nil)
	full := r.recheckInterval()

	if got := r.verdictRecheckDelay(storage.HealthHealthy); got != full {
		t.Fatalf("healthy delay = %v, want %v", got, full)
	}
	if got := r.verdictRecheckDelay(storage.HealthBroken); got != full {
		t.Fatalf("broken delay = %v, want %v", got, full)
	}
	unknown := r.verdictRecheckDelay(storage.HealthUnknown)
	if unknown >= full {
		t.Fatalf("unknown delay = %v, want shorter than the recheck interval %v", unknown, full)
	}
	if unknown != repairIndeterminateRetry {
		t.Fatalf("unknown delay = %v, want %v", unknown, repairIndeterminateRetry)
	}
}

// TestDowngradeUnverifiableHealthClearsStaleHealthy pins that an entry whose
// body can no longer be loaded stops reporting healthy (and stays
// non-actionable).
func TestDowngradeUnverifiableHealthClearsStaleHealthy(t *testing.T) {
	m, r := newProbeFixture(t, nil)
	h := &storage.EntryHealth{
		EntryName: "Vanished",
		Status:    storage.HealthHealthy,
		LastOKAt:  time.Now().Add(-time.Hour),
	}
	if err := m.storage.SaveEntryHealth(h); err != nil {
		t.Fatalf("SaveEntryHealth: %v", err)
	}

	r.downgradeUnverifiableHealth("Vanished")

	got, err := m.storage.GetEntryHealth("Vanished")
	if err != nil || got == nil {
		t.Fatalf("GetEntryHealth: %v", err)
	}
	if got.Status != storage.HealthUnknown {
		t.Fatalf("status = %q, want unknown (a vanished entry must stop asserting health)", got.Status)
	}
	if got.NextCheckDueAt.Sub(got.LastCheckedAt) >= r.recheckInterval() {
		t.Fatal("an unverifiable entry was parked for a full recheck interval")
	}
}

// TestUsenetIndeterminateProbeIsUnknown pins the nzb side of the sentinel
// mapping without needing a live NNTP substrate.
func TestUsenetIndeterminateProbeIsUnknown(t *testing.T) {
	indeterminate := fmt.Errorf("%w for %q: dial refused", usenet.ErrAvailabilityIndeterminate, "movie.mkv")
	if !errors.Is(indeterminate, usenet.ErrAvailabilityIndeterminate) {
		t.Fatal("sentinel does not survive wrapping")
	}
	if isDeadContentVerdict(indeterminate) {
		t.Fatal("an indeterminate availability answer must not be a dead-content verdict")
	}
	if isDeadContentVerdict(errors.New("boom")) {
		t.Fatal("an unclassified error must not be a dead-content verdict")
	}
	if isDeadContentVerdict(context.DeadlineExceeded) {
		t.Fatal("a timeout must not be a dead-content verdict")
	}
	if !isDeadContentVerdict(customerror.UsenetSegmentMissingError) {
		t.Fatal("a definitively missing segment must be a dead-content verdict")
	}
	if !isDeadContentVerdict(customerror.NewContentGoneError(errors.New("410"))) {
		t.Fatal("a permanent 410 must be a dead-content verdict")
	}
	if !isDeadContentVerdict(debridTypes.EmptyDownloadLinkError) {
		t.Fatal("an empty download link must be a dead-content verdict")
	}
	if isDeadContentVerdict(debridTypes.ErrAvailabilityIndeterminate) {
		t.Fatal("an indeterminate provider answer must not be a dead-content verdict")
	}
}

// TestManualRecheckMediaHonoursDeletionCap closes the bypass: RecheckMedia can
// resolve a whole series' worth of entries, yet it used to pass a nil (=
// unlimited) budget while only the scheduled sweep honoured
// max_deletions_per_run. This drives the exact sequence executeRecheckMedia now
// runs — newDeletionBudget -> probeAndHealCandidates -> recordBudgetStats.
func TestManualRecheckMediaHonoursDeletionCap(t *testing.T) {
	m, r, _, _ := newRepairCapFixture(t, 2)

	hashes := makeHashes("media-", 5)
	candidates := make(map[string]*candidate, len(hashes))
	names := make([]string, 0, len(hashes))
	for i, hash := range hashes {
		name := "MediaMovie" + string(rune('A'+i))
		seedBrokenEntry(t, m, hash, name)
		item, err := m.storage.GetEntryItem(name)
		if err != nil || item == nil {
			t.Fatalf("GetEntryItem(%s): %v", name, err)
		}
		candidates[name] = &candidate{
			name:    name,
			item:    item,
			arrName: "radarr",
			arrKind: storage.ArrKindRadarr,
			contentMap: map[string]arr.ContentFile{
				"file.mkv": {Id: 555, FileId: 777, Name: "file.mkv"},
			},
		}
		names = append(names, name)
	}

	run := &storage.RepairRun{ID: uuid.NewString(), Status: storage.RepairRunRunning}
	if err := m.storage.SaveRepairRun(run); err != nil {
		t.Fatalf("SaveRepairRun: %v", err)
	}

	budget := r.newDeletionBudget(run.ID)
	actions := repairActions{repair: true, prune: true, arrDelete: true}
	if err := r.probeAndHealCandidates(context.Background(), run, candidates, names, newHealCache(), RepairRunOptions{}, actions, budget); err != nil {
		t.Fatalf("probeAndHealCandidates: %v", err)
	}
	recordBudgetStats(run, budget)

	if budget.deletions() != 2 {
		t.Fatalf("manual media recheck performed %d deletions, want the cap of 2", budget.deletions())
	}
	if got := countExisting(t, m, hashes); got != 3 {
		t.Fatalf("%d/5 entries remain, want 3 (only 2 deletable under the cap)", got)
	}
	if run.Stats.Deletions != 2 || run.Stats.DeletionCapSkipped != 3 {
		t.Fatalf("run stats = deletions %d skipped %d, want 2/3", run.Stats.Deletions, run.Stats.DeletionCapSkipped)
	}
}

// TestProbeStampsCurrentProbeVersion closes the loop between the probe and the
// scheduler at the level the production failure actually happened.
//
// Deploying the corrected probe produced: candidates=0, probed=0,
// skipped_fresh=47627. Every record carried a next_check_due_at written by the
// OLD probe — the one that moved zero bytes and failed open — so the corrected
// probe was not allowed to re-examine the library for up to a full recheck
// interval. This drives the whole path: a pre-versioning record must survive
// filterDueCandidates (the function that computes skipped_fresh), and the probe
// that then runs must stamp its own algorithm identity so the NEXT sweep can
// legitimately skip it as fresh.
func TestProbeStampsCurrentProbeVersion(t *testing.T) {
	m, r, _, _ := newRepairCapFixture(t, 0)

	const name = "VersionedMovie"
	seedBrokenEntry(t, m, "version-stamp", name)

	// The production shape: healthy, probed "seconds ago" by the old probe, and
	// parked a full recheck interval out. ProbeVersion is deliberately left at
	// its zero value — that is what a record written before this field existed
	// decodes to.
	recheck := r.recheckInterval()
	stale := &storage.EntryHealth{
		EntryName:      name,
		Protocol:       config.ProtocolTorrent,
		Status:         storage.HealthHealthy,
		FileCount:      1,
		LastCheckedAt:  time.Now(),
		LastOKAt:       time.Now(),
		NextCheckDueAt: time.Now().Add(recheck),
	}
	if err := m.storage.SaveEntryHealth(stale); err != nil {
		t.Fatalf("SaveEntryHealth: %v", err)
	}

	item, err := m.storage.GetEntryItem(name)
	if err != nil || item == nil {
		t.Fatalf("GetEntryItem(%s): %v", name, err)
	}
	c := &candidate{name: name, item: item}

	// The sweep's own selection must pick it up, and must NOT count it as fresh.
	due, skippedFresh := r.filterDueCandidates(map[string]*candidate{name: c}, false)
	if _, ok := due[name]; !ok {
		t.Fatalf("an entry whose verdict came from an older probe was not selected; skipped_fresh=%d", skippedFresh)
	}
	if skippedFresh != 0 {
		t.Fatalf("skipped_fresh = %d, want 0 — a pre-versioning verdict must never suppress a re-probe", skippedFresh)
	}

	h, _ := r.probeEntry(context.Background(), "run-version-stamp", c, newHealCache(), RepairRunOptions{}, false)
	if h == nil {
		t.Fatal("probeEntry returned nil; it must record a verdict")
	}
	if h.ProbeVersion != repairProbeVersion {
		t.Fatalf("probeEntry wrote ProbeVersion = %d, want %d; an unstamped verdict is re-probed forever", h.ProbeVersion, repairProbeVersion)
	}

	saved, err := m.storage.GetEntryHealth(name)
	if err != nil || saved == nil {
		t.Fatalf("GetEntryHealth: %v", err)
	}
	if saved.ProbeVersion != storage.RepairProbeVersion {
		t.Fatalf("persisted ProbeVersion = %d, want %d", saved.ProbeVersion, storage.RepairProbeVersion)
	}

	// And the sweep AFTER this one must be allowed to skip it again on
	// freshness — otherwise the change trades a suppressed sweep for a permanent
	// full-library re-probe. (This entry probes broken, which is always-due by
	// design, so assert on a healthy record carrying the stamp the probe wrote.)
	saved.Status = storage.HealthHealthy
	saved.Structural = false
	if saved.IsDue(time.Now(), recheck) {
		t.Fatal("a freshly-stamped healthy verdict is still due; freshness must keep working at the current version")
	}
}

// TestStructuralProbeStampsCurrentProbeVersion covers the other verdict-writing
// path: an entry-item that lists no probeable file never reaches probeFiles, so
// it needs its own stamp or it would be re-probed on every single run forever.
func TestStructuralProbeStampsCurrentProbeVersion(t *testing.T) {
	_, r := newProbeFixture(t, nil)
	c := &candidate{name: "EmptyRelease", item: &storage.EntryItem{Name: "EmptyRelease", Files: map[string]*storage.File{}}}

	h, _ := r.probeEntry(context.Background(), "run-structural-version", c, newHealCache(), RepairRunOptions{}, false)
	if h == nil {
		t.Fatal("probeEntry returned nil for a readable zero-file entry")
	}
	if h.ProbeVersion != repairProbeVersion {
		t.Fatalf("structural verdict wrote ProbeVersion = %d, want %d", h.ProbeVersion, repairProbeVersion)
	}
}

// TestDowngradeUnverifiableHealthStampsCurrentProbeVersion covers the third
// verdict-writing path. It is a CHECK outcome from the current algorithm ("could
// not verify"), and it carries the short indeterminate retry — leaving it
// unstamped would make that retry unreachable and re-attempt the entry every run.
func TestDowngradeUnverifiableHealthStampsCurrentProbeVersion(t *testing.T) {
	m, r := newProbeFixture(t, nil)
	if err := m.storage.SaveEntryHealth(&storage.EntryHealth{
		EntryName: "Unloadable",
		Status:    storage.HealthHealthy,
		LastOKAt:  time.Now().Add(-time.Hour),
	}); err != nil {
		t.Fatalf("SaveEntryHealth: %v", err)
	}

	r.downgradeUnverifiableHealth("Unloadable")

	got, err := m.storage.GetEntryHealth("Unloadable")
	if err != nil || got == nil {
		t.Fatalf("GetEntryHealth: %v", err)
	}
	if got.ProbeVersion != repairProbeVersion {
		t.Fatalf("downgrade wrote ProbeVersion = %d, want %d", got.ProbeVersion, repairProbeVersion)
	}
	if got.IsDue(time.Now(), r.recheckInterval()) {
		t.Fatal("a freshly-stamped unverifiable verdict is due immediately; its short retry deadline must gate it")
	}
}

// TestRecheckEntryCarriesDeletionBudget pins that the single-entry manual path
// now runs under a real budget (it used to pass nil, silently opting out of the
// only mass-delete guard) without that budget blocking its one legitimate
// action.
func TestRecheckEntryCarriesDeletionBudget(t *testing.T) {
	m, r, _, _ := newRepairCapFixture(t, 2)

	const name = "SingleMovie"
	hash := "recheck-single"
	seedBrokenEntry(t, m, hash, name)

	if _, err := r.RecheckEntry(context.Background(), name, &ManualActions{Prune: true}, false); err != nil {
		t.Fatalf("RecheckEntry: %v", err)
	}
	r.runWG.Wait()

	if entryExists(t, m, hash) {
		t.Fatal("single-entry recheck did not prune the dead entry; the budget must not block one legitimate action")
	}
}

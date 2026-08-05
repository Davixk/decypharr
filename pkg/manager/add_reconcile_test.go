package manager

import (
	"errors"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/customerror"
)

// The received-but-lost window, and the doctrine line it must not cross.
//
// Recovering the ID of a transfer decypharr ITSELF submitted is not adoption.
// Adoption claims transfers we never started, presumes exclusive ownership of
// the account, and is forbidden. The ledger is what keeps these apart: only a
// hash written down seconds earlier can ever be matched, so a foreign transfer
// is invisible to this code by construction.

const reconcileHash = "0123456789abcdef0123456789abcdef01234567"

func TestOnlyLedgeredHashesAreReconcilable(t *testing.T) {
	l := newPendingAddLedger()
	now := time.Now()

	if _, ok := l.pending("rd", reconcileHash, now); ok {
		t.Fatal("a hash that was never submitted must not be reconcilable — that is the adoption line")
	}

	l.begin("rd", reconcileHash)
	if _, ok := l.pending("rd", reconcileHash, now); !ok {
		t.Fatal("a hash we just submitted must be reconcilable")
	}

	// Scoped per provider: submitting to one does not make it recoverable from
	// another, where it might belong to somebody else entirely.
	if _, ok := l.pending("ad", reconcileHash, now); ok {
		t.Fatal("the ledger must be scoped per provider")
	}
}

// TestTheWindowExpires. A long window would start to resemble a claim on
// anything that appeared later, which is the line this must not cross.
func TestTheWindowExpires(t *testing.T) {
	l := newPendingAddLedger()
	l.begin("rd", reconcileHash)

	if _, ok := l.pending("rd", reconcileHash, time.Now()); !ok {
		t.Fatal("expected the entry to be live")
	}
	if _, ok := l.pending("rd", reconcileHash, time.Now().Add(pendingAddTTL+time.Second)); ok {
		t.Fatal("a stale ledger entry must expire rather than authorise a late recovery")
	}
}

func TestResolveClearsTheLedger(t *testing.T) {
	l := newPendingAddLedger()
	l.begin("rd", reconcileHash)
	l.resolve("rd", reconcileHash)
	if _, ok := l.pending("rd", reconcileHash, time.Now()); ok {
		t.Fatal("a resolved add must leave nothing behind")
	}
}

func TestExpiredReturnsAndClears(t *testing.T) {
	l := newPendingAddLedger()
	l.begin("rd", reconcileHash)

	if got := l.expired(time.Now()); len(got) != 0 {
		t.Fatalf("expired = %v on a fresh entry", got)
	}
	got := l.expired(time.Now().Add(pendingAddTTL + time.Second))
	if len(got) != 1 || got[0].infoHash != reconcileHash {
		t.Fatalf("expired = %+v, want the stale entry", got)
	}
	if _, ok := l.pending("rd", reconcileHash, time.Now()); ok {
		t.Fatal("expired entries must be cleared as they are reported")
	}
}

// A CACHED LISTING MAY NOT SETTLE A MISS IT COULD NOT HAVE SEEN.
//
// This is the storm case, and getting it wrong reintroduces the exact orphan the
// ledger exists to prevent: ambiguous add A fetches a snapshot, ambiguous add B
// arrives seconds later and hits that same cached snapshot — which was taken
// BEFORE B was ever submitted. Reading its silence as "confirmed absent" would
// declare a clean failure on a transfer that landed.
func TestAStaleSnapshotCannotConfirmAbsence(t *testing.T) {
	r := newReconcileListing()
	snapshotAt := time.Now()

	// A snapshot exists and is within its TTL, but it predates this submission.
	r.byHash["rd"] = map[string][]providerMatch{}
	r.fetched["rd"] = snapshotAt

	submittedAt := snapshotAt.Add(5 * time.Second)
	now := snapshotAt.Add(6 * time.Second)

	if _, known := r.fromCacheLocked("rd", reconcileHash, submittedAt, now); known {
		t.Fatal("a listing taken before the add was submitted answered \"absent\"; " +
			"that orphans the transfer this code exists to recover")
	}

	// Taken after the submission, the same empty snapshot IS authoritative.
	r.fetched["rd"] = submittedAt.Add(time.Second)
	if _, known := r.fromCacheLocked("rd", reconcileHash, submittedAt, now); !known {
		t.Fatal("a listing taken after the submission must be able to confirm absence")
	}
}

// A HIT is good regardless of the snapshot's age: our own hash appearing in any
// listing proves the provider has it.
func TestAStaleSnapshotStillConfirmsPresence(t *testing.T) {
	r := newReconcileListing()
	snapshotAt := time.Now()
	r.byHash["rd"] = map[string][]providerMatch{reconcileHash: {{id: "TRANSFER-1"}}}
	r.fetched["rd"] = snapshotAt

	matches, known := r.fromCacheLocked("rd", reconcileHash, snapshotAt.Add(5*time.Second), snapshotAt.Add(6*time.Second))
	if !known || len(matches) != 1 || matches[0].id != "TRANSFER-1" {
		t.Fatalf("matches=%+v known=%v; presence in any snapshot proves the add landed", matches, known)
	}
}

func TestAnExpiredSnapshotAnswersNothing(t *testing.T) {
	r := newReconcileListing()
	at := time.Now()
	r.byHash["rd"] = map[string][]providerMatch{reconcileHash: {{id: "TRANSFER-1"}}}
	r.fetched["rd"] = at

	if _, known := r.fromCacheLocked("rd", reconcileHash, at, at.Add(reconcileListingTTL)); known {
		t.Fatal("a snapshot past its TTL must force a re-read")
	}
}

// ONE INFOHASH, TWO TRANSFERS — measured live, not theoretical.
//
// Pruning RealDebrid strays turned up two hashes each holding two distinct
// transfer ids; RD does not deduplicate by infohash, so a retried add that
// landed twice leaves two transfers, each consuming a download slot. "Present →
// recover THE id" is underspecified in that case, so the choice is pinned here.
func TestDuplicateTransfersKeepTheMostAdvanced(t *testing.T) {
	keep, extras := bestMatch([]providerMatch{
		{id: "F6TVAJLLLRQ6I", progress: 12.5},
		{id: "T4LOUDQDI76PQ", progress: 87.0},
	})
	if keep.id != "T4LOUDQDI76PQ" {
		t.Fatalf("kept %q; the most-advanced transfer represents real work the provider already did", keep.id)
	}
	if len(extras) != 1 || extras[0].id != "F6TVAJLLLRQ6I" {
		t.Fatalf("extras = %+v, want exactly the losing transfer so it can be released", extras)
	}
}

// The tie-break must be STABLE, or two runs against the same account disagree
// about which transfer is ours and each deletes the other's keeper.
func TestDuplicateTieBreakIsDeterministic(t *testing.T) {
	matches := []providerMatch{
		{id: "ZZZ", progress: 50},
		{id: "AAA", progress: 50},
		{id: "MMM", progress: 50},
	}
	first, _ := bestMatch(matches)
	for range 20 {
		again, _ := bestMatch([]providerMatch{
			{id: "MMM", progress: 50},
			{id: "ZZZ", progress: 50},
			{id: "AAA", progress: 50},
		})
		if again.id != first.id {
			t.Fatalf("tie-break is order-dependent: got %q then %q", first.id, again.id)
		}
	}
}

func TestSingleMatchYieldsNoExtras(t *testing.T) {
	keep, extras := bestMatch([]providerMatch{{id: "ONLY", progress: 3}})
	if keep.id != "ONLY" || len(extras) != 0 {
		t.Fatalf("keep=%+v extras=%+v; the ordinary case must not delete anything", keep, extras)
	}
}

// DECLINE CLASSIFICATION.
//
// The critical property is the one that is easiest to get wrong: a CAPACITY
// refusal must never be treated as a verdict about the release. Parking such a
// hash for hours would break the hold mechanism, which exists precisely to wait
// for capacity that frees in minutes.

func TestCapacityIsNeverAPermanentDecline(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"slots exhausted", customerror.TooManyActiveDownloadsError},
		{"add quota exhausted", customerror.ProviderAddQuotaExhaustedError},
		{"wrapped in a provider stage error",
			providerStageError("rd", "submit", customerror.TooManyActiveDownloadsError)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyDecline(tc.err); got != declineTransient {
				t.Fatal("a capacity refusal was classified as a verdict about the release; " +
					"parking it would starve an entry that is merely waiting for a slot")
			}
		})
	}
}

func TestPerReleaseRefusalIsPermanent(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"451 from the provider", errors.New("realdebrid API error: Status: 451")},
		{"wrapped in a stage error",
			providerStageError("rd", "submit", errors.New("realdebrid API error: Status: 451"))},
		{"named refusal", errors.New("infringing content")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyDecline(tc.err); got != declinePermanent {
				t.Fatalf("class = %v; this provider will refuse this release every time", got)
			}
		})
	}
}

// TestUnrecognisedFailuresAreTransient is the asymmetry, and it is deliberate.
//
// A transient misclassification costs one more attempt after a bounded backoff.
// A permanent one parks a perfectly good release for hours. So permanent
// requires a POSITIVE, release-specific signal and everything else falls
// through to transient — including account-wide failures like a bad API key,
// which are permanent for every release and must not be answered by quietly
// shedding one hash at a time.
func TestUnrecognisedFailuresAreTransient(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"timeout", errors.New("context deadline exceeded")},
		{"retries exhausted", errors.New("POST /torrents/addMagnet gave up after 4 attempt(s): status 429")},
		{"auth failure is account-wide, not per-release",
			customerror.NewPermanentError(errors.New("401 unauthorized"))},
		{"unknown", errors.New("something nobody classified")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyDecline(tc.err); got != declineTransient {
				t.Fatalf("class = %v; only a positive release-specific refusal may park a hash", got)
			}
		})
	}
}

func TestTransientDeclineBacksOffAndRecovers(t *testing.T) {
	l := newDeclineLedger()
	now := time.Now()

	first := l.record("rd", reconcileHash, declineTransient, "timeout", now)
	if first != declineBackoffBase {
		t.Fatalf("first cooldown = %s, want %s", first, declineBackoffBase)
	}
	if cooling, _ := l.cooling("rd", reconcileHash, now); !cooling {
		t.Fatal("a just-declined hash must be parked")
	}

	second := l.record("rd", reconcileHash, declineTransient, "timeout", now)
	if second <= first {
		t.Fatalf("second cooldown = %s, want longer than %s", second, first)
	}

	// Bounded, so a provider that recovers is retried within a known time.
	for range 20 {
		l.record("rd", reconcileHash, declineTransient, "timeout", now)
	}
	capped := l.record("rd", reconcileHash, declineTransient, "timeout", now)
	if capped != declineBackoffMax {
		t.Fatalf("cooldown = %s, want it capped at %s", capped, declineBackoffMax)
	}

	// Past its cooldown it is eligible again.
	if cooling, _ := l.cooling("rd", reconcileHash, now.Add(declineBackoffMax+time.Second)); cooling {
		t.Fatal("an expired cooldown must release the hash")
	}
}

// TestSuccessClearsTheCooldown: an add that finally lands must not leave the
// hash parked, or a recovered release would be refused on its next grab.
func TestSuccessClearsTheCooldown(t *testing.T) {
	l := newDeclineLedger()
	now := time.Now()
	l.record("rd", reconcileHash, declineTransient, "timeout", now)
	l.clear("rd", reconcileHash)

	if cooling, _ := l.cooling("rd", reconcileHash, now); cooling {
		t.Fatal("a successful add must clear the cooldown")
	}
}

func TestCooldownIsScopedPerProvider(t *testing.T) {
	l := newDeclineLedger()
	now := time.Now()
	l.record("rd", reconcileHash, declinePermanent, "451", now)

	if cooling, _ := l.cooling("ad", reconcileHash, now); cooling {
		t.Fatal("one provider refusing a release says nothing about another — the fallback " +
			"chain depends on being able to try the next one")
	}
}

// ⚠️ THE EXPENSIVE REFUSALS MUST COOL OFF TOO — and they were the only ones that
// did not.
//
// The cooldown was wired to the SubmitMagnet failure path alone, which is the
// CHEAP case: one rejected request. A post-submit refusal (seeder gate,
// uncached-disabled) has already spent a provider write, a status check, a UDP
// tracker scrape, and then a provider DELETE to clean up the transfer it just
// created — roughly four operations to reach the same "no".
//
// Caught in the field: an *arr walked three indexer variants of one dead release
// in fifteen seconds and paid that full cost each time, four seconds apart,
// because nothing remembered the previous answer.
func TestPostSubmitRefusalsCoolOff(t *testing.T) {
	m := newActionLifecycleFixture(t, 1)
	now := time.Now()

	if cooling, _ := m.declines.cooling("rd", reconcileHash, now); cooling {
		t.Fatal("fixture started with a parked hash")
	}

	// The seeder gate's verdict, verbatim from the field.
	m.parkPostSubmitRefusal("rd", reconcileHash,
		errors.New("uncached release has 0 seeders per udp_scrape, below the minimum of 1"))

	cooling, why := m.declines.cooling("rd", reconcileHash, now)
	if !cooling {
		t.Fatal("a seeder-gate refusal did not park the hash; the next indexer variant seconds later " +
			"pays for another submit + status check + scrape + delete to be told the same thing")
	}
	if why == "" {
		t.Fatal("the cooldown must carry the reason, or the decline log says nothing useful")
	}
}

// Stable but NOT permanent: seeders can return and a release can become cached.
// Same rule as everywhere else — permanent requires a positive, release-specific
// refusal from the provider, and this is neither.
func TestPostSubmitRefusalsAreTransientNotPermanent(t *testing.T) {
	m := newActionLifecycleFixture(t, 1)
	now := time.Now()

	m.parkPostSubmitRefusal("rd", reconcileHash,
		errors.New("torrent is not cached and uncached downloads are disabled"))

	// Past the transient ceiling, far short of the permanent duration.
	if cooling, _ := m.declines.cooling("rd", reconcileHash, now.Add(declineBackoffMax+time.Minute)); cooling {
		t.Fatalf("still parked after %s; parking a post-submit refusal for the permanent %s would "+
			"refuse a release that has since become cached or regained seeders",
			declineBackoffMax+time.Minute, declinePermanentCooldown)
	}
}

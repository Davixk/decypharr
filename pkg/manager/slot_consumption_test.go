package manager

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// CAPACITY WE SPEND OURSELVES HAS TO COUNT.
//
// The admission controller reads a capacity snapshot taken on someone else's
// schedule. Nothing decremented that snapshot when the provider accepted an
// add, so inside a single refresh interval every subsequent add read a number
// stale by at least one item — stale because WE made it so.
//
// The failure is not a slow drift. A burst of adds arriving inside one interval
// ALL read the same pre-burst value, so the error grows with concurrency: the
// precise condition the admission controller exists to survive. At a provider's
// active ceiling that is the difference between admitting what fits and
// admitting a storm the provider then refuses one request at a time — each
// refusal costing a rate-limit token and, on RealDebrid, counting against the
// same global 250/min budget as the adds trying to get through.

func burstRequest(provider, hash string) *ImportRequest {
	return &ImportRequest{
		SelectedDebrid: provider,
		Magnet: &utils.Magnet{
			Name:     "release-" + hash,
			InfoHash: hash,
			Size:     1234,
			Link:     "magnet:?xt=urn:btih:" + hash,
		},
		Arr: &arr.Arr{Name: "radarr"},
	}
}

// 🔴 THE DEFECT, REPRODUCED THROUGH THE REAL ADD PATH.
//
// Three free slots, four arrivals, no poller tick in between. The fourth has to
// be refused, and it can only be refused if the three that came before it were
// subtracted from the reading they all shared.
func TestABurstOfAddsCannotAllReadThePreBurstCapacity(t *testing.T) {
	const freeSlots = 3

	client := &fakeDebridClient{
		cfg:     config.Debrid{Name: "rd", DownloadUncached: boolPointer(true)},
		slotsFn: func() (int, error) { return freeSlots, nil },
	}
	m := fallbackTestManager(client)

	var refused []error
	for i := 0; i < freeSlots+1; i++ {
		_, err := m.SendToDebrid(context.Background(), burstRequest("rd", fmt.Sprintf("%040x", i)))
		if err != nil {
			refused = append(refused, err)
		}
	}

	submits, _, _ := client.counts()
	if submits != freeSlots {
		t.Fatalf("the provider took %d submits against %d free slots. Every add past the third was spent "+
			"on capacity we had already consumed ourselves, and on RealDebrid each of those refusals "+
			"costs a token from the same global budget the real adds are queued behind", submits, freeSlots)
	}
	if len(refused) != 1 {
		t.Fatalf("got %d refusals, want exactly 1: the first three fit and the fourth does not", len(refused))
	}
	if !isTooManyActiveDownloads(refused[0]) {
		t.Fatalf("the over-capacity add was refused as %v, which the job layer will treat as a hard "+
			"failure. Running out of slots is transient and must requeue", refused[0])
	}

	// ⚠️ AND IT MUST NOT HAVE ASKED. The whole reason capacity is polled out of
	// band is that a probe on this path waits on the same token bucket as the
	// submit, which is how one add came to cost two token waits.
	if n := client.slotQueries(); n != 1 {
		t.Fatalf("the provider was asked for capacity %d times; want exactly the 1 seeding tick. Any "+
			"query beyond that came from the request-serving path", n)
	}
}

// SPENDING CAPACITY IS NOT EVIDENCE THAT THE READING IS FRESH.
//
// consume must leave takenAt alone. If it renewed the timestamp, a steady
// stream of adds would keep an arbitrarily old reading looking current forever
// — and that reading is allowed to REFUSE, so it would go on refusing on the
// strength of a number nobody had checked in hours.
func TestConsumingCapacityDoesNotRefreshItsAge(t *testing.T) {
	c := newProviderSlotCache()
	client := &fakeDebridClient{
		cfg:     config.Debrid{Name: "rd"},
		slotsFn: func() (int, error) { return 5, nil },
	}

	taken := time.Now().Add(-30 * time.Second)
	c.refresh("rd", client, taken)
	c.consume("rd")

	_, known, age := c.reading("rd", taken.Add(30*time.Second))
	if !known {
		t.Fatal("a successful probe followed by a consume reported unknown")
	}
	if age < 30*time.Second {
		t.Fatalf("age is %s after consuming; consume advanced takenAt and laundered a 30s-old reading "+
			"into a fresh one", age)
	}
}

// 🛑 A STALE READING MAY NOT REFUSE. This is the .81 defect one layer up.
//
// If the poller stalls, the honest state is "we do not know", and not knowing
// admits. A zero that nobody has rechecked in a minute is not a verdict about
// capacity — it is the absence of one.
func TestAStaleReadingCannotRefuseAnAdd(t *testing.T) {
	client := &slotProbeClient{fillClient: fillClient{count: 10}}
	cfg := config.Debrid{Name: "rd", Provider: "realdebrid"}
	client.cfg = cfg
	m := newRefusalFixture(t, "rd", cfg, client)

	m.slotCache.byProvider["rd"] = providerSlotSnapshot{
		slots:   0,
		known:   true,
		takenAt: time.Now().Add(-providerSlotMaxAge - time.Second),
	}

	if err := m.admitToProvider(client, "rd"); err != nil {
		t.Fatalf("an add was refused on a reading older than %s: %v. A stalled poller must not be able "+
			"to manufacture refusals indefinitely without a single request going out", providerSlotMaxAge, err)
	}
}

// A PROBE THAT FAILED IS NOT A PROVIDER THAT IS FULL.
//
// The regression guard for the defect that shipped in .81, pinned at the layer
// that replaced it: a failed probe publishes known=false, and known=false
// admits. Storing it as slots=0 and reading that zero as "full" refused adds
// for a whole TTL on a question we never got an answer to.
func TestAFailedProbeAdmitsRatherThanRefusing(t *testing.T) {
	c := newProviderSlotCache()
	client := &fakeDebridClient{
		cfg:     config.Debrid{Name: "rd"},
		slotsFn: func() (int, error) { return 0, errors.New("provider unreachable") },
	}
	c.refresh("rd", client, time.Now())

	slots, known, _ := c.reading("rd", time.Now())
	if known {
		t.Fatalf("a failed probe published a KNOWN reading of %d slots. Admission refuses on a known "+
			"zero, so this turns a provider outage into a refusal storm", slots)
	}
}

// THE SAME GAP EXISTED IN THE FILL COUNT, WITH A THREE-MINUTE TTL.
//
// Every refresh path on the fill cache moved the count in the direction that
// FREES room — invalidate on delete, observe on enumeration — and nothing moved
// it in the direction that consumes room. So the AllDebrid cap check read a
// stored-item count we had already outgrown ourselves, for far longer than the
// slot reading was ever stale.
// ⚠️ DRIVEN THROUGH THE REAL ADD PATH, not by calling consume directly.
//
// Calling the cache method here would prove the method works and nothing else —
// the defect was never a broken method, it was a method nobody called. A test
// that reaches past the add path cannot tell those apart, which is the same
// hollow shape that let the .81 evidence look convincing.
func TestTheCapCheckSeesItemsWeStoredOurselves(t *testing.T) {
	const cap = 3

	stored := []*debridTypes.Torrent{{Id: "t0", InfoHash: "h0"}}
	client := &fakeDebridClient{
		cfg: config.Debrid{
			Name:             "ad",
			Provider:         "alldebrid",
			MaxMagnets:       intPtr(cap),
			DownloadUncached: boolPointer(true),
		},
		slotsFn:       func() (int, error) { return 500, nil },
		allTorrentsFn: func() ([]*debridTypes.Torrent, error) { return stored, nil },
	}
	m := fallbackTestManager(client)

	var refusals []error
	for i := 0; i < cap; i++ {
		_, err := m.SendToDebrid(context.Background(), burstRequest("ad", fmt.Sprintf("%040x", i)))
		if err != nil {
			refusals = append(refusals, err)
		}
	}

	// One item was already stored and the cap is three, so exactly two of those
	// three adds fit. The third has to be turned away on a count that includes
	// the two we just made ourselves.
	if len(refusals) != 1 {
		t.Fatalf("got %d refusals, want 1. The account reached its cap through our own adds and was still "+
			"admitting: the cap check is reading an enumeration taken before we filled it, and every item "+
			"admitted past that point pays a full doomed submission to be told no", len(refusals))
	}
	if !isProviderAddQuotaExhausted(refusals[0]) {
		t.Fatalf("refusal %v does not carry the add-quota sentinel, so the classifier cannot resolve it "+
			"against this account's cap and fill", refusals[0])
	}
}

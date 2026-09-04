package manager

import (
	"sync/atomic"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// A PROVIDER AT ITS STORED-ITEM CAP IS TURNED AWAY WITHOUT A SINGLE REQUEST.
//
// The fork.77 goroutine dump showed 151 of 176 workers queued for a rate-limit
// token on AllDebrid, every one of them waiting to submit an add to an account
// holding 4,998 of 4,998 items — an account that refuses deterministically. The
// pool was fully consumed by work whose answer was already known, so the
// qBittorrent add handler was never serviced and the *arrs saw a dead download
// client.
//
// The cheapest fix is not to survive that load but to stop creating it.
type slotProbeClient struct {
	fillClient
	slotCalls atomic.Int32
}

func (c *slotProbeClient) GetAvailableSlots() (int, error) {
	c.slotCalls.Add(1)
	return 100, nil
}

func TestAtCapProviderIsRefusedWithoutAskingIt(t *testing.T) {
	client := &slotProbeClient{fillClient: fillClient{count: 4998}}
	cfg := config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(4998)}
	client.cfg = cfg
	m := newRefusalFixture(t, "ad", cfg, client)

	err := m.admitToProvider(client, "ad")
	if err == nil {
		t.Fatal("an account at 4998/4998 was admitted. Every item then pays a full submission to be told no, " +
			"which is what consumed the whole worker pool")
	}

	// 🔑 NOT ONE REQUEST. The point is the cost, not the verdict: asking a
	// provider whether it has room when we already know it does not is exactly
	// the call that saturated its rate-limit queue.
	if n := client.slotCalls.Load(); n != 0 {
		t.Fatalf("the at-cap provider was still probed %d times; the skip has to happen before any request", n)
	}

	// The refusal must carry the provider's own quota sentinel, so the
	// classifier resolves it against this account rather than inventing a class.
	if !isProviderAddQuotaExhausted(err) {
		t.Fatalf("admission error %v does not carry the add-quota sentinel; classifyQuotaRefusal cannot "+
			"resolve it against the account's cap and fill", err)
	}
	if got := quotaRefusalProvider(err); got != "ad" {
		t.Fatalf("quota refusal attributed to %q, want \"ad\"", got)
	}
}

// SKIPPING THE REQUEST MUST NOT CHANGE THE VERDICT, only its cost. At cap the
// classifier still has to reach PERMANENT for that provider — the same answer a
// real submission would have produced.
func TestSkippedAtCapAdmissionStillClassifiesAsPermanent(t *testing.T) {
	client := &slotProbeClient{fillClient: fillClient{count: 4998}}
	cfg := config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(4998)}
	client.cfg = cfg
	m := newRefusalFixture(t, "ad", cfg, client)

	err := m.admitToProvider(client, "ad")
	refusal := m.classifyAddRefusal(err)

	if refusal.hold {
		t.Fatal("a skipped at-cap admission was HELD. The account cannot accept anything until items are " +
			"deleted there, so holding parks the *arr against a wall that never moves")
	}
	if refusal.standingCondition == "" {
		t.Fatal("the skip silenced the standing condition; that log line is the only thing telling the " +
			"operator to go delete items")
	}
}

// ⚠️ AND BELOW THE CAP NOTHING CHANGES. The skip is for accounts that cannot
// accept, not a general shortcut — a provider with room must still be asked,
// or a full account and a busy one become the same thing.
func TestBelowCapProviderIsStillProbed(t *testing.T) {
	client := &slotProbeClient{fillClient: fillClient{count: 10}}
	cfg := config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(4998)}
	client.cfg = cfg
	m := newRefusalFixture(t, "ad", cfg, client)

	if err := m.admitToProvider(client, "ad"); err != nil {
		t.Fatalf("a provider at 10/4998 was refused admission: %v", err)
	}
	if n := client.slotCalls.Load(); n != 1 {
		t.Fatalf("the provider was probed %d times, want 1; below the cap its own answer is the gate", n)
	}
}

// 🛑 THE EXACT-COMPARISON TRAP, PINNED. An account refusing at 4,998 against a
// configured 5,000 never trips this, and every item pays the doomed submission.
// That is not hypothetical: it is what the live deployment did for days, and it
// is why the fix and the config value are load-bearing together.
func TestCapOffByTwoLeavesTheSkipInert(t *testing.T) {
	client := &slotProbeClient{fillClient: fillClient{count: 4998}}
	cfg := config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(5000)}
	client.cfg = cfg
	m := newRefusalFixture(t, "ad", cfg, client)

	if err := m.admitToProvider(client, "ad"); err != nil {
		t.Fatalf("4998 against a cap of 5000 was skipped: %v. The comparison is exact by design, so this "+
			"must fall through — the fix depends on max_magnets holding the MEASURED ceiling", err)
	}
	if n := client.slotCalls.Load(); n != 1 {
		t.Fatalf("probed %d times, want 1", n)
	}
	_ = debridTypes.TorrentStatusQueued
	_ = customerror.ProviderAddQuotaExhaustedError
}

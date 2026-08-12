package manager

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func intPtr(v int) *int { return &v }

// fillClient reports a scripted stored-item count and counts enumerations.
type fillClient struct {
	fakeDebridClient
	count int
	err   error
	calls int
	mu    sync.Mutex
}

func (c *fillClient) GetAllTorrents() ([]*debridTypes.Torrent, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	out := make([]*debridTypes.Torrent, c.count)
	for i := range out {
		out[i] = &debridTypes.Torrent{Id: fmt.Sprintf("t%d", i), InfoHash: fmt.Sprintf("h%d", i)}
	}
	return out, nil
}

func newRefusalFixture(t *testing.T, name string, cfg config.Debrid, client debrid.Client) *Manager {
	t.Helper()
	m := newActionLifecycleFixture(t, 2)
	m.clients = xsync.NewMap[string, debrid.Client]()
	m.fillCache = newProviderFillCache()
	if fc, ok := client.(*fillClient); ok {
		fc.cfg = cfg
	}
	m.clients.Store(name, client)
	return m
}

func quotaErr(provider string) error {
	return providerStageError(provider, "submit",
		fmt.Errorf("%w: alldebrid MAGNET_TOO_MANY: Magnets limit reached (1000 accross all tabs)",
			customerror.ProviderAddQuotaExhaustedError))
}

// TestClassifyContentRefusalIsRefused: a dead or uncached release is refused so
// the arr takes its next candidate from a list it already holds.
func TestClassifyContentRefusalIsRefused(t *testing.T) {
	m := newRefusalFixture(t, "ad", config.Debrid{Name: "ad", Provider: "alldebrid"}, &fillClient{})

	for _, err := range []error{
		errors.New("torrent is not cached and uncached downloads are disabled"),
		providerStageError("ad", "status check", errors.New("no valid files found")),
		customerror.HosterUnavailableError,
	} {
		if r := m.classifyAddRefusal(err); r.hold {
			t.Errorf("content refusal %v was HELD; it must be refused so the arr moves on", err)
		}
	}
}

// TestClassifyConcurrencyIsAlwaysHeld: RealDebrid active slots and AllDebrid's
// 30 active magnets both free as work finishes. Unambiguously transient, and
// notably requiring NO fill check — this is not about how much is stored.
func TestClassifyConcurrencyIsAlwaysHeld(t *testing.T) {
	client := &fillClient{count: 4999}
	// Cap is FULL, to prove the concurrency verdict does not consult it.
	m := newRefusalFixture(t, "ad", config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(5000)}, client)

	r := m.classifyAddRefusal(customerror.TooManyActiveDownloadsError)
	if !r.hold {
		t.Fatal("concurrency exhaustion must be held; slots free as active downloads finish")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.calls != 0 {
		t.Fatalf("concurrency verdict cost %d fill enumerations, want 0", client.calls)
	}
}

// TestClassifyQuotaUnderCapIsHeld: fill below the cap means the DAILY allowance
// was spent, which resets on the provider's own boundary.
func TestClassifyQuotaUnderCapIsHeld(t *testing.T) {
	m := newRefusalFixture(t, "ad",
		config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(5000)},
		&fillClient{count: 1200})

	r := m.classifyAddRefusal(quotaErr("ad"))
	if !r.hold {
		t.Fatalf("quota refusal at 1200/5000 must be held as the daily allowance; got %+v", r)
	}
	if r.standingCondition != "" {
		t.Fatalf("a transient allowance must not raise a standing condition: %q", r.standingCondition)
	}
}

// 🔴 REVERSED BY OPERATOR RULING, AND THE TEST IS REWRITTEN RATHER THAN DELETED.
//
// This asserted that a full stored-item cap must be REFUSED, because nothing
// decypharr finishes or waits for frees it — the condition behind fork.34's
// 15.2-hour spin, observed refusing every add for 54.6 continuous hours.
//
// The ruling that replaced it: a grab is refused only when EVERY provider gives
// a PERMANENT refusal, and permanent means a verdict about the CONTENT. A full
// account is not a statement about this release, so it is a temporary inability
// and the grab is held.
//
// The loud standing condition survives unchanged and now matters MORE, not less:
// with the entry held rather than refused, that log line is the only thing that
// tells an operator why a queue is growing and never draining.
func TestClassifyQuotaAtCapIsHeldButStillLoud(t *testing.T) {
	m := newRefusalFixture(t, "ad",
		config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(5000)},
		&fillClient{count: 5000})

	r := m.classifyAddRefusal(quotaErr("ad"))
	if !r.hold {
		t.Fatal("a full stored-item cap was refused. Per the operator's ruling a full account is not a verdict " +
			"about the release, so the grab is held and retried rather than costing the arr a candidate")
	}
	if r.standingCondition == "" {
		t.Fatal("a full stored-item cap must STILL raise an operator-visible standing condition. Now that the " +
			"entry is held instead of refused, this log line is the only signal that the hold queue has no drain")
	}
	if !strings.Contains(r.standingCondition, "5000") {
		t.Fatalf("standing condition must state the arithmetic; got %q", r.standingCondition)
	}
}

// The fill no longer decides the verdict, so what is left to pin is that it
// still decides the MESSAGE — and those need opposite operator action:
//
//	under the cap and refused  -> the daily allowance, resets by itself, do nothing
//	at the cap and refused     -> the account is full, only deletions free it
//
// A margin used to live here and was removed for hiding exactly that
// distinction; keeping the comparison exact is what keeps the two messages
// truthful now that neither changes the outcome.
func TestAtCapComparisonStillDecidesTheMessage(t *testing.T) {
	cfg := config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(5000)}

	// The measured 4,998 case: UNDER the configured cap, so this is the daily
	// allowance — held, and NOT reported as a standing condition.
	underCap := newRefusalFixture(t, "ad", cfg, &fillClient{count: 4998})
	r := underCap.classifyAddRefusal(quotaErr("ad"))
	if !r.hold {
		t.Fatalf("4998/5000 must be held, got %+v", r)
	}
	if r.standingCondition != "" {
		t.Fatalf("the daily allowance is not a standing condition and must not be reported as one: %q", r.standingCondition)
	}

	// Exactly at the cap: held too, but loudly.
	atCap := newRefusalFixture(t, "ad", cfg, &fillClient{count: 5000})
	if r := atCap.classifyAddRefusal(quotaErr("ad")); !r.hold || r.standingCondition == "" {
		t.Fatalf("5000/5000 must be held AND raise a standing condition, got %+v", r)
	}

	// And an operator who knows the real ceiling is 4,998 sets the knob, which
	// then classifies exactly.
	tightened := config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(4998)}
	tight := newRefusalFixture(t, "ad", tightened, &fillClient{count: 4998})
	if r := tight.classifyAddRefusal(quotaErr("ad")); !r.hold || r.standingCondition == "" {
		t.Fatalf("with the cap set to the real ceiling, 4998/4998 must be held and loud, got %+v", r)
	}
}

// An enumeration that failed is not evidence of anything, and under the new
// ruling it does not need to be: the verdict is HOLD either way, because an add
// allowance is never a verdict about the release no matter how full the account
// turns out to be. A count we could not take now costs a vaguer log line and
// nothing else.
func TestClassifyQuotaUnknownFillIsStillHeld(t *testing.T) {
	m := newRefusalFixture(t, "ad",
		config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(5000)},
		&fillClient{err: errors.New("enumeration exploded")})

	r := m.classifyAddRefusal(quotaErr("ad"))
	if !r.hold {
		t.Fatal("a failed fill enumeration refused the grab. The fill no longer decides the verdict — it only " +
			"decides which message is printed — so a failed count must not cost the arr a candidate")
	}
	if r.standingCondition != "" {
		t.Fatalf("an unknown fill must not assert a standing condition it cannot see: %q", r.standingCondition)
	}
}

// TestClassifyQuotaUncappedIsHeld: with no cap configured and none known, there
// is no threshold to be at, so the refusal resolves to the transient case. It
// must NOT invent a number to compare against.
func TestClassifyQuotaUncappedIsHeld(t *testing.T) {
	client := &fillClient{count: 999999}
	m := newRefusalFixture(t, "rd", config.Debrid{Name: "rd", Provider: "realdebrid"}, client)

	r := m.classifyAddRefusal(quotaErr("rd"))
	if !r.hold {
		t.Fatalf("an uncapped provider must not be judged against an invented threshold; got %+v", r)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.calls != 0 {
		t.Fatalf("uncapped provider cost %d enumerations; there is no cap to compare against", client.calls)
	}
}

// TestQuotaRefusalProviderAttribution is the subtle one. A chain error joins
// every provider's refusal, so errors.As would return whichever *providerError
// sits first in the tree. Resolving a cap against the WRONG provider's fill is a
// confident wrong answer.
func TestQuotaRefusalProviderAttribution(t *testing.T) {
	// RealDebrid failed first, for an unrelated reason; AllDebrid raised quota.
	joined := joinDebridErrors([]error{
		providerStageError("rd", "submit", errors.New("connection reset")),
		quotaErr("ad"),
	})

	if got := quotaRefusalProvider(joined); got != "ad" {
		t.Fatalf("quotaRefusalProvider = %q, want \"ad\"; the quota refusal must be attributed to the provider that raised it", got)
	}
}

// TestClassifyHoldsWhenAnyProviderIsTransient: if one provider is permanently
// full but another is merely busy, holding is right — the busy one will have
// room later.
func TestClassifyHoldsWhenAnyProviderIsTransient(t *testing.T) {
	m := newRefusalFixture(t, "ad",
		config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(5000)},
		&fillClient{count: 5000})

	joined := joinDebridErrors([]error{
		quotaErr("ad"),
		customerror.TooManyActiveDownloadsError,
	})
	if r := m.classifyAddRefusal(joined); !r.hold {
		t.Fatal("a transient refusal anywhere in the chain must hold; that provider will have room later")
	}
}

// TestProviderFillCacheCollapsesEnumerations: the fill is read on refused adds,
// so a storm must cost ONE enumeration, not one per add.
func TestProviderFillCacheCollapsesEnumerations(t *testing.T) {
	client := &fillClient{count: 42}
	cache := newProviderFillCache()
	now := time.Now()

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, ok := cache.fill("ad", client, now); !ok || got != 42 {
				t.Errorf("fill = (%d,%v), want (42,true)", got, ok)
			}
		}()
	}
	wg.Wait()

	client.mu.Lock()
	calls := client.calls
	client.mu.Unlock()
	if calls != 1 {
		t.Fatalf("50 concurrent reads cost %d enumerations, want 1", calls)
	}

	// Past the TTL it re-reads, so a cap freed by a prune is noticed.
	if _, ok := cache.fill("ad", client, now.Add(providerFillTTL+time.Second)); !ok {
		t.Fatal("post-TTL read failed")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.calls != 2 {
		t.Fatalf("post-TTL read cost %d total enumerations, want 2", client.calls)
	}
}

// TestProviderFillUnknownIsNotZero: an enumeration failure must report UNKNOWN,
// never an empty account. Reading !known as zero would turn a provider outage
// into "there is plenty of room".
func TestProviderFillUnknownIsNotZero(t *testing.T) {
	cache := newProviderFillCache()
	count, known := cache.fill("ad", &fillClient{err: errors.New("down")}, time.Now())
	if known {
		t.Fatal("a failed enumeration reported a KNOWN count")
	}
	if count != 0 {
		t.Fatalf("count = %d; callers must read known=false, not the value", count)
	}
}

// TestMagnetCapTriState pins the three states across a save round-trip. With a
// plain int, "unlimited" and "absent" are both 0, so an operator who
// deliberately uncapped AllDebrid would silently get 5000 written back.
func TestMagnetCapTriState(t *testing.T) {
	// Absent -> provider default.
	ad := config.Debrid{Provider: "alldebrid"}
	if capacity, ok := ad.MagnetCap(); !ok || capacity != 5000 {
		t.Fatalf("absent AllDebrid cap = (%d,%v), want (5000,true)", capacity, ok)
	}

	// Explicit 0 -> UNLIMITED, and it must survive.
	uncapped := config.Debrid{Provider: "alldebrid", MaxMagnets: intPtr(0)}
	if capacity, ok := uncapped.MagnetCap(); ok {
		t.Fatalf("explicit 0 resolved to a cap of %d; an operator override to unlimited was discarded", capacity)
	}

	// Explicit N -> N.
	custom := config.Debrid{Provider: "alldebrid", MaxMagnets: intPtr(12345)}
	if capacity, ok := custom.MagnetCap(); !ok || capacity != 12345 {
		t.Fatalf("explicit cap = (%d,%v), want (12345,true)", capacity, ok)
	}

	// A provider with no measured ceiling is unlimited, not guessed at.
	rd := config.Debrid{Provider: "realdebrid"}
	if capacity, ok := rd.MagnetCap(); ok {
		t.Fatalf("RealDebrid resolved to a stored-item cap of %d; it bounds CONCURRENCY, not stored items", capacity)
	}
}

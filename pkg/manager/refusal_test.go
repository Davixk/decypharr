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

// TestClassifyQuotaAtCapIsRefusedLoudly: the stored-item cap is full. Nothing
// decypharr finishes or waits for frees it, so holding is the 15.2-hour spin.
func TestClassifyQuotaAtCapIsRefusedLoudly(t *testing.T) {
	m := newRefusalFixture(t, "ad",
		config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(5000)},
		&fillClient{count: 4998})

	r := m.classifyAddRefusal(quotaErr("ad"))
	if r.hold {
		t.Fatal("a full stored-item cap must be refused, not held; nothing we wait for frees it")
	}
	if r.standingCondition == "" {
		t.Fatal("a full stored-item cap must raise an operator-visible standing condition")
	}
	if !strings.Contains(r.standingCondition, "4998") || !strings.Contains(r.standingCondition, "5000") {
		t.Fatalf("standing condition must state the arithmetic; got %q", r.standingCondition)
	}
}

// TestFillIsAtCapErrsEarly pins the asymmetry. The live account refuses at
// 4,998/5,000, so `fill >= capacity` never fires and the entry would be held
// against a cap nothing frees — the fork.34 pathology. Calling at-cap early is
// the cheap mistake: a sync 400 sends the arr to its next candidate.
func TestFillIsAtCapErrsEarly(t *testing.T) {
	// The measured real condition.
	if !fillIsAtCap(4998, 5000) {
		t.Error("4998/5000 is the MEASURED full account; it must read as at-cap")
	}
	if !fillIsAtCap(5000, 5000) || !fillIsAtCap(5001, 5000) {
		t.Error("at or over the nominal cap must read as at-cap")
	}
	// A clearly-transient daily allowance must stay in the hold branch.
	if fillIsAtCap(1200, 5000) {
		t.Error("1200/5000 is the measured daily-allowance case; it must NOT read as at-cap")
	}
	if fillIsAtCap(4000, 5000) {
		t.Error("4000/5000 has ample room; it must NOT read as at-cap")
	}
	// No cap means no threshold to be at.
	if fillIsAtCap(999999, 0) {
		t.Error("an uncapped provider must never read as at-cap")
	}
}

// TestClassifyQuotaUnknownFillIsRefused: a count we could not take is not a
// count. Guessing here chooses between waiting forever on a full account and
// refusing work that would have succeeded; refusing is the cheap, visible,
// recoverable direction.
func TestClassifyQuotaUnknownFillIsRefused(t *testing.T) {
	m := newRefusalFixture(t, "ad",
		config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(5000)},
		&fillClient{err: errors.New("enumeration exploded")})

	r := m.classifyAddRefusal(quotaErr("ad"))
	if r.hold {
		t.Fatal("classified a hold from an UNKNOWN fill; absence of a count is not evidence of room")
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

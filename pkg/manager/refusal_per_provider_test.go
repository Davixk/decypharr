package manager

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
)

// PER-PROVIDER CLASSIFICATION OF CAPACITY REFUSALS.
//
// Making a full stored-item cap PERMANENT is one line of policy and a whole
// rewrite of the mechanism, because it destroyed an equivalence the classifier
// was built on. The add path joins every provider's refusal into one tree, and
// errors.Is walks the whole tree — so while every temporary sentinel was
// UNCONDITIONALLY temporary, a single whole-tree scan for "any temporary
// condition" was exactly the per-provider aggregate, for free.
//
// ProviderAddQuotaExhaustedError stopped being unconditional the moment its
// meaning began to depend on one account's fill against one account's cap. A
// whole-tree scan cannot ask whose. These tests pin what that costs and what it
// must never do.

// addProvider registers a second (or third) provider on a refusal fixture, each
// with its OWN config and its own stored-item count.
func addProvider(m *Manager, name string, cfg config.Debrid, client *fillClient) {
	client.cfg = cfg
	m.clients.Store(name, client)
}

func adAtCap(t *testing.T) *Manager {
	t.Helper()
	return newRefusalFixture(t, "ad",
		config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(5000)},
		&fillClient{count: 5000})
}

// 🛑 THE TRAP THE REWRITE EXISTS FOR.
//
// AllDebrid is permanently full. RealDebrid merely spent its add allowance,
// which resets on its own boundary. A classifier that saw "a quota error is in
// this tree, and quota-at-cap is permanent now" would refuse a grab RealDebrid
// accepts within the hour — spending a candidate, and eventually a whole
// indexer search, to dodge a condition that clears by itself.
func TestAtCapOnOneProviderHoldsWhenAnotherIsMerelyOutOfAllowance(t *testing.T) {
	m := adAtCap(t)
	// RealDebrid has no stored-item cap (it bounds concurrency, not storage), so
	// its own quota refusal can only be the transient kind.
	addProvider(m, "rd", config.Debrid{Name: "rd", Provider: "realdebrid"}, &fillClient{count: 12})

	r := m.classifyAddRefusal(joinDebridErrors([]error{quotaErr("ad"), quotaErr("rd")}))

	if !r.hold {
		t.Fatal("a grab was REFUSED because AllDebrid is full, while RealDebrid was only out of add allowance " +
			"for the day. One provider's permanent wall is not a verdict about the release, and the other " +
			"provider is going to accept this")
	}
	if r.standingCondition == "" {
		t.Fatal("the hold silenced AllDebrid's standing condition. A hold decided by a DIFFERENT provider must " +
			"not hide the account that can never accept anything again — that log line is the operator's only " +
			"signal to go delete items")
	}
	if !strings.Contains(r.standingCondition, `"ad"`) {
		t.Fatalf("standing condition does not name the full provider: %q", r.standingCondition)
	}
}

// The aggregate the operator asked for: when nothing left can ever start this
// item, refuse synchronously so the arr takes its next candidate immediately.
func TestAtCapRefusesWhenEveryProviderIsPermanent(t *testing.T) {
	m := adAtCap(t)
	addProvider(m, "rd", config.Debrid{Name: "rd", Provider: "realdebrid"}, &fillClient{count: 12})

	r := m.classifyAddRefusal(joinDebridErrors([]error{
		quotaErr("ad"),
		providerStageError("rd", "submit", errors.New("torrent is not cached and uncached downloads are disabled")),
	}))

	if r.hold {
		t.Fatal("every provider refused permanently and the grab was still HELD. This is the 11,399-row bug: " +
			"an entry accepted against a wall that never moves, with the arr waiting on it and its search spent")
	}
	if r.reason == "" {
		t.Fatal("a refused grab carries no reason. The qBittorrent shim answers this text as the 400 body and " +
			"it is the only thing an arr can show")
	}
}

// EACH ACCOUNT IS JUDGED AGAINST ITS OWN NUMBERS. Attribution is the entire
// point: resolving AllDebrid's ceiling against RealDebrid's fill is a
// confidently wrong answer, and the arithmetic in the message is what proves
// which account was read.
func TestBranchAttributionResolvesEachProvidersOwnAccount(t *testing.T) {
	// Both providers are capped, at different ceilings, with different fills.
	// Only "ad" is actually at its cap.
	m := newRefusalFixture(t, "ad",
		config.Debrid{Name: "ad", Provider: "alldebrid", MaxMagnets: intPtr(5000)},
		&fillClient{count: 5000})
	addProvider(m, "other",
		config.Debrid{Name: "other", Provider: "alldebrid", MaxMagnets: intPtr(9000)},
		&fillClient{count: 10})

	r := m.classifyAddRefusal(joinDebridErrors([]error{quotaErr("other"), quotaErr("ad")}))

	// "other" is under ITS cap, so the aggregate holds.
	if !r.hold {
		t.Fatalf("a provider at 10/9000 was treated as full; got %+v", r)
	}
	if !strings.Contains(r.standingCondition, "5000 of its 5000") {
		t.Fatalf("standing condition read the wrong account's numbers: %q", r.standingCondition)
	}
	if strings.Contains(r.standingCondition, "9000") {
		t.Fatalf("one provider's fill was resolved against another's cap: %q", r.standingCondition)
	}
}

// The join arrives WRAPPED — joinDebridErrors adds a message and singleLineError
// on top — so a splitter that only recognised a bare errors.Join would find one
// opaque branch and the whole rewrite would silently do nothing. That failure
// mode is invisible: every test above would still pass on single-provider input.
func TestSplitRefusalBranchesSeesThroughTheChainWrapper(t *testing.T) {
	joined := joinDebridErrors([]error{quotaErr("ad"), quotaErr("rd")})

	branches := splitRefusalBranches(joined)
	if len(branches) != 2 {
		t.Fatalf("split the wrapped chain into %d branches, want 2; the wrapper hid the join", len(branches))
	}

	seen := map[string]bool{}
	for _, b := range branches {
		seen[b.provider] = true
		if !errors.Is(b.err, customerror.ProviderAddQuotaExhaustedError) {
			t.Errorf("branch %q lost its sentinel during the walk: %v", b.provider, b.err)
		}
	}
	if !seen["ad"] || !seen["rd"] {
		t.Fatalf("branches attributed to %v, want both ad and rd", seen)
	}
}

// 🛑 THE WALK MUST NOT DESCEND TO THE LEAF.
//
// Walking a wrapper chain to its innermost error would hand each branch a naked
// error, and every classifier test matches by sentinel identity THROUGH the
// chain. A wrapped transient sentinel would stop matching and be classified as
// a permanent refusal — turning a rate limit that clears in seconds into a lost
// grab, which is the exact defect this file's cool-off wrapper once had.
func TestBranchKeepsItsFullChainSoSentinelsStillMatch(t *testing.T) {
	wrapped := fmt.Errorf("attempt 3 of 3: %w",
		providerStageError("rd", "submit",
			fmt.Errorf("giving up after retries: %w", customerror.RateLimitedError)))

	branches := splitRefusalBranches(wrapped)
	if len(branches) != 1 {
		t.Fatalf("a single-provider chain split into %d branches, want 1", len(branches))
	}
	if !errors.Is(branches[0].err, customerror.RateLimitedError) {
		t.Fatal("the branch lost its rate-limit sentinel; a transient condition would be refused as permanent")
	}

	m := newRefusalFixture(t, "rd", config.Debrid{Name: "rd", Provider: "realdebrid"}, &fillClient{})
	if r := m.classifyAddRefusal(wrapped); !r.hold {
		t.Fatalf("a wrapped rate limit was refused rather than held; got %+v", r)
	}
}

// An unattributed refusal still gets classified. Errors raised before any
// provider was selected have no branch owner, and dropping them would silently
// refuse conditions that used to hold.
func TestUnattributedRefusalsAreStillClassified(t *testing.T) {
	m := adAtCap(t)

	if r := m.classifyAddRefusal(customerror.TooManyActiveDownloadsError); !r.hold {
		t.Fatal("a bare concurrency sentinel was refused; slots free as active downloads finish")
	}
	if r := m.classifyAddRefusal(errors.New("no valid files found")); r.hold {
		t.Fatal("an unattributed content refusal was held")
	}

	// And mixed with an attributed at-cap refusal, the transient branch still
	// wins the aggregate.
	mixed := joinDebridErrors([]error{quotaErr("ad"), customerror.TooManyActiveDownloadsError})
	if r := m.classifyAddRefusal(mixed); !r.hold {
		t.Fatal("an unattributed transient branch was dropped from the aggregate, refusing a grab that a " +
			"provider with freeing slots is about to accept")
	}
}

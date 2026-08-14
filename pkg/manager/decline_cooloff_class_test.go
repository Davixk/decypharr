package manager

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
)

// 🔴 THE COOL-OFF WRAPPER MUST NOT CHANGE THE CLASS OF WHAT IT WRAPS.
//
// The loop this pins, measured on the live deployment for hash 8a0ea451:
//
//	AllDebrid at 5000/5000 stored items -> the add is ACCEPTED AND HELD (correct)
//	a slot frees, the held entry is re-admitted                          (correct)
//	every provider answers from its decline cool-off                     (correct)
//	the cool-off error is rebuilt as a STRING                            (the defect)
//	classifyAddRefusal cannot see MAGNET_TOO_MANY in it, so it refuses
//	the async path has no arr to refuse to, so the row parks as `error`
//
// 13 of 14 new parked error rows in two hours were exactly this. The
// classification matches by sentinel identity precisely so that no wrapper can
// misreport a class — and a wrapper upstream was flattening the sentinel to text
// before the classifier ever saw it.
func TestCoolOffPreservesTheTemporaryClass(t *testing.T) {
	m := newRefusalFixture(t, "ad", config.Debrid{Name: "ad", Provider: "alldebrid"}, &fillClient{})
	l := newDeclineLedger()
	now := time.Unix(1_700_000_000, 0).UTC()

	// The exact AllDebrid refusal from the field.
	original := fmt.Errorf("%w: alldebrid MAGNET_TOO_MANY: Magnets limit reached (1000 accross all tabs)",
		customerror.ProviderAddQuotaExhaustedError)
	l.record("ad", "8a0ea451", declineTransient, original, now)

	cooling, why := l.cooling("ad", "8a0ea451", now)
	if !cooling {
		t.Fatal("the hash did not cool off at all")
	}

	// Rebuilt exactly as the add path rebuilds it.
	coolErr := providerStageError("ad", "submit",
		fmt.Errorf("cooling off after an earlier decline: %w", why))

	if !errors.Is(coolErr, customerror.ProviderAddQuotaExhaustedError) {
		t.Fatalf("the cool-off wrapper destroyed the sentinel: %v", coolErr)
	}
	if r := m.classifyAddRefusal(coolErr); !r.hold {
		t.Fatalf("a cool-off-wrapped quota exhaustion was REFUSED (%+v). On the async re-admission path there is "+
			"no arr left to refuse to, so the row parks as `error` — the 13-of-14 shape", r)
	}
}

// 🛑 AND IT MUST NOT LAUNDER A VERDICT INTO A HOLD EITHER — the direction the
// NAS asked to be checked, and the one a careless fix breaks.
//
// "0 seeders" is a legitimate FAIL under the operator's ruling: the release is
// unservable and the arr should take its next candidate. Before the fix it
// refused for the WRONG reason (everything cool-off-wrapped was unclassifiable),
// so a fix that made cool-off errors hold indiscriminately would pass the test
// above and quietly convert every dead release into a permanent queuedDL row.
func TestCoolOffPreservesTheVerdictClass(t *testing.T) {
	m := newRefusalFixture(t, "rd", config.Debrid{Name: "rd", Provider: "realdebrid"}, &fillClient{})
	l := newDeclineLedger()
	now := time.Unix(1_700_000_000, 0).UTC()

	for _, tc := range []struct {
		name     string
		original error
	}{
		{
			"seeder gate",
			errors.New("uncached release has 0 seeders per udp_scrape, below the minimum of 1"),
		},
		{
			"legal takedown",
			customerror.NewContentTakedownError(errors.New("realdebrid: infringing_file (code 35)")),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hash := "verdict-" + tc.name
			l.record("rd", hash, classifyDecline(tc.original), tc.original, now)
			_, why := l.cooling("rd", hash, now)
			coolErr := providerStageError("rd", "submit",
				fmt.Errorf("cooling off after an earlier decline: %w", why))

			if r := m.classifyAddRefusal(coolErr); r.hold {
				t.Fatalf("a cool-off-wrapped %s was HELD. It is a verdict about the release: the grab must fail "+
					"so the arr replaces it, or the entry sits in queuedDL forever waiting on content that is "+
					"never coming", tc.name)
			}
		})
	}
}

// THE MIXED CHAIN, which is the field case end to end: AD cooling off on a
// quota (temporary) while RD cools off on a 451 (a verdict). Per the operator's
// aggregation rule the grab is HELD, because AD will have room later and a
// takedown on RD says nothing about AD.
func TestCoolOffChainAggregatesLikeAnyOtherRefusal(t *testing.T) {
	m := newRefusalFixture(t, "ad", config.Debrid{Name: "ad", Provider: "alldebrid"}, &fillClient{})
	l := newDeclineLedger()
	now := time.Unix(1_700_000_000, 0).UTC()

	l.record("ad", "mixed", declineTransient,
		fmt.Errorf("%w: alldebrid MAGNET_TOO_MANY", customerror.ProviderAddQuotaExhaustedError), now)
	l.record("rd", "mixed", declinePermanent,
		customerror.NewContentTakedownError(errors.New("realdebrid API error: Status: 451")), now)

	_, adWhy := l.cooling("ad", "mixed", now)
	_, rdWhy := l.cooling("rd", "mixed", now)
	chain := errors.Join(
		providerStageError("ad", "submit", fmt.Errorf("cooling off after an earlier decline: %w", adWhy)),
		providerStageError("rd", "submit", fmt.Errorf("cooling off after an earlier decline: %w", rdWhy)),
	)

	if r := m.classifyAddRefusal(chain); !r.hold {
		t.Fatalf("the whole chain refused (%+v). This is the live loop: every provider cooling off, one of them "+
			"on a condition that clears, and the entry parked instead of waiting", r)
	}
}

// The message survives too. A wrapped sentinel is worth nothing to an operator
// reading a log if the text it replaced is gone.
func TestCoolOffKeepsItsMessage(t *testing.T) {
	l := newDeclineLedger()
	now := time.Unix(1_700_000_000, 0).UTC()
	l.record("ad", "msg", declineTransient,
		fmt.Errorf("%w: alldebrid MAGNET_TOO_MANY: Magnets limit reached (1000 accross all tabs)",
			customerror.ProviderAddQuotaExhaustedError), now)

	_, why := l.cooling("ad", "msg", now)
	text := fmt.Errorf("cooling off after an earlier decline: %w", why).Error()
	for _, want := range []string{"cooling off after an earlier decline", "MAGNET_TOO_MANY", "1000 accross all tabs"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the cool-off message lost %q: %s", want, text)
		}
	}
}

// A record with no cause — an older entry, or a decline carrying no error — must
// still cool off rather than being treated as "not cooling". It simply will not
// classify, which is the honest outcome when the type was never captured.
func TestCoolOffWithoutACauseStillCools(t *testing.T) {
	l := newDeclineLedger()
	now := time.Unix(1_700_000_000, 0).UTC()
	l.record("ad", "nocause", declineTransient, nil, now)

	cooling, why := l.cooling("ad", "nocause", now)
	if !cooling {
		t.Fatal("a decline recorded without an error stopped cooling off entirely, which re-opens the storm the " +
			"ledger exists to stop")
	}
	if why == nil {
		t.Fatal("cooling must always return something wrappable; a nil here becomes %!w(<nil>) in the log")
	}
}

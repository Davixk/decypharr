package manager

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
)

// THE RATE CLASS IS CAPACITY, AND CAPACITY IS HELD.
//
// The operator's ruling: "if a quota is exhausted on ALL providers, the grab
// should be put in a QUEUE until it can be attempted again, until a verdict:
// FAIL or SUCCESS. For OTHER failure types, FAIL THE GRAB so the arr moves on."
//
// The rate class was the half that leaked. classifyAddRefusal held concurrency
// exhaustion and the add allowance and nothing else, so a 429 — the most
// transient refusal in the system, and the one decypharr causes itself — fell
// through to "refuse" and became a 400 on torrents/add. Measured: 595 refusals
// in one 30-minute window, with the same releases returning on a ~2-3 hour
// re-grab carousel because Radarr drops a refused grab instead of taking its
// next candidate.
func TestRateLimitedAddIsHeldNotRefused(t *testing.T) {
	client := &fillClient{count: 4999}
	// Stored cap deliberately FULL: a rate limit says nothing about how much the
	// account holds, so consulting the fill here would convert a seconds-long
	// condition into a permanent refusal.
	m := newRefusalFixture(t, "rd", config.Debrid{Name: "rd", Provider: "realdebrid", MaxMagnets: intPtr(5000)}, client)

	r := m.classifyAddRefusal(customerror.RateLimitedError)
	if !r.hold {
		t.Fatal("a 429 was refused. That answers the arr with 400, Radarr logs `Couldn't add release` and DROPS " +
			"the grab, and the release does not return until the next re-search ~2-3h later — all for a condition " +
			"that had cleared by the time the log line was written")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.calls != 0 {
		t.Fatalf("the rate-class verdict cost %d fill enumerations, want 0: a rate limit is not a statement "+
			"about how full the account is", client.calls)
	}
}

// The traffic/fair-usage codes are the same class arriving under a different
// name. RealDebrid 23/34/36 are allowances that return on the provider's own
// boundary, exactly like AllDebrid's daily add allowance.
func TestTrafficExceededAddIsHeld(t *testing.T) {
	m := newRefusalFixture(t, "rd", config.Debrid{Name: "rd", Provider: "realdebrid"}, &fillClient{})

	if r := m.classifyAddRefusal(customerror.TrafficExceededError); !r.hold {
		t.Fatal("a traffic/fair-usage refusal was refused; it clears on the provider's own boundary")
	}
}

// It has to survive the join. A multi-provider fallback chain joins one error per
// provider, so the rate limit may be buried several levels down next to a
// completely unrelated failure. errors.Is walks the tree; errors.As would return
// whichever *customerror.Error came first.
func TestRateClassSurvivesTheFallbackJoin(t *testing.T) {
	m := newRefusalFixture(t, "rd", config.Debrid{Name: "rd", Provider: "realdebrid"}, &fillClient{})

	joined := errors.Join(
		providerStageError("ad", "submit", errors.New("no valid files found")),
		providerStageError("rd", "submit", fmt.Errorf("POST /torrents/addMagnet gave up after 5 attempt(s): %w",
			customerror.RateLimitedError)),
	)
	if r := m.classifyAddRefusal(joined); !r.hold {
		t.Fatal("a rate limit buried in the fallback join was not seen; the whole chain refused and the grab was lost")
	}
}

// 🛑 THE TRAP THIS FIX HAD TO AVOID, AND ITS NEGATIVE CONTROL.
//
// The obvious implementation was to reuse isRateLimitSignal, which already
// existed and already recognised rate limits for the pacer. It is an unanchored
// substring scan for "429"/"503"/"slow down" — fine for choosing a backoff,
// where a false positive costs one slower minute, and unsafe for deciding
// whether to ACCEPT A GRAB, where a false positive parks an entry as queuedDL
// forever against something that will never clear.
//
// The digits are the problem. "429" and "503" occur inside infohashes, release
// names, byte counts and segment offsets, so a permanent content refusal about a
// torrent whose hash happens to start 429f… is read as a rate limit. The cases
// below are real error shapes with real hashes; each one is held by the scan and
// refused by the type.
func TestPermanentlyGoneContentIsNeverHeldHoweverItIsSpelled(t *testing.T) {
	m := newRefusalFixture(t, "rd", config.Debrid{Name: "rd", Provider: "realdebrid"}, &fillClient{})

	gone := []error{
		customerror.HosterUnavailableError,
		customerror.NewContentGoneError(errors.New("410 gone")),
		customerror.NewContentTakedownError(errors.New("realdebrid: infringing_file (code 35)")),
		providerStageError("rd", "status check", customerror.HosterUnavailableError),
	}
	for _, err := range gone {
		if r := m.classifyAddRefusal(err); r.hold {
			t.Errorf("a permanent content verdict was HELD: %v. It will never clear, so the entry waits forever "+
				"reporting queuedDL while the arr believes a download is coming", err)
		}
	}

	// The collision, demonstrated rather than asserted. These are content
	// verdicts — nothing about them is a rate limit — and the substring scan says
	// otherwise because of digits in a hash and digits in a size.
	spoofed := []error{
		errors.New("torrent 429f0ab3c1d2e4f5a6b7c8d9e0f1a2b3c4d5e6f7 is not cached and uncached downloads are disabled"),
		errors.New("no valid files found: largest candidate was 503 bytes"),
	}
	for _, err := range spoofed {
		if !isRateLimitSignal(err) {
			t.Fatalf("the premise of this test no longer holds: isRateLimitSignal did not match %v. "+
				"Re-derive the hazard before deleting the test", err)
		}
		if isRateClassRefusal(err) {
			t.Fatalf("isRateClassRefusal matched %v — it has been switched back to the substring scan, and "+
				"content refusals whose text happens to contain 429/503 will now be held as queuedDL forever", err)
		}
		if r := m.classifyAddRefusal(err); r.hold {
			t.Fatalf("a content refusal was held because its text contained rate-limit digits: %v", err)
		}
	}
}

// AND THE JOIN MUST STILL SCAN FOR THE BEST OUTCOME.
//
// This is the test that killed a guard I had already written. Adding a leading
// `if IsContentPermanentlyGone(err) { refuse }` to classifyAddRefusal looks like
// pure safety, and it reads that way, but errors.Is answers true if ANY provider
// in a joined chain reported a permanent verdict. A takedown on one debrid plus
// a rate limit on another would then refuse a grab the second debrid was going
// to accept seconds later — refusing on the WORST outcome in a function whose
// documented rule is to scan for the best one.
func TestATakedownOnOneProviderDoesNotRefuseARateLimitOnAnother(t *testing.T) {
	m := newRefusalFixture(t, "rd", config.Debrid{Name: "rd", Provider: "realdebrid"}, &fillClient{})

	joined := errors.Join(
		providerStageError("rd", "submit", customerror.NewContentTakedownError(
			errors.New("realdebrid: infringing_file (code 35)"))),
		providerStageError("ad", "submit", customerror.RateLimitedError),
	)
	if r := m.classifyAddRefusal(joined); !r.hold {
		t.Fatal("one provider's takedown refused a grab another provider had merely rate limited. " +
			"A takedown is provider-scoped; the rate-limited provider will have room shortly and the " +
			"release is fine there")
	}
}

// The mirror control: content refusals must STILL be refused. A hold-everything
// implementation would pass every test above and destroy the fast-fail path the
// operator explicitly kept ("For OTHER failure types, FAIL THE GRAB").
func TestContentRefusalsStillFailFastAfterTheRateClassLanded(t *testing.T) {
	m := newRefusalFixture(t, "rd", config.Debrid{Name: "rd", Provider: "realdebrid"}, &fillClient{})

	for _, err := range []error{
		errors.New("torrent is not cached and uncached downloads are disabled"),
		errors.New("release has 0 seeders, below the configured minimum"),
		providerStageError("rd", "status check", errors.New("no valid files found")),
		errors.New("realdebrid API error: Status: 401"),
	} {
		if r := m.classifyAddRefusal(err); r.hold {
			t.Errorf("%v was held; a verdict about the RELEASE must fail the grab so the arr can replace it", err)
		}
	}
}

// THE AGGREGATE VERDICT, stated as the operator stated it.
//
//	a PERMANENT refusal rules out only THAT provider for that hash —
//	  "infringing on RD means nothing on AD";
//	a TEMPORARY inability rules out nothing at all.
//
//	ALL permanent -> refuse synchronously.  ANY temporary -> accept and hold.
//
// The motivating case was Hustlers 2019: twelve consecutive manual grabs across
// nzb and torrents all died into one storm — an RD 451 wave on that title's
// hashes, AD's daily cap dead, dead-article nzbs — and almost all of them
// surfaced as silent 400s the arr could not tell apart from anything else.
func TestAggregateAddVerdict(t *testing.T) {
	m := newRefusalFixture(t, "rd", config.Debrid{Name: "rd", Provider: "realdebrid"}, &fillClient{})

	permanentRD := providerStageError("rd", "submit", customerror.NewContentTakedownError(
		errors.New("realdebrid: infringing_file (code 35)")))
	permanentAD := providerStageError("ad", "submit", errors.New("no valid files found"))
	temporaryAD := providerStageError("ad", "submit", customerror.ProviderAddQuotaExhaustedError)
	temporaryRD := providerStageError("rd", "submit", customerror.RateLimitedError)

	cases := []struct {
		name     string
		err      error
		wantHold bool
		why      string
	}{
		{
			"every provider permanent", errors.Join(permanentRD, permanentAD), false,
			"no provider can ever serve this release, so the grab must fail now and let the arr take its next candidate",
		},
		{
			"one permanent, one temporary", errors.Join(permanentRD, temporaryAD), true,
			"AD merely has no room right now — infringing on RD means nothing on AD, and refusing here throws away a release AD will accept",
		},
		{
			"temporary first in the join", errors.Join(temporaryRD, permanentAD), true,
			"order within the join must not change the verdict",
		},
		{
			"every provider temporary", errors.Join(temporaryRD, temporaryAD), true,
			"nothing here is a statement about the release at all",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := m.classifyAddRefusal(tc.err).hold; got != tc.wantHold {
				t.Fatalf("hold = %t, want %t — %s", got, tc.wantHold, tc.why)
			}
		})
	}
}

// A REFUSAL THE ARR CAN READ. The shim answers a refused add with this text as
// the 400 body, and that body is the only thing an *arr can show a human. The
// complaint being answered is "silent 400s the arr can't distinguish from
// anything else".
func TestRefusalReasonNamesTheCause(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"takedown", customerror.NewContentTakedownError(errors.New("infringing_file")), "legal reasons"},
		{"gone", customerror.NewContentGoneError(errors.New("410")), "gone"},
		{"uncached", errors.New("torrent is not cached and uncached downloads are disabled"), "not cached"},
		{"seeders", errors.New("release has 0 seeders, below the configured minimum"), "seeder minimum"},
		{"no files", errors.New("no valid files found in torrent"), "no usable files"},
		{"auth", errors.New("realdebrid API error: Status: 401"), "credentials"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := refusalReason(tc.err)
			if !strings.Contains(got, tc.want) {
				t.Fatalf("refusalReason(%v) = %q, want it to mention %q", tc.err, got, tc.want)
			}
			if !strings.HasPrefix(got, "refused:") {
				t.Fatalf("reason must lead with the verdict so it survives being read in an arr log: %q", got)
			}
		})
	}

	// An unrecognised failure gets an honest generic. A confidently wrong reason
	// in an arr's log is worse than a vague true one.
	generic := refusalReason(errors.New("something nobody has classified yet"))
	if !strings.Contains(generic, "no configured provider could serve") {
		t.Fatalf("unclassified refusal did not fall back honestly: %q", generic)
	}

	// A capacity class reaching this function at all is a bug, and it says so
	// rather than blaming the release.
	slipped := refusalReason(customerror.RateLimitedError)
	if !strings.Contains(slipped, "should have been held") {
		t.Fatalf("a capacity class on the refusal path must be reported as a bug, got %q", slipped)
	}
}

// 429 NEVER REACHES A PROVIDER'S STATUS SWITCH, WHICH IS WHY IT WAS UNTYPED.
//
// It is in the shared client's retryableStatus set, so retryablehttp retries it
// to exhaustion and returns through ErrorHandler — the provider code that maps
// status codes to typed errors is simply not on that path. Typing it in the
// give-up handler is what makes every provider's rate limit classifiable at
// once; the alternative was five switch statements that are all unreachable for
// this case.
func TestRateLimitGiveUpIsTypedForEveryProvider(t *testing.T) {
	// The exact shape internal/request produces on a 429 give-up.
	giveUp := fmt.Errorf("%w: %w", customerror.RateLimitedError,
		fmt.Errorf("POST https://api.real-debrid.com/rest/1.0/torrents/addMagnet gave up after 5 attempt(s): status %d: %s",
			http.StatusTooManyRequests, "(empty body)"))

	if !isRateClassRefusal(giveUp) {
		t.Fatal("a typed 429 give-up was not recognised as the rate class")
	}
	if customerror.IsContentPermanentlyGone(giveUp) {
		t.Fatal("a rate limit was classified as a permanent content verdict")
	}

	// An UNTYPED give-up is the pre-fix shape, and it must not be rescued by
	// accident — if this starts passing, something reintroduced a text scan.
	untyped := fmt.Errorf("POST https://api.real-debrid.com/rest/1.0/torrents/addMagnet gave up after 5 attempt(s): status 429: (empty body)")
	if isRateClassRefusal(untyped) {
		t.Fatal("isRateClassRefusal matched an untyped string; it must classify by type only")
	}
}

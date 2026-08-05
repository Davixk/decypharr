package manager

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

// ADD PACING — spend the provider's request budget, all of it, without tripping it.
//
// This replaces a flat constant ("admit at most N per tick"), which was a number
// nobody could justify: too low and capacity sits idle, too high and it is the
// storm again, and it was identical for providers whose real limits differ by
// more than 2x.
//
// 📏 THE DOCUMENTED LIMITS, from each vendor:
//
//	RealDebrid  250 requests/minute, GLOBAL across every endpoint.
//	            ⚠️ Refused requests COUNT TOWARD THE LIMIT. Retrying into a 429
//	            therefore makes the situation strictly worse — which is exactly
//	            how the measured storm sustained itself.
//	            No X-RateLimit-* headers; error 5 "Slow down", 34 "Too many requests".
//	AllDebrid   12 requests/second AND 600 requests/minute. 429 or 503 on exceed.
//
// 🔑 THE LIMIT IS GLOBAL, NOT PER-ENDPOINT, AND THAT SHAPES EVERYTHING BELOW.
// Adds share one budget with refreshes, availability probes, enumerations and
// deletes. A pacer that measured only adds would happily spend the whole
// allowance on them and starve — or trip — everything else. So the default add
// budget deliberately claims only a FRACTION of the documented rate and leaves
// the rest to the traffic decypharr cannot pause.
//
// One add is not one request: addMagnet, then selectFiles, usually then an info
// fetch. Budget in ADDS and cost them at roughly three requests each.
//
//	RealDebrid  250 req/min ÷ ~3 req/add ≈ 83 adds/min at 100% of the allowance.
//	            Claim ~35% -> 30 adds/min.
//	AllDebrid   600 req/min ÷ ~3 ≈ 200 adds/min; claim ~30% -> 60 adds/min,
//	            which is 3 req/s against a 12 req/s ceiling.
//
// Both are ceilings the pacer aims for, not rates it holds: AIMD backs off the
// moment the provider complains and climbs back when it stops.
const (
	// defaultAddRateRealDebrid — see the derivation above.
	defaultAddRateRealDebrid = "30/minute"
	// defaultAddRateAllDebrid — see the derivation above.
	defaultAddRateAllDebrid = "60/minute"
	// defaultAddRateUnknown is for providers whose limits we have NOT verified.
	// Deliberately the most conservative of the three: guessing high on an
	// unknown API is how you discover its limit the expensive way.
	defaultAddRateUnknown = "20/minute"
)

// addPacerBurst caps how many admissions may be spent at once after an idle
// period, no matter how many tokens accumulated.
//
// ⚠️ WITHOUT THIS THE PACER RE-CREATES THE STORM IT EXISTS TO PREVENT. A quiet
// hour banks an hour of tokens, and the next tick spends them all in one burst —
// which is precisely "admit all 90 free slots at once" wearing a rate limiter's
// clothes. The average would look perfectly correct while the instantaneous rate
// tripped the limit.
const addPacerBurst = 5

// addPacerFloor is the slowest the adaptive backoff may go: one add per this
// interval. It is a floor rather than zero because a provider that is refusing
// everything still has to be probed occasionally to notice it has recovered.
const addPacerFloor = 30 * time.Second

type addLane struct {
	budget float64 // adds/second — the configured ceiling, never exceeded
	rate   float64 // adds/second — current effective rate after AIMD
	tokens float64
	last   time.Time
}

// addPacer hands out admission permits per provider.
type addPacer struct {
	mu      sync.Mutex
	lanes   map[string]*addLane
	configs map[string]string // provider -> configured "N/unit", empty = default
}

func newAddPacer() *addPacer {
	return &addPacer{lanes: map[string]*addLane{}, configs: map[string]string{}}
}

// configure records a provider's override. An empty or unparseable spec falls
// back to the researched default for that provider.
func (p *addPacer) configure(provider, spec string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.configs[strings.ToLower(provider)] = spec
	delete(p.lanes, strings.ToLower(provider)) // rebuilt on next use
}

// defaultAddRateFor picks the researched default for a provider name.
func defaultAddRateFor(provider string) string {
	switch strings.ToLower(provider) {
	case "realdebrid", "real-debrid", "rd":
		return defaultAddRateRealDebrid
	case "alldebrid", "all-debrid", "ad":
		return defaultAddRateAllDebrid
	default:
		return defaultAddRateUnknown
	}
}

// parseAddRate turns "30/minute" into adds per second. Returns 0 when the spec
// cannot be understood, so the caller can fall back rather than silently pacing
// at some accidental rate.
func parseAddRate(spec string) float64 {
	parts := strings.SplitN(strings.TrimSpace(spec), "/", 2)
	if len(parts) != 2 {
		return 0
	}
	count, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || count <= 0 {
		return 0
	}
	var per time.Duration
	switch strings.TrimSuffix(strings.ToLower(strings.TrimSpace(parts[1])), "s") {
	case "second", "sec":
		per = time.Second
	case "minute", "min":
		per = time.Minute
	case "hour", "hr":
		per = time.Hour
	case "day", "d":
		per = 24 * time.Hour
	default:
		return 0
	}
	return float64(count) / per.Seconds()
}

func (p *addPacer) laneLocked(provider string) *addLane {
	key := strings.ToLower(provider)
	if lane, ok := p.lanes[key]; ok {
		return lane
	}
	rate := parseAddRate(p.configs[key])
	if rate <= 0 {
		rate = parseAddRate(defaultAddRateFor(provider))
	}
	// Starts with a full burst rather than empty. A cold lane is the state after
	// every restart, and making the first admissions wait out a refill would
	// stall recovery for no benefit — the burst cap already bounds what "full"
	// can mean, so this cannot be the storm.
	lane := &addLane{budget: rate, rate: rate, tokens: addPacerBurst, last: time.Now()}
	p.lanes[key] = lane
	return lane
}

// take returns how many admissions may proceed right now, up to want.
//
// Zero is a normal answer, not an error: it means the budget is spent for the
// moment and the caller should simply try again on its next tick. Nothing is
// failed, nothing is dropped.
func (p *addPacer) take(provider string, want int, now time.Time) int {
	if p == nil || want <= 0 {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	lane := p.laneLocked(provider)
	if elapsed := now.Sub(lane.last); elapsed > 0 {
		lane.tokens += elapsed.Seconds() * lane.rate
		lane.last = now
	}
	if lane.tokens > addPacerBurst {
		lane.tokens = addPacerBurst
	}
	granted := int(lane.tokens)
	if granted > want {
		granted = want
	}
	if granted < 0 {
		granted = 0
	}
	lane.tokens -= float64(granted)
	return granted
}

// penalise halves the effective rate after the provider signalled it is being
// pushed too hard.
//
// MULTIPLICATIVE DECREASE, because the cost of overshooting is asymmetric on
// RealDebrid in particular: refused requests count against the same budget, so
// an overspend does not merely waste a request, it shrinks the room available to
// recover. Backing off gently would keep paying that toll.
func (p *addPacer) penalise(provider string, now time.Time) time.Duration {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	lane := p.laneLocked(provider)
	floor := 1 / addPacerFloor.Seconds()
	lane.rate /= 2
	if lane.rate < floor {
		lane.rate = floor
	}
	// Spend the accumulated tokens too: keeping them would let the very next
	// tick burst at the old rate, which is the behaviour being penalised.
	lane.tokens = 0
	lane.last = now
	return time.Duration(float64(time.Second) / lane.rate)
}

// reward nudges the rate back toward the configured budget after a success.
//
// ADDITIVE INCREASE, one tenth of the budget per success, so recovery is
// gradual: a provider that just refused us is not proven healthy by one add
// landing, and jumping straight back to full rate would oscillate.
func (p *addPacer) reward(provider string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	lane := p.laneLocked(provider)
	if lane.rate >= lane.budget {
		return
	}
	lane.rate += lane.budget / 10
	if lane.rate > lane.budget {
		lane.rate = lane.budget
	}
}

// rates reports the configured and current effective rate, for logging. Without
// this the pacer is invisible: an operator seeing slow admissions could not tell
// a throttled lane from an empty queue.
func (p *addPacer) rates(provider string) (budgetPerMin, currentPerMin float64) {
	if p == nil {
		return 0, 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	lane := p.laneLocked(provider)
	return lane.budget * 60, lane.rate * 60
}

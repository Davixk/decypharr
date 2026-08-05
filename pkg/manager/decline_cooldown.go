package manager

import (
	"strings"
	"sync"
	"time"
)

// DECLINE COOLDOWN — stop re-offering a hash that just failed.
//
// Measured on a live deployment: ~280 held entries, each re-offered roughly
// every 62 seconds, producing ~4 provider calls per second of pure decline
// traffic — 12,123 "giving up after 4 attempt(s)" warnings in two hours while
// the provider had 90 of 100 slots FREE.
//
// Nothing throttled a hash that had just been declined. It became eligible
// again on the very next admission tick, forever, at whatever rate the
// controller admitted. Combined with an admission batch sized to free slots,
// that made the storm self-sustaining: the burst tripped the provider's rate
// limit, the adds failed, no slot was consumed, so the next tick saw the same
// free capacity and admitted the same entries again.
//
// THE CLASS MATTERS MORE THAN THE DELAY. A release the provider will never
// accept, a provider that is temporarily full, and a request that failed in
// transit need three different answers, and treating them alike is how a
// permanent refusal turned into an infinite loop.
type declineClass int

const (
	// declineTransient is a failure of the REQUEST, not a verdict about the
	// content: a timeout, a 5xx, retries exhausted. Worth trying again, later
	// and less often each time.
	declineTransient declineClass = iota
	// declinePermanent is the provider refusing this specific release, and it
	// will refuse it again every time. Retrying is pure noise.
	declinePermanent
)

const (
	// declineBackoffBase is the first cooldown after a transient failure.
	declineBackoffBase = 2 * time.Minute
	// declineBackoffMax caps it, so a provider that recovers is retried within
	// a bounded time rather than written off for the process lifetime.
	declineBackoffMax = 30 * time.Minute
	// declinePermanentCooldown is how long a per-release refusal is honoured.
	// Not forever: a release can become available, and the *arr may legitimately
	// re-grab it. Long enough that it stops being traffic.
	declinePermanentCooldown = 6 * time.Hour
)

type declineRecord struct {
	until    time.Time
	failures int
	class    declineClass
	reason   string
}

// declineLedger remembers which hashes are cooling off, per provider.
type declineLedger struct {
	mu      sync.Mutex
	records map[string]declineRecord
}

func newDeclineLedger() *declineLedger {
	return &declineLedger{records: map[string]declineRecord{}}
}

func declineKey(provider, infoHash string) string {
	return provider + "\x00" + strings.ToLower(infoHash)
}

// record notes a decline and returns how long the hash is now parked for.
func (l *declineLedger) record(provider, infoHash string, class declineClass, reason string, now time.Time) time.Duration {
	if l == nil || infoHash == "" {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	key := declineKey(provider, infoHash)
	rec := l.records[key]
	rec.failures++
	rec.class = class
	rec.reason = reason

	var cooldown time.Duration
	if class == declinePermanent {
		cooldown = declinePermanentCooldown
	} else {
		cooldown = declineBackoffBase << min(rec.failures-1, 8)
		if cooldown > declineBackoffMax || cooldown <= 0 {
			cooldown = declineBackoffMax
		}
	}
	rec.until = now.Add(cooldown)
	l.records[key] = rec
	return cooldown
}

// cooling reports whether this hash is parked, and why.
func (l *declineLedger) cooling(provider, infoHash string, now time.Time) (bool, string) {
	if l == nil {
		return false, ""
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	rec, ok := l.records[declineKey(provider, infoHash)]
	if !ok {
		return false, ""
	}
	if now.After(rec.until) {
		// Expired. Keep the failure count so a repeat offender backs off
		// further rather than restarting at the base delay — otherwise a
		// permanently broken hash cycles at the minimum interval forever.
		return false, ""
	}
	return true, rec.reason
}

// clear forgets a hash entirely, used when an add finally succeeds.
func (l *declineLedger) clear(provider, infoHash string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.records, declineKey(provider, infoHash))
}

// coolingCount reports how many hashes are currently parked, for the sweep log.
func (l *declineLedger) coolingCount(now time.Time) int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, rec := range l.records {
		if now.Before(rec.until) {
			n++
		}
	}
	return n
}

// classifyDecline decides how long a failed add should park.
//
// The distinction is between a verdict about the CONTENT and a failure of the
// REQUEST. A provider refusing this particular release will refuse it again
// every time, so retrying is pure noise; a timeout says nothing about the
// release at all.
//
// Capacity refusals are deliberately absent: those are handled by the hold
// ledger, which parks the entry until a slot frees and is the right mechanism.
// Classifying them here as well would park a hash that is merely waiting.
func classifyDecline(err error) declineClass {
	if err == nil {
		return declineTransient
	}

	// ⚠️ CAPACITY IS NOT A VERDICT ABOUT THE RELEASE, AND MUST NEVER PARK ONE.
	//
	// Checked FIRST, before the permanent-error test, because a quota refusal
	// can present as a permanent provider error while meaning only "not right
	// now". Parking such a hash for six hours would break the hold mechanism
	// outright: the entry would be waiting for a slot that frees in minutes
	// while we refuse to offer it.
	if isTooManyActiveDownloads(err) || isProviderAddQuotaExhausted(err) {
		return declineTransient
	}

	// PERMANENT REQUIRES A POSITIVE, RELEASE-SPECIFIC SIGNAL. Everything else
	// is transient.
	//
	// The asymmetry is deliberate and it is the safe direction. A transient
	// misclassification costs one more attempt after a bounded backoff; a
	// permanent one parks a perfectly good release for hours.
	//
	// customerror.IsPermanentError is deliberately NOT used here despite the
	// name. It matches on generic HTTP-failure text — auth failures, 4xx — and
	// those are permanent for EVERY release, not for this one. Parking a single
	// hash in response to a bad API key would be exactly the wrong reaction: it
	// would quietly shed one release at a time while the real fault went
	// unaddressed.
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "451"):
		// Unavailable for legal reasons. The provider's verdict on this
		// specific release, and it will be the same verdict every time.
		return declinePermanent
	case strings.Contains(text, "infringing"):
		return declinePermanent
	}
	return declineTransient
}

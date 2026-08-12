package customerror

import "errors"

var HosterUnavailableError = (&Error{
	statusCode: 503,
	err:        errors.New("hoster is unavailable"),
	Code:       "hoster_unavailable",
}).Retryable() // 503 Service Unavailable is transient

var UsenetSegmentMissingError = &Error{
	statusCode: 404,
	err:        errors.New("usenet segment is missing"),
	Code:       "usenet_segment_missing",
}

var TrafficExceededError = &Error{
	statusCode: 503,
	err:        errors.New("traffic limit exceeded"),
	Code:       "traffic_exceeded",
}

var TorrentNotFoundError = &Error{
	statusCode: 404,
	err:        errors.New("torrent not found"),
	Code:       "torrent_not_found",
}

var TooManyActiveDownloadsError = (&Error{
	statusCode: 509,
	err:        errors.New("too many active downloads"),
	Code:       "too_many_active_downloads",
}).Retryable() // slot exhaustion is transient — retry after backoff

// RateLimitedError is a provider refusing because we are asking too FAST.
//
// It is a third capacity condition, distinct from both of the ones below, and
// giving it its own identity is the point: it is not about how many items the
// account holds (ProviderAddQuotaExhausted) and not about how many are in
// flight (TooManyActiveDownloads). It clears on a timescale of seconds, and it
// is the ONE capacity refusal decypharr causes itself.
//
// WHY IT NEEDED A TYPE. The only recogniser was isRateLimitSignal, a substring
// scan for "429"/"503"/"slow down". That was fine for feeding the pacer's
// backoff, where a false positive costs one slower minute, and unusable for
// deciding whether to accept a grab: "503" also matches HosterUnavailableError,
// which is double-booked as both a hoster outage AND every provider's
// definitive 404/410 CheckFile verdict. Holding a grab on THAT string would
// park permanently-gone content forever. A type cannot be confused that way.
var RateLimitedError = (&Error{
	statusCode: 429,
	err:        errors.New("provider rate limit reached"),
	Code:       "rate_limited",
}).Retryable()

// ProviderAddQuotaExhaustedError is a provider refusing NEW items because an
// add or storage allowance is spent — NOT because its concurrency slots are
// full. The two look alike and clear on completely different timescales, which
// is why they must not share a retry cadence.
//
// AllDebrid's MAGNET_TOO_MANY is the case in hand: it was observed firing 6,715
// times while the account held ZERO active magnets. Nothing decypharr does
// locally frees it — no completion, no delete, no waiting a slot-length of
// time. Retrying it on the slot cadence would spin against a provider that has
// already said no, which is how a rate limit becomes a ban.
//
// Retryable, deliberately, but only on the long cadence: the allowance does
// come back, just not on our schedule.
var ProviderAddQuotaExhaustedError = (&Error{
	statusCode: 509,
	err:        errors.New("provider add quota exhausted"),
	Code:       "provider_add_quota_exhausted",
}).Retryable()

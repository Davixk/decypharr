package config

import "time"

var (
	DefaultPort     = "8282"
	DefaultLogLevel = "info"

	DefaultRateLimit                = "250/minute"
	DefaultTorrentsRefreshInterval  = "10m"
	DefaultDownloadsRefreshInterval = "5m"
	DefaultAutoExpireLinksAfter     = "3d"

	DefaultRclonePort = "5572"

	DefaultDFSChunkSize     = "8MB"
	DefaultDFSReadAheadSize = "128MB"
	DefaultDFSCacheExpiry   = "24h"
	DefaultDFSDiskCacheSize = "500MB"

	DefaultAccountSyncInterval = "10m"

	// DefaultMaxConcurrentJobs bounds in-flight import JOBS, not downloads. It
	// is a machine-overhead ceiling — the point at which goroutine fan-out
	// threatens the host — not a stand-in for any provider's limit. Provider
	// capacity is asked for per provider; local I/O is bounded by
	// max_active_downloads. Neither is this number.
	DefaultMaxConcurrentJobs = 500

	DefaultRetryDelay    = 500 * time.Millisecond
	DefaultRetryDelayMax = 30 * time.Second

	// DefaultDebridLinkTimeout is the ceiling on a reader's wait for a download
	// link. See Config.DebridLinkTimeout for the hang it prevents; the number
	// is a judgement call, so it is a knob and this is only its default.
	//
	// 20s sits between two hard facts: a healthy unrestrict call answers in
	// well under a second, and the per-attempt HTTP timeout on the download
	// client is 60s — so a SINGLE hung attempt already outlasts any reader's
	// patience, before the account fallback loop or a re-insertion cascade is
	// even reached. 20s leaves room for one slow-but-working mint plus a
	// fallback account, and returns an actionable error long before Plex, the
	// *arrs or rclone's own multi-minute timeouts fire.
	DefaultDebridLinkTimeout = "20s"

	// DefaultDebridStatusTimeout is the ceiling on a provider STATUS POLL — the
	// loop a freshly submitted magnet sits in while the provider is asked, over
	// and over, whether it has finished accepting it.
	//
	// 60s because the only branch that actually re-polls is RealDebrid's
	// "waiting_files_selection": decypharr sends the file selection, then polls
	// for the provider to act on it. A healthy account clears that in one or two
	// passes (~2-4s). At the 2s poll interval, 60s is thirty passes — an order of
	// magnitude more patience than the healthy case needs, so a provider that is
	// merely slow is never cut off, while one that is stuck stops burning a
	// repair worker within the minute.
	//
	// It is also deliberately small relative to the 5m in-flight repair budget:
	// FixTorrent cascades through every configured debrid in turn, so a ceiling
	// large enough to blow that budget would only move the wedge one level up.
	DefaultDebridStatusTimeout = "60s"

	// DefaultMetadataReadTimeout is the ceiling on a listing/HEAD wait. It is
	// deliberately SHORTER than the link ceiling: a listing needs no provider
	// round-trip at all for torrent entries, and for Usenet entries it is a
	// batch of header fetches off a warm connection pool. There is no
	// "slow but progressing" state to protect here — a listing either answers
	// or it is stuck.
	DefaultMetadataReadTimeout = "15s"

	// DefaultDebridTakedownThreshold is how many confirmed takedown refusals one
	// file needs before it counts as legally dead.
	//
	// 1, because the signal is unambiguous. RealDebrid code 35 / HTTP 451 is not
	// a failed request, a rate limit or an outage — it is the provider stating
	// that the release has been removed for legal reasons, and no retry, no wait
	// and no other account changes that answer. Demanding a second refusal would
	// only buy one more guaranteed-failing read before reaching the identical
	// conclusion.
	//
	// The asymmetry that governs every other threshold here points the same way
	// rather than against it: the expensive mistake is condemning content that is
	// FINE, and content that is fine does not produce a takedown refusal.
	DefaultDebridTakedownThreshold = 1

	// DefaultLivePrunePercent and DefaultLivePruneFloor bound the prunes that
	// happen on a READ rather than inside a repair run — a confirmed debrid
	// takedown, a confirmed usenet dead article.
	//
	// 🔻 A BARE 50/HOUR WAS TRIED HERE AND OVERRULED. Its derivation sized the
	// rail against genuine decay (~5 confirmed takedowns a day on the deployment
	// it was measured on) and never against what it MUST LET THROUGH. A real
	// takedown wave — a distributor clearing a catalogue, a batch of postings
	// expiring together — legitimately puts hundreds of entries into the read
	// path within an hour. 50 would have throttled every one of them, visibly,
	// while the library went on serving content that was already gone. This is a
	// circuit breaker for a source that is WRONG, not a speed limit on being
	// right.
	//
	// 10% of the library per rolling hour. A tenth of a media library going dead
	// inside one hour has no legitimate cause; the only mechanism that produces
	// it is a source wrong about everything at once, which is precisely the July
	// incident — ~5,000 files lost to one bad listing. Below that a wave passes
	// untouched: hundreds of entries is a fraction of a percent of any library
	// large enough to contain hundreds of takedowns.
	//
	// PROPORTIONAL rather than absolute because "catastrophic" is not a constant.
	// The same few hundred deletions are unremarkable in a large library and an
	// emergency in a small one, and a percentage says both without tuning. No
	// absolute number can: whichever is chosen is too tight for the big library
	// and too loose for the small one, simultaneously.
	DefaultLivePrunePercent = 10

	// DefaultLivePruneFloor keeps the percentage from pinning a small or
	// freshly-seeded library near zero — at 10%, a 300-entry library would
	// otherwise breach at 30.
	//
	// 250/hour. It has to clear the largest legitimate burst without ever being
	// the binding constraint in normal operation, and it does both by a wide
	// margin: above the operator's own "hundreds in an hour" figure for a real
	// wave, and roughly 1,200x the observed genuine decay rate of ~5 entries a
	// day. In a library big enough for the percentage to matter, the floor never
	// binds at all.
	DefaultLivePruneFloor = 250
)

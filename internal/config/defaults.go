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

	// DefaultMetadataReadTimeout is the ceiling on a listing/HEAD wait. It is
	// deliberately SHORTER than the link ceiling: a listing needs no provider
	// round-trip at all for torrent entries, and for Usenet entries it is a
	// batch of header fetches off a warm connection pool. There is no
	// "slow but progressing" state to protect here — a listing either answers
	// or it is stuck.
	DefaultMetadataReadTimeout = "15s"
)

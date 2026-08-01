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
)

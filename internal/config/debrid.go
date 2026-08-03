package config

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
)

type Debrid struct {
	Provider                     string   `json:"provider,omitempty"` // realdebrid, alldebrid, debridlink, torbox, premiumize
	Name                         string   `json:"name,omitempty"`
	APIKey                       string   `json:"api_key,omitempty"`
	DownloadAPIKeys              []string `json:"download_api_keys,omitempty"`
	DownloadUncached             *bool    `json:"download_uncached,omitempty"`
	RateLimit                    string   `json:"rate_limit,omitempty"` // 200/minute or 10/second
	RepairRateLimit              string   `json:"repair_rate_limit,omitempty"`
	DownloadRateLimit            string   `json:"download_rate_limit,omitempty"`
	Proxy                        string   `json:"proxy,omitempty"`
	UnpackRar                    bool     `json:"unpack_rar,omitempty"`
	// MinimumFreeSlot reserves N of this provider's concurrent slots for OTHER
	// consumers of the same account — another client, or the owner's manual
	// use. decypharr subtracts it from the capacity it will admit against, so
	// it never fills an account it does not exclusively own.
	//
	// Defaults to 0: decypharr is usually the only consumer, and a nonzero
	// default silently donates a paid slot to nobody.
	//
	// It is NOT "the minimum needed to enqueue", and NOT "don't use this
	// provider below N free" — both readings existed (the second in our own
	// docs) while the code did neither, because nothing called
	// GetAvailableSlots and the field had never once executed. It does now.
	MinimumFreeSlot int `json:"minimum_free_slot,omitempty"`

	// MaxMagnets is how many items this provider will STORE on the account
	// before it refuses new ones. It is the only threshold that can tell
	// AllDebrid's two opposite meanings of MAGNET_TOO_MANY apart:
	//
	//	fill BELOW the cap -> the DAILY add allowance is spent. Transient; it
	//	                      resets on the provider's own boundary, so a held
	//	                      item will eventually be submitted.
	//	fill AT the cap    -> the STORED-item cap is full. Permanent; nothing we
	//	                      finish or wait for frees it, only deletion does.
	//
	// AllDebrid returns the SAME error code for both, and its message cannot be
	// trusted to disambiguate either: the observed text said "Magnets limit
	// reached (1000 accross all tabs)" while the binding constraint was the
	// 5,000 stored cap. So neither the code nor the string is a source of truth
	// — the account's own fill level is.
	//
	// A KNOB, not a constant, because the limit belongs to the provider and can
	// change without notice.
	//
	// TRI-STATE, and it has to be. *int, not int, so the three states stay
	// distinct across a save round-trip:
	//
	//	nil (absent)  -> use the provider's default cap (AllDebrid 5000)
	//	explicit 0    -> UNLIMITED, an operator override that must survive
	//	explicit N    -> cap at N
	//
	// With a plain int, "unlimited" and "absent" are both 0, so an operator who
	// deliberately uncapped AllDebrid would silently have 5000 written back on
	// the next save. That is the same defect as the download_uncached checkbox
	// that armed vetoes nobody chose.
	//
	// Unlimited means exactly that: with no cap there is no threshold to be at,
	// so a quota refusal resolves to the TRANSIENT case rather than being
	// judged against an invented number. Never guess a provider's limit — the
	// discipline that removed the fabricated DefaultAvailableSlots of 100.
	MaxMagnets *int `json:"max_magnets,omitempty"`
	Priority                     int      `json:"priority,omitempty"`          // Lower values are tried first; defaults to config order
	ConfigOrder                  int      `json:"-"`                           // Stable tie-breaker derived from debrids[] order
	Limit                        int      `json:"limit,omitempty"`             // Maximum number of total torrents
	TorrentsRefreshInterval      string   `json:"torrents_refresh_interval,omitempty"`
	DownloadLinksRefreshInterval string   `json:"download_links_refresh_interval,omitempty"`
	Workers                      int      `json:"workers,omitempty"`
	AutoExpireLinksAfter         string   `json:"auto_expire_links_after,omitempty"`
	UserAgent                    string   `json:"user_agent,omitempty"`

	// Folder
	Folder        string `json:"folder,omitempty"`          // Deprecated. Use Mount MountPath instead.
	FolderNaming  string `json:"folder_naming,omitempty"`   // Deprecated. Use global setting instead.
	RcUrl         string `json:"rc_url,omitempty"`          // Deprecated. Use global setting instead.
	RcUser        string `json:"rc_user,omitempty"`         // Deprecated. Use global setting instead.
	RcPass        string `json:"rc_pass,omitempty"`         // Deprecated. Use global setting instead.
	RcRefreshDirs string `json:"rc_refresh_dirs,omitempty"` // Deprecated. Use global setting instead.

	// Directories
	Directories map[string]WebdavDirectories `json:"directories,omitempty"` // Deprecated. Use global setting instead.
}

// DownloadsUncached resolves the tri-state download_uncached setting. A nil
// value means the key is absent from config.json and keeps the historical
// default: false (only cached torrents may be imported). Explicit true/false
// values are persisted as-is; the field is *bool so an explicit false survives
// a save round-trip instead of being stripped by omitempty.
func (d *Debrid) DownloadsUncached() bool {
	return d.DownloadUncached != nil && *d.DownloadUncached
}

// MagnetCap resolves the stored-item cap for this provider.
//
// Returns (cap, true) when a real ceiling applies, and (0, false) for
// UNLIMITED — which covers both an explicit 0 and a provider with no known
// cap. Callers must treat !ok as "there is no threshold here", never as "the
// cap is zero", and must not substitute a number of their own.
func (d *Debrid) MagnetCap() (int, bool) {
	if d.MaxMagnets != nil {
		if *d.MaxMagnets <= 0 {
			return 0, false
		}
		return *d.MaxMagnets, true
	}
	if cap, ok := defaultMagnetCaps[d.Provider]; ok {
		return cap, true
	}
	return 0, false
}

// defaultMagnetCaps carries a cap ONLY for providers whose ceiling has been
// observed on a live account. Everything absent from this map is unlimited
// until an operator says otherwise, which is the honest default: a provider we
// have not measured is one whose limit we do not know.
//
// AllDebrid: 5,000 stored items, measured at 4,998/5,000 refusing every add for
// 54.6 continuous hours across two midnight boundaries.
//
// RealDebrid is deliberately ABSENT rather than listed as unlimited: it bounds
// CONCURRENT active downloads (reported by /torrents/activeCount and handled by
// the admission check), not stored items, so a stored-item cap would be a
// category error, not merely a wrong number.
var defaultMagnetCaps = map[string]int{
	"alldebrid": 5000,
}

func (c *Config) updateDebrid(index int, d Debrid) Debrid {
	workers := runtime.NumCPU() * 50
	perDebrid := workers / len(c.Debrids)
	// Priority is never materialized here: an unset (0) priority stays 0 so a
	// save round-trip keeps it absent from config.json, and selection derives
	// the effective order from ConfigOrder at sort time.
	d.ConfigOrder = index

	if d.Provider == "" {
		d.Provider = d.Name
	}

	var downloadKeys []string

	if len(d.DownloadAPIKeys) > 0 {
		downloadKeys = d.DownloadAPIKeys
	} else {
		// If no download API keys are specified, use the main API key
		downloadKeys = []string{d.APIKey}
	}
	d.DownloadAPIKeys = downloadKeys

	if d.TorrentsRefreshInterval == "" {
		d.TorrentsRefreshInterval = DefaultTorrentsRefreshInterval
	}
	if d.DownloadLinksRefreshInterval == "" {
		d.DownloadLinksRefreshInterval = DefaultDownloadsRefreshInterval
	}
	if d.Workers == 0 {
		d.Workers = perDebrid
	}
	if d.AutoExpireLinksAfter == "" {
		d.AutoExpireLinksAfter = DefaultAutoExpireLinksAfter
	}

	return d
}

func validateDebrids(debrids []Debrid) error {
	if len(debrids) == 0 {
		return nil
	}

	for _, debrid := range debrids {
		// Basic field validation
		if debrid.APIKey == "" {
			return errors.New("debrid api key is required")
		}
		if debrid.Priority < 0 {
			return errors.New("debrid priority cannot be negative")
		}
	}

	return nil
}

func (c *Config) applyDebridEnvVars() {
	// Debrid providers array
	for i := range 10 { // Support up to 10 debrid providers
		prefix := fmt.Sprintf("DEBRIDS__%d__", i)
		if val := getEnv(prefix + "NAME"); val != "" {
			// Ensure array is large enough
			if i >= len(c.Debrids) {
				c.Debrids = append(c.Debrids, make([]Debrid, i-len(c.Debrids)+1)...)
			}
			c.Debrids[i].Name = val

			// Set other debrid fields
			if apiKey := getEnv(prefix + "API_KEY"); apiKey != "" {
				c.Debrids[i].APIKey = apiKey
			}
			if folder := getEnv(prefix + "FOLDER"); folder != "" {
				c.Debrids[i].Folder = folder
			}
			if provider := getEnv(prefix + "PROVIDER"); provider != "" {
				c.Debrids[i].Provider = provider
			}
			if proxy := getEnv(prefix + "PROXY"); proxy != "" {
				c.Debrids[i].Proxy = proxy
			}
			if priority := getEnv(prefix + "PRIORITY"); priority != "" {
				if value, err := strconv.Atoi(priority); err == nil {
					c.Debrids[i].Priority = value
				}
			}
		}
	}
}

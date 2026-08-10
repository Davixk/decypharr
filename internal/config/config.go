package config

import (
	"cmp"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	json "github.com/bytedance/sonic"
)

type (
	WebDavFolderNaming string
	MountType          string
	DownloadAction     string
	Protocol           string
)

const (
	ProtocolTorrent Protocol = "torrent"
	ProtocolNZB     Protocol = "nzb"
	ProtocolAll     Protocol = ""
)

const (
	MountTypeRclone         MountType = "rclone"
	MountTypeDFS            MountType = "dfs"
	MountTypeExternalRclone MountType = "external_rclone"
	MountTypeNone           MountType = "none"
)

const (
	DownloadActionSymlink  DownloadAction = "symlink"
	DownloadActionDownload DownloadAction = "download"
	DownloadActionStrm     DownloadAction = "strm"
	DownloadActionNone     DownloadAction = "none"
)

const (
	WebDavUseFileName          WebDavFolderNaming = "filename"
	WebDavUseOriginalName      WebDavFolderNaming = "original"
	WebDavUseFileNameNoExt     WebDavFolderNaming = "filename_no_ext"
	WebDavUseOriginalNameNoExt WebDavFolderNaming = "original_no_ext"
	WebdavUseHash              WebDavFolderNaming = "infohash"
)

var (
	instance   *Config
	once       sync.Once
	configPath string
	runtimeMu  sync.RWMutex
)

// QBitTorrent is deprecated. Use Manager instead.
// Kept for backward compatibility with existing configs.
type QBitTorrent struct {
	DownloadFolder      string   `json:"download_folder,omitempty"`
	Categories          []string `json:"categories,omitempty"`
	RefreshInterval     int      `json:"refresh_interval,omitempty"`
	SkipPreCache        bool     `json:"skip_pre_cache,omitempty"`
	AlwaysRmTrackerUrls bool     `json:"always_rm_tracker_urls,omitempty"`
}

func (q QBitTorrent) IsZero() bool {
	return q.DownloadFolder == "" && len(q.Categories) == 0 && q.RefreshInterval == 0 && !q.SkipPreCache && !q.AlwaysRmTrackerUrls
}

type Arr struct {
	Name              string `json:"name,omitempty"`
	Host              string `json:"host,omitempty"`
	Token             string `json:"token,omitempty"`
	Cleanup           bool   `json:"cleanup,omitempty"`
	SkipRepair        bool   `json:"skip_repair,omitempty"`
	DownloadUncached  *bool  `json:"download_uncached,omitempty"`
	SelectedDebrid    string `json:"selected_debrid,omitempty"`
	FallbackOnFailure bool   `json:"fallback_on_failure,omitempty"`
	Source            string `json:"source,omitempty"` // The source of the arr, e.g. "auto", "config", "". Auto means it was automatically detected from the arr
}

func (a Arr) IsZero() bool {
	return a.Name == "" && a.Host == "" && a.Token == "" && !a.Cleanup && !a.SkipRepair && a.DownloadUncached == nil && a.SelectedDebrid == "" && !a.FallbackOnFailure && a.Source == ""
}

// QueueCleanup is the global policy that drives CleanupQueue. It maps
// Sonarr/Radarr queue warnings/errors to an action.
type QueueCleanup struct {
	Rules              []QueueCleanupRule `json:"rules,omitempty"`
	ConfirmationSweeps int                `json:"confirmation_sweeps,omitempty"` // Consecutive matching monitor sweeps required before acting.
	ConfirmationDelay  string             `json:"confirmation_delay,omitempty"`  // Minimum age of an unchanged condition, e.g. "5m".
}

// QueueCleanupRule maps a queue issue to a cleanup action.
//
// Catalog rules carry a non-empty ID whose match semantics are hardcoded in the
// arr package (keyed by ID) — for these the user only customizes Action, and
// Match is informational/display only. Custom rules have an empty ID and match
// Match as a case-insensitive substring of the queue item's statusMessages text.
type QueueCleanupRule struct {
	ID     string `json:"id,omitempty"`     // catalog key; "" = user custom rule
	Match  string `json:"match,omitempty"`  // custom: substring; catalog: display text
	Action string `json:"action,omitempty"` // "" (ignore) | "import" | "blacklist" | "blacklist_research"
}

// DefaultQueueCleanupRules returns the built-in catalog of known Servarr queue
// issues with sensible default actions. The order is significant: resolution is
// first-match-wins, so more specific entries should precede broader ones.
func DefaultQueueCleanupRules() []QueueCleanupRule {
	return []QueueCleanupRule{
		{ID: "failed_download", Match: "Failed download", Action: "blacklist_research"},
		{ID: "title_mismatch", Match: "Title mismatch; automatic import is not possible", Action: "import"},
		{ID: "matched_by_id", Match: "Matched to series/movie by ID", Action: "import"},
		{ID: "unable_to_parse", Match: "Unable to parse download", Action: "blacklist_research"},
		{ID: "no_eligible_files", Match: "No files found are eligible for import", Action: "blacklist_research"},
		{ID: "episodes_missing", Match: "Episodes not imported or missing from the release", Action: "blacklist_research"},
		{ID: "file_empty", Match: "Downloaded file is empty", Action: "blacklist_research"},
		{ID: "invalid_local_path", Match: "Not a valid local path (remote path mapping)", Action: ""},
		{ID: "not_grabbed", Match: "Not grabbed by the arr / no category", Action: ""},
	}
}

// mergeQueueCleanupRules reconciles a stored rule set with the current catalog.
// An empty set is seeded with the defaults. Otherwise the user's action choices
// for existing catalog IDs and all custom rules are preserved, while any catalog
// entries the user has never seen (e.g. added in a newer release) are appended
// with their default action. Catalog display text is refreshed from the catalog.
func mergeQueueCleanupRules(rules []QueueCleanupRule) []QueueCleanupRule {
	defaults := DefaultQueueCleanupRules()
	if len(rules) == 0 {
		return defaults
	}

	stored := make(map[string]QueueCleanupRule, len(rules))
	out := make([]QueueCleanupRule, 0, len(rules)+len(defaults))
	for _, r := range rules {
		if r.ID == "" {
			// Custom rule: keep as-is (drop empties defensively).
			if strings.TrimSpace(r.Match) != "" {
				out = append(out, r)
			}
			continue
		}
		stored[r.ID] = r
	}

	// Emit the catalog in canonical order, preserving stored actions.
	for _, d := range defaults {
		if s, ok := stored[d.ID]; ok {
			d.Action = s.Action
		}
		out = append(out, d)
	}

	// out currently holds [customs..., catalog...]; reorder so catalog rules
	// come first. First-match-wins then favors the specific known issues over
	// broad user substrings.
	catalogFirst := make([]QueueCleanupRule, 0, len(out))
	for _, r := range out {
		if r.ID != "" {
			catalogFirst = append(catalogFirst, r)
		}
	}
	for _, r := range out {
		if r.ID == "" {
			catalogFirst = append(catalogFirst, r)
		}
	}
	return catalogFirst
}

type CustomFolders struct {
	Filters map[string]string `json:"filters,omitempty"`
}

type Auth struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	APIToken string `json:"api_token,omitempty"`
}

// RepairSource selects where the health checker enumerates entries from.
type RepairSource string

const (
	RepairSourceArr     RepairSource = "arr"
	RepairSourceManaged RepairSource = "managed"
)

// RepairConfig is the single, global configuration for the health checker.
// When Enabled is true, a recurring sweep runs on Schedule and visits only
// entries that are unhealthy, dirty, or older than RecheckInterval.
type RepairConfig struct {
	Enabled               bool         `json:"enabled,omitempty"`
	Source                RepairSource `json:"source,omitempty"`
	Schedule              string       `json:"schedule,omitempty"`
	Workers               int          `json:"workers,omitempty"`
	NNTPConnectionPercent int          `json:"nntp_connection_percent,omitempty"`
	Strategy              string       `json:"strategy,omitempty"`
	RecheckInterval       string       `json:"recheck_interval,omitempty"`
	Arrs                  []string     `json:"arrs,omitempty"`
	// AutoRepair is DEPRECATED and no longer gates actions. It is retained only
	// so configs written before the REPAIR/PRUNE/ARR-DELETE component split still
	// round-trip through JSON; the resolver ignores it entirely.
	AutoRepair    bool `json:"auto_repair,omitempty"`
	SkipNZBRepair bool `json:"skip_nzb_repair,omitempty"`

	// The repair feature is split into four independent, named components:
	//
	//   CHECK    — always runs when Enabled. Enumerates the whole hosted library
	//              via the managed path and probes each item for liveness. Has no
	//              arr dependency and is not knob-gated.
	//   REPAIR   — Repair knob. On a dead item, re-acquire it: re-add to the same
	//              provider, falling back to other configured debrids in order.
	//              Non-destructive. Defaults to true when unset (nil).
	//   PRUNE    — Prune knob. On a dead item (REPAIR off or failed), delete it
	//              decypharr-side ONLY: provider placements + symlink/download
	//              folder + db entry. NEVER calls the arr. Destructive; defaults
	//              false. NOTE: this does NOT make the arr self-heal — see the
	//              "WHAT THIS DOES NOT DO" block on pruneDeadEntry.
	//   ARR-DELETE  — Regrab knob. On a dead item, DELETE the arr's file record. The
	//              only arr-coupled component. Independent of PRUNE (not gated
	//              behind it). Destructive; defaults false.
	//
	// Actions are driven directly by the REPAIR / PRUNE / ARR-DELETE knobs below:
	// each component acts whenever its own knob is on. A sweep is CHECK-only
	// (detect + record, no REPAIR/PRUNE/ARR-DELETE) exactly when all three are off.
	// There is no separate master gate; the deprecated AutoRepair field above is
	// ignored by the resolver.
	//
	// Repair is a *bool so an unset field (nil, e.g. a config written before this
	// split) resolves to the safe default of true via RepairEnabled(); Prune and
	// Regrab default to their false zero value. So a dead item is detected and
	// re-acquire-attempted by default, but nothing is DELETED until the operator
	// opts into PRUNE and/or ARR-DELETE.
	Repair *bool `json:"repair,omitempty"`
	Prune bool `json:"prune,omitempty"`

	// ArrDelete is the arr-side component: it deletes the arr's file record for a
	// dead item. It is a *bool purely so an unset value can be told apart from an
	// explicit false, which is what lets the deprecated Regrab alias below
	// migrate cleanly. Resolve it with ArrDeleteEnabled(), never directly.
	//
	// It was called ARR-DELETE, and that name is now wrong. The component used to
	// bundle three arr-side effects behind one checkbox — delete the file record,
	// blocklist the grab, trigger a search — so "re-grab" described the whole. The
	// three are now separately gated, and since ArrSearch and ArrBlocklist both
	// default OFF, **the default behaviour of this knob does not grab anything at
	// all**. It is arr-side record management. Naming it "regrab" would tell every
	// operator who reads the config the opposite of what it does, and the
	// sub-knobs would inherit the wrong stem: "regrab_search" reads as a variety
	// of re-grabbing when it is in fact the only part that re-grabs.
	//
	// Regrab is the deprecated alias, kept ONLY so configs written before the
	// rename keep working. setDefaults migrates it onto ArrDelete and clears it,
	// so nothing downstream reads it.
	ArrDelete *bool `json:"arr_delete,omitempty" alias:"regrab"`
	Regrab    *bool `json:"regrab,omitempty"` // Deprecated: use ArrDelete.

	// ArrSearch — after the delete, ask the arr to search for a replacement.
	//
	//   MUST be implemented as an explicit SearchMissing over EVERY deleted file.
	//   Do NOT reintroduce the old shortcut of letting MarkHistoryFailed's
	//   "Redownload Failed" side effect stand in for it: that only covers files
	//   whose grab-history record still exists, so it silently does nothing for
	//   the rest, and it couples searching to blocklisting all over again.
	//
	// ArrBlocklist — mark the grab failed in the arr, which blocklists the
	// release globally and permanently.
	//
	//   Reserve this for the BAD-RELEASE class: sample-only, wrong content, empty
	//   file, a .exe masquerading as an episode, malformed NZB. Those are global,
	//   permanent facts about the release itself and a permanent global ban is the
	//   correct record for them.
	//
	//   It is the WRONG record for bytes-unavailable — an article purged from one
	//   news server, a debrid returning 451 for one release, a provider-scoped or
	//   transient refusal. Those are provider-scoped and provisional; a global ban
	//   destroys a release another provider may serve fine, and the arr writes it
	//   as "Manually marked as failed" with no reason recorded, so it cannot even
	//   be audited afterwards. Measured on one production library: 4,724 of 11,362
	//   blocklist records carried that unauditable signature.
	//
	//   Blocklisting is also NOT what produces forward progress, which is the
	//   usual argument for keeping it bundled. On an add failure the arr walks its
	//   own ranked candidate list — observed: five distinct infohashes tried in
	//   sequence for one episode, and another satisfied on the twelfth candidate
	//   after eleven consecutive refusals. The blocklist only removes candidates
	//   permanently; it never advances anything.
	//
	// Both default OFF. Both are subordinate to ArrDelete: with it off there are
	// no arr calls at all, which keeps "the arr component is the only arr-coupled
	// component" true.
	ArrSearch    bool `json:"arr_search,omitempty"`
	ArrBlocklist bool `json:"arr_blocklist,omitempty"`

	// MaxDeletionsPerRun caps how many entries a single repair run may
	// destructively heal — i.e. how many entries can have their Arr file
	// records deleted and/or their decypharr entry deleted in one sweep/run.
	// A provider-wide false "unavailable" (e.g. a debrid outage returning
	// HosterUnavailable for everything) could otherwise mark the whole due set
	// broken and mass-delete Arr records + entries in a single run. The cap
	// makes recovery progressive: entries beyond the cap stay broken in
	// storage and are re-picked next run, so nothing is lost — just deferred.
	// 0/unset defaults to 100 (mirrors the missing-download reconciler); a
	// negative value (e.g. -1) means unlimited.
	MaxDeletionsPerRun int `json:"max_deletions_per_run,omitempty"`

	// StopSchedule, when set, stops an in-progress repair sweep at this time/interval
	// (same formats as Schedule: clock time, cron expression, or duration).
	// A repair sweep still running when StopSchedule fires is cancelled before it
	// finishes enumerating/probing every candidate. Empty disables the stop
	// schedule entirely - the repair sweep always runs to completion. When a stop
	// fires mid-repair-sweep, the enabled REPAIR / PRUNE / ARR-DELETE components
	// decide what happens to whatever was already found broken: acted on if any
	// component is on, left alone (CHECK-only) if all three are off.
	StopSchedule string `json:"stop_schedule,omitempty"`
}

func (r RepairConfig) IsZero() bool {
	return !r.Enabled && r.Source == "" && r.Schedule == "" && r.Workers == 0 &&
		r.NNTPConnectionPercent == 0 && r.Strategy == "" && r.RecheckInterval == "" && len(r.Arrs) == 0 &&
		!r.AutoRepair && !r.SkipNZBRepair && r.StopSchedule == "" && r.MaxDeletionsPerRun == 0 &&
		r.Repair == nil && !r.Prune && r.ArrDelete == nil && r.Regrab == nil &&
		!r.ArrSearch && !r.ArrBlocklist
}

// ArrDeleteEnabled resolves the arr-delete component knob, honouring the
// deprecated `regrab` alias for configs written before the rename. Always use
// this rather than reading either field: setDefaults normally migrates the alias
// away, but a RepairConfig built in memory (tests, API overrides) may not have
// been through it.
func (r RepairConfig) ArrDeleteEnabled() bool {
	if r.ArrDelete != nil {
		return *r.ArrDelete
	}
	return r.Regrab != nil && *r.Regrab
}

// migrateDeprecatedNames folds the deprecated `regrab` key onto ArrDelete and
// clears it, so exactly one field is authoritative afterwards. An explicit
// arr_delete always wins — if both keys are present the operator has already
// moved to the new name and the stale one must not resurrect an old value.
func (r *RepairConfig) migrateDeprecatedNames() {
	if r.Regrab == nil {
		return
	}
	if r.ArrDelete == nil {
		v := *r.Regrab
		r.ArrDelete = &v
	}
	r.Regrab = nil
}

// UnmarshalJSON folds the deprecated `regrab` key onto ArrDelete at DECODE time,
// which is the only point where it can be done safely.
//
// Doing it later does not work. The partial-update merge decides field by field
// on KEY PRESENCE in the posted body, so a client PATCHing `{"regrab": false}`
// posts no arr_delete key at all; the merge would treat arr_delete as "not
// mentioned" and restore the live value over it, leaving the component ON after
// the caller asked to turn it OFF. Folding here means the decoded struct already
// carries the value, and the `alias:"regrab"` tag on ArrDelete tells the merge
// the field WAS mentioned. Both halves are required — value and key presence.
func (r *RepairConfig) UnmarshalJSON(data []byte) error {
	type raw RepairConfig // shed the method set, or this recurses forever
	var decoded raw
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*r = RepairConfig(decoded)
	r.migrateDeprecatedNames()
	return nil
}

// RepairEnabled resolves the REPAIR (re-acquire) component knob. It defaults to
// true when unset (nil) — the safe default is to attempt re-acquisition of a
// dead item.
func (r RepairConfig) RepairEnabled() bool {
	if r.Repair == nil {
		return true
	}
	return *r.Repair
}

type Config struct {
	// server
	BindAddress string `json:"bind_address,omitempty"`
	URLBase     string `json:"url_base,omitempty"`
	AppURL      string `json:"app_url,omitempty"`
	Port        string `json:"port,omitempty"`

	LogLevel string   `json:"log_level,omitempty"`
	Debrids  []Debrid `json:"debrids,omitzero"`

	Arrs        []Arr       `json:"arrs,omitzero"`
	Usenet      Usenet      `json:"usenet,omitzero"`      // Usenet configuration
	QBitTorrent QBitTorrent `json:"qbittorrent,omitzero"` // Deprecated: use Manager instead
	Rclone      Rclone      `json:"rclone,omitzero"`      // Deprecated: use Mounts instead
	Mount       Mount       `json:"mount,omitzero"`

	AllowedExt         []string `json:"allowed_file_types,omitempty"`
	AllowSamples       bool     `json:"allow_samples,omitempty"`
	MinFileSize        string   `json:"min_file_size,omitempty"`
	MaxFileSize        string   `json:"max_file_size,omitempty"`
	RemoveStalledAfter string   `json:"remove_stalled_after,omitzero"`
	EnableWebdavAuth   bool     `json:"enable_webdav_auth,omitempty"`
	UseAuth            bool     `json:"use_auth,omitempty"`
	NZBUserAgent       string   `json:"nzb_user_agent,omitempty"` // User agent for downloading NZBs
	Auth               *Auth    `json:"-"`

	DisableWebDav bool `json:"disable_webdav,omitempty"`

	// Notifications configuration
	Notifications Notifications `json:"notifications"`

	// Deprecated: Use Notifications.WebhookURL instead
	DiscordWebhook string `json:"discord_webhook_url,omitempty"`
	// Deprecated: Use Notifications.CallbackURL instead
	CallbackURL string `json:"callback_url,omitempty"`

	// Manager settings
	DownloadFolder        string                   `json:"download_folder,omitempty"`
	RefreshInterval       string                   `json:"refresh_interval,omitempty"`
	// MaxActiveDownloads bounds concurrent post-download ACTIONS — mount waits,
	// local fetches, symlink creation. Real local I/O, and the only thing this
	// number has ever effectively controlled.
	//
	// It used to also size the job-worker pool, which is how a machine-resource
	// number ended up gating provider admission and, worse, budgeting usenet
	// imports that consume no provider resource at all. That job now belongs to
	// MaxConcurrentJobs; this keeps its name, its value and its meaning, which
	// is accurate for the first time.
	MaxActiveDownloads int `json:"max_active_downloads,omitempty"`

	// MaxConcurrentJobs bounds how many import jobs may be IN FLIGHT at once.
	//
	// This is JOB SLOTS, NOT DOWNLOADS. It exists to stop unbounded goroutine
	// fan-out from overwhelming the machine — the reason the unified job queue
	// was built (a52e56d, "fix issues with dfs hangs") — and it is not a proxy
	// for anybody's API limit. Real download parallelism is bounded elsewhere:
	// upstream by per-provider admission against what the provider reports, and
	// downstream by MaxActiveDownloads.
	//
	// Sized in the hundreds on purpose. The old value of 14 was a plausible
	// number of simultaneous debrid downloads and an absurd cap on job slots:
	// short jobs queued behind long ones in a strict FIFO, so an NZB import
	// taking 1-5s of real work waited up to 17 MINUTES behind uncached torrents
	// that hold a worker for the entire download.
	MaxConcurrentJobs int `json:"max_concurrent_jobs,omitempty"`

	StallPrune StallPruneConfig `json:"stall_prune,omitempty"`
	// SeederGate is a GRAB-TIME refusal, deliberately NOT part of StallPrune.
	// That one prunes asynchronously on a timer; this one answers while the arr
	// is still blocked on its add. Sharing a threshold does not make them the
	// same feature, and wiring this into the sweep inverts it.
	SeederGate            SeederGateConfig         `json:"seeder_gate,omitempty"`
	// NetworkBinding is COLD (restart-required): the shared HTTP transport is
	// built once at construction, so a changed binding would apply to new
	// sockets only and leave existing pooled connections on the old route —
	// which is precisely the silent-fallback failure this feature exists to
	// prevent. Deliberately absent from clearHotFields.
	NetworkBinding        NetworkBindingConfig     `json:"network_binding,omitempty"`
	SkipPreCache          bool                     `json:"skip_pre_cache,omitempty"`
	SkipMultiSeason       bool                     `json:"skip_multi_season,omitempty"`
	AlwaysRmTrackerUrls   bool                     `json:"always_rm_tracker_urls,omitempty"`
	Categories            []string                 `json:"categories,omitempty"`
	FolderNaming          WebDavFolderNaming       `json:"folder_naming,omitempty"`
	CustomFolders         map[string]CustomFolders `json:"custom_folders,omitempty"`
	DefaultDownloadAction DownloadAction           `json:"default_download_action,omitempty"`

	RefreshDirs  string `json:"refresh_dirs,omitempty"`
	Retries      int    `json:"retries,omitempty"`
	SkipAutoMove bool   `json:"skip_auto_move,omitempty"`

	// DebridReadTimeout bounds how long a debrid HTTP file read may deliver ZERO
	// bytes to the WebDAV/DFS client before the read is aborted. It is a
	// progress/idle deadline: a slow but still-progressing stream is never
	// affected; only a fully stalled read (a dead connection held open with no
	// byte-flow) trips it, so rclone/Plex see a prompt error instead of wedging
	// on their own multi-minute timeouts. Default "60s"; "0"/"off"/"none"
	// disables it (unbounded passthrough, matching usenet.read_timeout).
	DebridReadTimeout string `json:"debrid_read_timeout,omitempty"`

	// DebridLinkTimeout bounds how long a caller may WAIT for a debrid download
	// link to be minted. It covers the phase BEFORE any byte exists, which
	// DebridReadTimeout cannot see: that one is a byte-progress deadline and is
	// only armed once link resolution has already returned.
	//
	// THE HANG IT PREVENTS. Minting a link can reach the provider's
	// unrestrict endpoint, a per-account retry chain, every OTHER configured
	// account in turn, and — when the provider answers "hoster unavailable" —
	// a full inline re-insertion cascade (submit magnet + poll for status)
	// repeated up to MaxReinsertionAttempt times. None of that has a ceiling of
	// its own, and none of it is abortable by the reader: the provider call
	// chain does not carry the request context. A WebDAV GET could therefore sit
	// in the handler for MINUTES on a provider flap, which is what puts a FUSE
	// reader into uninterruptible sleep instead of giving it a prompt error.
	//
	// The wait is bounded, not the work: an over-running resolution keeps
	// running detached and its result still lands in the provider's link cache,
	// so the client's retry after the 503 is usually served instantly. That is
	// the "absorb" half; the 503 + Retry-After is the "fail fast" half.
	//
	// Default "20s" — a healthy mint is well under a second, and 20s still
	// leaves room for one slow-but-working mint plus a fallback account while
	// staying far below every reader's own patience. "0"/"off"/"none" disables
	// the ceiling and restores the historical unbounded, context-blind wait.
	DebridLinkTimeout string `json:"debrid_link_timeout,omitempty"`

	// DebridStatusTimeout bounds how long a provider STATUS POLL may run after a
	// magnet has been submitted, before the caller gives up on that provider.
	//
	// THE WEDGE IT PREVENTS. RealDebrid.CheckStatus re-polls while the provider
	// reports "waiting_files_selection": select the files, sleep, ask again. That
	// loop had no context, no deadline and no iteration cap, so a provider that
	// never advanced past that state held the caller forever. It is reached from
	// re-insertion (Fixer.MoveTorrent), from the queue's completion wait, and
	// from the grab path — and until the same release moved the provider calls
	// out from under Manager.copyEntryMu, one such poll also blocked every WebDAV
	// DELETE/COPY/MOVE in the process.
	//
	// A ceiling firing here yields a TRANSIENT, retryable 503
	// (customerror.NewBackendTimeoutError) and never a content verdict: a
	// provider that will not answer says nothing whatsoever about whether the
	// content is good, so the repair cascade explicitly declines to mark the
	// entry Bad on it.
	//
	// Default "60s"; "0"/"off"/"none" disables the ceiling and restores the
	// historical unbounded poll, so the knob stays a genuine escape hatch.
	DebridStatusTimeout string `json:"debrid_status_timeout,omitempty"`

	// MetadataReadTimeout bounds how long a WebDAV metadata request — PROPFIND
	// listings and HEAD — may wait on a backend before answering 503.
	//
	// THE HANG IT PREVENTS. Only byte-streams had a deadline. A listing had
	// none of any kind, so it waited on whatever the upstream metadata call did:
	// for Usenet entries that is a live per-file article/segment preparation
	// against the NNTP providers, with nothing to trip if it stalls. A stalled
	// listing wedges `ls`, an *arr scan and a Plex library refresh just as hard
	// as a stalled read does, and it never produced an error the client could
	// act on.
	//
	// Default "15s" — a listing has no bytes to trickle, so there is no
	// "slow but progressing" state to protect; a client still waiting after 15s
	// has already stalled its own queue. "0"/"off"/"none" disables it.
	MetadataReadTimeout string `json:"metadata_read_timeout,omitempty"`

	Repair RepairConfig `json:"repair,omitzero"`

	// QueueCleanup is the global arr queue-cleanup policy (see CleanupQueue).
	QueueCleanup QueueCleanup `json:"queue_cleanup"`
}

func (c *Config) JsonFile() string {
	return filepath.Join(GetMainPath(), "config.json")
}
func (c *Config) AuthFile() string {
	return filepath.Join(GetMainPath(), "auth.json")
}

func (c *Config) TorrentsFile() string {
	return filepath.Join(GetMainPath(), "torrents.json")
}

func (c *Config) loadConfig() error {
	// Load the config file
	// Read the JSON config file directly
	configFile := c.JsonFile()
	fmt.Printf("Loading config from %s\n", configFile)
	data, err := os.ReadFile(configFile)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("Config file not found, creating a new one at %s\n", configFile)
			// Create a default config file if it doesn't exist
			if err := c.createConfig(); err != nil {
				return fmt.Errorf("failed to create config file: %w", err)
			}
			return c.Save()
		}
		return fmt.Errorf("error reading config file: %w", err)
	}

	// Parse JSON
	if err := json.Unmarshal(data, &c); err != nil {
		return fmt.Errorf("error parsing config JSON: %w", err)
	}

	// Set defaults for any missing values
	c.setDefaults()

	// Apply environment variable overrides
	c.applyEnvOverrides()

	// Guardrail only — warn (never fail) when the parallel-import connection
	// demand can exhaust the configured NNTP providers.
	if warning := c.UsenetConnectionBudgetWarning(); warning != "" {
		_, _ = fmt.Fprintf(os.Stderr, "WARNING: %s\n", warning)
	}

	return nil
}

func (c *Config) Validate() error {
	if err := validateDebrids(c.Debrids); err != nil {
		return err
	}

	if err := validateUsenet(c.Usenet.Providers); err != nil {
		return err
	}

	if c.DownloadFolder == "" {
		return errors.New("download folder is required")
	}

	// If either debrid or usenet is enabled, at least one must be configured
	if len(c.Debrids) == 0 && len(c.Usenet.Providers) == 0 {
		return errors.New("at least one debrid provider or usenet provider must be configured")
	}

	return nil
}

// generateAPIToken creates a new random API token
func generateAPIToken() (string, error) {
	bytes := make([]byte, 32) // 256-bit token
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func SetConfigPath(path string) {
	configPath = path
}

func GetMainPath() string {
	return configPath
}

func Get() *Config {
	once.Do(func() {
		instance = &Config{} // Initialize instance first
		if err := instance.loadConfig(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "configuration Error: %v\n", err)
			os.Exit(1)
		}
	})
	return instance
}

func (c *Config) GetMinFileSize() int64 {
	// 0 means no limit
	if c.MinFileSize == "" {
		return 0
	}
	s, err := ParseSize(c.MinFileSize)
	if err != nil {
		return 0
	}
	return s
}

func (c *Config) GetMaxFileSize() int64 {
	// 0 means no limit
	if c.MaxFileSize == "" {
		return 0
	}
	s, err := ParseSize(c.MaxFileSize)
	if err != nil {
		return 0
	}
	return s
}

func (c *Config) SecretKey() string {
	return cmp.Or(getEnv("SECRET_KEY"), "\"wqj(v%lj*!-+kf@4&i95rhh_!5_px5qnuwqbr%cjrvrozz_r*(\"")
}

func (c *Config) GetAuth() *Auth {
	if !c.UseAuth {
		return nil
	}
	if c.Auth == nil {
		c.Auth = &Auth{}
		if _, err := os.Stat(c.AuthFile()); err == nil {
			file, err := os.ReadFile(c.AuthFile())
			if err == nil {
				_ = json.Unmarshal(file, c.Auth)
			}
		}
	}
	return c.Auth
}

func (c *Config) SaveAuth(auth *Auth) error {
	c.Auth = auth
	data, err := json.Marshal(auth)
	if err != nil {
		return err
	}
	return os.WriteFile(c.AuthFile(), data, 0644)
}

func (c *Config) NeedsAuth() bool {
	return c.UseAuth && (c.Auth == nil || c.Auth.Username == "" || c.Auth.Password == "")
}

// migrateQBitTorrentToManager migrates deprecated QBitTorrent config to Manager
// This ensures backward compatibility with existing configs
func (c *Config) migrateQBitTorrentToManager() {
	// If Manager fields are not set but QBitTorrent fields are, migrate them
	if c.DownloadFolder == "" && c.QBitTorrent.DownloadFolder != "" {
		c.DownloadFolder = c.QBitTorrent.DownloadFolder
	}

	if len(c.Categories) == 0 && len(c.QBitTorrent.Categories) > 0 {
		c.Categories = c.QBitTorrent.Categories
	}

	if c.RefreshInterval == "" && c.QBitTorrent.RefreshInterval > 0 {
		c.RefreshInterval = fmt.Sprintf("%ds", c.QBitTorrent.RefreshInterval)
	}

	if !c.SkipPreCache && c.QBitTorrent.SkipPreCache {
		c.SkipPreCache = c.QBitTorrent.SkipPreCache
	}

	if !c.AlwaysRmTrackerUrls && c.QBitTorrent.AlwaysRmTrackerUrls {
		c.AlwaysRmTrackerUrls = c.QBitTorrent.AlwaysRmTrackerUrls
	}

	// Set default download folder if not set
	if c.DownloadFolder == "" {
		c.DownloadFolder = filepath.Join(GetMainPath(), "downloads")
	}

	// Set default categories if not set
	if len(c.Categories) == 0 {
		c.Categories = []string{"sonarr", "radarr"}
	}

	// Set default refresh interval if not set
	if c.RefreshInterval == "" {
		c.RefreshInterval = "30s"
	}
}

// migrateNotifications migrates deprecated DiscordWebhook and CallbackURL to Notifications
// This ensures backward compatibility with existing configs
func (c *Config) migrateNotifications() {
	// Migrate deprecated webhook URL to Notifications
	if c.Notifications.WebhookURL == "" && c.DiscordWebhook != "" {
		c.Notifications.WebhookURL = c.DiscordWebhook
		c.Notifications.Enabled = true
	}

	// Migrate deprecated callback URL to Notifications
	if c.Notifications.CallbackURL == "" && c.CallbackURL != "" {
		c.Notifications.CallbackURL = c.CallbackURL
		c.Notifications.Enabled = true
	}

	// Auto-enable notifications if any URL is configured
	if c.Notifications.WebhookURL != "" || c.Notifications.CallbackURL != "" {
		c.Notifications.Enabled = true
	}
}

func (c *Config) setDefaults() {
	// Migrate deprecated fields to Manager (backward compatibility)
	c.migrateQBitTorrentToManager()
	c.migrateNotifications()
	c.Repair.migrateDeprecatedNames()

	if c.DefaultDownloadAction == "" {
		c.DefaultDownloadAction = DownloadActionSymlink
	}
	if c.MaxActiveDownloads <= 0 {
		c.MaxActiveDownloads = 5
	}
	// Deliberately NOT aliased to max_active_downloads. Reading the old key
	// here would hand every existing config its old 5-or-14 as a job-slot
	// ceiling and silently deliver none of the fix, while looking migrated.
	// A new key with its own default is the only version that actually lands.
	if c.MaxConcurrentJobs <= 0 {
		c.MaxConcurrentJobs = DefaultMaxConcurrentJobs
	}
	if c.DebridReadTimeout == "" {
		c.DebridReadTimeout = "60s"
	}
	if c.DebridLinkTimeout == "" {
		c.DebridLinkTimeout = DefaultDebridLinkTimeout
	}
	if c.DebridStatusTimeout == "" {
		c.DebridStatusTimeout = DefaultDebridStatusTimeout
	}
	if c.MetadataReadTimeout == "" {
		c.MetadataReadTimeout = DefaultMetadataReadTimeout
	}

	for i, debrid := range c.Debrids {
		c.Debrids[i] = c.updateDebrid(i, debrid)
	}

	// Set usenet defaults
	c.updateUsenetConfig()

	firstDebrid := Debrid{}
	if len(c.Debrids) > 0 {
		firstDebrid = c.Debrids[0]
	}

	if c.Mount.Type == "" {
		if c.Rclone.Enabled {
			c.Mount.Type = MountTypeRclone
			c.Mount.Rclone = c.Rclone
		}
	}

	if c.Mount.MountPath == "" {
		// Set MountPath from debridConfig.Folder by splliting it
		// debrid.Folder is usually {mount_path}/{debrid_name}/__all__ or {mount_path}/{debrid_name}/torrents
		if len(c.Debrids) > 0 {
			folder := filepath.Clean(firstDebrid.Folder)
			c.Mount.MountPath = filepath.Dir(folder)
		}
	}

	// Move WebDav global settings to Manager if not set
	if c.Mount.ExternalRclone.RCUrl == "" {
		c.Mount.ExternalRclone.RCUrl = firstDebrid.RcUrl
	}
	if c.Mount.ExternalRclone.RCUsername == "" {
		c.Mount.ExternalRclone.RCUsername = firstDebrid.RcUser
	}
	if c.Mount.ExternalRclone.RCPassword == "" {
		c.Mount.ExternalRclone.RCPassword = firstDebrid.RcPass
	}

	if c.FolderNaming == "" {
		c.FolderNaming = WebDavFolderNaming(firstDebrid.FolderNaming)
	}

	// Set default allowed extensions if not set in Manager
	if len(c.AllowedExt) == 0 {
		c.AllowedExt = getDefaultExtensions()
	}

	// Set default error threshold for multi-debrid switching
	if c.Retries == 0 {
		c.Retries = 3 // Default to 3 consecutive errors before switching
	}

	c.QueueCleanup.Rules = mergeQueueCleanupRules(c.QueueCleanup.Rules)
	if c.QueueCleanup.ConfirmationSweeps <= 0 {
		c.QueueCleanup.ConfirmationSweeps = 3
	}
	if delay, err := time.ParseDuration(c.QueueCleanup.ConfirmationDelay); err != nil || delay <= 0 {
		c.QueueCleanup.ConfirmationDelay = "5m"
	}

	// Basic defaults
	if c.URLBase == "" {
		c.URLBase = "/"
	}
	// validate url base starts with /
	if !strings.HasPrefix(c.URLBase, "/") {
		c.URLBase = "/" + c.URLBase
	}
	if !strings.HasSuffix(c.URLBase, "/") {
		c.URLBase += "/"
	}

	if c.Port == "" {
		c.Port = DefaultPort
	}

	if c.LogLevel == "" {
		c.LogLevel = DefaultLogLevel
	}

	// Rclone defaults
	if c.Mount.Type == MountTypeRclone {
		c.Mount.Rclone.Port = cmp.Or(c.Rclone.Port, DefaultRclonePort)
		if c.Mount.Rclone.AsyncRead == nil {
			_asyncTrue := true
			c.Mount.Rclone.AsyncRead = &_asyncTrue
		}
		c.Mount.Rclone.VfsCacheMode = cmp.Or(c.Mount.Rclone.VfsCacheMode, "off")
		if c.Mount.Rclone.UID == 0 {
			c.Mount.Rclone.UID = uint32(os.Getuid())
		}
		if c.Mount.Rclone.GID == 0 {
			if runtime.GOOS == "windows" {
				// On Windows, we use the current user's SID as GID
				c.Mount.Rclone.GID = uint32(os.Getuid()) // Windows does not have GID, using UID instead
			} else {
				c.Mount.Rclone.GID = uint32(os.Getgid())
			}
		}
		if c.Mount.Rclone.Transfers == 0 {
			c.Mount.Rclone.Transfers = 4 // Default number of transfers
		}
		if c.Mount.Rclone.VfsCacheMode != "off" {
			c.Mount.Rclone.VfsCachePollInterval = cmp.Or(c.Rclone.VfsCachePollInterval, "1m") // Clean cache every minute
		}
		c.Mount.Rclone.DirCacheTime = cmp.Or(c.Rclone.DirCacheTime, "5m")
		c.Mount.Rclone.LogLevel = cmp.Or(c.Rclone.LogLevel, strings.ToUpper(DefaultLogLevel))
	}

	// DFS defaults
	if c.Mount.Type == MountTypeDFS {
		if c.Mount.DFS.ChunkSize == "" {
			c.Mount.DFS.ChunkSize = DefaultDFSChunkSize
		}
		if c.Mount.DFS.ReadAheadSize == "" {
			c.Mount.DFS.ReadAheadSize = DefaultDFSReadAheadSize
		}
		if c.Mount.DFS.CacheExpiry == "" {
			c.Mount.DFS.CacheExpiry = DefaultDFSCacheExpiry
		}
		if c.Mount.DFS.DiskCacheSize == "" {
			c.Mount.DFS.DiskCacheSize = DefaultDFSDiskCacheSize
		}

		if c.Mount.DFS.UID == 0 {
			c.Mount.DFS.UID = uint32(os.Getuid())
		}
		if c.Mount.DFS.GID == 0 {
			if runtime.GOOS == "windows" {
				// On Windows, we use the current user's SID as GID
				c.Mount.DFS.GID = uint32(os.Getuid()) // Windows does not have GID, using UID instead
			} else {
				c.Mount.DFS.GID = uint32(os.Getgid())
			}
		}
	}
	// Load the auth file
	c.Auth = c.GetAuth()

	// Generate API token if auth is enabled and no token exists
	if c.UseAuth {
		if c.Auth == nil {
			c.Auth = &Auth{}
		}
		if c.Auth.APIToken == "" {
			if token, err := generateAPIToken(); err == nil {
				c.Auth.APIToken = token
				// Save the updated auth config
				_ = c.SaveAuth(c.Auth)
			}
		}
	}

	// Set folder naming from first debrid if available
	if len(c.Debrids) > 0 && c.FolderNaming == "" {
		c.FolderNaming = WebDavFolderNaming(c.Debrids[0].FolderNaming)
	}

	c.applyRepairDefaults()
}

func (c *Config) applyRepairDefaults() {
	if c.Repair.Source == "" {
		c.Repair.Source = RepairSourceArr
	}
	if c.Repair.Workers <= 0 {
		c.Repair.Workers = 5
	}
	if c.Repair.Strategy == "" {
		c.Repair.Strategy = "per_entry"
	}
	if c.Repair.RecheckInterval == "" {
		c.Repair.RecheckInterval = "168h"
	}

	if c.Repair.NNTPConnectionPercent == 0 {
		c.Repair.NNTPConnectionPercent = 20
	}
}

func (c *Config) Save() error {
	c.setDefaults()
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(c.JsonFile(), data, 0644); err != nil {
		fmt.Printf("Failed to write config file: %v\n", err)
		return err
	}
	return nil
}

func Reset() {
	once = sync.Once{}
	instance = nil
}

// clearHotFields zeroes every field that can be applied at runtime without a
// full service restart. It is used by RequiresRestart so that only the
// remaining ("cold") fields participate in the change comparison.
//
// IMPORTANT: any new Config field defaults to "cold" (restart-required) unless
// it is added here. That is deliberate — it is always safe to fall back to a
// restart, but never safe to skip one for a field that needs it.
func clearHotFields(c *Config) {
	// Auth lives in auth.json and is preserved separately by the caller.
	c.Auth = nil

	// AppURL is only read live (e.g. STRM URL generation in the downloader);
	// it is never cached in a service struct, so it applies without a restart.
	c.AppURL = ""

	// Auth toggles are evaluated live per-request by every auth middleware
	// (main app, qbit, sabnzbd, and webdav), so they apply without a restart.
	c.UseAuth = false
	c.EnableWebdavAuth = false

	// Manager / processing settings — read live via config.Get() on the
	// relevant code paths, or applied lazily on the next natural restart.
	c.Arrs = nil
	c.AllowedExt = nil
	c.AllowSamples = false
	c.MinFileSize = ""
	c.MaxFileSize = ""
	c.RemoveStalledAfter = ""
	c.NZBUserAgent = ""
	c.Notifications = Notifications{}
	c.DiscordWebhook = ""
	c.CallbackURL = ""
	c.DownloadFolder = ""
	c.RefreshInterval = ""
	// MaxActiveDownloads and MaxConcurrentJobs are deliberately NOT cleared here
	// (i.e. both are cold, restart-required fields): the manager sizes the
	// post-download action gate and the job-worker pool from them once at
	// construction, and nothing rebuilds either on a live config apply.
	// Treating them as hot made changes silently require a restart without
	// telling the user.
	// Read live by every stall-prune sweep, so changes take effect on the next
	// pass without a restart. That matters specifically here: this deletes
	// data, and an operator who has set a threshold too aggressively must be
	// able to turn it off immediately rather than wait for a restart window.
	c.StallPrune = StallPruneConfig{}
	// Read live on each grab, so a gate refusing too much can be turned off
	// without a restart. That matters here for the same reason it matters for
	// stall pruning: this one deletes the transfer it refuses.
	c.SeederGate = SeederGateConfig{}
	c.SkipPreCache = false
	c.SkipMultiSeason = false
	c.AlwaysRmTrackerUrls = false
	c.Categories = nil
	c.FolderNaming = ""
	c.CustomFolders = nil
	c.DefaultDownloadAction = ""
	c.RefreshDirs = ""
	c.Retries = 0
	c.SkipAutoMove = false
	c.Repair = RepairConfig{}

	// Every read-ceiling knob is resolved live, per call, on the path that
	// enforces it (Manager.debridLinkTimeout, Handler.metadataReadTimeout,
	// RealDebrid.statusPollCeiling). Nothing caches them in a service struct, so
	// they apply without a restart. That matters HERE in particular: these are
	// the knobs an operator reaches for while a backend is actively flapping, and
	// telling them to restart mid-incident would take the mount down to fix the
	// mount.
	c.DebridLinkTimeout = ""
	c.DebridStatusTimeout = ""
	c.MetadataReadTimeout = ""

	// Queue cleanup rules are read live via config.Get() inside CleanupQueue,
	// so changes apply on the next cleanup cycle without a restart.
	c.QueueCleanup = QueueCleanup{}

	// Deprecated, migrated into Manager fields above.
	c.QBitTorrent = QBitTorrent{}

	// Usenet is mostly cold (providers, connection pool sizing, socket buffers,
	// and the streaming buffer pool are all established at startup). But the
	// availability sampling percentages are read live on each repair/import
	// check (see Usenet.CheckFile / checkNZBAvailability), so they apply without
	// a restart. Everything else in Usenet stays restart-required.
	c.Usenet.AvailabilitySamplePercent = 0
	c.Usenet.ImportAvailabilitySamplePercent = 0
}

// RequiresRestart reports whether applying n on top of c needs a full service
// restart (re-binding the HTTP listener, recreating debrid/usenet clients, or
// re-mounting the filesystem). It returns false when only runtime-applicable
// ("hot") fields changed, in which case the caller can use ApplyRuntime to
// update the live config in place without tearing anything down.
//
// Both configs are compared after their defaults have been applied (see
// setDefaults / Save), so callers should persist n before calling this.
func (c *Config) RequiresRestart(n *Config) bool {
	a, b := *c, *n
	clearHotFields(&a)
	clearHotFields(&b)
	return !reflect.DeepEqual(a, b)
}

// ApplyRuntime copies n into the live config in place, preserving the in-memory
// Auth pointer. Because every holder of the *Config singleton shares this
// struct, the updated values become visible everywhere without a restart.
//
// Only call this when RequiresRestart(n) is false: the cold fields are then
// identical between c and n, so this effectively updates just the hot fields.
func (c *Config) ApplyRuntime(n *Config) {
	runtimeMu.Lock()
	defer runtimeMu.Unlock()
	auth := c.Auth
	*c = *n
	c.Auth = auth
}

// SnapshotQueueCleanup returns an immutable copy suitable for a complete
// cleanup sweep while ApplyRuntime may replace the live configuration.
func (c *Config) SnapshotQueueCleanup() QueueCleanup {
	runtimeMu.RLock()
	defer runtimeMu.RUnlock()
	policy := c.QueueCleanup
	policy.Rules = append([]QueueCleanupRule(nil), c.QueueCleanup.Rules...)
	return policy
}

func (c *Config) createConfig() error {
	// Create the directory if it doesn't exist
	if err := os.MkdirAll(GetMainPath(), 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}
	c.URLBase = "/"
	c.Port = DefaultPort
	c.LogLevel = DefaultLogLevel
	c.UseAuth = true
	return nil
}

func (c *Config) SetupComplete() error {
	return c.Validate()
}

func (c *Config) SetupError() string {
	if err := c.Validate(); err != nil {
		return err.Error()
	}
	return ""
}

package manager

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/manager/link"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
	"github.com/sirrobot01/decypharr/pkg/version"
	"golang.org/x/sync/singleflight"
)

// Manager handles unified torrent management - replaces wire.Store completely
type Manager struct {
	storage      *storage.Storage
	migrator     *Migrator
	repair       *Repair
	clients      *xsync.Map[string, debrid.Client]
	// fillCache memoizes per-provider stored-item counts, used to tell
	// AllDebrid's transient daily allowance apart from its permanent
	// stored-item cap. Both raise the same error code.
	fillCache    *providerFillCache
	// capacityHold holds grabs accepted at add time that no provider had room
	// for yet. Drained on slot-free events and by the per-provider admission
	// controller — never by a per-entry timer.
	capacityHold *capacityHoldQueue
	arr          *arr.Storage
	logger       zerolog.Logger
	ready        chan struct{}
	readyOnce    sync.Once
	// restoreDone is closed when the backgrounded boot restore finishes. See
	// WaitForRestore for why callers that seed entries right after construction
	// need it.
	restoreDone     chan struct{}
	restoreDoneOnce sync.Once
	streamClient *http.Client

	// Migration jobs tracking
	migrationJobs   *xsync.Map[string, *storage.SwitcherJob]
	refreshInterval time.Duration

	config *config.Config

	// Processing workers
	scheduler    gocron.Scheduler
	cetScheduler gocron.Scheduler
	queue        *Queue

	// downloading
	refreshSG   singleflight.Group
	linkService *link.Service

	// repair
	fixer *Fixer
	ctx   context.Context

	customFolders *CustomFolders
	mountManager  MountManager

	startTime     time.Time
	usenetTimeout time.Duration

	rootInfo            *FileInfo
	entry               *EntryCache
	downloader          *Downloader
	usenet              *usenet.Usenet
	copyEntryMu         sync.Mutex
	copyEntryTestHook   func(stage string)
	deleteEntryTestHook func(stage string)

	// Debrid speed test results storage
	debridSpeedTestResults *xsync.Map[string, debridTypes.SpeedTestResult]

	// Active streams tracking
	activeStreams *xsync.Map[string, *ActiveStream]

	// In-flight queue-processor dispatches, keyed by InfoHash, to prevent
	// duplicate goroutines from processing the same entry when the scheduler
	// re-fires before the previous pass has updated the queue row.
	processingEntries *xsync.Map[string, struct{}]

	// Unified active-download queue for torrent and NZB imports.
	jobQueue       *JobQueue
	nzbSyncMu      sync.Mutex
	nzbAdmissionMu sync.Mutex

	// processSem bounds concurrent heavy NZB Process/availability passes so
	// their combined connection demand never oversubscribes the provider pool.
	// Sized max(1, floor(providerPoolSize / processing_max_connections)) so
	// concurrentProcess x processing_max_connections <= poolSize. This decouples
	// Process concurrency from MaxActiveDownloads: a Process call therefore
	// always gets enough connections to finish within processing_timeout instead
	// of timing out on a starved pool and re-parsing forever. nil when usenet is
	// not configured (no Process calls happen).
	processSem chan struct{}
	// processGateObserver, when set, is invoked with +1 immediately after a
	// Process call is admitted past processSem and -1 when it releases, so tests
	// can assert the gate never admits more than its configured width.
	processGateObserver func(delta int)

	// revivalDoomWarned dedups the once-per-entry-per-boot WARN emitted when
	// the revival sweep skips an entry whose queued rebuild is guaranteed to
	// fail (no parsed segments in metadata and no NZB source on disk).
	revivalDoomWarned sync.Map

	// actionSem bounds concurrently running post-download actions (mount
	// waits, local fetches, symlink creation) — real local I/O, and
	// deliberately NOT scaled with the job-slot ceiling. Measured over the
	// heaviest load this stack has seen: 3,958 symlink creations, 97/min peak,
	// 3 failures and zero mount faults at width 14. There is no evidence
	// arguing for more, and scaling it with MaxConcurrentJobs would turn a
	// queue fix into a mount storm. Sized max(4, MaxActiveDownloads)
	// so a reboot backlog of claimed actions drains progressively instead of
	// stampeding a cold mount and the persist mutex, while never dropping
	// below the worker count so claimed work cannot starve behind imports.
	actionSem chan struct{}
	// actionInflight tracks hashes with a pending/running post-download action
	// in this process (claimed and waiting on the gate, or executing). It
	// prevents duplicate submissions - and later the orphaned-claim
	// reconciler - from double-running a claimed action whose goroutine is
	// still alive.
	actionInflight *xsync.Map[string, struct{}]
	// claimedActionTestHook, when set, replaces the real post-download action
	// body so tests can observe gate concurrency without touching mounts.
	claimedActionTestHook func(*storage.Entry)
	// restoreCanaryTestHook, when set, replaces the restore circuit-breaker's
	// NNTP canary probe so tests can drive pause/resume deterministically.
	restoreCanaryTestHook func(ctx context.Context) error

	// Notifications service
	Notifications *notifications.Service
}

// New creates a new Manager instance
func New() *Manager {
	cfg := config.Get()
	_logger := logger.New("manager")

	strg, err := storage.NewStorage(filepath.Join(config.GetMainPath(), "db"))
	if err != nil {
		panic(fmt.Errorf("failed to create manager storage: %w", err))
	}

	// Initialize debrid registry
	ctx := context.Background()

	// Optimized transport for high-performance streaming with HTTP/2 multiplexing
	// DNS resolver with caching
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,  // Fast connection timeout
		KeepAlive: 30 * time.Second, // Keep connections alive
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
			ClientSessionCache: tls.NewLRUClientSessionCache(200),
		},
		TLSHandshakeTimeout:    20 * time.Second,
		MaxIdleConns:           1000,
		MaxIdleConnsPerHost:    500,
		MaxConnsPerHost:        500,
		IdleConnTimeout:        120 * time.Second,
		DisableCompression:     false, // Enable compression for better multiplexing
		DialContext:            dialer.DialContext,
		Proxy:                  http.ProxyFromEnvironment,
		MaxResponseHeaderBytes: 1 << 20,  // 1MB header buffer for CDN responses
		WriteBufferSize:        32 << 10, // 32KB write buffer
		ReadBufferSize:         32 << 10, // 32KB read buffer
	}

	streamClient := &http.Client{
		Timeout:   0,
		Transport: transport,
	}

	usenetTimeout, err := utils.ParseDuration(cfg.Usenet.ProcessingTimeout)
	if err != nil {
		usenetTimeout = 10 * time.Minute
	}

	instance := &Manager{
		storage:                strg,
		clients:                xsync.NewMap[string, debrid.Client](),
		fillCache:              newProviderFillCache(),
		capacityHold:           newCapacityHoldQueue(),
		logger:                 _logger,
		migrationJobs:          xsync.NewMap[string, *storage.SwitcherJob](),
		config:                 cfg,
		arr:                    arr.NewStorage(),
		queue:                  newQueue(strg, cfg.RemoveStalledAfter),
		ctx:                    ctx,
		ready:                  make(chan struct{}),
		restoreDone:            make(chan struct{}),
		streamClient:           streamClient,
		usenetTimeout:          usenetTimeout,
		debridSpeedTestResults: xsync.NewMap[string, debridTypes.SpeedTestResult](),
		activeStreams:          xsync.NewMap[string, *ActiveStream](),
		processingEntries:      xsync.NewMap[string, struct{}](),
	}

	instance.init()

	// Create migrator
	return instance
}

func (m *Manager) init() {
	cfg := config.Get()
	scheduler, err := gocron.NewScheduler(gocron.WithLocation(time.Local), gocron.WithGlobalJobOptions(gocron.WithTags("decypharr-manager")))
	if err != nil {
		scheduler, _ = gocron.NewScheduler(gocron.WithGlobalJobOptions(gocron.WithTags("decypharr-manager")))
	}

	// Create CET scheduler for time-specific jobs
	cetLocation, err := time.LoadLocation("CET")
	if err != nil {
		cetLocation = time.UTC
	}
	cetScheduler, err := gocron.NewScheduler(gocron.WithLocation(cetLocation), gocron.WithGlobalJobOptions(gocron.WithTags("decypharr-cet")))
	if err != nil {
		cetScheduler, _ = gocron.NewScheduler(gocron.WithGlobalJobOptions(gocron.WithTags("decypharr-cet")))
	}

	m.config = cfg

	// Recreate queue with new config
	m.queue = newQueue(m.storage, cfg.RemoveStalledAfter)

	// Clear debrid clients so they get recreated with new config
	m.clients = xsync.NewMap[string, debrid.Client]()

	// Reset ready channel and syncTorrents.Once for the next start
	m.ready = make(chan struct{})
	m.readyOnce = sync.Once{}

	m.scheduler = scheduler
	m.cetScheduler = cetScheduler
	m.migrator = NewMigrator(m.storage)
	m.downloader = NewDownloadManager(m)

	// Initialize HTTP pool for streaming
	// Note: We can't create a single pool for all files because the LinkRefresh callback
	// needs torrent+filename context. Instead, manager.Stream will create a pool per request
	// and cache it. This is actually better because different files may have different
	// download links from different CDNs.

	refreshInterval, err := utils.ParseDuration(cfg.RefreshInterval)
	if err != nil {
		refreshInterval = 15 * time.Minute
	}
	m.refreshInterval = refreshInterval

	// initialize debrid clients
	m.initDebridClients()

	// Initialize usenet client
	m.initUsenet()

	// Initialize link service
	m.initLinkService()

	// Init custom folders
	m.initCustomFolders()

	// Initialize fixer
	m.fixer = NewFixer(m)

	// Set mount paths
	m.setMountPaths()

	m.initEntryCache()

	// Initialize notifications service
	m.Notifications = notifications.New(&m.config.Notifications, m.logger)

	// Initialize repair service. It registers with the scheduler in StartWorker.
	m.repair = NewRepair(m)

	// Initialize the post-download action gate before the job queue: restored
	// ResumeAction jobs are routed through it as soon as workers start.
	m.actionSem = make(chan struct{}, actionGateSize(cfg.MaxActiveDownloads))
	m.actionInflight = xsync.NewMap[string, struct{}]()

	// Initialize the unified active-download queue after all processors exist.
	m.initJobQueue()
}

func (m *Manager) initUsenet() {
	usenetClient, err := usenet.New()
	if err != nil {
		m.logger.Warn().Msg("Usenet client not configured")
		m.usenet = nil
		m.processSem = nil
		return
	}
	m.usenet = usenetClient
	// Fit concurrent heavy Process/availability passes to the provider pool so
	// they cannot oversubscribe it and time out en masse (the substrate-wedge
	// mechanism behind the livelock). Sized from the pool, NOT from
	// MaxActiveDownloads, so the worker/action count stays independent.
	gate := m.config.UsenetProcessConcurrency()
	m.processSem = make(chan struct{}, gate)
	m.logger.Info().
		Int("process_concurrency", gate).
		Int("provider_pool", m.config.UsenetProviderConnectionTotal()).
		Int("processing_max_connections", m.config.Usenet.ProcessingMaxConnections).
		Msg("Bounded concurrent NZB processing to the provider connection pool")
	// Guardrail only — warn (never fail) when parallel imports can exhaust
	// the configured provider connection budget. With the Process gate above
	// this can no longer wedge the substrate; the warning remains as advice.
	if warning := m.config.UsenetConnectionBudgetWarning(); warning != "" {
		m.logger.Warn().Msg(warning)
	}
}

// initLinkService initializes the link service
func (m *Manager) initLinkService() {
	m.linkService = link.New(
		m.clients,
		m.refreshTorrent,
		m.ReinsertEntry,
		m.persistLinkEntryBad,
		m.streamClient,
		m.config.Retries,
		logger.New("link"),
	)
}

func (m *Manager) initJobQueue() {
	if err := m.cleanupOrphanedStagedNZBs(); err != nil {
		m.logger.Warn().Err(err).Msg("Failed to reconcile orphaned staged NZBs")
	}
	// Sized by the machine ceiling, NOT by max_active_downloads. This pool is
	// job slots; a short NZB import must not queue behind an uncached torrent
	// that holds a worker for the whole download. Provider capacity is enforced
	// per provider at the debrid call site, and local I/O by actionSem.
	m.jobQueue = NewJobQueue(m.ctx, m.config.MaxConcurrentJobs, m.processJob)
	// Restore persisted active/queued downloads in the background. With large
	// queues this re-parses thousands of NZBs over the network, and running it
	// inline blocked manager construction — and therefore the HTTP server —
	// for 60-90 minutes on big libraries, during which every arr reported
	// "download client unavailable". Backgrounding lets the API serve and the
	// worker pool drain immediately while the restore catches up.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				m.logger.Error().Interface("panic", r).Msg("Recovered from panic while restoring active downloads")
			}
			// Signals completion whether the restore finished or panicked, so a
			// waiter can never be blocked forever by a crash inside it.
			m.restoreDoneOnce.Do(func() { close(m.restoreDone) })
		}()
		m.restoreActiveDownloadJobs()
	}()
}

// WaitForRestore blocks until the background boot restore has finished, or ctx
// ends.
//
// It exists because that restore runs in a goroutine started during
// construction, so ANYTHING that adds entries immediately after New() races it:
// the restore lists the queue at its own moment, and an entry created a
// microsecond earlier is swept into a boot pass that then revives or rebuilds
// it. That is invisible in production, where nothing seeds an entry and asserts
// its state in the same breath, and it made TestRetryRevivesFailedEntryAndClears-
// FailedHistory fail intermittently on slower CI runners — a flaky gate that
// invites re-running until green, which is exactly how a real failure gets
// waved through as "probably the flaky one".
func (m *Manager) WaitForRestore(ctx context.Context) error {
	if m.restoreDone == nil {
		return nil
	}
	select {
	case <-m.restoreDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) cleanupOrphanedStagedNZBs() error {
	if m.usenet == nil || m.storage == nil {
		return nil
	}

	// Keep StageNZBForGeneration -> Queue.Add atomic with respect to the scan.
	// This matters during Reset as well as initial construction: a newly staged
	// file must either be absent from the directory snapshot or already have a
	// durable Magnet reference before cleanup can consider it orphaned.
	m.nzbAdmissionMu.Lock()
	defer m.nzbAdmissionMu.Unlock()

	entries, err := m.storage.FilterQueued(func(entry *storage.Entry) bool {
		return entry != nil && entry.IsNZB() && entry.Magnet != ""
	})
	if err != nil {
		return fmt.Errorf("load live staged NZB paths: %w", err)
	}
	livePaths := make([]string, 0, len(entries))
	for _, entry := range entries {
		livePaths = append(livePaths, entry.Magnet)
	}
	removed, err := m.usenet.CleanupOrphanedStagedNZBs(livePaths)
	if removed > 0 {
		m.logger.Info().Int("removed", removed).Msg("Removed orphaned staged NZBs")
	}
	return err
}

func (m *Manager) processJob(ctx context.Context, job *Job) {
	if job == nil {
		return
	}
	if job.ResumeAction {
		// Never run the claimed action on this worker: submit it through the
		// action gate on a detached goroutine so a restore backlog of claimed
		// entries cannot occupy every active-download slot. The claim is
		// already durable, so the slot frees immediately and the action drains
		// at gate width.
		m.submitResumeAction(job.Entry)
		return
	}
	if job.Entry != nil && job.Request == nil && job.DebridTorrent == nil && job.NZBMeta == nil && !job.ResumeExisting && !job.RebuildQueued {
		// A bare Entry job with no rebuild request only waits for an in-flight
		// download to finish. A RebuildQueued job must NOT land here: it carries
		// no NZBMeta yet precisely because it needs to re-parse from its staged
		// source, so it has to reach processNZBJob. Without this guard the
		// infra-retry and revival/park-sweep re-feeds (which submit
		// RebuildQueued jobs with a nil NZBMeta) would silently park a worker in
		// waitForDownloadCompletion instead of rebuilding.
		m.waitForDownloadCompletion(ctx, job.Entry)
		return
	}

	var err error
	switch job.Type {
	case JobTypeTorrent:
		err = m.processTorrentJob(ctx, job)
	case JobTypeNZB:
		err = m.processNZBJob(ctx, job)
	default:
		err = fmt.Errorf("unknown job type: %s", job.Type)
	}

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		// Provider capacity. Both cadences put the entry back to Queued — so it
		// reports queuedDL/Queued rather than pretending to download — and hand
		// the job back to the queue instead of holding a worker.
		//
		// The two delays are deliberately different and must stay that way.
		// Slot exhaustion clears as our OWN active work finishes, so a short
		// retry is right. An add/storage allowance does not: AllDebrid's
		// MAGNET_TOO_MANY was observed firing 6,715 times with ZERO active
		// magnets, meaning nothing we finish releases it. Retrying that every
		// 30s is a spin against a provider already saying no.
		// Provider capacity failures are NOT retried. They fall through to the
		// ordinary error path, which surfaces the failure to the *arr.
		//
		// Both cadences were deleted rather than re-tuned. The question nobody
		// had asked was whether to retry at ALL, and the answer is no: the
		// *arr's own re-search cycle is the retry, it is better informed than
		// we are about which release to try next, and it costs us no held
		// state and no invented interval.
		//
		// Failing is free, which is the measurement that settles it. On a live
		// Sonarr, 8,243 blocklisted entries contain exactly FOUR torrents,
		// against many thousands of add rejections across 12+ days — a failed
		// ADD is treated as a download-client problem, not a release problem.
		// Nothing is blocklisted, no candidate list is burned.
		//
		// The quota case is the clearest: it was observed refusing every add
		// for 54.6 continuous hours because the provider's stored-item cap was
		// full. No cadence recovers from that; only deleting entries on the
		// provider does. Holding a job for it helped nobody and hid it.
		// Same classification as the synchronous add path, and it must stay the
		// same: a held entry re-attempted here has to reach the identical
		// verdict, or an entry accepted at grab time would be failed on its
		// first retry.
		if refusal := m.classifyAddRefusal(err); refusal.hold {
			if job.Entry != nil {
				job.Entry.Status = debridTypes.TorrentStatusQueued
				if updateErr := m.queue.Update(job.Entry); updateErr != nil {
					m.logger.Debug().Err(updateErr).Str("job_id", job.ID).Msg("Failed to persist capacity hold")
				}
			}
			m.logger.Info().
				Str("job_id", job.ID).
				Str("provider", refusal.provider).
				Msgf("Still holding until a provider slot frees: %s", refusal.detail)
			m.holdForCapacity(job)
			return
		} else if refusal.standingCondition != "" {
			// LOUD and operator-addressed. This is the one capacity condition
			// no amount of waiting resolves, so it must not read like the
			// transient ones above.
			m.logger.Error().
				Str("job_id", job.ID).
				Str("provider", refusal.provider).
				Msg(refusal.standingCondition)
		}
		if errors.Is(err, parser.ErrProbeInfrastructure) {
			// The availability probe failed on the NNTP substrate, not on the
			// content — there is no verdict about the articles. Keep the entry
			// queued and retry (capped) instead of surfacing a Failed history
			// entry for a possibly healthy release. deferInfraRetry bounds the
			// fast retry loop: after nzbInfraFastRetryCap cumulative attempts the
			// entry is parked for the slow revival sweep so it cannot pin the
			// worker pool with unbounded re-parses.
			m.logger.Warn().
				Err(err).
				Str("job_id", job.ID).
				Msg("NZB availability probe hit an infrastructure failure; deferring retry")
			if job.Entry != nil {
				if deferErr := m.deferInfraRetry(job.Entry); deferErr != nil {
					m.logger.Debug().Err(deferErr).Str("job_id", job.ID).Msg("Failed to record NZB infrastructure retry")
				}
			}
			return
		}
		m.logger.Error().Err(err).Str("job_id", job.ID).Str("type", string(job.Type)).Msg("Active download failed")
		if job.Entry != nil {
			job.Entry.MarkAsError(err)
			_ = m.queue.Update(job.Entry)
		}
		return
	}

	m.waitForDownloadCompletion(ctx, job.Entry)
}

func (m *Manager) resumeClaimedAction(entry *storage.Entry) {
	if entry == nil {
		return
	}
	current, err := m.queue.RefreshSnapshot(entry)
	if err != nil || !current {
		if err != nil {
			m.logger.Warn().Err(err).Str("infohash", entry.InfoHash).Msg("Failed to refresh claimed action during restore")
		}
		return
	}
	if entry.State != storage.EntryStateDownloading || !entry.IsDownloading || entry.Status != debridTypes.TorrentStatusDownloaded {
		return
	}
	m.runClaimedAction(entry)
}

// downloadCompletionSlack pads the worst-case post-download pipeline (mount
// visibility wait + usenet processing) when computing the defensive park cap
// for waitForDownloadCompletion.
const downloadCompletionSlack = 5 * time.Minute

// downloadCompletionParkCap bounds how long a single worker may stay parked on
// one entry. It covers the longest legitimate pipeline (the mount wait plus
// the usenet processing timeout) with slack; anything beyond that means the
// entry's lifecycle has wedged and the slot is worth more than the wait.
func (m *Manager) downloadCompletionParkCap() time.Duration {
	return symlinkMountWaitTimeout + m.usenetTimeout + downloadCompletionSlack
}

// waitForDownloadCompletion parks a worker slot only while the entry still
// represents in-flight provider/import work. It returns as soon as the entry
// leaves the downloading state, the job is cancelled, or the post-download
// action has been durably claimed (Status downloaded + IsDownloading): from
// that point the detached action goroutine owns the entry lifecycle, and
// keeping the slot parked would serialize slow mount-visibility waits behind
// real download work (MaxActiveDownloads workers x mount refresh interval).
func (m *Manager) waitForDownloadCompletion(ctx context.Context, entry *storage.Entry) {
	if entry == nil {
		return
	}
	// However this returns — completed, failed, or capped — this entry has
	// stopped occupying a provider download slot, which is precisely the event
	// a held entry is waiting for. Admitting one here is why the hold needs no
	// polling: the slot's owner tells us it is done.
	defer m.releaseHeldEntryOnSlotFree()
	// Wait on a private snapshot. Detached post-download action goroutines and
	// restore paths can retain the original pointer; refreshing a shared
	// pointer from this loop would race with their own snapshot refreshes.
	snapshot := *entry
	maxPark := m.downloadCompletionParkCap()
	capTimer := time.NewTimer(maxPark)
	defer capTimer.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		current, err := m.queue.RefreshSnapshot(&snapshot)
		if err != nil || !current || snapshot.State != storage.EntryStateDownloading {
			return
		}
		if snapshot.Status == debridTypes.TorrentStatusDownloaded && snapshot.IsDownloading {
			// The post-download action is durably claimed; the action owns the
			// entry from here on and the worker slot must free.
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-capTimer.C:
			// Defensive: never let one wedged entry pin a worker forever. The
			// slot frees; whatever state remains is picked up by the periodic
			// scheduler or the orphaned-claim reconciler.
			m.logger.Error().
				Str("infohash", snapshot.InfoHash).
				Str("name", snapshot.Name).
				Str("state", string(snapshot.State)).
				Str("status", string(snapshot.Status)).
				Dur("waited", maxPark).
				Msg("Download completion wait exceeded its cap; releasing the worker slot")
			return
		case <-ticker.C:
		}
	}
}

func (m *Manager) migrate() {
	// Check if migration has already been done
	status, err := m.migrator.GetStatus()
	if err == nil && !status.Running && status.Completed > 0 {
		m.logger.Info().
			Int("completed", status.Completed).
			Int("errors", status.Errors).
			Msg("Migration already completed previously")
		return
	}

	// GetReader migration stats to see if there are cache files
	stats, err := m.migrator.GetStats()
	if err != nil {
		m.logger.Warn().Err(err).Msg("Failed to get migration stats")
		return
	}

	cacheFiles, ok := stats["cache_files"].(int)
	if !ok || cacheFiles == 0 {
		return
	}

	cacheTorrents, ok := stats["cache_torrents"].(int)
	if !ok {
		cacheTorrents = 0
	}

	m.logger.Info().
		Int("cache_files", cacheFiles).
		Int("unique_torrents", cacheTorrents).
		Msg("Found cache files, starting automatic migration...")

	// Start migration with backup
	if err := m.migrator.Start(); err != nil {
		m.logger.Error().Err(err).Msg("Failed to start automatic migration")
		return
	}

	m.logger.Info().Msg("Automatic migration started successfully")
}

// Start starts the manager and all its components
func (m *Manager) Start(ctx context.Context) error {
	m.startTime = time.Now()
	m.logger.Info().
		Str("version", version.GetInfo().String()).
		Str("mount_type", string(m.config.Mount.Type)).
		Str("notifications", fmt.Sprintf("%v", m.Notifications.IsEnabled())).
		Str("mount_path", m.config.Mount.MountPath).
		Msg("Starting manager")

	// run the migration process
	m.migrate()

	go func() {
		m.syncTorrents(ctx)
		// Sync NZBs
		if err := m.syncNZBs(ctx); err != nil {
			m.logger.Error().Err(err).Msg("Failed to perform initial NZB syncTorrents")
		}
		if fixNZB := os.Getenv("DECYPHARR_FIX_NZB_SIZES"); fixNZB == "1" {
			m.logger.Info().Msg("Starting NZB file size correction as requested by environment variable")
			m.fixNZBFileSizes(ctx)
		}
	}()

	// Watch for the queue index and a full scan disagreeing. The condition is
	// erased by a restart, so it can only be observed from inside the running
	// process.
	go m.watchQueueConsistency(ctx)

	// Start workers
	if err := m.StartWorker(ctx); err != nil {
		return fmt.Errorf("failed to start manager worker: %w", err)
	}

	// Close ready channel once, safe for multiple calls
	m.readyOnce.Do(func() {
		close(m.ready)
	})

	// Start the mount manager if set
	// This also start thr mounting process
	if m.mountManager != nil {
		if err := m.mountManager.Start(ctx); err != nil {
			// If mount manager fails to start, we log the error but continue running the manager
			m.logger.Error().Err(err).Msg("Failed to start mount manager, continuing without mounting")
			return nil
		}
	}

	return nil
}

// Stop stops the manager and cleans up all resources
func (m *Manager) Stop() error {
	m.logger.Info().Msg("Stopping manager")

	// Stop mount manager first
	if m.mountManager != nil {
		m.logger.Info().Msg("Stopping mount manager")
		if err := m.mountManager.Stop(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to stop mount manager")
		}
	}

	// Stop schedulers
	if m.scheduler != nil {
		if err := m.scheduler.Shutdown(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to shutdown scheduler")
		}
	}
	if m.cetScheduler != nil {
		if err := m.cetScheduler.Shutdown(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to shutdown CET scheduler")
		}
	}

	if m.jobQueue != nil {
		m.logger.Info().Msg("Closing active download queue")
		m.jobQueue.Close()
	}

	// Close usenet connection manager if active
	if m.usenet != nil {
		m.logger.Info().Msg("Closing usenet connections")
		if err := m.usenet.Close(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to close usenet")
		}
	}

	if m.repair != nil {
		m.repair.Stop()
	}

	// Close storage
	if m.storage != nil {
		m.logger.Info().Msg("Closing storage database")
		if err := m.storage.Close(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to close storage")
		}
	}

	m.logger.Info().Msg("Manager stopped successfully")
	return nil
}

// Reset resets the manager with the new configuration
// This is called after config changes (e.g., setup wizard) to apply new settings
func (m *Manager) Reset() error {
	m.logger.Info().Msg("Resetting manager with new configuration")

	// Stop resources before resetting
	if err := m.Stop(); err != nil {
		m.logger.Warn().Err(err).Msg("Failed to stop manager during reset")
	}

	// Reopen storage database (it was closed by Stop)
	strg, err := storage.NewStorage(filepath.Join(config.GetMainPath(), "db"))
	if err != nil {
		return fmt.Errorf("failed to reopen storage after reset: %w", err)
	}
	m.storage = strg

	// Reload configuration
	m.init()
	m.logger.Info().Msg("Manager reset complete")
	return nil
}

func (m *Manager) GetStats() (map[string]any, error) {
	count, err := m.storage.Count()
	if err != nil {
		return nil, err
	}

	diskSize := m.storage.DiskSize()
	activeJobs := 0
	completedJobs := 0
	failedJobs := 0
	m.migrationJobs.Range(func(_ string, job *storage.SwitcherJob) bool {
		switch job.Status {
		case storage.SwitcherStatusPending, storage.SwitcherStatusInProgress:
			activeJobs++
		case storage.SwitcherStatusCompleted:
			completedJobs++
		case storage.SwitcherStatusFailed, storage.SwitcherStatusCancelled:
			failedJobs++
		}
		return true
	})

	return map[string]any{
		"total_torrents": count,
		"storage_stats":  map[string]any{"total_size": diskSize},
		"active_jobs":    activeJobs,
		"completed_jobs": completedJobs,
		"failed_jobs":    failedJobs,
	}, nil
}

func (m *Manager) IsReady() chan struct{} {
	return m.ready
}

func (m *Manager) Uptime() time.Duration {
	return time.Since(m.startTime)
}

func (m *Manager) StartTime() time.Time {
	return m.startTime
}

// CRUD operations

func (m *Manager) GetEntryItem(torrentName string) (*storage.EntryItem, error) {
	return m.storage.GetEntryItem(torrentName)
}

func (m *Manager) GetEntryByName(torrentName, filename string) (*storage.Entry, error) {
	// First get entry
	entry, err := m.storage.GetEntryItem(torrentName)
	if err != nil {
		return nil, err
	}

	// Find the file in the entry
	file, err := entry.GetFile(filename)
	if err != nil {
		return nil, err
	}
	return m.GetEntry(file.InfoHash)
}

func (m *Manager) AddOrUpdate(entry *storage.Entry, callback func(t *storage.Entry)) error {
	entry.UpdatedAt = time.Now()
	if err := m.storage.AddOrUpdate(entry); err != nil {
		return err
	}
	if callback != nil {
		go callback(entry)
	}
	return nil
}

// GetEntry gets a torrent by name
func (m *Manager) GetEntry(infohash string) (*storage.Entry, error) {
	return m.storage.Get(infohash)
}

func (m *Manager) EntryExists(infohash string) (bool, error) {
	return m.storage.Exists(infohash)
}

func (m *Manager) GetTorrents(filter func(*storage.Entry) bool) ([]*storage.Entry, error) {
	// Use streaming to avoid loading all torrents into memory at once
	var torrents []*storage.Entry
	err := m.storage.ForEach(func(t *storage.Entry) error {
		if filter == nil || filter(t) {
			torrents = append(torrents, t)
		}
		return nil
	})
	return torrents, err
}

func (m *Manager) GetTorrentsCount() (int, error) {
	return m.storage.Count()
}

// DeleteEntry deletes a torrent by infohash
func (m *Manager) DeleteEntry(infohash string, removePlacements bool) error {
	expected, err := m.GetEntry(infohash)
	if err != nil {
		return err
	}
	m.runDeleteEntryTestHook("snapshot-loaded")
	var cleanup func(*storage.Entry) error
	if removePlacements {
		cleanup = m.removeTorrentPlacementsLocked
	}
	loadCurrent := func() (*storage.Entry, error) {
		current, loadErr := m.GetEntry(infohash)
		if loadErr != nil {
			return nil, loadErr
		}
		if !storage.SameMainGeneration(expected, current) {
			return nil, fmt.Errorf("entry %s was replaced before deletion", infohash)
		}
		return current, nil
	}
	validateMain := func() error {
		_, validateErr := loadCurrent()
		return validateErr
	}
	deleteMain := func(_ *storage.Entry) error {
		m.runDeleteEntryTestHook("before-copy-lock")
		m.copyEntryMu.Lock()
		defer m.copyEntryMu.Unlock()
		current, loadErr := loadCurrent()
		if loadErr != nil {
			return loadErr
		}
		deleted, deleteErr := m.storage.DeleteIfCurrentWithCleanup(current, cleanup)
		if deleteErr != nil {
			return deleteErr
		}
		if !deleted {
			return fmt.Errorf("entry %s changed before deletion", infohash)
		}
		return nil
	}
	if m.queue != nil {
		if err := m.queue.withDeletionBarrier(infohash, validateMain, deleteMain); err != nil {
			return err
		}
	} else if err := deleteMain(nil); err != nil {
		return err
	}
	// Refresh entry cache
	if m.entry != nil {
		m.RefreshEntries(true)
	}
	return nil
}

func (m *Manager) runDeleteEntryTestHook(stage string) {
	if m.deleteEntryTestHook != nil {
		m.deleteEntryTestHook(stage)
	}
}

func (m *Manager) DeleteTorrents(infohashes []string, removeFromDebrid bool) error {
	for _, infohash := range infohashes {
		if err := m.DeleteEntry(infohash, removeFromDebrid); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) GetMigrationJob(jobID string) (*storage.SwitcherJob, error) {
	job, exists := m.migrationJobs.Load(jobID)
	if !exists {
		return nil, fmt.Errorf("migration job not found: %s", jobID)
	}
	return job, nil
}

// SubmitJob submits an import to the unified active-download queue.
func (m *Manager) SubmitJob(job *Job) error {
	if m.jobQueue == nil {
		return fmt.Errorf("active download queue not initialized")
	}
	return m.jobQueue.Submit(job)
}

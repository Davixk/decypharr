package link

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"golang.org/x/sync/singleflight"
)

const (
	MaxReinsertionAttempt = 3

	// backgroundResolveBudget bounds a resolution that has outlived the caller
	// that started it. Such a resolution is deliberately NOT cancelled — see
	// GetLink — but it still needs a ceiling or a wedged provider leaks one
	// goroutine (and one singleflight slot) per file forever.
	//
	// 5 minutes because that is already the ceiling Fixer applies to an
	// in-flight repair (pkg/manager/fixer.go): past it the repair this
	// resolution may be waiting on has itself given up, so anything still
	// blocked here is a leak, not work.
	backgroundResolveBudget = 5 * time.Minute
)

var (
	emptyDownloadLink = types.DownloadLink{}
)

// EntryRefresher refreshes the exact lifecycle represented by the supplied
// snapshot. Implementations must reject a delete/re-add replacement rather
// than applying an old provider response to it.
type EntryRefresher func(expected *storage.Entry) (*storage.Entry, error)
type EntryRepairer func(ctx context.Context, entry *storage.Entry) error
type EntrySaver func(entry *storage.Entry) error

// EntryReacquirer re-inserts an entry on providers OTHER than the one it is
// currently active on.
//
// It exists for exactly one caller: a confirmed legal takedown. Re-submitting
// the same magnet to the provider that just answered "this release has been
// removed for legal reasons" cannot possibly succeed, and doing it on every read
// is how a takedown turns into a permanent re-insertion storm against the
// provider. The other configured debrids, though, may well still serve the
// content — a takedown is provider-scoped — and asking them once is the
// difference between correctly parking dead content and destroying content that
// was fine somewhere else.
type EntryReacquirer func(ctx context.Context, entry *storage.Entry) error

// EntryPruner removes an entry decypharr-side. It is the SAME operation the
// repair sweep's PRUNE component performs, wired here so a confirmed takedown
// reaches it without waiting for a sweep to come round.
//
// It makes no arr calls, by construction. decypharr is the download client: it
// stops listing the content, the library symlink dangles, and the arr discovers
// that for itself on its next disk scan. That is the whole mechanism, and
// reaching into an arr API to hurry it along is not part of it.
type EntryPruner func(entry *storage.Entry) error

// Service handles download link fetching and validation.
// It uses the account-level cache for storing links and only tracks validation state.
// ResolveCeiling reports how long a caller may be held waiting for a download
// link, or <= 0 to disable the ceiling. It is a function, not a value, so the
// knob stays hot: an operator can shorten it while a provider is flapping
// without restarting the mount.
type ResolveCeiling func() time.Duration

type Service struct {
	validated      *xsync.Map[string, error]
	singleflight   singleflight.Group
	clients        *xsync.Map[string, debrid.Client]
	entryRefresher EntryRefresher
	repairer       EntryRepairer
	reacquirer     EntryReacquirer
	entrySaver     EntrySaver
	pruner         EntryPruner
	takedowns      takedownLedger
	httpClient     *http.Client
	retries        int
	resolveCeiling ResolveCeiling
	logger         zerolog.Logger
}

// New creates a new LinkService
func New(
	clients *xsync.Map[string, debrid.Client],
	entryRefresher EntryRefresher,
	entryReinsert EntryRepairer,
	entryReacquire EntryReacquirer,
	entrySaver EntrySaver,
	entryPruner EntryPruner,
	httpClient *http.Client,
	retries int,
	resolveCeiling ResolveCeiling,
	logger zerolog.Logger,
) *Service {
	return &Service{
		validated:      xsync.NewMap[string, error](),
		clients:        clients,
		entryRefresher: entryRefresher,
		repairer:       entryReinsert,
		reacquirer:     entryReacquire,
		entrySaver:     entrySaver,
		pruner:         entryPruner,
		httpClient:     httpClient,
		retries:        retries,
		resolveCeiling: resolveCeiling,
		logger:         logger,
	}
}

// ceiling resolves the caller-wait ceiling, treating an unset resolver as
// "disabled" so a Service built without one keeps its historical behaviour.
func (s *Service) ceiling() time.Duration {
	if s.resolveCeiling == nil {
		return 0
	}
	return s.resolveCeiling()
}

// GetLink fetches and validates a download link for a file in an entry.
// Links are cached at the account level; this service only tracks validation state.
//
// THE WAIT IS BOUNDED; THE WORK IS NOT.
//
// Resolving a link is not one call. It is the provider's unrestrict endpoint,
// behind a per-account retry chain, repeated across every other configured
// account, and — when the provider answers "hoster unavailable" or hands back an
// empty link — a full inline re-insertion cascade (submit magnet, then poll the
// provider for completion) run up to MaxReinsertionAttempt times. Not one link
// in that chain carries the caller's context: the provider clients build their
// requests with http.NewRequest, so a disconnected reader cannot abort any of
// it, and singleflight.Do could not be interrupted either. A WebDAV GET could
// therefore sit inside this function for MINUTES during a provider flap, which
// is exactly how a FUSE reader ends up in uninterruptible sleep instead of
// receiving an error it can act on.
//
// So the CALLER is released on a ceiling while the resolution keeps running on
// a DETACHED context. Both halves matter:
//
//   - Releasing the caller is what turns a multi-minute wedge into a prompt,
//     typed 503 the client can retry or surface as EIO.
//   - NOT cancelling the work is what makes the flap absorbable: a mint that
//     lands after the caller gave up is still stored in the provider's
//     account-level link cache, so the client's next attempt is served from it
//     with no provider call at all. Cancelling would throw that away and
//     guarantee the retry pays the same cost again.
//
// A late arrival joins the in-flight resolution through singleflight rather
// than starting a second one, so a retrying client cannot amplify load against
// a provider that is already struggling.
func (s *Service) GetLink(ctx context.Context, entry *storage.Entry, filename string) (types.DownloadLink, error) {
	// Use singleflight to deduplicate concurrent requests for the same file
	key := entrySingleflightKey(entry, filename)

	ceiling := s.ceiling()
	if ceiling <= 0 {
		// Ceiling disabled ("0"/"off"/"none"). This is the historical path,
		// preserved verbatim so the knob is a genuine escape hatch: an operator
		// who suspects the ceiling itself can turn it off and get back exactly
		// the old behaviour, unbounded wait included.
		v, err, _ := s.singleflight.Do(key, func() (any, error) {
			return s.fetchAndValidate(ctx, entry, filename, 0)
		})
		if err != nil {
			return emptyDownloadLink, err
		}
		return v.(types.DownloadLink), nil
	}

	if err := ctx.Err(); err != nil {
		return emptyDownloadLink, err
	}

	result := s.singleflight.DoChan(key, func() (any, error) {
		// Detached from the caller, bounded by its own budget. context.Values
		// are kept so anything reading request-scoped state still works.
		workCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), backgroundResolveBudget)
		defer cancel()
		return s.fetchAndValidate(workCtx, entry, filename, 0)
	})

	timer := time.NewTimer(ceiling)
	defer timer.Stop()

	select {
	case res := <-result:
		if res.Err != nil {
			return emptyDownloadLink, res.Err
		}
		link, _ := res.Val.(types.DownloadLink)
		return link, nil
	case <-ctx.Done():
		// The reader hung up. Its own error, not ours — the resolution carries
		// on detached and warms the cache for whoever asks next.
		return emptyDownloadLink, ctx.Err()
	case <-timer.C:
		return emptyDownloadLink, customerror.NewBackendTimeoutError(
			fmt.Errorf("download link for %s/%s was not resolved within %s", entry.GetFolder(), filename, ceiling),
		)
	}
}

func entrySingleflightKey(entry *storage.Entry, filename string) string {
	identity := storage.EntryLifecycleIdentity(entry)
	if identity == "" {
		// Unpersisted entries have no reusable lifecycle identity. Pointer scope
		// still deduplicates concurrent calls sharing that exact workflow object
		// without conflating an unrelated same-hash object.
		identity = fmt.Sprintf("ptr:%p", entry)
	}
	return identity + "\x00" + entry.InfoHash + "\x00" + filename
}

func (s *Service) getClient(provider string) (debrid.Client, error) {
	c, ok := s.clients.Load(provider)
	if !ok {
		return nil, fmt.Errorf("client for provider %s not found", provider)
	}
	return c, nil
}

// fetchAndValidate fetches a download link and validates it.
// attempt tracks how many re-insertion cycles we've already paid for during
// this GetLink call so we can bail out instead of looping forever when the
// underlying file never resolves (see fetchLink/handleBadLink).
func (s *Service) fetchAndValidate(ctx context.Context, entry *storage.Entry, filename string, attempt int) (types.DownloadLink, error) {
	if err := ctx.Err(); err != nil {
		return emptyDownloadLink, err
	}
	// Short-circuit a bad-marked entry BEFORE any provider work. Bad is only
	// set after re-insertion has already been exhausted, so every read of it is
	// guaranteed to fail — yet the test used to sit after a store read+decode,
	// a possible live entry refresh, and a GetDownloadLink call that the
	// account manager repeats across every active account. Those calls consume
	// the provider's download rate-limit bucket and actively starve legitimate
	// streams. Rejecting here costs zero provider API calls.
	//
	// This only short-circuits the SERVING path (GetLink). Repair/re-insertion
	// (Fixer.MoveTorrent, ReinsertEntry) does not route through the link
	// service, so it can still clear Bad; and the post-repair re-entries below
	// only happen once Bad is known to be false.
	if err := badEntryError(entry); err != nil {
		return emptyDownloadLink, err
	}
	link, err := s.fetchLink(ctx, entry, filename, attempt)
	if err != nil {
		return s.handleBadLink(ctx, err, entry, filename, link, attempt)
	}

	// Is link already validated
	// Check if we've already validated this link
	if validationErr, exists := s.validated.Load(link.DownloadLink); exists {
		if validationErr == nil {
			return link, nil // Already validated successfully
		}
		// Previous validation failed - check if we should retry
		if linkErr := GetLinkError(validationErr); linkErr != nil {
			if linkErr.ShouldRefetch() {
				// Invalidate and refetch
				return s.invalidateAndRefetch(ctx, entry, link, attempt)
			}
		}
		return emptyDownloadLink, validationErr
	}

	// Validate the link
	validationErr := s.validateLink(ctx, &link)

	if validationErr != nil {
		// Handle link error categories
		if linkErr := GetLinkError(validationErr); linkErr != nil {
			if linkErr.ShouldDisableAccount() {
				if err := s.disableLinkAccount(link, linkErr); err != nil {
					s.logger.Error().
						Err(err).
						Str("debrid", link.Debrid).
						Str("token", utils.Mask(link.Token)).
						Str("reason", linkErr.Code).
						Msg("Failed to disable account after link error")
				} else {
					// This will use the next available account and fetch a new link, so we need to refetch and revalidate.
					// Account swap doesn't consume a re-insertion attempt.
					return s.fetchAndValidate(ctx, entry, filename, attempt)
				}
			} else if linkErr.ShouldRefetch() {
				// Invalidate and refetch
				return s.invalidateAndRefetch(ctx, entry, link, attempt)
			}
		}
	}

	// Store validation result
	s.validated.Store(link.DownloadLink, validationErr)

	if validationErr == nil {
		return link, nil
	}
	return emptyDownloadLink, validationErr
}

// badEntryError converts the durable Bad flag into the typed, permanent 410 a
// serving read must surface. A bare error here became a generic HTTP 500 in the
// WebDAV layer, and 500 is RETRYABLE: rclone retries it instead of converting
// it to EIO, so the reader never fails and re-requests the dead entry forever.
// 410 Gone is both semantically correct and the status that actually stops the
// retry loop. The message text is preserved verbatim — operators grep for it.
func badEntryError(entry *storage.Entry) error {
	if entry == nil || !entry.Bad {
		return nil
	}
	return customerror.NewContentGoneError(
		fmt.Errorf("can't repair %s since it's been marked as bad", entry.GetFolder()),
	)
}

// reinsertReason names the failure class that triggered a re-insertion cycle,
// or "" when err is not a re-insertion trigger.
//
// Both classes mean "the provider has no usable copy of this file right now",
// which re-inserting the entry can genuinely fix. EmptyDownloadLinkError used to
// reach this path indirectly: the account manager swallowed provider errors and
// returned an EMPTY link with a nil error, and fetchLink's downloadLink.Empty()
// branch drove the recovery. The account manager now returns the typed error
// instead, so the empty-link case must be routed here explicitly or the whole
// re-insertion recovery silently stops happening.
//
// Deliberately NOT included: 429/auth/5xx on the download endpoint. Those also
// used to surface as an empty link and could permanently condemn an entry
// (Bad = true) after three attempts. They are typed errors now and are left
// alone — a transient throttle must never permanently kill an entry.
//
// Also deliberately NOT included, and checked before this function is even
// called: a confirmed legal takedown. It arrives with the same shape as a
// hoster-unavailable refusal but means the opposite thing, and re-inserting on
// the provider that issued it is guaranteed futile. See handleTakedown.
func reinsertReason(err error) string {
	switch {
	case errors.Is(err, customerror.HosterUnavailableError):
		return "hoster_unavailable"
	case errors.Is(err, types.EmptyDownloadLinkError):
		return "empty_link"
	default:
		return ""
	}
}

func (s *Service) handleBadLink(ctx context.Context, err error, entry *storage.Entry, filename string, dl types.DownloadLink, attempt int) (types.DownloadLink, error) {
	// A CONFIRMED LEGAL TAKEDOWN IS NOT A RE-INSERTION TRIGGER.
	//
	// It used to be, because it arrived as HosterUnavailableError like every
	// transient refusal did. That is the whole 695-refusals-a-day loop: submit
	// the magnet again to the provider that has legally removed the release, get
	// a placement, resolve a link, get refused again, repeat — three times per
	// read, for every read, forever. It is checked before reinsertReason so the
	// old lumped route cannot reclaim it.
	if customerror.IsContentTakedown(err) {
		return s.handleTakedown(ctx, err, entry, filename, attempt)
	}
	reason := reinsertReason(err)
	if reason == "" {
		// Just return the error
		return dl, err
	}
	if badErr := badEntryError(entry); badErr != nil {
		return emptyDownloadLink, badErr
	}
	if attempt >= MaxReinsertionAttempt {
		// EXHAUSTED, NOT CONDEMNED.
		//
		// This branch used to set the durable Bad flag, which short-circuits
		// every future read of the entry and is cleared by nothing except a
		// re-insertion that succeeds. The trigger reaching it, though, is by
		// definition a TRANSIENT class — hoster unavailable (RealDebrid 19/24) or
		// an empty link — so a provider having a bad hour was enough to condemn
		// an entry permanently, and the more re-insertion cycles the outage
		// broke, the more certain the condemnation looked.
		//
		// Nothing durable happens here now. The reader gets the provider's own
		// answer (503, retryable) instead of a generic 500 that rclone would
		// retry blindly, the entry's health is untouched, and the next read
		// starts from the same state. An outage of any length therefore costs
		// failed reads and nothing else.
		s.logger.Warn().
			Str("infohash", entry.InfoHash).
			Str("name", entry.Name).
			Str("filename", filename).
			Int("attempts", attempt).
			Str("reason", reason).
			Msg("Re-insertion exhausted on a transient provider failure; entry left unmarked")
		return emptyDownloadLink, fmt.Errorf("%w: entry %s file %s still unresolvable (%s) after %d re-insertion attempts",
			customerror.HosterUnavailableError, entry.GetFolder(), filename, reason, attempt)
	}
	if repairErr := s.repairer(ctx, entry); repairErr != nil {
		return emptyDownloadLink, repairErr
	}

	if entry.Bad {
		// Entry is still bad
		return emptyDownloadLink, fmt.Errorf("entry %s(%s) still bad after repair, un-repairable", entry.GetFolder(), dl.Link)
	}
	// Bypass singleflight re-entry to avoid deadlock. filename comes from the
	// caller, not from dl: the provider returns an EMPTY DownloadLink alongside
	// its error, so dl.Filename is "" and retrying with it resolved to a bogus
	// "file not found" and logged a blank filename when marking the entry bad.
	return s.fetchAndValidate(ctx, entry, filename, attempt+1)
}

// handleTakedown is the confirmed-legal-takedown path: the provider has stated
// that this release was removed for legal reasons (RealDebrid code 35 / HTTP
// 451), which is a verdict about the CONTENT and not about the machinery.
//
// It answers the three questions the old lumped code never had to ask.
//
// HOW MUCH EVIDENCE. One refusal per file, by default
// (Config.DebridTakedownThreshold). There is no retry, no wait and no other
// account that turns a 451 into bytes, so a second refusal would only mean one
// more guaranteed-failing read on the way to the same conclusion. A negative
// threshold disables the verdict outright for operators who distrust a
// provider's classification.
//
// WHOSE FAULT IT IS. A takedown is PROVIDER-SCOPED. Before condemning anything,
// the other configured debrids are asked once — skipping the provider that
// issued the refusal, which is guaranteed futile. If one of them can serve the
// content, the entry is simply re-acquired there and nothing is condemned at
// all. That is the asymmetry being respected: wrongly condemning good content
// costs a re-download plus an indexer search, and content alive on another
// debrid is good content.
//
// HOW MUCH TO CONDEMN. The whole entry, only once EVERY live file in it has
// been confirmed taken down. A partially dead entry keeps serving its surviving
// files — the same rule the repair sweep's PRUNE already applies, for the same
// reason: a thirteen-file pack must not be deleted because one file died.
func (s *Service) handleTakedown(ctx context.Context, err error, entry *storage.Entry, filename string, attempt int) (types.DownloadLink, error) {
	threshold := takedownThreshold()
	if threshold < 0 {
		// Disabled by configuration. The reader still gets the real cause; we
		// simply never convert it into a verdict.
		return emptyDownloadLink, err
	}

	key := takedownLedgerKey(entry)
	fileDead, deadFiles := s.takedowns.record(key, filename, threshold)
	live := liveFileCount(entry)
	s.logger.Warn().
		Str("infohash", entry.InfoHash).
		Str("name", entry.Name).
		Str("filename", filename).
		Int("threshold", threshold).
		Int("dead_files", deadFiles).
		Int("live_files", live).
		Msg("Provider reports this content was removed for legal reasons")

	if !fileDead {
		// Below threshold: refuse this read with its real cause and wait for
		// corroboration. Nothing durable happens on the way there.
		return emptyDownloadLink, err
	}

	// Ask the OTHER debrids once. attempt bounds this the same way it bounds the
	// transient re-insertion loop, so a takedown on every configured provider
	// cannot turn into an unbounded cascade.
	if s.reacquirer != nil && attempt < MaxReinsertionAttempt {
		if reacquireErr := s.reacquirer(ctx, entry); reacquireErr != nil {
			s.logger.Debug().Err(reacquireErr).
				Str("infohash", entry.InfoHash).
				Msg("No other debrid could re-acquire content taken down on the active provider")
		} else if !entry.Bad {
			// Somebody else has it. The refusal was about one provider, not about
			// the content, so the evidence gathered against it is void.
			s.takedowns.forget(key)
			s.logger.Info().
				Str("infohash", entry.InfoHash).
				Str("name", entry.Name).
				Str("provider", entry.ActiveProvider).
				Msg("Content taken down on one debrid was re-acquired on another; not condemning")
			return s.fetchAndValidate(ctx, entry, filename, attempt+1)
		}
	}

	if live > 0 && deadFiles < live {
		// Partially dead. The surviving files still serve, so their symlinks
		// stay valid and the arr keeps the release. Say so — a correct refusal
		// to act is otherwise indistinguishable from a broken one.
		s.logger.Info().
			Str("infohash", entry.InfoHash).
			Str("name", entry.Name).
			Int("dead_files", deadFiles).
			Int("live_files", live).
			Msg("Entry is partially taken down; leaving it in place so its surviving files keep serving")
		return emptyDownloadLink, err
	}

	s.condemnTakenDownEntry(entry, filename, deadFiles)
	return emptyDownloadLink, err
}

// condemnTakenDownEntry records the verdict and then stops the content being
// served AND being listed.
//
// THE MARK AND THE PRUNE ARE TWO DIFFERENT THINGS. Bad stops it being SERVED:
// every subsequent read short-circuits to a permanent 410 before any provider
// call. That alone is not enough, and not being enough is the defect: a
// bad-marked entry stays in the mount listing, so its library symlink still
// resolves, so the arr never notices anything is wrong and never re-searches.
// The content is dead and the library goes on believing it has it.
//
// PRUNE is what closes that. It is the identical operation the repair sweep's
// PRUNE component performs on dead content, gated by the identical knob
// (repair.prune), and it makes ZERO arr calls: it deletes the entry
// decypharr-side, the symlink dangles, and the arr reaps it on its own next disk
// scan and searches for a replacement by its own rules. decypharr is the
// download client; it marks and parks, and the arr learns by looking.
//
// With repair.prune off, the entry is condemned but left in place — visible in
// the __bad__ folder, still refusing reads. That is the honest degradation: the
// destructive step stays behind the operator's existing destructive consent
// rather than inventing a new one that defaults to deleting things.
func (s *Service) condemnTakenDownEntry(entry *storage.Entry, filename string, deadFiles int) {
	s.markEntryBad(entry, filename, deadFiles, "content_takedown")
	s.takedowns.forget(takedownLedgerKey(entry))

	if !config.Get().Repair.Prune {
		s.logger.Warn().
			Str("infohash", entry.InfoHash).
			Str("name", entry.Name).
			Msg("Content is confirmed taken down but repair.prune is off, so it stays in the mount listing and its " +
				"library symlink keeps resolving; enable PRUNE to have the entry removed and the arr re-search it")
		return
	}
	if s.pruner == nil {
		s.logger.Warn().
			Str("infohash", entry.InfoHash).
			Msg("Content is confirmed taken down but no pruner is wired; entry left in place")
		return
	}
	if pruneErr := s.pruner(entry); pruneErr != nil {
		s.logger.Warn().Err(pruneErr).
			Str("component", "PRUNE").
			Str("infohash", entry.InfoHash).
			Str("name", entry.Name).
			Msg("PRUNE: failed to delete taken-down entry decypharr-side")
		return
	}
	s.logger.Info().
		Str("component", "PRUNE").
		Str("infohash", entry.InfoHash).
		Str("name", entry.Name).
		Msg("PRUNE: deleted taken-down entry decypharr-side (no arr call is made, so the library symlink is left " +
			"dangling; whether it is reaped depends on the arr's own dangling-symlink handling)")
}

// markEntryBad sets entry.Bad and persists it so subsequent GetLink calls
// for the same entry short-circuit instead of triggering another re-insertion
// cycle. Logged once per call.
//
// ONLY AN UNAMBIGUOUS CONTENT VERDICT REACHES HERE. It used to be reachable from
// the exhausted-re-insertion branch as well, which is how transient provider
// failures acquired a permanent, self-sustaining Bad flag — see handleBadLink
// for what that cost. The flag is durable and effectively one-way, so the bar
// for setting it is a provider stating the content itself is gone.
func (s *Service) markEntryBad(entry *storage.Entry, filename string, attempt int, reason string) {
	entry.Bad = true
	if s.entrySaver != nil {
		if err := s.entrySaver(entry); err != nil {
			s.logger.Warn().
				Err(err).
				Str("infohash", entry.InfoHash).
				Msg("Failed to persist Bad flag after exhausting re-insertion attempts")
		}
	}
	s.logger.Warn().
		Str("infohash", entry.InfoHash).
		Str("name", entry.Name).
		Str("filename", filename).
		Int("attempts", attempt).
		Str("reason", reason).
		Msg("Giving up on entry after repeated failed re-insertions")
}

// fetchLink fetches a download link from the debrid provider (via account cache)
func (s *Service) fetchLink(ctx context.Context, entry *storage.Entry, filename string, attempt int) (types.DownloadLink, error) {
	file, err := entry.GetFile(filename)
	if err != nil {
		return emptyDownloadLink, NewPermanentError(
			fmt.Errorf("file %s not found in entry %s: %w", filename, entry.Name, err),
			"file_not_found",
		)
	}

	placementFile, err := s.getPlacementFile(entry, filename)
	if err != nil {
		return emptyDownloadLink, err
	}

	if placementFile.Link == "" && placementFile.Id == "" {
		return emptyDownloadLink, NewPermanentError(
			fmt.Errorf("file link is missing for %s in entry %s", filename, entry.Name),
			"link_missing",
		)
	}

	client, err := s.getClient(entry.ActiveProvider)
	if err != nil {
		return emptyDownloadLink, NewPermanentError(
			fmt.Errorf("debrid client not found: %s", entry.ActiveProvider),
			"client_not_found",
		)
	}

	placement := entry.Providers[entry.ActiveProvider]
	if placement == nil {
		return emptyDownloadLink, NewPermanentError(
			fmt.Errorf("no placement found for debrid %s with infohash %s", entry.ActiveProvider, entry.InfoHash),
			"placement_not_found",
		)
	}

	debridFile := &types.File{
		Id:        placementFile.Id,
		Link:      placementFile.Link,
		Path:      placementFile.Path,
		Name:      file.Name,
		Size:      file.Size,
		ByteRange: file.ByteRange,
		Deleted:   file.Deleted,
	}

	// This uses account-level caching internally
	downloadLink, err := client.GetDownloadLink(placement.ID, debridFile)
	if err != nil {
		return downloadLink, err
	}

	if downloadLink.Empty() {
		// Defensive only. The account manager now returns a typed error for
		// every invalid link, so (empty link, nil error) should be unreachable;
		// if a provider ever produces it again, convert it to the canonical
		// empty-link error so it takes the SAME re-insertion recovery path
		// through handleBadLink rather than handing an unusable link upstream.
		return emptyDownloadLink, types.EmptyDownloadLinkError
	}

	return downloadLink, nil
}

// getPlacementFile retrieves the placement file with refresh fallback
func (s *Service) getPlacementFile(entry *storage.Entry, filename string) (*storage.ProviderFile, error) {
	_, ok := entry.Files[filename]
	if !ok {
		return nil, NewPermanentError(
			fmt.Errorf("file %s not found in entry", filename),
			"file_not_found",
		)
	}

	placement := entry.Providers[entry.ActiveProvider]
	if placement == nil {
		return nil, NewPermanentError(
			fmt.Errorf("no placement found for debrid %s with infohash %s", entry.ActiveProvider, entry.InfoHash),
			"placement_not_found",
		)
	}

	placementFile := placement.Files[filename]
	if placementFile == nil || (placementFile.Link == "" && placementFile.Id == "") {
		if s.entryRefresher == nil {
			return nil, NewPermanentError(
				fmt.Errorf("file %s not available and no refresher configured", filename),
				"no_refresher",
			)
		}

		refreshed, err := s.entryRefresher(entry)
		if err != nil {
			return nil, NewRefetchableError(
				fmt.Errorf("failed to refresh entry: %w", err),
				"refresh_failed",
			)
		}
		if refreshed == nil {
			return nil, NewRefetchableError(
				fmt.Errorf("refresh returned an empty entry"),
				"refresh_empty",
			)
		}

		file := refreshed.Files[filename]
		if file == nil {
			return nil, NewPermanentError(
				fmt.Errorf("file disappeared after refresh"),
				"file_disappeared",
			)
		}

		placement = refreshed.Providers[refreshed.ActiveProvider]
		if placement == nil {
			return nil, NewPermanentError(
				fmt.Errorf("placement disappeared after refresh for debrid %s", refreshed.ActiveProvider),
				"placement_disappeared",
			)
		}

		placementFile = placement.Files[filename]
		if placementFile == nil || (placementFile.Link == "" && placementFile.Id == "") {
			return nil, NewPermanentError(
				fmt.Errorf("file %s not available after refresh", filename),
				"file_not_available",
			)
		}

		mergeRefreshedProviderView(entry, refreshed)
		placement = entry.Providers[entry.ActiveProvider]
		placementFile = placement.Files[filename]
	}

	return placementFile, nil
}

// mergeRefreshedProviderView deliberately copies only provider-owned state.
// In particular it does not replace queue workflow fields or either store's
// private lifecycle tokens on a long-lived downloader snapshot.
func mergeRefreshedProviderView(dst, src *storage.Entry) {
	if dst == nil || src == nil {
		return
	}
	dst.ActiveProvider = src.ActiveProvider
	dst.Providers = cloneProviderEntries(src.Providers)
	dst.Files = cloneEntryFiles(src.Files)
	dst.Size = src.Size
	dst.Bytes = src.Bytes
}

func cloneProviderEntries(source map[string]*storage.ProviderEntry) map[string]*storage.ProviderEntry {
	if source == nil {
		return nil
	}
	result := make(map[string]*storage.ProviderEntry, len(source))
	for name, placement := range source {
		if placement == nil {
			result[name] = nil
			continue
		}
		copyPlacement := *placement
		if placement.Files != nil {
			copyPlacement.Files = make(map[string]*storage.ProviderFile, len(placement.Files))
			for filename, file := range placement.Files {
				if file == nil {
					copyPlacement.Files[filename] = nil
					continue
				}
				copyFile := *file
				copyPlacement.Files[filename] = &copyFile
			}
		}
		result[name] = &copyPlacement
	}
	return result
}

func cloneEntryFiles(source map[string]*storage.File) map[string]*storage.File {
	if source == nil {
		return nil
	}
	result := make(map[string]*storage.File, len(source))
	for name, file := range source {
		if file == nil {
			result[name] = nil
			continue
		}
		copyFile := *file
		if file.ByteRange != nil {
			copyRange := *file.ByteRange
			copyFile.ByteRange = &copyRange
		}
		result[name] = &copyFile
	}
	return result
}

// validateLink validates a download link by making a HEAD request
func (s *Service) validateLink(ctx context.Context, link *types.DownloadLink) error {
	if link == nil {
		return NewPermanentError(ErrEmptyLink, "empty_link")
	}
	if link.Empty() {
		return NewPermanentError(fmt.Errorf("download url is empty for %s||%s", link.Filename, link.Link), "empty_link")
	}

	req, err := http.NewRequestWithContext(ctx, "HEAD", link.DownloadLink, nil)
	if err != nil {
		return NewPermanentError(
			fmt.Errorf("failed to create HEAD request: %w", err),
			"request_creation_failed",
		)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return NewRetryableError(
			fmt.Errorf("HEAD request failed: %w", err),
			"network_error",
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	errorCode := resp.Header.Get("X-Error")
	if errorCode == "" {
		errorCode = strconv.Itoa(resp.StatusCode)
	}

	return ErrorCodeToLinkError(errorCode)
}

// disableLinkAccount handles errors that require disabling an account
func (s *Service) disableLinkAccount(link types.DownloadLink, linkErr *Error) error {
	client, err := s.getClient(link.Debrid)
	if err != nil {
		return fmt.Errorf("failed to get client for debrid %s: %w", link.Debrid, err)
	}

	accountManager := client.AccountManager()
	account, err := accountManager.GetAccount(link.Token)
	if err != nil {
		return fmt.Errorf("failed to get account for token %s: %w", utils.Mask(link.Token), err)
	}

	if account == nil {
		return fmt.Errorf("account not found for token %s", utils.Mask(link.Token))
	}

	accountManager.Disable(account)

	// Remove all validations for all the links
	s.validated.Clear()
	s.logger.Warn().
		Str("debrid", link.Debrid).
		Str("token", utils.Mask(account.Token)).
		Str("account", utils.Mask(account.Username)).
		Str("reason", linkErr.Code).
		Msg("Disabled account due to error")
	return nil
}

// invalidateAndRefetch removes a link from both validation tracking and account cache
func (s *Service) invalidateAndRefetch(ctx context.Context, entry *storage.Entry, link types.DownloadLink, attempt int) (types.DownloadLink, error) {
	// Remove from validation tracking
	s.validated.Delete(link.DownloadLink)

	// Remove from account cache
	if link.Debrid == "" {
		return emptyDownloadLink, fmt.Errorf("invalid link")
	}

	client, err := s.getClient(link.Debrid)
	if err != nil {
		return emptyDownloadLink, err
	}

	_ = client.DeleteLink(link) // This might fail, doesnt matter

	return s.fetchLink(ctx, entry, link.Filename, attempt)
}

// Clear removes all validation tracking entries
func (s *Service) Clear() {
	s.validated.Clear()
}

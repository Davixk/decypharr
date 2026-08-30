package manager

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Add-refusal classification.
//
// A provider saying no is not one condition, and the whole point of this file
// is that answering it uniformly is wrong in both directions.
//
//	CONTENT       the release is unservable — dead, uncached with uncached
//	              downloads disabled, no seeders. Another candidate might not
//	              be, so REFUSE and let the arr take its next one.
//	CAPACITY      nothing to do with this release; the provider is busy, its
//	              allowance is spent, or we are asking too fast. decypharr's own
//	              queue is not bounded by the provider's, so ACCEPT and hold.
//
// And CAPACITY is itself two conditions that must not share an answer:
//
//	TRANSIENT     concurrency (RealDebrid active slots, AllDebrid's 30 active
//	              magnets), AllDebrid's DAILY add allowance, and the RATE class
//	              — HTTP 429 and RealDebrid's traffic/fair-usage codes. All of
//	              these clear on their own: slots as downloads finish, the daily
//	              allowance on the provider's boundary, the rate limit in
//	              seconds.
//	STANDING      AllDebrid's STORED-item cap. Nothing decypharr finishes or
//	              waits for frees it; only deleting items does. Observed
//	              refusing every add for 54.6 continuous hours across two
//	              midnights.
//
// WHY THE HOLD HAS NO DEADLINE. An earlier design bounded it and failed the
// entry on expiry, reasoning from fork.34's 15.2-hour spin. That reasoning does
// not survive the change in conditions: fork.34 looped forever because NOTHING
// was reclaiming capacity, with AllDebrid permanently full and RealDebrid
// pinned at 99/100. Stall pruning now continuously reclaims provider slots, so
// a hold is a queue with a working drain rather than an open-ended wait. A
// deadline under those conditions would fail entries that were going to
// succeed — and under the sync/async cost model each of those failures buys a
// full arr search across every indexer, where the hold costs nothing.
//
// The STANDING case is what actually needs bounding, and it is bounded by being
// refused outright rather than by a timer.

// addRefusal is the verdict for one failed add attempt.
type addRefusal struct {
	// hold is true when the entry should be accepted and retried rather than
	// failed back to the arr.
	hold bool
	// provider and detail name the refusal that drove the verdict, for logs.
	provider string
	detail   string
	// reason is the sentence an *arr shows when this refusal is answered
	// synchronously. Set only where the generic refusalReason text would be
	// wrong — a stored-item cap is a legitimate refusal, not the "should have
	// been held, please report this" case.
	reason string
	// standingCondition is set when a provider refused because a cap that does
	// NOT self-clear is full. It is surfaced loudly: the operator has to act.
	standingCondition string
}

// classifyAddRefusal decides whether a failed add is held or refused.
//
// THE VERDICT IS PER-PROVIDER, THEN AGGREGATED. Operator's ruling:
//
//	a PERMANENT refusal rules out only THAT provider for that hash —
//	  "infringing on RD means nothing on AD";
//	a TEMPORARY inability (add allowance spent, rate pacing, slots full) rules
//	  out nothing at all; it means retry-later on that provider.
//
//	ALL providers permanent  -> refuse synchronously, with a reason the arr can
//	                            surface, so it takes its next candidate.
//	ANY provider temporary   -> accept and HOLD as queuedDL, retrying internally
//	                            until a real verdict.
//
// 🔴 THE EQUIVALENCE THAT USED TO MAKE THIS FUNCTION SHORT IS GONE, AND IT HAD
// TO GO. The error arrives as a join across every provider in the chain, and
// errors.Is walks the whole tree, so a single scan for "any temporary
// condition" WAS the aggregate rule above — exactly and for free. That held
// only while every temporary sentinel was UNCONDITIONALLY temporary.
//
// ProviderAddQuotaExhaustedError is not. AllDebrid returns it both for a daily
// add allowance (transient) and for a full stored-item cap (permanent — nothing
// but deletion frees it), and which one it means is decided by that account's
// own fill against that account's own cap. A whole-tree scan cannot ask that
// question: it does not know WHICH provider raised the quota error, so it would
// resolve AllDebrid's ceiling against RealDebrid's fill, or refuse a grab that
// RealDebrid's daily allowance will accept in an hour.
//
// So the tree is split into per-provider branches and each is classified
// against its own account. The aggregate rule above is then applied across the
// branch verdicts, unchanged. Every test still matches by SENTINEL IDENTITY: a
// text or status test would answer for the wrong provider's error even inside a
// correctly attributed branch.
//
// Anything not positively identified as a temporary inability refuses. That is
// the conservative direction: refusing costs the arr one candidate from a
// result set it is still holding, while a wrong hold parks an entry as queuedDL
// against a condition that may never clear, and the arr waits on it.
//
// ⚠️ AUTH AND TRANSPORT FAILURES REFUSE, and that is a deliberate reading of the
// ruling rather than an omission. Neither is a content verdict, so neither is
// "permanent" in the operator's sense — but holding a bad API key behind a
// growing queue of queuedDL rows hides a fault only the operator can fix, and
// hides it in the one place nobody looks.
func (m *Manager) classifyAddRefusal(err error) addRefusal {
	if err == nil {
		return addRefusal{}
	}

	// 🛑 NO "IS IT PERMANENTLY GONE?" SHORT-CIRCUIT HERE, DELIBERATELY.
	//
	// One was written and removed before it shipped. The reasoning for it was
	// sound — HosterUnavailableError is double-booked (a definitive 404/410 from
	// CheckFile AND a transient outage from fetchDownloadLink) and carries status
	// 503, so any test that reasons from status or text could hold gone content
	// forever. The reasoning for the FIX was not: a leading
	// IsContentPermanentlyGone check applied to the WHOLE tree refuses a grab
	// another provider is about to accept. Per-branch classification is what
	// makes such a test safe, and it is now what this function does.
	//
	// The double-booking is still handled by matching on SENTINEL IDENTITY and
	// never on status or text. See isRateClassRefusal for why that distinction
	// is load-bearing rather than stylistic.

	branches := splitRefusalBranches(err)
	if len(branches) == 0 {
		return addRefusal{}
	}

	agg := addRefusal{}
	for _, branch := range branches {
		one := m.classifyBranchRefusal(branch.provider, branch.err)

		// A standing condition is reported whichever way the aggregate lands.
		// It says "this account is full and only deletion frees it", which the
		// operator has to act on even when some other provider's transient
		// condition is what holds this particular grab.
		if agg.standingCondition == "" && one.standingCondition != "" {
			agg.standingCondition = one.standingCondition
		}
		if agg.reason == "" && one.reason != "" {
			agg.reason = one.reason
		}
		if agg.provider == "" {
			agg.provider = one.provider
		}

		// THE FIRST TEMPORARY INABILITY DECIDES THE AGGREGATE. The loop does not
		// break, because the standing conditions of the remaining branches still
		// have to be collected — a hold must not silence the one log line that
		// explains why the account can never accept anything again.
		if one.hold && !agg.hold {
			agg.hold = true
			agg.provider = one.provider
			agg.detail = one.detail
		}
	}
	return agg
}

// classifyBranchRefusal is the verdict for ONE provider's refusal.
//
// name is that provider, resolved from the branch rather than from the whole
// tree — the distinction that lets a capacity refusal be judged against the cap
// and fill of the account that actually raised it.
func (m *Manager) classifyBranchRefusal(name string, err error) addRefusal {
	// Concurrency exhaustion is unambiguously transient on every provider that
	// reports it: RealDebrid's active-slot count and AllDebrid's 30 active
	// magnets both free as work finishes. No fill check is needed or relevant —
	// this is not about how much the account STORES.
	if isTooManyActiveDownloads(err) {
		return addRefusal{
			hold:     true,
			provider: name,
			detail:   "provider concurrency limit reached; slots free as active downloads finish",
		}
	}

	// THE RATE CLASS — "you are asking too fast", not "there is no room".
	//
	// This is the refusal decypharr CAUSES, which is exactly why answering it
	// with a 400 was indefensible: the grab was lost because of our own request
	// pattern, and the condition it was lost to had cleared by the time the *arr
	// finished logging it. Measured: 595 refusals in a single 30-minute window,
	// with the same releases reappearing on a ~2-3 hour re-grab loop.
	//
	// No fill check, deliberately. A rate limit says nothing about how full the
	// account is — resolving it against a stored-item cap would be answering a
	// question nobody asked and would turn a seconds-long condition into a
	// permanent refusal on a full account.
	if isRateClassRefusal(err) {
		return addRefusal{
			hold:     true,
			provider: name,
			detail:   "provider is rate limiting or its traffic allowance is spent; this clears on its own and the add-rate pacer is already backing off",
		}
	}

	if isProviderAddQuotaExhausted(err) {
		return m.classifyQuotaRefusal(name, err)
	}

	// Content, auth, parse, transport — permanent for this provider.
	return addRefusal{provider: name}
}

// classifyQuotaRefusal decides whether an add-allowance refusal is transient.
//
// AllDebrid returns MAGNET_TOO_MANY for two conditions that need opposite
// answers, and its message cannot tell them apart — the observed text says
// "Magnets limit reached (1000 accross all tabs)" while the binding constraint
// is 5,000. The account's own fill is the only discriminator:
//
//	fill BELOW the cap -> the DAILY add allowance is spent. Transient. HOLD.
//	fill AT the cap    -> the STORED-item cap is full. Nothing decypharr
//	                      finishes or waits for frees it, only deletion does.
//	                      PERMANENT for this provider. REFUSE.
//
// ⚠️ THIS REVERSES THE HOLD RULING, AND THE HISTORY IS THE POINT, so the third
// version of this decision is written down rather than quietly replacing the
// second. It refused at-cap; a ruling then made it hold, on the grounds that a
// full account is not a verdict about the release and that stall pruning gives
// the stored cap a working drain. The comment that shipped with that change
// named the risk exactly:
//
//	"if the drain ever stops and the account stays full, held entries
//	 accumulate and the arrs wait on downloads that never start."
//
// It did. Measured over 24h on a live deployment: 11,827 admissions, held rows
// going 495 -> 11,399, 6,391 at-cap hold lines and ZERO refusals, against an
// account pinned at exactly 5,000/5,000. Stall pruning does not out-pace new
// grabs, so the drain assumption that made holding safe is false.
//
// What settles it beyond our own inference: the operator called AllDebrid
// directly, with decypharr out of the path, and a single magnet/upload was
// hard-refused at 5,000 stored. The wall is AllDebrid's, not a threshold of
// ours — so a hold here is a promise decypharr cannot keep, and the
// fail-fast economics apply. A synchronous refusal costs the arr one candidate
// from a list it is still holding; an acceptance that can never start costs it
// the whole search.
//
// The verdict stays HELD wherever the at-cap case cannot be PROVEN: no cap
// known, no fill readable, no provider attributable. Refusing on a guess would
// permanently fail a daily allowance that resets by itself, which is the error
// this whole file exists to avoid.
func (m *Manager) classifyQuotaRefusal(name string, err error) addRefusal {
	if name == "" {
		name = quotaRefusalProvider(err)
	}
	if name == "" {
		// Not attributable to a provider. Still held — an unattributable add
		// allowance is not a content verdict either.
		return addRefusal{
			hold:   true,
			detail: "add allowance exhausted on an unidentified provider; holding, since an allowance is never a verdict about the release",
		}
	}

	held := addRefusal{hold: true, provider: name}

	cfg, ok := m.providerConfig(name)
	if !ok {
		held.detail = "add allowance exhausted on an unconfigured provider"
		return held
	}

	capacity, capped := cfg.MagnetCap()
	if !capped {
		// No cap configured and none known for this provider, so there is no
		// threshold to compare against and no fill worth enumerating for.
		held.detail = "add allowance exhausted and no stored-item cap is configured; it resets on the provider's own boundary"
		return held
	}

	fill, known := m.providerFill(name)
	if !known {
		held.detail = "add allowance exhausted; the account's fill could not be read, so it is unclear whether this is the daily allowance or the stored-item cap"
		return held
	}

	if fill >= capacity {
		// PERMANENT for this provider — the one branch of this function that
		// does not hold. It still says nothing about the release, which is why
		// the aggregate in classifyAddRefusal can and does overrule it the
		// moment any other provider is merely busy.
		return addRefusal{
			provider: name,
			detail:   fmt.Sprintf("stored-item cap reached (%d/%d)", fill, capacity),
			reason: fmt.Sprintf(
				"refused: %s is storing %d of its %d items and will not accept another until items are deleted "+
					"there; this is a limit on the account, not a problem with this release",
				name, fill, capacity),
			standingCondition: fmt.Sprintf(
				"provider %q is storing %d of its %d items and is REFUSING new grabs. Nothing decypharr does "+
					"frees this — not waiting, not retrying — only deleting items on the provider, or raising "+
					"max_magnets if the provider's real cap is higher. Grabs are being refused so the *arr takes "+
					"its next candidate immediately instead of queueing behind a wall that never moves.",
				name, fill, capacity),
		}
	}

	held.detail = fmt.Sprintf("daily add allowance spent while stored fill is %d/%d; it resets on the provider's own boundary", fill, capacity)
	return held
}

// holdTorrentForCapacity accepts a grab no provider had room for, and holds it.
//
// The entry stays QUEUED — it reports queuedDL to the arr rather than
// pretending to download — and a retry job is submitted so it is re-attempted
// when capacity exists. Returning nil is what tells the arr the grab was
// accepted.
//
// THE HONEST STATEMENT OF THE TRADE: this is accept-then-maybe-later, which
// this codebase refuses everywhere it means accept-then-FAIL-later. The
// difference is that a held entry is not failing, it is waiting on a queue that
// drains — stall pruning reclaims provider slots continuously — and the arr's
// queue row stays truthfully "queued" the whole time. If that drain ever stops
// working, this becomes the fork.34 spin again, so the drain is the load-bearing
// assumption and not an incidental one.
func (m *Manager) holdTorrentForCapacity(importReq *ImportRequest, torrent *storage.Entry, refusal addRefusal, cause error) error {
	torrent.Status = debridTypes.TorrentStatusQueued
	if updateErr := m.queue.Update(torrent); updateErr != nil {
		// Could not even persist the hold, so we cannot honestly claim to be
		// holding it. Fail the add instead of accepting something we have not
		// recorded.
		return fmt.Errorf("failed to hold torrent for provider capacity: %w", errors.Join(updateErr, cause))
	}

	importReq.Status = "queued"
	importReq.CompletedAt = time.Time{}
	importReq.Error = ""
	job := NewJob(JobTypeTorrent, importReq)
	job.ID = torrent.InfoHash
	job.Entry = torrent

	m.logger.Info().
		Str("provider", refusal.provider).
		Str("hash", torrent.InfoHash).
		Str("name", torrent.Name).
		Msgf("Accepted and holding until a provider slot frees: %s", refusal.detail)

	// Parked, not polled. It is admitted the moment a slot frees — an event this
	// process witnesses and usually causes — rather than on a cadence that would
	// re-ask a question we already know the answer to, N/30 times a second.
	m.holdForCapacity(job)
	return nil
}

// AT-CAP IS AN EXACT COMPARISON: fill >= capacity, nothing else.
//
// A margin was tried here and removed. It was justified by the account refusing
// at 4,998 against a 5,000 cap, so that `fill >= capacity` never fired — but
// that observation is evidence about the CAP, not an argument for a fudge
// factor. max_magnets is a value the operator sets; padding it is a threshold
// invented to compensate for a threshold already supplied, which is the same
// mistake as the fabricated DefaultAvailableSlots of 100.
//
// Worse, a margin HIDES which of two conditions is true, and they need opposite
// handling:
//
//	the real stored ceiling here is lower than 5,000  -> the KNOB should say so,
//	   and exact comparison then works perfectly;
//	the refusal at 4,998 is the DAILY add cap, not the stored cap -> it is
//	   TRANSIENT and must be HELD. A margin would permanently refuse a condition
//	   that clears every day.
//
// Exact comparison surfaces that honestly: under the configured cap and still
// refused means the daily allowance, and the hold branch already handles it.

// refusalReason renders a refused add as one line a human can act on.
//
// It exists because the qBittorrent shim answers a refused add with this text as
// the 400 body, and that body is the ONLY thing an *arr can show. The field
// complaint it answers is precise: twelve consecutive manual grabs of one title
// died across nzb and torrents, "most as silent 400s the arr can't distinguish
// from anything else". The wrapped chain is still attached underneath for the
// log; this is the sentence that goes first.
//
// The classes are the ones a person would act on differently — taken down, not
// cached, no seeders, auth. Anything unrecognised gets an honest generic rather
// than a guess, because a confidently wrong reason in an arr's log is worse than
// a vague true one.
func refusalReason(err error) string {
	switch {
	case err == nil:
		return "the release was refused"
	case customerror.IsContentTakedown(err):
		return "refused: the provider reports this release was removed for legal reasons (HTTP 451), so no retry or other account can serve it"
	case customerror.IsContentPermanentlyGone(err):
		return "refused: the provider reports this content is gone"
	case errors.Is(err, customerror.ProviderAddQuotaExhaustedError),
		errors.Is(err, customerror.TooManyActiveDownloadsError),
		errors.Is(err, customerror.RateLimitedError),
		errors.Is(err, customerror.TrafficExceededError):
		// Reachable only when something upstream declined to hold — a capacity
		// class that got here is a bug, and the message says so rather than
		// pretending the release was at fault.
		return "refused: a provider capacity limit reached the refusal path, which should have been held instead — please report this"
	}

	// Below here there is no sentinel to match, so the text is the only
	// evidence there is. Scoped to the add path's own messages, which this
	// package writes, rather than to anything a provider might phrase freely.
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "uncached"):
		return "refused: this release is not cached on any configured provider and uncached downloads are disabled"
	case strings.Contains(text, "seeder"):
		return "refused: this release is below the configured seeder minimum"
	case strings.Contains(text, "no valid files"):
		return "refused: the provider accepted the release but it contains no usable files"
	case strings.Contains(text, "unauthorized"), strings.Contains(text, "401"), strings.Contains(text, "403"):
		return "refused: a provider rejected decypharr's credentials — this is a configuration fault, not a problem with the release"
	}
	return "refused: no configured provider could serve this release"
}

// providerConfig returns the configured Debrid block for a provider name.
func (m *Manager) providerConfig(name string) (config.Debrid, bool) {
	client := m.ProviderClient(name)
	if client == nil {
		return config.Debrid{}, false
	}
	return client.Config(), true
}

// refusalBranch is one provider's refusal, lifted out of the joined tree.
//
// provider is empty when nothing in that branch said who refused — a bare
// sentinel, or a failure raised before any provider was selected. Such a branch
// is still classified; it just cannot have a cap resolved against it.
type refusalBranch struct {
	provider string
	err      error
}

// splitRefusalBranches separates a joined chain error into per-provider
// branches.
//
// WHY THIS EXISTS AT ALL: the add path tries every configured provider and
// joins their refusals, then wraps the join (joinDebridErrors adds a message
// and singleLineError). A verdict that needs to know WHICH account refused —
// and the stored-item cap does — cannot be reached by scanning that tree with
// errors.Is, because errors.Is answers "somewhere in here" and never "whose".
//
// The walk descends single wrappers so a join nested under them is still found,
// and carries the enclosing providerError's name DOWNWARD so attribution
// survives that descent. A branch with no join above it is returned whole.
func splitRefusalBranches(err error) []refusalBranch {
	return appendRefusalBranches(nil, err, "", 0)
}

// maxRefusalBranchDepth bounds the walk. Error trees are built by this package
// and are shallow, but a cyclic Unwrap would otherwise hang the add path — a
// classifier is the wrong place to trust a stranger's linked list.
const maxRefusalBranchDepth = 32

func appendRefusalBranches(out []refusalBranch, err error, inherited string, depth int) []refusalBranch {
	if err == nil || depth > maxRefusalBranchDepth {
		return out
	}
	// The NEAREST enclosing providerError wins: a branch is attributed to the
	// provider whose attempt produced it, not to an outer wrapper's.
	if pErr, ok := err.(*providerError); ok && pErr.provider != "" {
		inherited = pErr.provider
	}
	if multi, ok := err.(interface{ Unwrap() []error }); ok {
		for _, sub := range multi.Unwrap() {
			out = appendRefusalBranches(out, sub, inherited, depth+1)
		}
		return out
	}
	if single, ok := err.(interface{ Unwrap() error }); ok {
		// 🛑 DESCEND ONLY TOWARDS A JOIN, NEVER TO THE LEAF.
		//
		// Walking a wrapper chain to its innermost error would hand each branch
		// a naked leaf, and every sentinel test in classifyBranchRefusal matches
		// by identity through the chain — so a TrafficExceededError wrapped by
		// anything would stop matching, and a transient condition would be
		// classified as a permanent refusal. The branch keeps its FULL chain;
		// this descent exists purely to reach a join hiding under one.
		if inner := single.Unwrap(); inner != nil && hasJoinBelow(inner, depth+1) {
			return appendRefusalBranches(out, inner, inherited, depth+1)
		}
	}
	return append(out, refusalBranch{provider: inherited, err: err})
}

func hasJoinBelow(err error, depth int) bool {
	for err != nil && depth <= maxRefusalBranchDepth {
		if _, ok := err.(interface{ Unwrap() []error }); ok {
			return true
		}
		single, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = single.Unwrap()
		depth++
	}
	return false
}

// quotaRefusalProvider extracts which provider raised an ADD-QUOTA refusal.
//
// The traversal is manual rather than errors.As, and that is the point. A
// multi-provider chain joins every provider's refusal together, so errors.As
// would return whichever *providerError sits first in the tree — quite possibly
// RealDebrid's submit failure rather than the AllDebrid quota refusal we are
// trying to attribute. Resolving a cap against the wrong provider's fill is a
// confident wrong answer, so only a providerError whose OWN wrapped error is a
// quota exhaustion counts.
func quotaRefusalProvider(err error) string {
	if err == nil {
		return ""
	}
	if pErr, ok := err.(*providerError); ok {
		if pErr.provider != "" && errors.Is(pErr.err, customerror.ProviderAddQuotaExhaustedError) {
			return pErr.provider
		}
		return quotaRefusalProvider(pErr.err)
	}
	if single, ok := err.(interface{ Unwrap() error }); ok {
		return quotaRefusalProvider(single.Unwrap())
	}
	if multi, ok := err.(interface{ Unwrap() []error }); ok {
		for _, sub := range multi.Unwrap() {
			if name := quotaRefusalProvider(sub); name != "" {
				return name
			}
		}
	}
	return ""
}

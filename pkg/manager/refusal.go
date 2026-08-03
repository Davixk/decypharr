package manager

import (
	"errors"
	"fmt"
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
//	CAPACITY      nothing to do with this release; the provider is busy or its
//	              allowance is spent. decypharr's own queue is not bounded by
//	              the provider's, so ACCEPT and hold.
//
// And CAPACITY is itself two conditions that must not share an answer:
//
//	TRANSIENT     concurrency (RealDebrid active slots, AllDebrid's 30 active
//	              magnets) and AllDebrid's DAILY add allowance. All of these
//	              clear on their own — slots as downloads finish, the daily
//	              allowance on the provider's boundary.
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
	// standingCondition is set when a provider refused because a cap that does
	// NOT self-clear is full. It is surfaced loudly: the operator has to act.
	standingCondition string
}

// classifyAddRefusal decides whether a failed add is held or refused.
//
// The error may be a join across every provider in the chain, so the scan is
// for the BEST outcome available: if any provider's refusal was transient
// capacity, holding is right, because that provider will have room later even
// if another is permanently full.
//
// Anything this cannot positively identify as transient capacity refuses. That
// is the conservative direction here — a refusal is a cheap synchronous 400
// that sends the arr to its next candidate, while a wrong hold parks an entry
// against a condition that may never clear.
func (m *Manager) classifyAddRefusal(err error) addRefusal {
	if err == nil {
		return addRefusal{}
	}

	// Concurrency exhaustion is unambiguously transient on every provider that
	// reports it: RealDebrid's active-slot count and AllDebrid's 30 active
	// magnets both free as work finishes. No fill check is needed or relevant —
	// this is not about how much the account STORES.
	if isTooManyActiveDownloads(err) {
		return addRefusal{
			hold:     true,
			detail:   "provider concurrency limit reached; slots free as active downloads finish",
		}
	}

	// The ambiguous one. AllDebrid returns MAGNET_TOO_MANY for both its daily
	// add allowance and its stored-item cap, so the account's fill decides.
	if isProviderAddQuotaExhausted(err) {
		return m.classifyQuotaRefusal(err)
	}

	// Content, auth, parse, transport — refuse. The arr moves on.
	return addRefusal{}
}

// classifyQuotaRefusal resolves an add-allowance refusal against the provider's
// actual fill.
//
// Every path that cannot reach a confident "below the cap" refuses, and each
// declines for a NAMED reason rather than falling through silently.
func (m *Manager) classifyQuotaRefusal(err error) addRefusal {
	name := quotaRefusalProvider(err)
	if name == "" {
		// No provider attributable — cannot resolve a cap for nobody.
		return addRefusal{detail: "add allowance exhausted on an unidentified provider; refusing rather than guessing"}
	}

	cfg, ok := m.providerConfig(name)
	if !ok {
		return addRefusal{provider: name, detail: "add allowance exhausted on an unconfigured provider; refusing rather than guessing"}
	}

	capacity, capped := cfg.MagnetCap()
	if !capped {
		// UNLIMITED — no cap configured and none known for this provider. There
		// is no threshold to be at, so the refusal cannot be the stored-item
		// case; treat it as the transient allowance. Deliberately does NOT
		// invent a number to compare against.
		return addRefusal{
			hold:     true,
			provider: name,
			detail:   "add allowance exhausted and no stored-item cap is configured; treating as the transient daily allowance",
		}
	}

	fill, known := m.providerFill(name)
	if !known {
		// The enumeration failed. A count we could not take is not a count, and
		// guessing here picks between "wait forever on a full account" and
		// "refuse work that would have succeeded". Refuse: it is the cheap,
		// visible, recoverable direction.
		return addRefusal{
			provider: name,
			detail:   "add allowance exhausted and the account's fill could not be read; refusing rather than classifying on an unknown",
		}
	}

	if fill >= capacity {
		return addRefusal{
			provider: name,
			detail:   fmt.Sprintf("stored-item cap reached (%d/%d)", fill, capacity),
			standingCondition: fmt.Sprintf(
				"provider %q is holding %d of its %d stored items. This does NOT clear on its own — "+
					"nothing decypharr finishes, waits for, or deletes locally frees it. Delete items on the "+
					"provider, or raise max_magnets if the provider's real cap is higher.",
				name, fill, capacity),
		}
	}

	return addRefusal{
		hold:     true,
		provider: name,
		detail:   fmt.Sprintf("daily add allowance spent while stored fill is %d/%d; it resets on the provider's own boundary", fill, capacity),
	}
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

// providerConfig returns the configured Debrid block for a provider name.
func (m *Manager) providerConfig(name string) (config.Debrid, bool) {
	client := m.ProviderClient(name)
	if client == nil {
		return config.Debrid{}, false
	}
	return client.Config(), true
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

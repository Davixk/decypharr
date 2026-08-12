package manager

import (
	"github.com/sirrobot01/decypharr/internal/config"
)

// THE USENET HALF OF THE CONDEMN→PRUNE DOCTRINE.
//
// Operator's ruling: NNTP 430 No-Such-Article is a confirmed-dead signal — the
// live case was a viewer's stream dying mid-file on one — and dead usenet
// content must behave exactly like a confirmed debrid takedown. Stop serving,
// let the library symlink dangle, let the arr notice on its own scan and
// replace the release. No arr calls.
//
// WHAT WAS ALREADY TRUE, AND WHAT WAS MISSING. The verdict itself was already
// solid and already durable: a 430 reaches recordPermanentArticleFailure only
// after per-segment failover across every configured provider, the file is
// written IsDeleted, the NZB is IsBad/Failed, its cached readers are retired and
// every later read short-circuits to a permanent 410. What was missing was the
// consequence. Nothing touched the storage.Entry, so the entry stayed listed in
// the mount, its library symlink kept resolving, and the *arr went on believing
// it had a file it could not play. The nightly sweep would eventually classify
// it broken — "eventually" being up to a day of a dead episode sitting in the
// library looking fine.
//
// THE THREE JUDGEMENTS, and each is the one the debrid takedown path makes:
//
// HOW MUCH EVIDENCE. One confirmed 430 per file, and no knob for it, unlike the
// debrid side. That asymmetry is deliberate. DebridTakedownThreshold exists
// because a 451 is ONE provider's classification and can be re-tested by reading
// again. A 430 has already been corroborated across every configured provider
// before it arrives here, and the file is marked deleted the moment it does — so
// a second read never reaches a provider and a threshold above one would be a
// number that could never be counted to.
//
// HOW MUCH TO CONDEMN. The whole entry, and only once EVERY file in it is
// confirmed dead. This is the rule that needed the census: markFilePermanently-
// Failed sets IsBad on the FIRST dead file, so an entry with one dead episode
// and twelve live ones carries the identical flag as one with nothing left.
// Condemning on IsBad would delete a thirteen-file pack because one episode
// expired.
//
// WHAT DELETION IS ALLOWED. repair.prune, the operator's existing destructive
// consent — no second knob defaulting to on — plus a rate limit, because unlike
// the debrid case this verdict can arrive en masse from a single provider
// changing its retention. See livePruneBudget.
func (m *Manager) onUsenetDeadContent(nzoID, filename string, dead, total int, cause error) {
	if nzoID == "" {
		return
	}

	logger := m.logger.With().
		Str("nzo_id", nzoID).
		Str("filename", filename).
		Int("dead_files", dead).
		Int("total_files", total).
		Logger()

	if total <= 0 || dead < total {
		// Partially dead, or a census we could not take. The surviving files
		// still stream, so their symlinks stay valid and the arr keeps the
		// release. Said out loud, because a correct refusal to act is otherwise
		// indistinguishable from a broken one.
		logger.Info().Err(cause).
			Msg("Usenet article confirmed permanently missing; the entry still has live files so it stays in place " +
				"and keeps serving them")
		return
	}

	// The nzoID IS the entry's InfoHash for usenet entries — see the usenet add
	// path, which stamps entry.InfoHash = meta.ID. No lookup table, and nothing
	// to fall out of sync.
	entry, err := m.GetEntry(nzoID)
	if err != nil || entry == nil {
		logger.Debug().Err(err).
			Msg("No decypharr entry for a dead usenet NZB; nothing to condemn")
		return
	}

	// MARK FIRST, PRUNE SECOND, AND THE MARK IS UNCONDITIONAL.
	//
	// Bad stops the entry being SERVED. Prune stops it being LISTED. They are
	// different jobs and only one of them is destructive, so only one of them
	// is gated. With prune off, or over budget, or failing, the entry still
	// refuses every read — the honest degradation, and the same shape the
	// debrid takedown path lands in.
	// persistLinkEntryBad, NOT queue.Update. It was queue.Update here first, and
	// that is the wrong-store defect this codebase has already shipped twice: an
	// imported entry being streamed lives in the MAIN store, so the flag landed
	// in the queue store where nothing serving the file would ever read it. The
	// existing primitive writes both stores under generation-safe snapshot
	// mutation and is the identical call the debrid takedown path makes.
	entry.Bad = true
	if saveErr := m.persistLinkEntryBad(entry); saveErr != nil {
		logger.Warn().Err(saveErr).
			Msg("Failed to persist the Bad flag for an NZB whose articles are all confirmed missing")
	}
	logger.Warn().Err(cause).
		Str("name", entry.Name).
		Msg("Every file in this NZB is confirmed missing on every configured provider; condemning the entry")

	if !config.Get().Repair.Prune {
		logger.Warn().
			Str("name", entry.Name).
			Msg("NZB is confirmed dead but repair.prune is off, so it stays in the mount listing and its library " +
				"symlink keeps resolving; enable PRUNE to have the entry removed and the arr re-search it")
		return
	}

	// pruneDeadEntryLive, not pruneTakenDownEntry: the same deletion, behind the
	// live-prune budget shared with the debrid takedown path. ONE place decides
	// whether an event-driven deletion is allowed, so a skipped prune has one
	// explanation and one log line rather than two that can drift apart.
	if pruneErr := m.pruneDeadEntryLive(entry); pruneErr != nil {
		logger.Warn().Err(pruneErr).
			Str("component", "PRUNE").
			Str("name", entry.Name).
			Msg("PRUNE: failed to delete dead NZB entry decypharr-side")
		return
	}
	logger.Info().
		Str("component", "PRUNE").
		Str("name", entry.Name).
		Msg("PRUNE: requested deletion of a dead NZB entry decypharr-side (no arr call is made; if the live-prune " +
			"budget deferred it, the line above says so)")
}

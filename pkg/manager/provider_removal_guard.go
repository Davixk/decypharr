package manager

// Mass-removal circuit breaker for the provider refresh.
//
// 🔴 WHAT THIS STOPS: LOSING A PROVIDER IS NOT THE SAME AS THE PROVIDER LOSING
// EVERYTHING, AND THE REFRESH CANNOT TELL THEM APART.
//
// doRefreshTorrents treats a provider's listing as truth about what it holds.
// An entry whose placement is missing from that listing becomes a removal
// candidate, and removeProviderPlacement deletes the ENTRY — not just the
// placement — when the removed one is its only one:
//
//	if len(current.Providers) == 1 {
//	    deleted, deleteErr := m.storage.DeleteIfCurrent(current)
//
// So a provider that answers 200 with an empty or short list empties the
// library of everything placed on it. With arr_delete enabled that is also one
// arr deletion and one fresh indexer search per entry.
//
// ⚠️ THIS IS NOT HYPOTHETICAL AND THE TRIGGER IS ROUTINE. A lapsed subscription,
// a revoked key that still answers, an account in a read-only state, a provider
// mid-incident serving an empty collection — each is a successful HTTP response
// carrying no rows. An outright ERROR is already safe: doRefreshTorrents returns
// before removals are computed. It is the successful-but-empty answer that is
// dangerous, and before this guard the only thing standing between it and the
// library was a debug log:
//
//	if len(remote) == 0 { m.logger.Debug()...Msg("No remote found") }
//
// Third instance of the same class in this codebase, after config.Limit
// truncating an enumeration and returning success, and providerDumpMaxItems
// capping a report. This is the one that deletes.
//
// 🔑 THE RULE IT ENCODES is the one restore_reconcile.go already states as
// load-bearing — "ABSENCE IS NOT EVIDENCE" — applied where absence is actually
// destructive. A removal batch that would take out a large fraction of what we
// hold on a provider is not evidence the account was emptied; it is evidence the
// enumeration is not describing the account.
//
// The safe direction is explicit: refusing leaves stale placements, which cost
// failed reads and are repaired by the next good enumeration. Acting wrongly
// deletes content and spends an indexer search per entry to get it back. Those
// are not close.

const (
	// massRemovalFloor is the batch size below which no fraction test applies.
	//
	// Ordinary churn — an item deleted on the provider, a torrent replaced, a
	// handful pruned — must pass untouched, or the guard becomes a brake on
	// normal operation. Anything under this is normal by inspection.
	massRemovalFloor = 100

	// massRemovalFraction is the share of a provider's locally-held placements
	// that one refresh may remove before the batch is treated as a bad
	// enumeration rather than a real change.
	//
	// A tenth is deliberately generous: deleting 10% of a provider's library in
	// the window between two refreshes is already extraordinary, and the cost of
	// pausing on a real one is a delay. There is no operation decypharr performs
	// that legitimately produces a larger batch — the repair sweep's own
	// deletions have their own cap (max_deletions_per_run) and do not route
	// through here.
	massRemovalFraction = 10
)

// removalBatchIsImplausible reports whether a refresh's removal batch is too
// large to be believed, given how many placements we hold on that provider.
//
// heldLocally is the number of entries carrying a placement on this provider —
// the denominator that makes the batch interpretable. Without it a batch of
// 5,000 is indistinguishable from a busy day on a huge account.
func removalBatchIsImplausible(removals, heldLocally int) bool {
	if removals <= massRemovalFloor {
		return false
	}
	if heldLocally <= 0 {
		// We hold nothing on this provider, so a large batch cannot be about
		// it. Nothing to protect; let the ordinary path deal with it.
		return false
	}
	return removals*massRemovalFraction > heldLocally
}

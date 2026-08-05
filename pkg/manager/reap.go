package manager

import (
	"fmt"
	"strings"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// REAPING — what decypharr does with a queue row it has given up on.
//
// 🔑 DECYPHARR IS THE DOWNLOAD CLIENT. THAT IS THE WHOLE DOCTRINE.
//
// The operator's words: decypharr marks the download FAILED through the shim
// (qBittorrent or SABnzbd) and PARKS IT THERE as failed, until the *arr finds it
// by polling and acts. decypharr never calls an *arr API — no blocklisting, no
// re-searching, no queue manipulation. What the *arr does with a failed download
// is entirely the *arr's business, configured on the *arr.
//
// ⚠️ WHICH MEANS ROW DELETION IS NOT DECYPHARR'S ACT AT ALL. A real download
// client does not delete its own rows because it decided a download was bad; it
// shows the failure and waits for the client's owner to deal with it. Rows leave
// the queue store by exactly three routes:
//
//	1. the *arr deletes it through the shim's own delete API  (the normal route)
//	2. completion migration                                    (it succeeded)
//	3. a synchronous add-refusal                               (the row never existed for the arr)
//
// 🔴 THE DEFECT THIS FIXES. A cleanup sweep ran every 60 seconds and DELETED
// rows autonomously, with a nil cleanup — dropping the local row and telling
// nobody. Measured over 24h: 15,004 rows removed against 78 + 13 = 91
// downloadFailed events at the two *arrs. ~99% of removals informed no one.
//
// The mechanism is worth stating precisely, because it was misdiagnosed twice.
// The *arr learns by POLLING: it reads the shim, sees a row in state=error, and
// records the failure itself. That pull IS the design. So deleting the row does
// not merely skip a notification — it destroys the only evidence the *arr was
// ever going to read, and a vanished queue item reads as removed-by-user, not
// failed: no blocklist, no re-search, the episode silently stays missing.
//
// It follows that the prune was CORRECT ALL ALONG — release the placement, mark
// the row failed, stop — and that the fix is simply to stop other sweeps from
// deleting what it parked.

// 🔑 REAP vs PARK — and "reap" now means MARK FAILED AND LEAVE IT, not remove.
//
//	A REAP IS A VERDICT ON THE RELEASE — "this will not deliver". ETA past the
//	ceiling after a fair sampling window, failsafe age, dead swarm, content the
//	provider does not have. The row is marked failed so the *arr can see it.
//
//	CAPACITY / RATE / TRANSPORT CONDITIONS ARE OUR STATE, NOT A VERDICT ON ANY
//	RELEASE. They must never mark a row failed, because a failed row invites the
//	*arr to blocklist a release that was never bad. They park under the existing
//	machinery: capacity -> the hold ledger, rate -> the AIMD pacer, transient ->
//	decline backoff, 451 -> a provider-scoped cooldown.
//
// ⚠️ BIAS ON UNCERTAINTY: WHEN A SWEEP CANNOT CLASSIFY, IT PARKS. Wrongly parked
// costs one retry cycle. Wrongly failed invites a blocklist on a good release
// and spends a full indexer search replacing something that was fine.

// reapVerdict answers "is this a statement about the release?".
type reapVerdict int

const (
	// reapPark — not a verdict. Leave the row exactly as it is.
	reapPark reapVerdict = iota
	// reapFail — a genuine release verdict. Mark the row failed and park it
	// there for the *arr to find.
	reapFail
)

func (v reapVerdict) String() string {
	if v == reapFail {
		return "fail"
	}
	return "park"
}

// classifyReap decides whether an errored row represents a release verdict.
//
// The DEFAULT IS PARK — an error nobody taught this function about must not cost
// a release its place in the library.
func classifyReap(entry *storage.Entry) (reapVerdict, string) {
	if entry == nil {
		return reapPark, "no entry"
	}

	// ⚠️ NEVER PLACED = QUEUED, AND QUEUED IS UNTOUCHABLE.
	//
	// The operator's doctrine is that no reaper touches a queued entry. The old
	// sweep honoured that by skipping provider status "queued" — but a row that
	// never got a placement has an EMPTY provider status, not "queued", so it
	// fell straight through and was deleted. Under a provider storm, "never got
	// a placement" IS the normal condition, which is how a doctrine that reads
	// as honoured swept away thousands of never-dispatched rows.
	if placementIDOf(entry) == "" && strings.TrimSpace(string(entry.Status)) == "" {
		return reapPark, "never placed on a provider (queued)"
	}

	reason := strings.ToLower(entry.LastError)
	switch {
	case reason == "":
		return reapPark, "error state carries no reason"

	// ── OUR STATE, NOT THE RELEASE'S FAULT. Park, every one. ──────────────────
	case containsAny(reason, "too many active", "quota", "capacity", "no available slot"):
		return reapPark, "provider capacity — the hold ledger owns this"
	case containsAny(reason, "429", "503", "slow down", "too many requests", "rate limit"):
		return reapPark, "provider rate limit — the pacer owns this"
	case containsAny(reason, "timeout", "deadline exceeded", "connection refused",
		"connection reset", "eof", "no such host", "temporarily unavailable"):
		return reapPark, "transport failure — says nothing about the release"
	case containsAny(reason, "451", "infringing"):
		// Provider-scoped, deliberately NOT a release verdict: another provider
		// may serve the same release perfectly well.
		return reapPark, "provider refused this release — provider-scoped cooldown, not a global verdict"

	// ── VERDICTS ON THE RELEASE. These get marked failed for the arr. ─────────
	case containsAny(reason, "will not finish", "eta ", "stalled", "no progress"):
		return reapFail, "will not deliver: " + entry.LastError
	case containsAny(reason, "dead swarm", "no seeders", "no peers"):
		return reapFail, "dead swarm: " + entry.LastError
	case containsAny(reason, "articles missing", "not cached", "unavailable on provider"):
		return reapFail, "content unavailable: " + entry.LastError
	}

	return reapPark, "unclassified error — parking rather than inviting a blocklist on a possibly-good release"
}

func containsAny(haystack string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(haystack, needle) {
			return true
		}
	}
	return false
}

// markFailedAndPark is what a reap now IS: set the row to failed so the shim
// presents it as a failed download, write it back, and leave it alone.
//
// ⚠️ IT NEVER DELETES, AND THAT IS THE ENTIRE POINT. The parked row is the
// message. It stays until the *arr collects it through the shim's delete API,
// exactly as a real download client behaves.
//
// Returns true when the row was newly marked failed, so sweeps can report what
// they actually did rather than logging a count of nothing.
func (m *Manager) markFailedAndPark(entry *storage.Entry, source string) (bool, error) {
	if entry == nil {
		return false, nil
	}
	verdict, why := classifyReap(entry)
	if verdict == reapPark {
		m.logger.Debug().
			Str("source", source).
			Str("infohash", entry.InfoHash).
			Str("reason", why).
			Msg("Not a verdict on the release; leaving the row for the cooldown/hold machinery")
		return false, nil
	}

	if entry.State == storage.EntryStateError && strings.TrimSpace(entry.LastError) != "" {
		// Already presented as failed. Re-writing it would churn the row and
		// reset nothing useful.
		return false, nil
	}

	entry.MarkAsError(fmt.Errorf("%s", why))
	if err := m.queue.Update(entry); err != nil {
		return false, fmt.Errorf("mark failed %s: %w", entry.InfoHash, err)
	}
	m.logger.Info().
		Str("source", source).
		Str("infohash", entry.InfoHash).
		Str("reason", why).
		Msg("Marked failed and parked for the arr to collect; decypharr takes no further action")
	return true, nil
}

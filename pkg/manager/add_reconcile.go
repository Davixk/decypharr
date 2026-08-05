package manager

import (
	"strings"
	"sync"
	"time"

	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// PENDING ADDS — closing the received-but-lost window.
//
// THE HOLE. `addMagnet` is a POST. When the provider receives it and creates the
// transfer but the response dies on the wire, we record a failure and nothing
// else — while the provider holds a live transfer whose ID we never learned. No
// retry is needed for this: attempt one is enough. Retrying only changes how
// many chances there are for it to happen.
//
// So the fix cannot be "retry less". The fix is that WE ALWAYS KNOW THE
// INFOHASH WE JUST SUBMITTED, which makes the add idempotent by reconciliation:
// on any ambiguous outcome, ask the provider whether it has that hash. Present
// means the add SUCCEEDED and we recover the ID. Absent means it genuinely
// failed.
//
// ⚠️ THIS IS NOT ADOPTION, AND THE DISTINCTION IS THE OPERATOR'S RULE.
//
// Adoption — claiming transfers decypharr did not start — is forbidden, because
// it presumes exclusive ownership of the provider account and would silently
// seize another client's downloads. This does the opposite: it is scoped to a
// ledger of hashes THIS process submitted seconds ago, keyed by the exact hash
// it submitted, and it recovers only those. "decypharr knows what it starts and
// does not lose track of it" — including when the network eats the receipt.
//
// Anything not in the ledger is invisible here. A foreign transfer can never be
// matched, because we never wrote it down.

// pendingAddTTL bounds how long a submitted hash stays reconcilable.
//
// Short on purpose. It exists to cover a lost response, which resolves in
// seconds, and a long window would start to resemble a claim on anything that
// happened to appear later — which is the line this must not cross.
const pendingAddTTL = 5 * time.Minute

type pendingAdd struct {
	provider string
	infoHash string
	at       time.Time
}

// pendingAddLedger records adds that are in flight.
type pendingAddLedger struct {
	mu      sync.Mutex
	entries map[string]pendingAdd
}

func newPendingAddLedger() *pendingAddLedger {
	return &pendingAddLedger{entries: map[string]pendingAdd{}}
}

func pendingKey(provider, infoHash string) string {
	return provider + "\x00" + strings.ToLower(infoHash)
}

// begin records an add BEFORE the request goes out. Written first on purpose:
// a crash between the write and the POST leaves a harmless stale entry, while
// the reverse order leaves an unrecoverable transfer.
func (l *pendingAddLedger) begin(provider, infoHash string) {
	if l == nil || infoHash == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries[pendingKey(provider, infoHash)] = pendingAdd{
		provider: provider,
		infoHash: strings.ToLower(infoHash),
		at:       time.Now(),
	}
}

// resolve clears an add whose outcome is known, ambiguous or not.
func (l *pendingAddLedger) resolve(provider, infoHash string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.entries, pendingKey(provider, infoHash))
}

// pending reports whether this hash is still within its reconcilable window,
// and WHEN it was submitted.
//
// The timestamp is not incidental. A listing fetched before the submission
// cannot answer "is it there?" in the negative, so every caller needs to know
// how old the answer is allowed to be. See reconcileListing.fromCacheLocked.
func (l *pendingAddLedger) pending(provider, infoHash string, now time.Time) (time.Time, bool) {
	if l == nil {
		return time.Time{}, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.entries[pendingKey(provider, infoHash)]
	if !ok {
		return time.Time{}, false
	}
	if now.Sub(entry.at) > pendingAddTTL {
		delete(l.entries, pendingKey(provider, infoHash))
		return time.Time{}, false
	}
	return entry.at, true
}

// expired returns and clears entries past the window, for the final lookup.
func (l *pendingAddLedger) expired(now time.Time) []pendingAdd {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []pendingAdd
	for key, entry := range l.entries {
		if now.Sub(entry.at) > pendingAddTTL {
			out = append(out, entry)
			delete(l.entries, key)
		}
	}
	return out
}

// reconcileListingTTL is how long one account listing answers hash lookups.
//
// A storm of ambiguous adds must not become a storm of full enumerations —
// that is the same thundering-herd mistake the admission cap fixes, one layer
// down. One listing serves every reconcile in the window.
const reconcileListingTTL = 30 * time.Second

// providerMatch is one provider-side transfer carrying a hash we submitted.
//
// ⚠️ THERE CAN BE MORE THAN ONE, AND THIS IS MEASURED, NOT THEORETICAL. Pruning
// live RealDebrid strays turned up two infohashes each holding TWO distinct
// transfer ids (7c07c505… → F6TVAJLLLRQ6I + T4LOUDQDI76PQ; 3a3f44ea… →
// VDRJ2STDL6CW2 + RMDJA7NB3JHFM). RealDebrid does NOT deduplicate by infohash,
// so a retried add that landed twice leaves two transfers, both consuming a slot.
type providerMatch struct {
	id       string
	progress float64
	status   debridTypes.TorrentStatus
}

type reconcileListing struct {
	mu       sync.Mutex
	byHash   map[string]map[string][]providerMatch // provider -> infohash -> matches
	fetched  map[string]time.Time
	inflight map[string]*sync.WaitGroup
}

func newReconcileListing() *reconcileListing {
	return &reconcileListing{
		byHash:   map[string]map[string][]providerMatch{},
		fetched:  map[string]time.Time{},
		inflight: map[string]*sync.WaitGroup{},
	}
}

// bestMatch picks the transfer to keep when a hash resolves to several.
//
// MOST-ADVANCED WINS, and the tie-break is deliberate rather than arbitrary: a
// transfer that is already downloading or finished represents real work the
// provider has done, and discarding it to keep a fresher duplicate would throw
// that away. Ties fall back to the lexicographically smallest id purely so the
// choice is STABLE — two runs against the same account must not disagree about
// which transfer is ours.
func bestMatch(matches []providerMatch) (providerMatch, []providerMatch) {
	best := 0
	for i := 1; i < len(matches); i++ {
		switch {
		case matches[i].progress > matches[best].progress:
			best = i
		case matches[i].progress < matches[best].progress:
		case matches[i].id < matches[best].id:
			best = i
		}
	}
	extras := make([]providerMatch, 0, len(matches)-1)
	for i, m := range matches {
		if i != best {
			extras = append(extras, m)
		}
	}
	return matches[best], extras
}

// fromCacheLocked answers from the cached listing, but only when that listing is
// entitled to answer.
//
// ⚠️ A HIT AND A MISS DO NOT HAVE THE SAME EVIDENTIAL VALUE, and treating them
// alike reintroduces the exact orphan this file exists to prevent.
//
//	HIT  — our own hash appearing in ANY snapshot proves the provider has it.
//	       Always meaningful, however old the snapshot is.
//	MISS — only meaningful if the snapshot was taken AFTER we submitted. A
//	       listing fetched before the POST could not possibly have contained the
//	       hash, so reading its silence as "confirmed absent" would declare a
//	       clean failure on a transfer that actually landed.
//
// The dangerous case is not hypothetical and not rare — it is the storm. One
// ambiguous add fetches a snapshot; the next ambiguous add, seconds later, hits
// that same cached snapshot, which predates it entirely.
func (r *reconcileListing) fromCacheLocked(name, hash string, since, now time.Time) ([]providerMatch, bool) {
	at, ok := r.fetched[name]
	if !ok || now.Sub(at) >= reconcileListingTTL {
		return nil, false
	}
	if matches := r.byHash[name][hash]; len(matches) > 0 {
		return matches, true
	}
	if at.After(since) {
		return nil, true
	}
	// Absent from a listing older than the submission: no information at all.
	return nil, false
}

// lookup returns the provider's ID for a hash, and whether the listing could
// answer the question.
//
// The second return is KNOWN. False never means "the provider does not have it"
// — it means we could not ask, and a caller must treat that as "still
// ambiguous" rather than as a confirmed failure.
//
// `since` is when the add was submitted; see fromCacheLocked for why a cached
// listing older than that cannot settle a miss.
func (r *reconcileListing) lookup(name string, client debrid.Client, infoHash string, since, now time.Time) ([]providerMatch, bool) {
	if r == nil || client == nil {
		return nil, false
	}
	hash := strings.ToLower(infoHash)

	// Bounded, and exhaustion returns UNKNOWN rather than "absent" — the safe
	// direction. Normal paths settle in at most three passes: wait on someone
	// else's fetch, find it predates us, fetch our own.
	for range 4 {
		r.mu.Lock()
		if matches, known := r.fromCacheLocked(name, hash, since, now); known {
			r.mu.Unlock()
			return matches, true
		}
		if wg, ok := r.inflight[name]; ok {
			// Single-flight: one enumeration serves every waiter, so a storm of
			// ambiguous adds cannot become a storm of full listings. On waking we
			// re-test rather than trusting the result, because the fetch we
			// waited on may itself have started before our submission.
			r.mu.Unlock()
			wg.Wait()
			continue
		}
		wg := &sync.WaitGroup{}
		wg.Add(1)
		r.inflight[name] = wg
		r.mu.Unlock()

		torrents, err := client.GetAllTorrents()
		fetchedAt := time.Now()

		r.mu.Lock()
		delete(r.inflight, name)
		if err == nil {
			index := make(map[string][]providerMatch, len(torrents))
			for _, t := range torrents {
				if t == nil || t.InfoHash == "" || t.Id == "" {
					continue
				}
				// APPEND, never overwrite: a hash can legitimately resolve to
				// several transfers, and keeping only the last one seen would
				// hide the duplicates rather than let us clean them up.
				key := strings.ToLower(t.InfoHash)
				index[key] = append(index[key], providerMatch{
					id:       t.Id,
					progress: t.Progress,
					status:   t.Status,
				})
			}
			r.byHash[name] = index
			r.fetched[name] = fetchedAt
		}
		r.mu.Unlock()
		wg.Done()

		if err != nil {
			return nil, false
		}
		// Our own fetch strictly follows the submission, so on the next pass the
		// cache is authoritative in BOTH directions.
	}
	return nil, false
}

// reconcileAmbiguousAdd asks whether a submitted hash actually landed.
//
// Returns the provider's ID when the add succeeded despite appearing to fail.
// Returns "" when the provider demonstrably does not have it, OR when we could
// not ask — the two are deliberately collapsed for the CALLER's purposes,
// because both mean "no ID to recover here"; they are distinguished in the log
// so an unreachable provider is not mistaken for a clean failure.
func (m *Manager) reconcileAmbiguousAdd(providerName, infoHash string) string {
	if infoHash == "" {
		return ""
	}
	submittedAt, ok := m.pendingAdds.pending(providerName, infoHash, time.Now())
	if !ok {
		return ""
	}
	client := m.ProviderClient(providerName)
	if client == nil {
		return ""
	}

	// submittedAt is what keeps a stale snapshot from answering "absent" for a
	// hash it could never have contained.
	matches, known := m.reconcileList.lookup(providerName, client, infoHash, submittedAt, time.Now())
	if !known {
		m.logger.Warn().
			Str("provider", providerName).
			Str("infohash", infoHash).
			Msg("Add outcome is ambiguous and the provider could not be listed to settle it. If the add " +
				"actually landed, that transfer is now untracked — the provider stall prune will reap it.")
		return ""
	}
	if len(matches) == 0 {
		// Confirmed absent. The add genuinely failed; nothing was created.
		return ""
	}

	keep, extras := bestMatch(matches)

	// ⚠️ MORE THAN ONE TRANSFER FOR ONE HASH — measured on RealDebrid, which does
	// NOT deduplicate by infohash. Every duplicate holds its own download slot, so
	// leaving them is not merely untidy: it is the capacity leak wearing yet
	// another hat. We claim one and release the rest.
	//
	// Deleting is safe HERE specifically, and nowhere else: these ids came back
	// keyed by a hash this process submitted seconds ago and recorded in the
	// pending ledger, so they are all our own duplicate submissions. This is not
	// licence to delete provider-side transfers in general.
	for _, extra := range extras {
		if err := client.DeleteTorrent(extra.id); err != nil {
			m.logger.Warn().Err(err).
				Str("provider", providerName).
				Str("infohash", infoHash).
				Str("duplicate_id", extra.id).
				Msg("A duplicate transfer of our own add could not be released; it will keep holding a slot " +
					"until the provider stall prune reaches it")
			continue
		}
		m.logger.Warn().
			Str("provider", providerName).
			Str("infohash", infoHash).
			Str("kept_id", keep.id).
			Str("deleted_id", extra.id).
			Float64("kept_progress", keep.progress).
			Float64("deleted_progress", extra.progress).
			Msg("Our add landed MORE THAN ONCE — the provider does not deduplicate by infohash. " +
				"Kept the most-advanced transfer and released the duplicate.")
	}

	m.logger.Warn().
		Str("provider", providerName).
		Str("infohash", infoHash).
		Str("recovered_id", keep.id).
		Int("matches", len(matches)).
		Msg("Add reported failure but the provider HAS the transfer — the response was lost, not the request. " +
			"Recovered its id instead of leaving it untracked.")
	return keep.id
}

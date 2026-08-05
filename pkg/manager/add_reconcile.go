package manager

import (
	"strings"
	"sync"
	"time"

	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
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

type reconcileListing struct {
	mu       sync.Mutex
	byHash   map[string]map[string]string // provider -> infohash -> id
	fetched  map[string]time.Time
	inflight map[string]*sync.WaitGroup
}

func newReconcileListing() *reconcileListing {
	return &reconcileListing{
		byHash:   map[string]map[string]string{},
		fetched:  map[string]time.Time{},
		inflight: map[string]*sync.WaitGroup{},
	}
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
func (r *reconcileListing) fromCacheLocked(name, hash string, since, now time.Time) (string, bool) {
	at, ok := r.fetched[name]
	if !ok || now.Sub(at) >= reconcileListingTTL {
		return "", false
	}
	if id := r.byHash[name][hash]; id != "" {
		return id, true
	}
	if at.After(since) {
		return "", true
	}
	// Absent from a listing older than the submission: no information at all.
	return "", false
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
func (r *reconcileListing) lookup(name string, client debrid.Client, infoHash string, since, now time.Time) (string, bool) {
	if r == nil || client == nil {
		return "", false
	}
	hash := strings.ToLower(infoHash)

	// Bounded, and exhaustion returns UNKNOWN rather than "absent" — the safe
	// direction. Normal paths settle in at most three passes: wait on someone
	// else's fetch, find it predates us, fetch our own.
	for range 4 {
		r.mu.Lock()
		if id, known := r.fromCacheLocked(name, hash, since, now); known {
			r.mu.Unlock()
			return id, true
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
			index := make(map[string]string, len(torrents))
			for _, t := range torrents {
				if t == nil || t.InfoHash == "" || t.Id == "" {
					continue
				}
				index[strings.ToLower(t.InfoHash)] = t.Id
			}
			r.byHash[name] = index
			r.fetched[name] = fetchedAt
		}
		r.mu.Unlock()
		wg.Done()

		if err != nil {
			return "", false
		}
		// Our own fetch strictly follows the submission, so on the next pass the
		// cache is authoritative in BOTH directions.
	}
	return "", false
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
	id, known := m.reconcileList.lookup(providerName, client, infoHash, submittedAt, time.Now())
	if !known {
		m.logger.Warn().
			Str("provider", providerName).
			Str("infohash", infoHash).
			Msg("Add outcome is ambiguous and the provider could not be listed to settle it. If the add " +
				"actually landed, that transfer is now untracked — the provider stall prune will reap it.")
		return ""
	}
	if id == "" {
		// Confirmed absent. The add genuinely failed; nothing was created.
		return ""
	}

	m.logger.Warn().
		Str("provider", providerName).
		Str("infohash", infoHash).
		Str("recovered_id", id).
		Msg("Add reported failure but the provider HAS the transfer — the response was lost, not the request. " +
			"Recovered its id instead of leaving it untracked.")
	return id
}

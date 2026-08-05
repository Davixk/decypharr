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

// pending reports whether this hash is still within its reconcilable window.
func (l *pendingAddLedger) pending(provider, infoHash string, now time.Time) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.entries[pendingKey(provider, infoHash)]
	if !ok {
		return false
	}
	if now.Sub(entry.at) > pendingAddTTL {
		delete(l.entries, pendingKey(provider, infoHash))
		return false
	}
	return true
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

// lookup returns the provider's ID for a hash, and whether the listing could be
// obtained at all.
//
// The second return is KNOWN. False never means "the provider does not have it"
// — it means we could not ask, and a caller must treat that as "still
// ambiguous" rather than as a confirmed failure.
func (r *reconcileListing) lookup(name string, client debrid.Client, infoHash string, now time.Time) (string, bool) {
	if r == nil || client == nil {
		return "", false
	}
	hash := strings.ToLower(infoHash)

	r.mu.Lock()
	if at, ok := r.fetched[name]; ok && now.Sub(at) < reconcileListingTTL {
		id := r.byHash[name][hash]
		r.mu.Unlock()
		return id, true
	}
	if wg, ok := r.inflight[name]; ok {
		r.mu.Unlock()
		wg.Wait()
		r.mu.Lock()
		id, ok := "", false
		if _, fresh := r.fetched[name]; fresh {
			id, ok = r.byHash[name][hash], true
		}
		r.mu.Unlock()
		return id, ok
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	r.inflight[name] = wg
	r.mu.Unlock()

	torrents, err := client.GetAllTorrents()

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
		r.fetched[name] = now
	}
	id, known := "", false
	if _, fresh := r.fetched[name]; fresh && err == nil {
		id, known = r.byHash[name][hash], true
	}
	r.mu.Unlock()
	wg.Done()
	return id, known
}

// invalidate drops a provider's cached listing, so a reconcile after a known
// change re-reads rather than answering from a snapshot taken before it.
func (r *reconcileListing) invalidate(name string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	delete(r.fetched, name)
	r.mu.Unlock()
}

// reconcileAmbiguousAdd asks whether a submitted hash actually landed.
//
// Returns the provider's ID when the add succeeded despite appearing to fail.
// Returns "" when the provider demonstrably does not have it, OR when we could
// not ask — the two are deliberately collapsed for the CALLER's purposes,
// because both mean "no ID to recover here"; they are distinguished in the log
// so an unreachable provider is not mistaken for a clean failure.
func (m *Manager) reconcileAmbiguousAdd(providerName, infoHash string) string {
	if infoHash == "" || !m.pendingAdds.pending(providerName, infoHash, time.Now()) {
		return ""
	}
	client := m.ProviderClient(providerName)
	if client == nil {
		return ""
	}

	id, known := m.reconcileList.lookup(providerName, client, infoHash, time.Now())
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

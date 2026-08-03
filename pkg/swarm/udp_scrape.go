package swarm

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

// UDP TRACKER SCRAPE — BEP 15.
//
// The only source that answers with the swarm as it is RIGHT NOW. That matters
// because this runs inside a synchronous grab: an index's cached number, however
// convenient, describes a swarm that may have died two days ago, and refusing a
// release on a stale count is a guess wearing a measurement's clothes.
//
// ⚠️⚠️ THIS REVEALS THE HOST'S IP TO EVERY TRACKER IT CONTACTS. ⚠️⚠️
//
// A scrape is a direct UDP conversation with a third party that logs who asked.
// Nothing else in decypharr talks to a tracker — the whole point of a debrid
// service is that the provider faces the swarm and we never do. Turning this on
// undoes that for one packet per grab.
//
// Hence BindAddr. An operator whose other indexing tools sit behind a VPN can
// point this at the VPN interface's local address so the scrape egresses the
// same way. It is deliberately NOT defaulted to anything: there is no address
// this code could pick that is right for someone else's network, and silently
// choosing the container's default route is precisely the leak. The feature is
// opt-in at the config layer for the same reason.
//
// SOCKS5 is not supported and that is not an oversight — proxying UDP requires
// UDP ASSOCIATE, which most SOCKS5 endpoints do not implement, and a proxy that
// silently fell back to a direct connection would leak while appearing not to.
// A bind address either works or fails visibly.

const (
	// bep15ProtocolID is the fixed magic every connect request must carry.
	bep15ProtocolID = int64(0x41727101980)

	actionConnect = uint32(0)
	actionScrape  = uint32(2)
	actionError   = uint32(3)
)

// UDPScrape reads swarm counts from a pool of UDP trackers.
type UDPScrape struct {
	// LocalAddr supplies the local address to egress from, resolved fresh per
	// dial. Nil, or a nil address with no error, uses the OS default route.
	//
	// A FUNCTION rather than a fixed address on purpose: an interface name has
	// to be resolved at connect time so a reconnected tunnel is picked up, and
	// so a tunnel that has GONE returns an error instead of a stale address
	// that no longer routes. An error here fails the scrape — never a fallback.
	LocalAddr func() (net.Addr, error)
	// PerTracker bounds one tracker's full connect+scrape exchange.
	PerTracker time.Duration

	// Trackers is the configured pool, used IN ADDITION to whatever the magnet
	// carries.
	//
	// It was originally a fallback for DHT-only magnets. Measured on a live
	// deployment, that assumption was wrong in the strongest possible way:
	// 3,276 of 3,276 stored magnets carried ZERO tr= entries, so the "fallback"
	// is in practice the ONLY source of trackers that deployment will ever
	// have. A gate whose tracker list is always empty is not conservative, it
	// is inert — and indistinguishable from a working one.
	Trackers []string

	// PerLookup caps how many trackers one lookup asks. Asking the whole pool
	// every time would multiply our request rate against EVERY member of it,
	// which is the opposite of what a pool is for. Rotating a small subset
	// spreads load so no single tracker sees our full grab rate.
	PerLookup int

	mu sync.Mutex
	// blockedUntil is per-tracker adaptive backoff. A tracker that stops
	// answering has almost certainly rate-limited us, and continuing to ask
	// both deepens the penalty and burns the lookup budget on a host that will
	// not reply. Measured: 8 back-to-back scrapes answered 2, and spacing them
	// 4s apart answered 0 — the penalty is cumulative, so the only useful
	// response is to stop asking that tracker for a while.
	blockedUntil map[string]time.Time
	failures     map[string]int
	rotation     int
}

const (
	// trackerBackoffBase is the first penalty after a failed exchange.
	trackerBackoffBase = 30 * time.Second
	// trackerBackoffMax caps it, so a tracker that recovers is retried within a
	// bounded time rather than being written off for the process lifetime.
	trackerBackoffMax = 15 * time.Minute
	// defaultPerLookup is how many trackers a single lookup asks.
	defaultPerLookup = 3
)

func (u *UDPScrape) Name() string { return "udp_scrape" }

// Lookup scrapes every UDP tracker in parallel and takes the HIGHEST count.
//
// Highest, not first or average, because trackers disagree by design: each sees
// only the peers that announced to it, so a low reading is a partial view of the
// swarm and not evidence about its size. Taking the maximum means a tracker that
// knows nothing cannot condemn a torrent another tracker can see is healthy.
func (u *UDPScrape) Lookup(ctx context.Context, infoHash string, trackers []string) (Metadata, bool) {
	hash, err := hex.DecodeString(strings.ToLower(strings.TrimSpace(infoHash)))
	if err != nil || len(hash) != 20 {
		return Metadata{}, false
	}

	// The torrent's OWN trackers first — they are the ones that actually track
	// this swarm — then the configured pool. On a deployment whose magnets
	// carry no announce list at all, the pool is the entire list.
	endpoints := udpEndpoints(append(append([]string{}, trackers...), u.Trackers...))
	if len(endpoints) == 0 {
		// Nobody to ask. An ABSENCE, which says nothing about the swarm.
		return Metadata{}, false
	}
	endpoints = u.selectEndpoints(endpoints, time.Now())
	if len(endpoints) == 0 {
		// Every candidate is in backoff. Still an absence — and specifically
		// evidence about the TRACKERS, not about this torrent.
		return Metadata{}, false
	}

	perTracker := u.PerTracker
	if perTracker <= 0 {
		perTracker = time.Second
	}

	type result struct {
		md Metadata
		ok bool
	}
	results := make([]result, len(endpoints))
	var wg sync.WaitGroup
	for i, endpoint := range endpoints {
		wg.Add(1)
		go func() {
			defer wg.Done()
			md, ok := u.scrapeOne(ctx, endpoint, hash, perTracker)
			// A failure here is evidence about the TRACKER, not the swarm, so
			// it feeds the backoff rather than the reading.
			u.recordOutcome(endpoint, ok, time.Now())
			results[i] = result{md: md, ok: ok}
		}()
	}
	wg.Wait()

	best := Metadata{Source: u.Name()}
	answered := false
	for _, r := range results {
		if !r.ok {
			continue
		}
		answered = true
		if r.md.Seeders > best.Seeders {
			best.Seeders = r.md.Seeders
			best.Completed = r.md.Completed
			best.Leechers = r.md.Leechers
		}
	}
	if !answered {
		// Every tracker failed or timed out. Unknown, never zero.
		return Metadata{}, false
	}
	return best, true
}

// selectEndpoints drops trackers in backoff and rotates through the rest.
//
// Rotation is what makes this a pool rather than a list: without it every
// lookup would hammer the same first N trackers and the remainder would never
// absorb any load, which is exactly the single-tracker rate limit again with
// more configuration.
func (u *UDPScrape) selectEndpoints(endpoints []string, now time.Time) []string {
	limit := u.PerLookup
	if limit <= 0 {
		limit = defaultPerLookup
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	available := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if until, blocked := u.blockedUntil[endpoint]; blocked && now.Before(until) {
			continue
		}
		available = append(available, endpoint)
	}
	if len(available) == 0 || len(available) <= limit {
		return available
	}

	start := u.rotation % len(available)
	u.rotation = (u.rotation + limit) % len(available)

	picked := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		picked = append(picked, available[(start+i)%len(available)])
	}
	return picked
}

// recordOutcome advances or clears a tracker's penalty.
func (u *UDPScrape) recordOutcome(endpoint string, ok bool, now time.Time) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.blockedUntil == nil {
		u.blockedUntil = make(map[string]time.Time)
		u.failures = make(map[string]int)
	}
	if ok {
		delete(u.blockedUntil, endpoint)
		delete(u.failures, endpoint)
		return
	}
	u.failures[endpoint]++
	backoff := trackerBackoffBase << min(u.failures[endpoint]-1, 16)
	if backoff > trackerBackoffMax || backoff <= 0 {
		backoff = trackerBackoffMax
	}
	u.blockedUntil[endpoint] = now.Add(backoff)
}

// scrapeOne performs the two-step BEP 15 exchange against one tracker.
func (u *UDPScrape) scrapeOne(ctx context.Context, endpoint string, hash []byte, budget time.Duration) (Metadata, bool) {
	deadline := time.Now().Add(budget)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}

	dialer := &net.Dialer{Deadline: deadline}
	if u.LocalAddr != nil {
		local, err := u.LocalAddr()
		if err != nil {
			// A binding that cannot be honoured must FAIL, not silently fall
			// back to the default route. Falling back sends this packet
			// exactly where it was configured not to go, and says nothing.
			return Metadata{}, false
		}
		if local != nil {
			dialer.LocalAddr = local
		}
	}

	conn, err := dialer.DialContext(ctx, "udp", endpoint)
	if err != nil {
		return Metadata{}, false
	}
	defer conn.Close()
	_ = conn.SetDeadline(deadline)

	// --- connect ---
	connectTxn, err := transactionID()
	if err != nil {
		return Metadata{}, false
	}
	req := make([]byte, 16)
	binary.BigEndian.PutUint64(req[0:8], uint64(bep15ProtocolID))
	binary.BigEndian.PutUint32(req[8:12], actionConnect)
	binary.BigEndian.PutUint32(req[12:16], connectTxn)
	if _, err := conn.Write(req); err != nil {
		return Metadata{}, false
	}

	resp := make([]byte, 16)
	n, err := conn.Read(resp)
	if err != nil || n < 16 {
		return Metadata{}, false
	}
	if binary.BigEndian.Uint32(resp[0:4]) != actionConnect ||
		binary.BigEndian.Uint32(resp[4:8]) != connectTxn {
		// Wrong action, or a reply to somebody else's transaction. Both mean we
		// have no trustworthy answer.
		return Metadata{}, false
	}
	connectionID := binary.BigEndian.Uint64(resp[8:16])

	// --- scrape ---
	scrapeTxn, err := transactionID()
	if err != nil {
		return Metadata{}, false
	}
	sreq := make([]byte, 16+20)
	binary.BigEndian.PutUint64(sreq[0:8], connectionID)
	binary.BigEndian.PutUint32(sreq[8:12], actionScrape)
	binary.BigEndian.PutUint32(sreq[12:16], scrapeTxn)
	copy(sreq[16:36], hash)
	if _, err := conn.Write(sreq); err != nil {
		return Metadata{}, false
	}

	sresp := make([]byte, 8+12)
	n, err = conn.Read(sresp)
	if err != nil || n < 20 {
		return Metadata{}, false
	}
	action := binary.BigEndian.Uint32(sresp[0:4])
	if action == actionError || action != actionScrape {
		return Metadata{}, false
	}
	if binary.BigEndian.Uint32(sresp[4:8]) != scrapeTxn {
		return Metadata{}, false
	}

	return Metadata{
		Seeders:   int(int32(binary.BigEndian.Uint32(sresp[8:12]))),
		Completed: int(int32(binary.BigEndian.Uint32(sresp[12:16]))),
		Leechers:  int(int32(binary.BigEndian.Uint32(sresp[16:20]))),
		Source:    "udp_scrape",
	}, true
}

// transactionID returns a random request identifier. Random rather than
// sequential so a stray or spoofed datagram cannot be mistaken for our reply.
func transactionID() (uint32, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b[:]), nil
}

// udpEndpoints keeps only the announce URLs this protocol can actually talk to.
//
// HTTP(S) trackers are dropped rather than attempted: BEP 15 is UDP-only, and a
// scrape over HTTP is a different protocol entirely. Dropping them can leave
// zero endpoints, which is an absence and therefore allows.
func udpEndpoints(trackers []string) []string {
	seen := make(map[string]struct{}, len(trackers))
	out := make([]string, 0, len(trackers))
	for _, raw := range trackers {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Scheme != "udp" || parsed.Host == "" {
			continue
		}
		host := parsed.Host
		if parsed.Port() == "" {
			// BEP 15 carries no default port; a tracker without one cannot be
			// dialled and guessing 80 would just burn the budget on a timeout.
			continue
		}
		if _, dup := seen[host]; dup {
			continue
		}
		seen[host] = struct{}{}
		out = append(out, host)
	}
	return out
}

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

// UDPScrape reads swarm counts straight from a torrent's own trackers.
type UDPScrape struct {
	// BindAddr is the local address to egress from, e.g. "10.2.0.2:0". Empty
	// uses the default route — see the IP-exposure note above.
	BindAddr string
	// PerTracker bounds one tracker's full connect+scrape exchange.
	PerTracker time.Duration
	// Fallback trackers are used ONLY when the magnet carries none (a DHT-only
	// magnet). Absence of trackers is otherwise an absence, not a licence to
	// invent an announce list.
	Fallback []string
}

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

	endpoints := udpEndpoints(trackers)
	if len(endpoints) == 0 {
		// A DHT-only magnet has no announce list. That is an ABSENCE — there is
		// nobody to ask, which says nothing about the swarm.
		endpoints = udpEndpoints(u.Fallback)
	}
	if len(endpoints) == 0 {
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

// scrapeOne performs the two-step BEP 15 exchange against one tracker.
func (u *UDPScrape) scrapeOne(ctx context.Context, endpoint string, hash []byte, budget time.Duration) (Metadata, bool) {
	deadline := time.Now().Add(budget)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}

	dialer := &net.Dialer{Deadline: deadline}
	if u.BindAddr != "" {
		local, err := net.ResolveUDPAddr("udp", u.BindAddr)
		if err != nil {
			// A bind address that does not resolve must FAIL, not silently fall
			// back to the default route — falling back is the leak this exists
			// to prevent.
			return Metadata{}, false
		}
		dialer.LocalAddr = local
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

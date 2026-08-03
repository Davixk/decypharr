package swarm

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// BEP 15 on the wire, against a real UDP socket.
//
// The protocol is the part most likely to be subtly wrong — byte offsets, big
// endian, the two-step connect handshake, transaction-id matching — and a
// mistake there does not crash. It returns a confident zero, which is exactly
// the value that deletes a transfer. So these tests speak the protocol back.

const scrapeTestHash = "0123456789abcdef0123456789abcdef01234567"

// fakeTracker answers BEP 15 with scripted counts. Returning ok=false from
// respond simulates a tracker that ignores us.
type fakeTracker struct {
	t        *testing.T
	conn     *net.UDPConn
	seeders  int32
	leechers int32
	done     int32
	// misbehave, when set, corrupts the reply in a named way.
	misbehave string
}

func newFakeTracker(t *testing.T, seeders, leechers, done int32, misbehave string) *fakeTracker {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ft := &fakeTracker{t: t, conn: conn, seeders: seeders, leechers: leechers, done: done, misbehave: misbehave}
	go ft.serve()
	t.Cleanup(func() { _ = conn.Close() })
	return ft
}

func (f *fakeTracker) addr() string { return f.conn.LocalAddr().String() }

func (f *fakeTracker) serve() {
	buf := make([]byte, 1024)
	var connectionID uint64 = 0x1122334455667788
	for {
		n, peer, err := f.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if n < 16 {
			continue
		}
		action := binary.BigEndian.Uint32(buf[8:12])
		txn := binary.BigEndian.Uint32(buf[12:16])

		switch action {
		case actionConnect:
			if f.misbehave == "silent" {
				continue
			}
			resp := make([]byte, 16)
			binary.BigEndian.PutUint32(resp[0:4], actionConnect)
			if f.misbehave == "wrong_connect_txn" {
				binary.BigEndian.PutUint32(resp[4:8], txn+1)
			} else {
				binary.BigEndian.PutUint32(resp[4:8], txn)
			}
			binary.BigEndian.PutUint64(resp[8:16], connectionID)
			_, _ = f.conn.WriteToUDP(resp, peer)

		case actionScrape:
			if f.misbehave == "silent_scrape" {
				continue
			}
			if f.misbehave == "error_action" {
				resp := make([]byte, 8)
				binary.BigEndian.PutUint32(resp[0:4], actionError)
				binary.BigEndian.PutUint32(resp[4:8], txn)
				_, _ = f.conn.WriteToUDP(resp, peer)
				continue
			}
			if f.misbehave == "truncated" {
				_, _ = f.conn.WriteToUDP(make([]byte, 10), peer)
				continue
			}
			resp := make([]byte, 20)
			binary.BigEndian.PutUint32(resp[0:4], actionScrape)
			binary.BigEndian.PutUint32(resp[4:8], txn)
			binary.BigEndian.PutUint32(resp[8:12], uint32(f.seeders))
			binary.BigEndian.PutUint32(resp[12:16], uint32(f.done))
			binary.BigEndian.PutUint32(resp[16:20], uint32(f.leechers))
			_, _ = f.conn.WriteToUDP(resp, peer)
		}
	}
}

func udpTracker(addr string) string { return "udp://" + addr + "/announce" }

func TestScrapeReadsLiveCounts(t *testing.T) {
	tr := newFakeTracker(t, 42, 7, 100, "")
	u := &UDPScrape{PerTracker: 2 * time.Second}

	md, ok := u.Lookup(context.Background(), scrapeTestHash, []string{udpTracker(tr.addr())})
	if !ok {
		t.Fatal("a responding tracker must produce a reading")
	}
	if md.Seeders != 42 || md.Leechers != 7 || md.Completed != 100 {
		t.Fatalf("md = %+v, want seeders=42 leechers=7 completed=100 — check the BEP 15 field order", md)
	}
	if md.Source != "udp_scrape" {
		t.Fatalf("source = %q", md.Source)
	}
}

// TestScrapeTakesTheHighestCount. Trackers disagree by design — each sees only
// the peers that announced to it — so a low reading is a partial view of the
// swarm, not evidence about its size. A tracker that knows nothing must not be
// able to condemn a torrent another tracker can see is alive.
func TestScrapeTakesTheHighestCount(t *testing.T) {
	blind := newFakeTracker(t, 0, 0, 0, "")
	seeing := newFakeTracker(t, 31, 4, 90, "")
	u := &UDPScrape{PerTracker: 2 * time.Second}

	md, ok := u.Lookup(context.Background(), scrapeTestHash,
		[]string{udpTracker(blind.addr()), udpTracker(seeing.addr())})
	if !ok {
		t.Fatal("expected a reading")
	}
	if md.Seeders != 31 {
		t.Fatalf("seeders = %d, want 31: the maximum across trackers, not the first or the lowest", md.Seeders)
	}
}

// TestScrapeIsUnknownNotZero covers every wire-level failure. Each one must
// report UNKNOWN — returning a confident 0 here deletes a transfer.
func TestScrapeIsUnknownNotZero(t *testing.T) {
	cases := []string{"silent", "silent_scrape", "wrong_connect_txn", "error_action", "truncated"}
	for _, mode := range cases {
		t.Run(mode, func(t *testing.T) {
			tr := newFakeTracker(t, 99, 0, 0, mode)
			u := &UDPScrape{PerTracker: 250 * time.Millisecond}

			md, ok := u.Lookup(context.Background(), scrapeTestHash, []string{udpTracker(tr.addr())})
			if ok {
				t.Fatalf("md = %+v; a %s tracker gives no trustworthy answer and must report unknown", md, mode)
			}
		})
	}
}

func TestScrapeHasNothingToAsk(t *testing.T) {
	u := &UDPScrape{PerTracker: time.Second}
	for _, name := range []string{"no trackers", "http only", "udp without a port"} {
		var trackers []string
		switch name {
		case "http only":
			trackers = []string{"http://tracker.example/announce", "https://other.example/announce"}
		case "udp without a port":
			trackers = []string{"udp://tracker.example/announce"}
		}
		t.Run(name, func(t *testing.T) {
			if _, ok := u.Lookup(context.Background(), scrapeTestHash, trackers); ok {
				t.Fatal("no reachable tracker means UNKNOWN, which allows — never a zero, which refuses")
			}
		})
	}
}

// TestScrapePoolIsUsedWithAndWithoutMagnetTrackers.
//
// The pool started life as a DHT-only fallback. Measured on a live deployment
// that was wrong in the strongest way available: 3,276 of 3,276 stored magnets
// carried ZERO tr= entries, so the pool is the only tracker list that
// deployment will ever have. It is therefore used ALONGSIDE the magnet's own
// trackers, not instead of them.
func TestScrapePoolIsUsedWithAndWithoutMagnetTrackers(t *testing.T) {
	pool := newFakeTracker(t, 55, 0, 0, "")
	own := newFakeTracker(t, 3, 0, 0, "")
	u := &UDPScrape{PerTracker: 2 * time.Second, Trackers: []string{udpTracker(pool.addr())}}

	// No trackers in the magnet — the pool carries the whole lookup.
	md, ok := u.Lookup(context.Background(), scrapeTestHash, nil)
	if !ok || md.Seeders != 55 {
		t.Fatalf("md=%+v ok=%v; a magnet with no tr= list must still reach the pool", md, ok)
	}

	// With its own tracker, BOTH are asked and the highest wins — so the pool
	// cannot drag a reading down and the magnet's tracker cannot hide a better
	// one.
	md, ok = u.Lookup(context.Background(), scrapeTestHash, []string{udpTracker(own.addr())})
	if !ok || md.Seeders != 55 {
		t.Fatalf("md=%+v ok=%v; the pool augments the magnet's list rather than replacing it", md, ok)
	}
}

// TestTrackerBackoffStopsAskingAFailingTracker.
//
// Measured against a real public tracker: 8 back-to-back scrapes answered 2,
// and 5 spaced four seconds apart answered ZERO — spacing made it worse, so the
// penalty is cumulative. Continuing to ask both deepens it and burns the lookup
// budget on a host that will not reply.
func TestTrackerBackoffStopsAskingAFailingTracker(t *testing.T) {
	silent := newFakeTracker(t, 0, 0, 0, "silent")
	u := &UDPScrape{PerTracker: 100 * time.Millisecond, Trackers: []string{udpTracker(silent.addr())}}

	if _, ok := u.Lookup(context.Background(), scrapeTestHash, nil); ok {
		t.Fatal("a silent tracker cannot answer")
	}

	// Now in backoff: the next lookup must not even try, so it returns fast.
	start := time.Now()
	if _, ok := u.Lookup(context.Background(), scrapeTestHash, nil); ok {
		t.Fatal("expected unknown")
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("second lookup took %s; a tracker in backoff must be skipped, not re-dialled", elapsed)
	}

	// And a failure must never read as a swarm verdict.
	u.mu.Lock()
	blocked := len(u.blockedUntil)
	u.mu.Unlock()
	if blocked != 1 {
		t.Fatalf("blocked trackers = %d, want 1", blocked)
	}
}

// TestBackoffClearsOnSuccess: a tracker that recovers must come back.
func TestBackoffClearsOnSuccess(t *testing.T) {
	tr := newFakeTracker(t, 12, 0, 0, "")
	u := &UDPScrape{PerTracker: time.Second, Trackers: []string{udpTracker(tr.addr())}}
	endpoint := udpEndpoints([]string{udpTracker(tr.addr())})[0]

	u.recordOutcome(endpoint, false, time.Now())
	u.mu.Lock()
	_, blocked := u.blockedUntil[endpoint]
	u.mu.Unlock()
	if !blocked {
		t.Fatal("a failure must register a penalty")
	}

	u.recordOutcome(endpoint, true, time.Now())
	u.mu.Lock()
	_, stillBlocked := u.blockedUntil[endpoint]
	failures := u.failures[endpoint]
	u.mu.Unlock()
	if stillBlocked || failures != 0 {
		t.Fatalf("a success must clear the penalty and the failure count (blocked=%v failures=%d)",
			stillBlocked, failures)
	}
}

// TestPoolRotatesSoNoTrackerSeesEveryLookup. Without rotation a pool is just a
// list whose first N members absorb the entire rate — the single-tracker limit
// again, with more configuration.
func TestPoolRotatesSoNoTrackerSeesEveryLookup(t *testing.T) {
	pool := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		pool = append(pool, udpTracker(newFakeTracker(t, int32(i+1), 0, 0, "").addr()))
	}
	u := &UDPScrape{PerTracker: time.Second, Trackers: pool, PerLookup: 2}

	seen := map[string]bool{}
	now := time.Now()
	for i := 0; i < 3; i++ {
		for _, endpoint := range u.selectEndpoints(udpEndpoints(pool), now) {
			seen[endpoint] = true
		}
	}
	if len(seen) < 4 {
		t.Fatalf("3 lookups of 2 trackers touched only %d distinct hosts; the pool must rotate", len(seen))
	}
}

// TestPerLookupCapsTheFanOut: asking the whole pool every time multiplies our
// rate against every member, which is the opposite of what a pool is for.
func TestPerLookupCapsTheFanOut(t *testing.T) {
	pool := make([]string, 0, 8)
	for i := 0; i < 8; i++ {
		pool = append(pool, udpTracker(newFakeTracker(t, 1, 0, 0, "").addr()))
	}
	u := &UDPScrape{PerTracker: time.Second, Trackers: pool, PerLookup: 3}

	if got := len(u.selectEndpoints(udpEndpoints(pool), time.Now())); got != 3 {
		t.Fatalf("selected %d trackers, want 3", got)
	}
}

func TestScrapeRejectsANonHash(t *testing.T) {
	tr := newFakeTracker(t, 9, 0, 0, "")
	u := &UDPScrape{PerTracker: time.Second}
	for _, bad := range []string{"", "short", strings.Repeat("z", 40), scrapeTestHash + "0"} {
		if _, ok := u.Lookup(context.Background(), bad, []string{udpTracker(tr.addr())}); ok {
			t.Fatalf("hash %q was scraped; only a real 20-byte infohash may go on the wire", bad)
		}
	}
}

// TestScrapeHonoursTheBudget: the caller is an arr blocked on an add.
func TestScrapeHonoursTheBudget(t *testing.T) {
	silent := newFakeTracker(t, 0, 0, 0, "silent")
	u := &UDPScrape{PerTracker: 200 * time.Millisecond}

	start := time.Now()
	if _, ok := u.Lookup(context.Background(), scrapeTestHash, []string{udpTracker(silent.addr())}); ok {
		t.Fatal("a silent tracker cannot produce a reading")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("scrape took %s against a 200ms per-tracker budget", elapsed)
	}
}

// TestScrapeStopsOnACancelledContext keeps a shutdown from being outlived.
func TestScrapeStopsOnACancelledContext(t *testing.T) {
	tr := newFakeTracker(t, 10, 0, 0, "")
	u := &UDPScrape{PerTracker: 2 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, ok := u.Lookup(ctx, scrapeTestHash, []string{udpTracker(tr.addr())}); ok {
		t.Fatal("a cancelled context must not yield a reading")
	}
}

// TestUnresolvableBindingFailsRatherThanLeaks.
//
// The binding is the operator's control over WHICH interface a scrape leaves
// from. Falling back to the default route when it cannot be honoured sends the
// packet exactly where he configured it not to go, and says nothing — so a
// failed binding must fail the operation.
func TestUnresolvableBindingFailsRatherThanLeaks(t *testing.T) {
	tr := newFakeTracker(t, 77, 0, 0, "")
	u := &UDPScrape{
		PerTracker: time.Second,
		LocalAddr: func() (net.Addr, error) {
			return nil, errors.New("no interface named \"wgProtonCH\"")
		},
	}

	if _, ok := u.Lookup(context.Background(), scrapeTestHash, []string{udpTracker(tr.addr())}); ok {
		t.Fatal("an unresolvable binding must fail the scrape, never silently use the default route")
	}
}

// TestNilBindingUsesTheDefaultRoute: unconfigured is not an error.
func TestNilBindingUsesTheDefaultRoute(t *testing.T) {
	tr := newFakeTracker(t, 8, 0, 0, "")

	for _, name := range []string{"nil func", "nil addr"} {
		u := &UDPScrape{PerTracker: 2 * time.Second}
		if name == "nil addr" {
			u.LocalAddr = func() (net.Addr, error) { return nil, nil }
		}
		t.Run(name, func(t *testing.T) {
			md, ok := u.Lookup(context.Background(), scrapeTestHash, []string{udpTracker(tr.addr())})
			if !ok || md.Seeders != 8 {
				t.Fatalf("md=%+v ok=%v; an unconfigured binding must simply use the OS default", md, ok)
			}
		})
	}
}

// TestBindingIsResolvedPerDial. An interface name has to be re-resolved so a
// reconnected tunnel is picked up — and so a tunnel that has GONE starts
// failing instead of resolving to a stale address that no longer routes.
func TestBindingIsResolvedPerDial(t *testing.T) {
	tr := newFakeTracker(t, 4, 0, 0, "")
	var calls atomic.Int32
	u := &UDPScrape{
		PerTracker: 2 * time.Second,
		LocalAddr: func() (net.Addr, error) {
			calls.Add(1)
			return nil, nil
		},
	}

	for i := 0; i < 3; i++ {
		if _, ok := u.Lookup(context.Background(), scrapeTestHash, []string{udpTracker(tr.addr())}); !ok {
			t.Fatalf("lookup %d failed", i)
		}
	}
	if calls.Load() < 3 {
		t.Fatalf("binding resolved %d times across 3 lookups; it must be resolved per dial", calls.Load())
	}
}

func TestChainTakesTheFirstAnswer(t *testing.T) {
	quiet := newFakeTracker(t, 0, 0, 0, "silent")
	loud := newFakeTracker(t, 21, 0, 0, "")

	chain := Chain{
		&UDPScrape{PerTracker: 200 * time.Millisecond, Trackers: []string{udpTracker(quiet.addr())}},
		&UDPScrape{PerTracker: time.Second, Trackers: []string{udpTracker(loud.addr())}},
	}
	md, ok := chain.Lookup(context.Background(), scrapeTestHash, nil)
	if !ok || md.Seeders != 21 {
		t.Fatalf("md=%+v ok=%v; the chain must fall through a source that cannot answer", md, ok)
	}

	if _, ok := (Chain{}).Lookup(context.Background(), scrapeTestHash, nil); ok {
		t.Fatal("an empty chain must report unknown")
	}
}

func TestHexHashDecodesToTwentyBytes(t *testing.T) {
	raw, err := hex.DecodeString(scrapeTestHash)
	if err != nil || len(raw) != 20 {
		t.Fatalf("test fixture hash is not a valid infohash: %v", err)
	}
}

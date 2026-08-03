package swarm

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"net"
	"strings"
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

// TestScrapeUsesFallbackOnlyWhenTheMagnetHasNone: a DHT-only magnet may fall
// back to a configured set, but a magnet with its own list must not be
// second-guessed.
func TestScrapeUsesFallbackOnlyWhenTheMagnetHasNone(t *testing.T) {
	fallback := newFakeTracker(t, 55, 0, 0, "")
	own := newFakeTracker(t, 3, 0, 0, "")
	u := &UDPScrape{PerTracker: 2 * time.Second, Fallback: []string{udpTracker(fallback.addr())}}

	md, ok := u.Lookup(context.Background(), scrapeTestHash, nil)
	if !ok || md.Seeders != 55 {
		t.Fatalf("md=%+v ok=%v; a DHT-only magnet should reach the fallback set", md, ok)
	}

	md, ok = u.Lookup(context.Background(), scrapeTestHash, []string{udpTracker(own.addr())})
	if !ok || md.Seeders != 3 {
		t.Fatalf("md=%+v ok=%v; the magnet's own tracker must be used, not the fallback", md, ok)
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

// TestBadBindAddressFailsRatherThanLeaks. The bind address is the operator's
// control over which interface a scrape egresses from; falling back to the
// default route on a typo is precisely the IP leak it exists to prevent.
func TestBadBindAddressFailsRatherThanLeaks(t *testing.T) {
	tr := newFakeTracker(t, 77, 0, 0, "")
	u := &UDPScrape{PerTracker: time.Second, BindAddr: "not-an-address:::"}

	if _, ok := u.Lookup(context.Background(), scrapeTestHash, []string{udpTracker(tr.addr())}); ok {
		t.Fatal("an unresolvable bind address must fail the scrape, never silently use the default route")
	}
}

func TestChainTakesTheFirstAnswer(t *testing.T) {
	quiet := newFakeTracker(t, 0, 0, 0, "silent")
	loud := newFakeTracker(t, 21, 0, 0, "")

	chain := Chain{
		&UDPScrape{PerTracker: 200 * time.Millisecond, Fallback: []string{udpTracker(quiet.addr())}},
		&UDPScrape{PerTracker: time.Second, Fallback: []string{udpTracker(loud.addr())}},
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

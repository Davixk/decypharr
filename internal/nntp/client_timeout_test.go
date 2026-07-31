package nntp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	nntpyenc "github.com/sirrobot01/decypharr/internal/nntp/yenc"
)

func TestArticleNotFoundIsNotRetriedOnSameProvider(t *testing.T) {
	client, pool, cleanup := newPoolTestClient(t, 5)
	defer cleanup()

	var calls atomic.Int32
	err := client.ExecuteWithFailover(context.Background(), func(*Connection) error {
		calls.Add(1)
		return &Error{Type: ErrorTypeArticleNotFound, Code: 430, Message: "No Such Article"}
	})

	if !IsArticleNotFoundError(err) {
		t.Fatalf("ExecuteWithFailover error = %v, want article not found", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider callback count = %d, want 1", got)
	}
	if got := len(pool.slots); got != 0 {
		t.Fatalf("checked-out slots after 430 = %d, want 0", got)
	}
}

func TestProviderPoolAcquireTimeoutDoesNotPoisonRecovery(t *testing.T) {
	client, pool, cleanup := newPoolTestClient(t, 0)
	defer cleanup()

	// Simulate a saturated provider. Acquisition must honor the caller context.
	pool.slots <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, _, err := client.getAnyAvailableConnection(ctx, providerExclusions{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pool acquisition error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("pool acquisition took %s, want a bounded wait", elapsed)
	}

	// Release the simulated borrower. The same pool must immediately be usable;
	// a timed-out waiter must not retain a semaphore slot in the background.
	<-pool.slots
	conn, provider, err := client.getAnyAvailableConnection(context.Background(), providerExclusions{})
	if err != nil {
		t.Fatalf("pool did not recover after timeout: %v", err)
	}
	client.put(conn, provider)
	if got := len(pool.slots); got != 0 {
		t.Fatalf("checked-out slots after recovery = %d, want 0", got)
	}
}

// Both providers answered, and both said the article is not there. "No provider
// serves this" IS established, so the caller gets a content verdict.
func TestFailoverExcludesProviderThatProducedTerminalArticleMissing(t *testing.T) {
	testFailoverExcludesTerminalProvider(t,
		&Error{Type: ErrorTypeArticleNotFound, Code: 430, Message: "No Such Article"},
		true)
}

// Provider B was asked and could NOT answer (it disconnected); provider A said
// 423. That does not establish that no provider serves the article — B never
// gave an opinion — so the result must be indeterminate, NOT a content verdict.
//
// This assertion is the reverse of what it was before 2026-08-01. It used to
// expect article-not-found, conflating "the one provider that answered says no"
// with "nobody has it". Under the rule the operator stated — at least one
// provider must be able to serve it, and refuse only once that is *established*
// — an unreachable provider must never harden into a permanent statement about
// somebody else's release. Same principle as an all-infrastructure failure
// staying indeterminate; this is just the mixed case.
//
// Consequence worth being explicit about: this widens beyond the usenet add
// gate that motivated it. Every consumer of the verdict (repair probes, the
// permanent-failure path) now also refuses to call an article dead while a
// provider was unreachable. That is the intended direction — a false permanent
// verdict is worse than a retry — but it is a real change in blast radius.
func TestFailoverConnectionErrorOnOneProviderYieldsIndeterminate(t *testing.T) {
	testFailoverExcludesTerminalProvider(t,
		&Error{Type: ErrorTypeConnection, Message: "provider B disconnected"},
		false)
}

// wantContentVerdict selects which outcome the caller must see; the provider
// callback sequence and slot accounting are identical either way, and are what
// pins the exclusion behaviour this helper was originally written for.
func testFailoverExcludesTerminalProvider(t *testing.T, providerBError error, wantContentVerdict bool) {
	t.Helper()
	providerA := config.UsenetProvider{Host: "provider-a", MaxConnections: 4, Priority: 1}
	providerB := config.UsenetProvider{Host: "provider-b", MaxConnections: 4, Priority: 1}
	poolA, cleanupA := newTestProviderPool(providerA, 4)
	defer cleanupA()
	poolB, cleanupB := newTestProviderPool(providerB, 4)
	defer cleanupB()

	client := &Client{
		pools: map[string]*ProviderPool{
			providerA.Host: poolA,
			providerB.Host: poolB,
		},
		providers: []config.UsenetProvider{providerA, providerB},
		retries:   1,
		logger:    zerolog.Nop(),
	}

	var sequence []string
	providerACalls := 0
	err := client.ExecuteWithFailover(context.Background(), func(conn *Connection) error {
		sequence = append(sequence, conn.address)
		switch conn.address {
		case providerA.Host:
			providerACalls++
			if providerACalls == 1 {
				return &Error{Type: ErrorTypeTimeout, Message: "provider A stalled"}
			}
			return &Error{Type: ErrorTypeArticleNotFound, Code: 423, Message: "No Such Article"}
		case providerB.Host:
			return providerBError
		default:
			return errors.New("unexpected provider")
		}
	})

	if got := IsArticleNotFoundError(err); got != wantContentVerdict {
		if wantContentVerdict {
			t.Fatalf("ExecuteWithFailover error = %v with sequence %v, want article not found "+
				"(every provider answered, so the verdict is established)", err, sequence)
		}
		t.Fatalf("ExecuteWithFailover error = %v with sequence %v, want an INDETERMINATE error: "+
			"provider B could not answer, so one provider's 423 does not establish that nobody serves it",
			err, sequence)
	}
	wantSequence := []string{providerA.Host, providerB.Host, providerA.Host}
	if len(sequence) != len(wantSequence) {
		t.Fatalf("provider callback sequence = %v, want %v", sequence, wantSequence)
	}
	for i := range wantSequence {
		if sequence[i] != wantSequence[i] {
			t.Fatalf("provider callback sequence = %v, want %v", sequence, wantSequence)
		}
	}
	if got := len(poolA.slots); got != 0 {
		t.Fatalf("provider A checked-out slots = %d, want 0", got)
	}
	if got := len(poolB.slots); got != 0 {
		t.Fatalf("provider B checked-out slots = %d, want 0", got)
	}
}

func TestRaceForConnectionReturnsBeforeBlockedLoserHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	host, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse listener port: %v", err)
	}

	accepted := make(chan struct{})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		close(accepted)
		var one [1]byte
		_, _ = conn.Read(one[:])
	}()

	fastProvider := config.UsenetProvider{Host: "fast-provider", MaxConnections: 1, Priority: 1}
	slowProvider := config.UsenetProvider{Host: host, Port: port, MaxConnections: 1, Priority: 1}
	fastPool, cleanupFast := newTestProviderPool(fastProvider, 1)
	defer cleanupFast()
	slowPool := &ProviderPool{
		slots:  make(chan struct{}, 1),
		max:    1,
		config: slowProvider,
	}
	client := &Client{
		pools: map[string]*ProviderPool{
			fastProvider.Host: fastPool,
			slowProvider.Host: slowPool,
		},
		logger: zerolog.Nop(),
	}

	// Hold the fast slot until the slow worker has connected and is blocked on
	// its NNTP greeting. Releasing it then deterministically creates one winner
	// and one handshake-blocked loser.
	fastPool.slots <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type raceResult struct {
		conn     *Connection
		provider config.UsenetProvider
		err      error
	}
	resultCh := make(chan raceResult, 1)
	go func() {
		conn, provider, raceErr := client.raceForConnection(ctx, []config.UsenetProvider{fastProvider, slowProvider})
		resultCh <- raceResult{conn: conn, provider: provider, err: raceErr}
	}()

	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("slow provider did not reach blocked greeting")
	}
	started := time.Now()
	<-fastPool.slots

	var result raceResult
	select {
	case result = <-resultCh:
	case <-time.After(time.Second):
		t.Fatal("connection race waited for blocked loser")
	}
	if result.err != nil {
		t.Fatalf("raceForConnection: %v", result.err)
	}
	if result.provider.Host != fastProvider.Host {
		t.Fatalf("winner = %q, want %q", result.provider.Host, fastProvider.Host)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("winner return took %s; blocked loser delayed it", elapsed)
	}
	client.put(result.conn, result.provider)

	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("losing handshake connection was not canceled")
	}
	waitForPoolSlots(t, slowPool, 0)
	if got := len(fastPool.slots); got != 0 {
		t.Fatalf("fast provider checked-out slots = %d, want 0", got)
	}
}

// TestMidBodyWriteFailureDoesNotPoolPoisonedConnection reproduces the framing
// corruption scenario: a cache-write failure mid-BODY is classified as a yEnc
// decode error, which previously fell into ExecuteWithFailover's default
// branch and POOLED the connection with unread body bytes still buffered. The
// next borrower would then read leftover article data instead of a status
// line. The failing connection must be closed; the follow-up request must
// arrive on a NEW connection and decode cleanly.
func TestMidBodyWriteFailureDoesNotPoolPoisonedConnection(t *testing.T) {
	oldPureGo := nntpyenc.UsePureGo
	nntpyenc.UsePureGo = true
	t.Cleanup(func() { nntpyenc.UsePureGo = oldPureGo })

	server := newBodyTestServer(t)
	t.Cleanup(server.Close)

	provider := config.UsenetProvider{Host: server.host, Port: server.port, MaxConnections: 1, Priority: 1}
	pool := &ProviderPool{
		conns:  make([]*connectionEntry, 0, 1),
		slots:  make(chan struct{}, 1),
		max:    1,
		config: provider,
	}
	client := &Client{
		pools:     map[string]*ProviderPool{provider.Host: pool},
		providers: []config.UsenetProvider{provider},
		retries:   1,
		logger:    zerolog.Nop(),
	}

	// Request 1: the destination writer fails mid-body (e.g. disk cache
	// io.ErrShortWrite), leaving the response partially consumed.
	writeErr := errors.New("cache write failed mid-body")
	err := client.ExecuteWithFailover(context.Background(), func(conn *Connection) error {
		_, streamErr := conn.StreamBody("<seg-0@test>", failingWriter{err: writeErr})
		return streamErr
	})
	var nntpErr *Error
	if !errors.As(err, &nntpErr) || nntpErr.Type != ErrorTypeYencDecode {
		t.Fatalf("mid-body write failure classified as %v, want yEnc decode error", err)
	}
	if got := len(pool.slots); got != 0 {
		t.Fatalf("checked-out slots after mid-body failure = %d, want 0", got)
	}
	// The poisoned connection must NOT have been pooled for reuse.
	pool.mu.Lock()
	pooled := len(pool.conns)
	pool.mu.Unlock()
	if pooled != 0 {
		t.Fatalf("poisoned connection was returned to the pool (pooled=%d), want 0", pooled)
	}

	// Request 2 must arrive on a fresh connection and decode cleanly.
	var decoded bytes.Buffer
	err = client.ExecuteWithFailover(context.Background(), func(conn *Connection) error {
		_, streamErr := conn.StreamBody("<seg-0@test>", &decoded)
		return streamErr
	})
	if err != nil {
		t.Fatalf("follow-up request failed: %v", err)
	}
	if decoded.String() != bodyTestPayload {
		t.Fatalf("follow-up decode = %q, want %q", decoded.String(), bodyTestPayload)
	}
	if got := server.connections.Load(); got != 2 {
		t.Fatalf("server connection count = %d, want 2 (second request must use a new connection)", got)
	}
}

type failingWriter struct {
	err error
}

func (w failingWriter) Write([]byte) (int, error) {
	return 0, w.err
}

// bodyTestPayload decodes from bodyTestYenc ('A'+42 = 'k', no escapes needed).
const bodyTestPayload = "AAAA"

const bodyTestYenc = "=ybegin line=128 size=4 name=t.bin\r\nkkkk\r\n=yend size=4\r\n.\r\n"

type bodyTestServer struct {
	listener    net.Listener
	host        string
	port        int
	connections atomic.Int32

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
	once  sync.Once
}

func newBodyTestServer(t *testing.T) *bodyTestServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for fake NNTP server: %v", err)
	}
	host, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split fake NNTP address: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse fake NNTP port: %v", err)
	}
	s := &bodyTestServer{
		listener: listener,
		host:     host,
		port:     port,
		conns:    make(map[net.Conn]struct{}),
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s
}

func (s *bodyTestServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.connections.Add(1)
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serve(conn)
	}
}

func (s *bodyTestServer) serve(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		_ = conn.Close()
	}()

	writer := bufio.NewWriter(conn)
	if _, err := writer.WriteString("200 body test server ready\r\n"); err != nil {
		return
	}
	if err := writer.Flush(); err != nil {
		return
	}
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		switch strings.ToUpper(fields[0]) {
		case "BODY":
			if _, err := fmt.Fprintf(writer, "222 0 %s\r\n%s", fields[1], bodyTestYenc); err != nil {
				return
			}
			if err := writer.Flush(); err != nil {
				return
			}
		case "DATE":
			_, _ = writer.WriteString("111 20260718000000\r\n")
			if err := writer.Flush(); err != nil {
				return
			}
		case "QUIT":
			_, _ = writer.WriteString("205 closing connection\r\n")
			_ = writer.Flush()
			return
		default:
			_, _ = writer.WriteString("500 unsupported command\r\n")
			if err := writer.Flush(); err != nil {
				return
			}
		}
	}
}

func (s *bodyTestServer) Close() {
	s.once.Do(func() {
		_ = s.listener.Close()
		s.mu.Lock()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
}

func newPoolTestClient(t *testing.T, retries int) (*Client, *ProviderPool, func()) {
	t.Helper()
	provider := config.UsenetProvider{Host: "test-provider", MaxConnections: 1, Priority: 1}
	pool, cleanup := newTestProviderPool(provider, 1)
	client := &Client{
		pools:     map[string]*ProviderPool{provider.Host: pool},
		providers: []config.UsenetProvider{provider},
		retries:   retries,
		logger:    zerolog.Nop(),
	}
	return client, pool, cleanup
}

func newTestProviderPool(provider config.UsenetProvider, connectionCount int) (*ProviderPool, func()) {
	provider.MaxConnections = connectionCount
	entries := make([]*connectionEntry, 0, connectionCount)
	connections := make([]net.Conn, 0, connectionCount*2)
	for range connectionCount {
		clientConn, serverConn := net.Pipe()
		reader := bufio.NewReader(clientConn)
		conn := &Connection{
			address: provider.Host,
			conn:    clientConn,
			reader:  reader,
			text:    textproto.NewReader(reader),
			writer:  bufio.NewWriter(clientConn),
			logger:  zerolog.Nop(),
		}
		entries = append(entries, &connectionEntry{conn: conn, provider: provider, lastUsed: time.Now()})
		connections = append(connections, clientConn, serverConn)
	}
	pool := &ProviderPool{
		conns:  entries,
		slots:  make(chan struct{}, connectionCount),
		max:    connectionCount,
		config: provider,
	}
	return pool, func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
	}
}

func waitForPoolSlots(t *testing.T, pool *ProviderPool, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for len(pool.slots) != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := len(pool.slots); got != want {
		t.Fatalf("checked-out slots = %d, want %d", got, want)
	}
}

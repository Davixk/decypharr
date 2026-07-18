package nntp

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/textproto"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
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

func TestFailoverExcludesProviderThatProducedTerminalArticleMissing(t *testing.T) {
	testFailoverExcludesTerminalProvider(t, &Error{Type: ErrorTypeArticleNotFound, Code: 430, Message: "No Such Article"})
}

func TestFailoverExcludesProviderThatProducedTerminalConnectionError(t *testing.T) {
	testFailoverExcludesTerminalProvider(t, &Error{Type: ErrorTypeConnection, Message: "provider B disconnected"})
}

func testFailoverExcludesTerminalProvider(t *testing.T, providerBError error) {
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

	if !IsArticleNotFoundError(err) {
		t.Fatalf("ExecuteWithFailover error = %v with sequence %v, want article not found", err, sequence)
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

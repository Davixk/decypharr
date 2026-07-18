package nntp

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/textproto"
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

func newPoolTestClient(t *testing.T, retries int) (*Client, *ProviderPool, func()) {
	t.Helper()
	provider := config.UsenetProvider{Host: "test-provider", MaxConnections: 1, Priority: 1}
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
	pool := &ProviderPool{
		conns:  []*connectionEntry{{conn: conn, provider: provider, lastUsed: time.Now()}},
		slots:  make(chan struct{}, 1),
		max:    1,
		config: provider,
	}
	client := &Client{
		pools:     map[string]*ProviderPool{provider.Host: pool},
		providers: []config.UsenetProvider{provider},
		retries:   retries,
		logger:    zerolog.Nop(),
	}
	cleanup := func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	}
	return client, pool, cleanup
}

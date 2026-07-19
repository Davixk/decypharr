package nntp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

// TestIsInfrastructureError pins the verdict boundary: substrate failures
// (connection, timeout, busy, auth, permission, no-available-connection) are
// infrastructure; article-not-found is a genuine content verdict and must
// never classify as infrastructure.
func TestIsInfrastructureError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"connection", NewConnectionError(errors.New("dial refused")), true},
		{"timeout", NewTimeoutError(errors.New("i/o timeout")), true},
		{"server busy", &Error{Type: ErrorTypeServerBusy, Code: 400, Message: "busy"}, true},
		{"authentication", &Error{Type: ErrorTypeAuthentication, Code: 481, Message: "auth rejected"}, true},
		{"permission denied", &Error{Type: ErrorTypePermissionDenied, Code: 502, Message: "denied"}, true},
		{"no available connection", NewNoAvailableConnectionError("no eligible providers available", nil), true},
		{"article not found 430", &Error{Type: ErrorTypeArticleNotFound, Code: 430, Message: "no such article"}, false},
		{"article not found 423", &Error{Type: ErrorTypeArticleNotFound, Code: 423, Message: "no such number"}, false},
		{"group not found", &Error{Type: ErrorTypeGroupNotFound, Code: 411, Message: "no such group"}, false},
		{"invalid command", &Error{Type: ErrorTypeInvalidCommand, Code: 500, Message: "unknown"}, false},
		{"protocol", NewProtocolError(599, "weird"), false},
		{"yenc decode", NewYencDecodeError(errors.New("bad crc")), false},
		{"unknown type", &Error{Type: ErrorTypeUnknown, Message: "?"}, false},
		{"wrapped connection", fmt.Errorf("stat segment: %w", NewConnectionError(errors.New("reset"))), true},
		{"wrapped article not found", fmt.Errorf("stat segment: %w", &Error{Type: ErrorTypeArticleNotFound, Code: 430}), false},
		{"non-nntp error", errors.New("some random failure"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsInfrastructureError(tc.err); got != tc.want {
				t.Fatalf("IsInfrastructureError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestIsArticleNotFoundErrorStaysGenuineVerdict guards the counterpart helper:
// only 430/423 answers classify as the genuine content-missing verdict.
func TestIsArticleNotFoundErrorStaysGenuineVerdict(t *testing.T) {
	if !IsArticleNotFoundError(&Error{Type: ErrorTypeArticleNotFound, Code: 430}) {
		t.Fatal("430 must classify as article-not-found")
	}
	if IsArticleNotFoundError(NewConnectionError(errors.New("refused"))) {
		t.Fatal("connection failure must not classify as article-not-found")
	}
	if IsArticleNotFoundError(NewNoAvailableConnectionError("no eligible providers available", nil)) {
		t.Fatal("acquire failure must not classify as article-not-found")
	}
}

// TestGetAnyAvailableConnectionReturnsTypedError verifies the acquire layer no
// longer surfaces bare string errors: when every provider is excluded, the
// failure is a typed *Error with ErrorTypeNoAvailableConnection so callers can
// classify it as infrastructure.
func TestGetAnyAvailableConnectionReturnsTypedError(t *testing.T) {
	client, _, cleanup := newPoolTestClient(t, 0)
	defer cleanup()

	var exclusions providerExclusions
	exclusions.excludeHost("test-provider")

	_, _, err := client.getAnyAvailableConnection(context.Background(), exclusions)
	if err == nil {
		t.Fatal("expected an error when every provider is excluded")
	}
	var nntpErr *Error
	if !errors.As(err, &nntpErr) {
		t.Fatalf("acquire error = %v (%T), want typed *nntp.Error", err, err)
	}
	if nntpErr.Type != ErrorTypeNoAvailableConnection {
		t.Fatalf("acquire error type = %v, want ErrorTypeNoAvailableConnection", nntpErr.Type)
	}
	if !IsInfrastructureError(err) {
		t.Fatalf("acquire error %v must classify as infrastructure", err)
	}
	if IsArticleNotFoundError(err) {
		t.Fatalf("acquire error %v must not classify as article-not-found", err)
	}
}

// TestExecuteWithFailoverAcquireFailureIsInfrastructure exercises the real
// acquisition path against a dead provider (connection refused): the terminal
// error must be typed and classify as infrastructure, never as a content
// verdict.
func TestExecuteWithFailoverAcquireFailureIsInfrastructure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split reserved address: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse reserved port: %v", err)
	}
	_ = listener.Close() // free the port so dialing it is refused

	provider := config.UsenetProvider{Host: "127.0.0.1", Port: port, MaxConnections: 1, Priority: 1}
	pool := &ProviderPool{
		conns:  nil,
		slots:  make(chan struct{}, 1),
		max:    1,
		config: provider,
	}
	client := &Client{
		pools:     map[string]*ProviderPool{provider.Host: pool},
		providers: []config.UsenetProvider{provider},
		logger:    zerolog.Nop(),
	}

	execErr := client.ExecuteWithFailover(context.Background(), func(*Connection) error {
		t.Fatal("callback must not run when no connection can be acquired")
		return nil
	})
	if execErr == nil {
		t.Fatal("expected an error from a dead provider")
	}
	if !IsInfrastructureError(execErr) {
		t.Fatalf("acquire failure %v must classify as infrastructure", execErr)
	}
	if IsArticleNotFoundError(execErr) {
		t.Fatalf("acquire failure %v must not classify as article-not-found", execErr)
	}
}

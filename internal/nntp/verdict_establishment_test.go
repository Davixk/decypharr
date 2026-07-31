package nntp

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

// A content verdict ("this article is not here") only generalises to "no
// configured provider serves this" once EVERY provider has actually answered.
// A provider that was unreachable said nothing, and must not be counted as
// agreeing.
//
// The tests below guard the two directions of that rule. The single-provider
// case is the one that matters most in practice: it is the shape the add-time
// refusal was built for, and a careless implementation of the multi-provider
// rule silently disables it by treating every 430 as unestablished.

// TestSingleProviderArticleNotFoundStaysAContentVerdict is the regression guard
// for the add-time usenet refusal. With one provider configured, that provider
// answering 430 IS the whole population — the verdict is established and must
// survive as a content verdict, or nothing would ever be refused at add time
// and the refusal feature would be silently dead.
func TestSingleProviderArticleNotFoundStaysAContentVerdict(t *testing.T) {
	client, _, cleanup := newPoolTestClient(t, 0)
	defer cleanup()

	err := client.ExecuteWithFailover(context.Background(), func(*Connection) error {
		return &Error{Type: ErrorTypeArticleNotFound, Code: 430, Message: "No Such Article"}
	})

	if !IsArticleNotFoundError(err) {
		t.Fatalf("ExecuteWithFailover error = %v, want a content verdict: with ONE provider configured, "+
			"its 430 establishes that no configured provider serves the article. Downgrading it to "+
			"indeterminate would silently disable the add-time refusal for every single-provider setup", err)
	}
}

// The mirror: with one provider that is unreachable, there is no verdict at all
// and the error must stay infrastructure-class so the caller queues for retry
// rather than condemning the release.
func TestSingleProviderConnectionFailureIsNotAContentVerdict(t *testing.T) {
	client, _, cleanup := newPoolTestClient(t, 0)
	defer cleanup()

	err := client.ExecuteWithFailover(context.Background(), func(*Connection) error {
		return &Error{Type: ErrorTypeConnection, Message: "provider unreachable"}
	})

	if IsArticleNotFoundError(err) {
		t.Fatalf("ExecuteWithFailover error = %v, want an infrastructure error: a provider we could not "+
			"reach has said nothing about the article", err)
	}
}

// With two providers that BOTH answer "not found", the verdict is established
// across the whole population and must be reported as a content verdict —
// otherwise a multi-provider setup could never refuse anything.
func TestAllProvidersAnsweringNotFoundEstablishesTheVerdict(t *testing.T) {
	providerA := config.UsenetProvider{Host: "provider-a", MaxConnections: 2, Priority: 1}
	providerB := config.UsenetProvider{Host: "provider-b", MaxConnections: 2, Priority: 1}
	poolA, cleanupA := newTestProviderPool(providerA, 2)
	defer cleanupA()
	poolB, cleanupB := newTestProviderPool(providerB, 2)
	defer cleanupB()

	client := &Client{
		pools: map[string]*ProviderPool{
			providerA.Host: poolA,
			providerB.Host: poolB,
		},
		providers: []config.UsenetProvider{providerA, providerB},
		retries:   0,
		logger:    zerolog.Nop(),
	}

	answered := map[string]bool{}
	err := client.ExecuteWithFailover(context.Background(), func(conn *Connection) error {
		answered[conn.address] = true
		return &Error{Type: ErrorTypeArticleNotFound, Code: 430, Message: "No Such Article"}
	})

	if len(answered) != 2 {
		t.Fatalf("providers asked = %v, want both to be asked before any verdict is formed", answered)
	}
	if !IsArticleNotFoundError(err) {
		t.Fatalf("ExecuteWithFailover error = %v, want a content verdict: every configured provider "+
			"answered 'not found', so it is established", err)
	}
}

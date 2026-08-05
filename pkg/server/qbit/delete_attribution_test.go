package qbit

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// SHIM-DELETE ATTRIBUTION.
//
// Measured on fork.53: 170 deletes arrived with delete_files=false in four
// hours against 108 with true. Each false one is decypharr correctly keeping the
// provider copy the caller asked it to keep — and each one leaves a transfer
// burning a provider slot for content the *arr has already replaced. The
// behaviour is right; the REQUESTS are the open question, and answering it needs
// to know who sent them.

// 🔴 THE CREDENTIAL MUST NEVER REACH THE LOG.
//
// An API key in a log line is a leaked secret that outlives the investigation it
// was added for: it gets copied into bug reports, pasted into chat, and survives
// in log shipping nobody audits. The scheme is what the question needs; the value
// adds nothing to it.
func TestDeleteAttributionNeverCarriesTheCredential(t *testing.T) {
	const secret = "super-secret-api-key-value"

	for _, tc := range []struct {
		name     string
		setup    func(*http.Request)
		wantAuth string
	}{
		{
			name:     "api key header",
			setup:    func(r *http.Request) { r.Header.Set("X-Api-Key", secret) },
			wantAuth: "apikey",
		},
		{
			name:     "basic auth",
			setup:    func(r *http.Request) { r.Header.Set("Authorization", "Basic "+secret) },
			wantAuth: "basic",
		},
		{
			name:     "bearer token",
			setup:    func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+secret) },
			wantAuth: "authorization-header",
		},
		{
			name:     "session cookie",
			setup:    func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "SID", Value: secret}) },
			wantAuth: "cookie",
		},
		{
			name:     "unauthenticated",
			setup:    func(*http.Request) {},
			wantAuth: "none",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/api/v2/torrents/delete", nil)
			r.Header.Set("User-Agent", "Sonarr/4.0.0")
			tc.setup(r)

			got := describeDeleteCaller(r)

			if got.auth != tc.wantAuth {
				t.Fatalf("auth = %q, want %q", got.auth, tc.wantAuth)
			}
			for _, field := range []string{got.agent, got.addr, got.auth} {
				if strings.Contains(field, secret) {
					t.Fatalf("THE CREDENTIAL LEAKED INTO ATTRIBUTION FIELD %q. A key in a log line "+
						"outlives the investigation it was added for.", field)
				}
			}
		})
	}
}

// The User-Agent is the field that separates "sonarr's own client" from "the
// operator's resolver script", which is the whole question.
func TestDeleteAttributionCarriesTheUserAgent(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/api/v2/torrents/delete", nil)
	r.Header.Set("User-Agent", "Sonarr/4.0.14.2939")

	got := describeDeleteCaller(r)
	if got.agent != "Sonarr/4.0.14.2939" {
		t.Fatalf("agent = %q, want the User-Agent verbatim", got.agent)
	}
}

// A missing User-Agent must be visibly missing rather than an empty string that
// reads as a formatting bug in the log.
func TestMissingUserAgentIsNamed(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/api/v2/torrents/delete", nil)
	r.Header.Del("User-Agent")

	if got := describeDeleteCaller(r); got.agent == "" {
		t.Fatal("an absent User-Agent must be named, not blank")
	}
}

// The ephemeral source port changes per connection and identifies nothing, while
// making every line look distinct — which defeats grouping by caller.
func TestDeleteAttributionStripsTheEphemeralPort(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/api/v2/torrents/delete", nil)
	r.RemoteAddr = "192.168.1.42:54321"

	got := describeDeleteCaller(r)
	if got.addr != "192.168.1.42" {
		t.Fatalf("addr = %q, want the host alone so lines from one caller group together", got.addr)
	}
}

package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

// TestConfigRouteVerbs pins the routing table itself, not just the handlers:
// the config resources answer GET/PATCH/PUT, and nothing else. In particular
// POST is GONE from /api/config — it used to be the only way to update the
// config, with merge semantics that made it an undeclared PATCH.
//
// The requests go through the real router, so this also proves PATCH survives
// the setup gate and the auth middleware.
func TestConfigRouteVerbs(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{"patch config", http.MethodPatch, "/api/config", `{"log_level":"debug"}`, http.StatusOK},
		{"put config", http.MethodPut, "/api/config", `{"log_level":"debug"}`, http.StatusOK},
		{"post config is gone", http.MethodPost, "/api/config", `{"log_level":"debug"}`, http.StatusMethodNotAllowed},
		{"patch repair config", http.MethodPatch, "/api/repair/config", `{"workers":8}`, http.StatusOK},
		{"put repair config", http.MethodPut, "/api/repair/config", `{"workers":8}`, http.StatusOK},
		{"post repair config is gone", http.MethodPost, "/api/repair/config", `{"workers":8}`, http.StatusMethodNotAllowed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// The fixture keeps use_auth false (auth middleware passes through)
			// and satisfies Validate() so the setup gate does not 503 the
			// non-exempt /api/repair/config path.
			setupConfigAPITest(t)

			s := &Server{}
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			s.WebRoutes().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("%s %s = %d, want %d (body: %s)",
					tc.method, tc.path, rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}

// TestConfigRouteVerbSemanticsEndToEnd walks the two verbs through the real
// router and checks they do different things to the SAME body — the whole point
// of the change. `{"log_level":"debug"}` preserves the configured debrids under
// PATCH and clears them under PUT.
func TestConfigRouteVerbSemanticsEndToEnd(t *testing.T) {
	send := func(t *testing.T, method, body string) {
		t.Helper()
		s := &Server{}
		req := httptest.NewRequest(method, "/api/config", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		s.WebRoutes().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s = %d, want 200 (body: %s)", method, rec.Code, rec.Body.String())
		}
	}

	cfgFile := setupConfigAPITest(t)
	send(t, http.MethodPatch, `{"log_level":"debug"}`)
	if saved := readSavedConfig(t, cfgFile); len(saved.Debrids) != 2 || saved.LogLevel != "debug" {
		t.Fatalf("PATCH must apply log_level and preserve debrids: %+v", saved.Debrids)
	}

	cfgFile = setupConfigAPITest(t)
	send(t, http.MethodPut, `{"log_level":"debug"}`)
	if saved := readSavedConfig(t, cfgFile); len(saved.Debrids) != 0 || saved.LogLevel != "debug" {
		t.Fatalf("PUT must apply log_level and clear the omitted debrids: %+v", saved.Debrids)
	}

	// Sanity: the fixture really does leave auth off, so the middleware was a
	// pass-through rather than the reason a request succeeded.
	if config.Get().UseAuth {
		t.Fatal("fixture unexpectedly enabled auth")
	}
}

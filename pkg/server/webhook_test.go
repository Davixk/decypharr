package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/sirrobot01/decypharr/internal/config"
)

const webhookTestToken = "b0a1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1"

// setupWebhookAuthTest installs a config with auth enabled and a known API
// token, mirroring a normal deployment (setup complete, credentials set).
func setupWebhookAuthTest(t *testing.T, useAuth bool) {
	t.Helper()
	config.Reset()
	t.Cleanup(config.Reset)
	dir := t.TempDir()
	config.SetConfigPath(dir)

	cfgJSON := `{"log_level":"info","download_folder":"/downloads","use_auth":` + boolLiteral(useAuth) + `}`
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfgJSON), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	authJSON := `{"username":"admin","password":"$2a$10$hashhashhashhashhashhu","api_token":"` + webhookTestToken + `"}`
	if err := os.WriteFile(filepath.Join(dir, "auth.json"), []byte(authJSON), 0644); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	cfg := config.Get()
	if cfg.UseAuth != useAuth {
		t.Fatalf("fixture use_auth = %v, want %v", cfg.UseAuth, useAuth)
	}
	if useAuth {
		if auth := cfg.GetAuth(); auth == nil || auth.APIToken != webhookTestToken {
			t.Fatalf("fixture api token did not load: %+v", auth)
		}
	}
}

func boolLiteral(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

// tautulliPayload is a valid, TARGETLESS webhook body — the shape that used to
// launch a full repair sweep.
const tautulliPayloadNoMediaID = `{"topic":"tautulli","fix":true}`

// TestTautulliWebhookRequiresAuth is the regression test for the unauthenticated
// destructive webhook: POST /webhooks/tautulli was registered on the parent
// router, OUTSIDE the authMiddleware group, so anyone who could reach the port
// could trigger repair work (PRUNE/ARR-DELETE per the operator's configured knobs).
//
// It exercises the REAL route wiring (webhookRoutes, as mounted by New) with a
// nil manager: a request that gets past both the middleware and the media-id
// guard would dereference that nil manager and panic the test, so "no panic"
// is itself part of the assertion.
func TestTautulliWebhookRequiresAuth(t *testing.T) {
	tests := []struct {
		name     string
		useAuth  bool
		target   string
		headers  map[string]string
		wantCode int
	}{
		{
			name:     "no credentials is rejected",
			useAuth:  true,
			target:   "/tautulli",
			wantCode: http.StatusUnauthorized,
		},
		{
			name:     "wrong bearer token is rejected",
			useAuth:  true,
			target:   "/tautulli",
			headers:  map[string]string{"Authorization": "Bearer not-the-token"},
			wantCode: http.StatusUnauthorized,
		},
		{
			name:     "wrong query token is rejected",
			useAuth:  true,
			target:   "/tautulli?token=not-the-token",
			wantCode: http.StatusUnauthorized,
		},
		{
			name:     "empty query token is rejected",
			useAuth:  true,
			target:   "/tautulli?token=",
			wantCode: http.StatusUnauthorized,
		},
		// Authorized calls reach the handler, which rejects this targetless
		// payload with 400 (see TestTautulliWebhookNoMediaIDDoesNotSweep).
		{
			name:     "bearer token is accepted",
			useAuth:  true,
			target:   "/tautulli",
			headers:  map[string]string{"Authorization": "Bearer " + webhookTestToken},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "token header form is accepted",
			useAuth:  true,
			target:   "/tautulli",
			headers:  map[string]string{"Authorization": "Token " + webhookTestToken},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "x-api-token header is accepted",
			useAuth:  true,
			target:   "/tautulli",
			headers:  map[string]string{"X-API-Token": webhookTestToken},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "query token is accepted for clients that cannot set headers",
			useAuth:  true,
			target:   "/tautulli?token=" + webhookTestToken,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "apikey query alias is accepted",
			useAuth:  true,
			target:   "/tautulli?apikey=" + webhookTestToken,
			wantCode: http.StatusBadRequest,
		},
		// use_auth:false is the documented "server is intentionally open" mode;
		// this endpoint must not be special-cased around it.
		{
			name:     "use_auth false leaves the server open as before",
			useAuth:  false,
			target:   "/tautulli",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setupWebhookAuthTest(t, tc.useAuth)

			s := &Server{}
			req := httptest.NewRequest(http.MethodPost, tc.target, strings.NewReader(tautulliPayloadNoMediaID))
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			s.webhookRoutes().ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if tc.wantCode == http.StatusUnauthorized && strings.Contains(rec.Body.String(), "media_id") {
				t.Fatalf("unauthenticated request reached the handler: %s", rec.Body.String())
			}
		})
	}
}

// TestTautulliWebhookNoMediaIDDoesNotSweep is the defense-in-depth half of the
// fix: even an AUTHENTICATED webhook with no media id must not fall through to
// svc.RunNow(...), i.e. a full library sweep with the configured
// REPAIR/PRUNE/ARR-DELETE knobs.
//
// The Server has a nil manager, so any path that reached the repair service
// would panic instead of returning 400 — the assertion is therefore both on the
// status code and on the absence of a panic.
func TestTautulliWebhookNoMediaIDDoesNotSweep(t *testing.T) {
	for _, body := range []string{
		`{"topic":"tautulli"}`,
		`{"topic":"tautulli","fix":true}`,
		`{"topic":"tautulli","arr":"sonarr","fix":true}`,
		`{"topic":"tautulli","media_id":"   ","tvdb_id":"","tmdb_id":""}`,
	} {
		t.Run(body, func(t *testing.T) {
			setupWebhookAuthTest(t, true)

			s := &Server{}
			req := httptest.NewRequest(http.MethodPost, "/tautulli", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+webhookTestToken)
			rec := httptest.NewRecorder()
			s.webhookRoutes().ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 — a targetless webhook must never launch a sweep (body: %s)",
					rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "media_id") {
				t.Fatalf("expected a media_id error, got %q", rec.Body.String())
			}
		})
	}
}

// TestWebhookMountResolvesUnderTheCatchAll mirrors the chi nesting in New():
// the webhook router is mounted at /webhooks next to the catch-all Mount("/")
// that serves the web UI. It pins that /webhooks/tautulli actually resolves to
// the webhook router — and therefore to its auth middleware — instead of
// falling through to the catch-all.
func TestWebhookMountResolvesUnderTheCatchAll(t *testing.T) {
	setupWebhookAuthTest(t, true)

	s := &Server{}
	root := chi.NewRouter()
	root.Route("/", func(r chi.Router) {
		// Stands in for s.WebRoutes().
		r.Mount("/", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTeapot)
		}))
		r.Mount("/webhooks", s.webhookRoutes())
	})

	req := httptest.NewRequest(http.MethodPost, "/webhooks/tautulli", strings.NewReader(tautulliPayloadNoMediaID))
	rec := httptest.NewRecorder()
	root.ServeHTTP(rec, req)

	if rec.Code == http.StatusTeapot {
		t.Fatal("/webhooks/tautulli fell through to the catch-all mount instead of the webhook router")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 from the webhook auth middleware (body: %s)", rec.Code, rec.Body.String())
	}
}

// TestTautulliWebhookRejectsForeignTopic keeps the pre-existing topic guard.
func TestTautulliWebhookRejectsForeignTopic(t *testing.T) {
	setupWebhookAuthTest(t, true)

	s := &Server{}
	req := httptest.NewRequest(http.MethodPost, "/tautulli", strings.NewReader(`{"topic":"plex","media_id":"123"}`))
	req.Header.Set("Authorization", "Bearer "+webhookTestToken)
	rec := httptest.NewRecorder()
	s.webhookRoutes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a foreign topic (body: %s)", rec.Code, rec.Body.String())
	}
}

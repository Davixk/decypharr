package server

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// TestDecodeOptionalJSONBody pins the safety contract of the optional-body
// repair endpoints: an ABSENT body means "use defaults", but a body that is
// PRESENT and malformed must be rejected outright.
//
// Regression: the previous guard was `err != nil && err != io.EOF`, and a
// truncated document such as "{" surfaces from the decoder as io.EOF. That let
// a typo'd body fall through to a zero-value request — which on these handlers
// does not mean "do nothing", it means "run a full default sweep"
// (handleRunRepair) or "act on EVERY broken entry with the configured knobs"
// (handleFixBroken, which can PRUNE/RE-GRAB destructively). A live operator
// probing the API with `{` launched a real sweep this way.
func TestDecodeOptionalJSONBody(t *testing.T) {
	type payload struct {
		Names []string `json:"names,omitempty"`
		Fix   bool     `json:"fix,omitempty"`
	}

	t.Run("absent body is accepted and leaves defaults", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/x", nil)
		var got payload
		if err := decodeOptionalJSONBody(req, &got); err != nil {
			t.Fatalf("absent body must be accepted, got %v", err)
		}
		if got.Fix || len(got.Names) != 0 {
			t.Fatalf("absent body must leave the zero value, got %+v", got)
		}
	})

	t.Run("valid body is decoded", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/x", strings.NewReader(`{"fix":true,"names":["a"]}`))
		var got payload
		if err := decodeOptionalJSONBody(req, &got); err != nil {
			t.Fatalf("valid body must decode, got %v", err)
		}
		if !got.Fix || len(got.Names) != 1 || got.Names[0] != "a" {
			t.Fatalf("body not decoded, got %+v", got)
		}
	})

	for _, tc := range []struct {
		name   string
		body   string
		accept bool
	}{
		{"empty", "", true},
		{"whitespace only", "   \n\t ", true},
		{"empty object", `{}`, true},
		// The regression cases: all of these must 400, not default to a full run.
		{"truncated object", `{`, false},
		{"truncated nested", `{"names":[`, false},
		{"unterminated string", `{"names":["a`, false},
		{"not json", `not json`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/x", strings.NewReader(tc.body))
			var got payload
			err := decodeOptionalJSONBody(req, &got)
			if tc.accept && err != nil {
				t.Fatalf("body %q must be accepted, got %v", tc.body, err)
			}
			if !tc.accept && err == nil {
				t.Fatalf("body %q must be REJECTED: a malformed body must never silently become a full default run", tc.body)
			}
		})
	}
}

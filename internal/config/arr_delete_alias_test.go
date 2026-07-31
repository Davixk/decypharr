package config

import (
	"testing"

	json "github.com/bytedance/sonic"
)

// `regrab` was renamed to `arr_delete` when the arr-side component stopped
// bundling delete + blocklist + search. The old key is still accepted, and
// getting that right needs TWO things that are easy to get half-done:
//
//  1. the VALUE has to land on ArrDelete   → RepairConfig.UnmarshalJSON
//  2. the KEY has to count as "mentioned"  → the `alias:"regrab"` struct tag,
//     read by the partial-update merge
//
// Miss (2) and a PATCH carrying only the deprecated key looks to the merge like
// the field was never mentioned, so it restores the live value over the decoded
// one. The caller asks to turn the component off, gets told it worked, and it
// stays on. That is exactly the failure the preserve-on-missing merge exists to
// prevent, reintroduced through the back door of a rename.

func TestArrDeleteAcceptsDeprecatedRegrabKeyOnDecode(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want bool
	}{
		{"deprecated key true", `{"regrab":true}`, true},
		{"deprecated key false", `{"regrab":false}`, false},
		{"current key true", `{"arr_delete":true}`, true},
		{"current key false", `{"arr_delete":false}`, false},
		// Both present: the caller has already moved to the new name, so the
		// stale key must not resurrect an old value.
		{"both, current wins", `{"arr_delete":false,"regrab":true}`, false},
		{"both, current wins inverted", `{"arr_delete":true,"regrab":false}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg RepairConfig
			if err := json.Unmarshal([]byte(tc.body), &cfg); err != nil {
				t.Fatalf("Unmarshal(%s): %v", tc.body, err)
			}
			if got := cfg.ArrDeleteEnabled(); got != tc.want {
				t.Fatalf("ArrDeleteEnabled() = %v, want %v (body %s)", got, tc.want, tc.body)
			}
			if cfg.Regrab != nil {
				t.Fatalf("deprecated Regrab survived decode: %v — exactly one field must be authoritative", *cfg.Regrab)
			}
		})
	}
}

// TestPartialPatchWithDeprecatedRegrabKeyTurnsComponentOff is the regression
// test for the half-done rename. The live config has the component ON; a
// partial PATCH using only the OLD key asks for it OFF and mentions nothing
// else.
func TestPartialPatchWithDeprecatedRegrabKeyTurnsComponentOff(t *testing.T) {
	live := RepairConfig{
		Enabled:            true,
		Schedule:           "0 3 * * *",
		MaxDeletionsPerRun: 42,
		Prune:              true,
		ArrDelete:          boolPtr(true),
	}

	body := []byte(`{"regrab":false}`)
	var patched RepairConfig
	if err := json.Unmarshal(body, &patched); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if err := patched.PreserveMissingFields(live, body); err != nil {
		t.Fatalf("PreserveMissingFields: %v", err)
	}

	if patched.ArrDeleteEnabled() {
		t.Fatal("PATCH {\"regrab\":false} left the arr component ON — the deprecated key was " +
			"treated as 'not mentioned', so the live value was restored over the caller's explicit false")
	}
	// Everything the caller did NOT mention must survive, or the alias handling
	// has broken the merge it was threaded through.
	if !patched.Enabled || patched.Schedule != "0 3 * * *" || patched.MaxDeletionsPerRun != 42 || !patched.Prune {
		t.Fatalf("partial PATCH wiped unmentioned fields: %+v", patched)
	}
}

// The mirror case: the old key turning the component ON must also survive a
// merge whose live value is off.
func TestPartialPatchWithDeprecatedRegrabKeyTurnsComponentOn(t *testing.T) {
	live := RepairConfig{Enabled: true, ArrDelete: boolPtr(false)}

	body := []byte(`{"regrab":true}`)
	var patched RepairConfig
	if err := json.Unmarshal(body, &patched); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if err := patched.PreserveMissingFields(live, body); err != nil {
		t.Fatalf("PreserveMissingFields: %v", err)
	}
	if !patched.ArrDeleteEnabled() {
		t.Fatal("PATCH {\"regrab\":true} did not turn the arr component on")
	}
}

// A config written before the rename must load with the component in the state
// its author chose — this is the compatibility that actually matters, since it
// is the operator's config file on disk rather than an API client.
func TestStoredConfigWithRegrabKeyMigratesOnLoad(t *testing.T) {
	var cfg Config
	if err := json.Unmarshal([]byte(`{"repair":{"enabled":true,"regrab":true,"prune":true}}`), &cfg); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	cfg.setDefaults()

	if !cfg.Repair.ArrDeleteEnabled() {
		t.Fatal("a pre-rename config with regrab:true lost the arr component on load")
	}
	if cfg.Repair.Regrab != nil {
		t.Fatal("setDefaults left the deprecated key populated; it must be cleared so one field is authoritative")
	}

	// And it must not come back on the next write.
	out, err := json.Marshal(cfg.Repair)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var reread map[string]any
	if err := json.Unmarshal(out, &reread); err != nil {
		t.Fatalf("Unmarshal round-trip: %v", err)
	}
	if _, stale := reread["regrab"]; stale {
		t.Fatalf("saving re-emitted the deprecated regrab key: %s", out)
	}
	if _, current := reread["arr_delete"]; !current {
		t.Fatalf("saving did not emit arr_delete: %s", out)
	}
}

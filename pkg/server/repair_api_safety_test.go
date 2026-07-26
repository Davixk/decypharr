package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	json "github.com/bytedance/sonic"

	"github.com/sirrobot01/decypharr/internal/config"
)

// repairFixtureJSON is an operator config with every safety knob deliberately
// set: a destructive-action cap, a stop schedule, PRUNE on, RE-GRAB off and an
// explicit repair:false (the *bool tri-state at its non-default value).
const repairFixtureJSON = `{
  "log_level": "info",
  "download_folder": "/downloads",
  "repair": {
    "enabled": true,
    "source": "managed",
    "schedule": "0 3 * * *",
    "stop_schedule": "06:00",
    "workers": 3,
    "recheck_interval": "168h",
    "max_deletions_per_run": 5,
    "repair": false,
    "prune": true,
    "regrab": false,
    "skip_nzb_repair": true,
    "arrs": ["sonarr"]
  }
}`

func setupRepairConfigTest(t *testing.T, fixture string) string {
	t.Helper()
	config.Reset()
	t.Cleanup(config.Reset)
	dir := t.TempDir()
	config.SetConfigPath(dir)
	cfgFile := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgFile, []byte(fixture), 0644); err != nil {
		t.Fatalf("write initial config: %v", err)
	}
	if got := config.Get().Repair; got.Schedule != "0 3 * * *" {
		t.Fatalf("fixture did not load: %+v", got)
	}
	return cfgFile
}

// patchRepairConfig exercises PATCH /api/repair/config — the partial update.
// A bare Server is enough: the handler only needs config.Get(), and the manager
// lookup is nil-guarded.
func patchRepairConfig(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	s := &Server{}
	req := httptest.NewRequest(http.MethodPatch, "/api/repair/config", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.handlePatchRepairConfig(rec, req)
	return rec
}

// putRepairConfig exercises PUT /api/repair/config — the full replacement.
func putRepairConfig(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	s := &Server{}
	req := httptest.NewRequest(http.MethodPut, "/api/repair/config", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleReplaceRepairConfig(rec, req)
	return rec
}

func readSavedRepairConfig(t *testing.T, cfgFile string) config.RepairConfig {
	t.Helper()
	data, err := os.ReadFile(cfgFile)
	if err != nil {
		t.Fatalf("read saved config: %v", err)
	}
	var saved config.Config
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatalf("unmarshal saved config: %v", err)
	}
	return saved.Repair
}

// TestPatchRepairConfigPartialPreservesSafetyKnobs is the regression test for
// the silent safety-knob reset: the update handler assigned the decoded body
// wholesale, so a partial update reset max_deletions_per_run to 0,
// stop_schedule to "" (stop schedule disabled) and prune/regrab to false while
// answering 200. Preserving them is PATCH's contract.
func TestPatchRepairConfigPartialPreservesSafetyKnobs(t *testing.T) {
	cfgFile := setupRepairConfigTest(t, repairFixtureJSON)

	rec := patchRepairConfig(t, `{"workers":8}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	for _, got := range []config.RepairConfig{config.Get().Repair, readSavedRepairConfig(t, cfgFile)} {
		if got.Workers != 8 {
			t.Fatalf("submitted workers not applied: %+v", got)
		}
		if got.MaxDeletionsPerRun != 5 {
			t.Fatalf("partial PATCH wiped max_deletions_per_run: %d", got.MaxDeletionsPerRun)
		}
		if got.StopSchedule != "06:00" {
			t.Fatalf("partial PATCH wiped stop_schedule: %q", got.StopSchedule)
		}
		if !got.Prune {
			t.Fatalf("partial PATCH wiped prune")
		}
		if got.Regrab {
			t.Fatalf("partial PATCH turned regrab on")
		}
		if !got.Enabled || got.Schedule != "0 3 * * *" || got.Source != config.RepairSourceManaged {
			t.Fatalf("partial PATCH wiped scheduling fields: %+v", got)
		}
		if !got.SkipNZBRepair || len(got.Arrs) != 1 || got.Arrs[0] != "sonarr" {
			t.Fatalf("partial PATCH wiped the remaining fields: %+v", got)
		}
		// Repair *bool tri-state: the fixture's explicit false must survive.
		if got.Repair == nil || *got.Repair {
			t.Fatalf("partial PATCH lost the explicit repair:false tri-state: %v", got.Repair)
		}
		if got.RepairEnabled() {
			t.Fatalf("RepairEnabled() = true after a partial PATCH that never mentioned repair")
		}
	}
}

// TestPutRepairConfigPartialClearsOmittedFields is PUT's honest contract: the
// submitted document IS the repair config, so everything the caller omitted
// reverts to its zero value — the safety knobs included. This is deliberate and
// must stay visible: a PUT of `{"workers":8}` DOES clear a configured deletion
// cap and stop schedule.
//
// It is nonetheless safe by construction, because each knob's zero value is the
// conservative one: max_deletions_per_run 0 resolves to the default cap of 100
// (unlimited is -1), prune/regrab false delete nothing, and the repair
// tri-state back at nil resolves to true (re-acquire, non-destructive).
func TestPutRepairConfigPartialClearsOmittedFields(t *testing.T) {
	cfgFile := setupRepairConfigTest(t, repairFixtureJSON)

	rec := putRepairConfig(t, `{"workers":8}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	for _, got := range []config.RepairConfig{config.Get().Repair, readSavedRepairConfig(t, cfgFile)} {
		if got.Workers != 8 {
			t.Fatalf("submitted workers not applied: %+v", got)
		}
		if got.MaxDeletionsPerRun != 0 {
			t.Fatalf("PUT must clear max_deletions_per_run (0 ⇒ default cap 100): %d", got.MaxDeletionsPerRun)
		}
		if got.StopSchedule != "" {
			t.Fatalf("PUT must clear stop_schedule: %q", got.StopSchedule)
		}
		if got.Prune || got.Regrab {
			t.Fatalf("PUT must clear prune/regrab (⇒ delete nothing): %+v", got)
		}
		if got.Enabled || got.Schedule != "" || got.SkipNZBRepair || len(got.Arrs) != 0 {
			t.Fatalf("PUT must clear every omitted field: %+v", got)
		}
		// Fields that HAVE a default revert to it (not to the stored value):
		// the fixture's source=managed is gone, replaced by the default arr.
		if got.Source != config.RepairSourceArr || got.RecheckInterval != "168h" {
			t.Fatalf("PUT kept stored values instead of reverting to defaults: %+v", got)
		}
		if got.Repair != nil {
			t.Fatalf("PUT must clear the repair tri-state to nil (⇒ defaults true): %v", *got.Repair)
		}
		if !got.RepairEnabled() {
			t.Fatal("a cleared repair tri-state must resolve to true (re-acquire, non-destructive)")
		}
	}
}

// TestPutRepairConfigRejectsAnInvalidReplacement: PUT runs the SAME validation
// the merge path does, on the resulting document. A replacement that enables
// repair without carrying a schedule is therefore a 400, not a saved config
// that can never schedule — and the stored config is left untouched.
func TestPutRepairConfigRejectsAnInvalidReplacement(t *testing.T) {
	cfgFile := setupRepairConfigTest(t, repairFixtureJSON)

	rec := putRepairConfig(t, `{"enabled":true}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (a replacement that enables repair must carry a schedule): %s",
			rec.Code, rec.Body.String())
	}
	for _, got := range []config.RepairConfig{config.Get().Repair, readSavedRepairConfig(t, cfgFile)} {
		if got.Schedule != "0 3 * * *" || got.MaxDeletionsPerRun != 5 {
			t.Fatalf("a rejected PUT still mutated the repair config: %+v", got)
		}
	}

	// The same replacement WITH a schedule is accepted.
	setupRepairConfigTest(t, repairFixtureJSON)
	if rec := putRepairConfig(t, `{"enabled":true,"schedule":"@daily"}`); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
}

// TestPatchRepairConfigExplicitValuesStillApply: presence of a key, even with
// a zero/false value, is a real instruction and must overwrite.
func TestPatchRepairConfigExplicitValuesStillApply(t *testing.T) {
	cfgFile := setupRepairConfigTest(t, repairFixtureJSON)

	rec := patchRepairConfig(t, `{"max_deletions_per_run":0,"stop_schedule":"","prune":false,"regrab":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	for _, got := range []config.RepairConfig{config.Get().Repair, readSavedRepairConfig(t, cfgFile)} {
		if got.MaxDeletionsPerRun != 0 {
			t.Fatalf("explicit max_deletions_per_run:0 not applied: %d", got.MaxDeletionsPerRun)
		}
		if got.StopSchedule != "" {
			t.Fatalf("explicit stop_schedule:\"\" not applied: %q", got.StopSchedule)
		}
		if got.Prune {
			t.Fatalf("explicit prune:false not applied")
		}
		if !got.Regrab {
			t.Fatalf("explicit regrab:true not applied")
		}
		if got.Schedule != "0 3 * * *" {
			t.Fatalf("omitted schedule was not preserved: %q", got.Schedule)
		}
	}
}

// TestPatchRepairConfigRepairTriState pins every transition of the Repair
// *bool under PATCH: nil (unset ⇒ defaults true), explicit true and explicit
// false, each preserved when omitted and overwritten when submitted.
func TestPatchRepairConfigRepairTriState(t *testing.T) {
	tests := []struct {
		name    string
		current string // "repair" fragment in the stored config, "" = key absent
		body    string
		wantPtr *bool
		wantEff bool
	}{
		{name: "absent stays unset", current: ``, body: `{"workers":4}`, wantPtr: nil, wantEff: true},
		{name: "explicit false survives omission", current: `"repair": false,`, body: `{"workers":4}`, wantPtr: boolPtrTest(false), wantEff: false},
		{name: "explicit true survives omission", current: `"repair": true,`, body: `{"workers":4}`, wantPtr: boolPtrTest(true), wantEff: true},
		{name: "submitted false overwrites unset", current: ``, body: `{"repair":false}`, wantPtr: boolPtrTest(false), wantEff: false},
		{name: "submitted true overwrites false", current: `"repair": false,`, body: `{"repair":true}`, wantPtr: boolPtrTest(true), wantEff: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := `{"log_level":"info","download_folder":"/downloads","repair":{"enabled":true,"schedule":"0 3 * * *",` +
				tc.current + `"max_deletions_per_run":5}}`
			cfgFile := setupRepairConfigTest(t, fixture)

			rec := patchRepairConfig(t, tc.body)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}

			for _, got := range []config.RepairConfig{config.Get().Repair, readSavedRepairConfig(t, cfgFile)} {
				switch {
				case tc.wantPtr == nil && got.Repair != nil:
					t.Fatalf("repair tri-state = %v, want unset (nil)", *got.Repair)
				case tc.wantPtr != nil && (got.Repair == nil || *got.Repair != *tc.wantPtr):
					t.Fatalf("repair tri-state = %v, want %v", got.Repair, *tc.wantPtr)
				}
				if got.RepairEnabled() != tc.wantEff {
					t.Fatalf("RepairEnabled() = %v, want %v", got.RepairEnabled(), tc.wantEff)
				}
				if got.MaxDeletionsPerRun != 5 {
					t.Fatalf("max_deletions_per_run lost: %d", got.MaxDeletionsPerRun)
				}
			}
		})
	}
}

// TestPutRepairConfigRepairTriState is the same matrix under PUT: an omitted
// "repair" key is not preserved, it goes back to nil (unset ⇒ defaults true).
// Only an explicitly submitted value survives a replacement.
func TestPutRepairConfigRepairTriState(t *testing.T) {
	tests := []struct {
		name    string
		current string // "repair" fragment in the stored config, "" = key absent
		body    string
		wantPtr *bool
		wantEff bool
	}{
		{name: "omitted clears explicit false", current: `"repair": false,`, body: `{"workers":4}`, wantPtr: nil, wantEff: true},
		{name: "omitted clears explicit true", current: `"repair": true,`, body: `{"workers":4}`, wantPtr: nil, wantEff: true},
		{name: "submitted false applies", current: ``, body: `{"repair":false}`, wantPtr: boolPtrTest(false), wantEff: false},
		{name: "submitted true applies", current: `"repair": false,`, body: `{"repair":true}`, wantPtr: boolPtrTest(true), wantEff: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := `{"log_level":"info","download_folder":"/downloads","repair":{"enabled":true,"schedule":"0 3 * * *",` +
				tc.current + `"max_deletions_per_run":5}}`
			cfgFile := setupRepairConfigTest(t, fixture)

			rec := putRepairConfig(t, tc.body)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}

			for _, got := range []config.RepairConfig{config.Get().Repair, readSavedRepairConfig(t, cfgFile)} {
				switch {
				case tc.wantPtr == nil && got.Repair != nil:
					t.Fatalf("repair tri-state = %v, want unset (nil)", *got.Repair)
				case tc.wantPtr != nil && (got.Repair == nil || *got.Repair != *tc.wantPtr):
					t.Fatalf("repair tri-state = %v, want %v", got.Repair, *tc.wantPtr)
				}
				if got.RepairEnabled() != tc.wantEff {
					t.Fatalf("RepairEnabled() = %v, want %v", got.RepairEnabled(), tc.wantEff)
				}
				// The cap was never submitted, so a replacement clears it.
				if got.MaxDeletionsPerRun != 0 {
					t.Fatalf("PUT must clear the omitted cap: %d", got.MaxDeletionsPerRun)
				}
			}
		})
	}
}

func boolPtrTest(v bool) *bool { return &v }

// TestUpdateRepairConfigRejectsEmptyAndMalformedBody: neither verb may turn a
// missing or broken body into a silent 200 — those are client errors. Under PUT
// a zero-value document would additionally wipe the whole repair block.
func TestUpdateRepairConfigRejectsEmptyAndMalformedBody(t *testing.T) {
	for _, tc := range []struct {
		name string
		do   func(*testing.T, string) *httptest.ResponseRecorder
	}{
		{"patch", patchRepairConfig},
		{"put", putRepairConfig},
	} {
		for _, body := range []string{``, `   `, `{`, `{"arrs":[`, `not json`} {
			t.Run(tc.name+" body="+body, func(t *testing.T) {
				cfgFile := setupRepairConfigTest(t, repairFixtureJSON)
				if rec := tc.do(t, body); rec.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want 400 for body %q", rec.Code, body)
				}
				if got := readSavedRepairConfig(t, cfgFile); got.MaxDeletionsPerRun != 5 {
					t.Fatalf("a rejected body still rewrote the repair config: %+v", got)
				}
			})
		}
	}
}

// TestPatchRepairConfigValidatesMergedResult: validation sees the merged
// config, so a body that only flips "enabled" is accepted when the stored
// schedule is valid, and a submitted invalid schedule is still rejected.
func TestPatchRepairConfigValidatesMergedResult(t *testing.T) {
	setupRepairConfigTest(t, repairFixtureJSON)
	if rec := patchRepairConfig(t, `{"enabled":true}`); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (stored schedule is valid): %s", rec.Code, rec.Body.String())
	}

	setupRepairConfigTest(t, repairFixtureJSON)
	if rec := patchRepairConfig(t, `{"schedule":"not a schedule"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for an invalid submitted schedule", rec.Code)
	}
	if got := config.Get().Repair.Schedule; got != "0 3 * * *" {
		t.Fatalf("rejected PATCH still mutated the live schedule: %q", got)
	}
}

// TestResolveLegacyFixFlag pins FIX 3's decision: an "actions" object that is
// PRESENT but names no component is an explicit "do nothing" and must never be
// promoted to the legacy fix flag, which the manager resolves to the operator's
// configured REPAIR/PRUNE/RE-GRAB knobs (possibly destructive). "actions"
// ABSENT keeps the documented fall-back behavior.
func TestResolveLegacyFixFlag(t *testing.T) {
	tests := []struct {
		name     string
		actions  *repairActionsBody
		fix      bool
		want     bool
		wantNone bool
	}{
		{name: "actions absent keeps fix=true (configured knobs)", actions: nil, fix: true, want: true},
		{name: "actions absent keeps fix=false", actions: nil, fix: false, want: false},
		{name: "explicit all-false forces check-only", actions: &repairActionsBody{}, fix: true, want: false, wantNone: true},
		{name: "explicit all-false stays check-only", actions: &repairActionsBody{}, fix: false, want: false, wantNone: true},
		{name: "prune only keeps its selection", actions: &repairActionsBody{Prune: true}, fix: false, want: false},
		{name: "repair only with fix=true", actions: &repairActionsBody{Repair: true}, fix: true, want: true},
		{name: "regrab only", actions: &repairActionsBody{Regrab: true}, fix: true, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.actions.explicitNone(); got != tc.wantNone {
				t.Fatalf("explicitNone() = %v, want %v", got, tc.wantNone)
			}
			if got := resolveLegacyFixFlag(tc.actions, tc.fix); got != tc.want {
				t.Fatalf("resolveLegacyFixFlag() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestRepairActionsBodyToManagerAllFalse: an all-false selection must reach the
// manager as a NON-nil, all-false ManualActions. Combined with fix=false (see
// TestResolveLegacyFixFlag) that resolves to CHECK-only; a nil selection would
// instead mean "unspecified" and fall back to the configured knobs.
func TestRepairActionsBodyToManagerAllFalse(t *testing.T) {
	got := (&repairActionsBody{}).toManager()
	if got == nil {
		t.Fatal("an explicit all-false actions object must not decay to a nil selection")
	}
	if got.Repair || got.Prune || got.Regrab {
		t.Fatalf("all-false selection was not preserved: %+v", got)
	}
	if (*repairActionsBody)(nil).toManager() != nil {
		t.Fatal("an absent actions object must stay nil (unspecified)")
	}
}

// TestFixBrokenRejectsExplicitAllFalseActions: /api/repair/fix has nothing to
// do without a component, and the manager resolves its selection with fix=true
// unconditionally — so an explicit all-false "actions" used to run the
// configured knobs on EVERY broken entry. It must be a 400 instead.
//
// The Server has a nil manager: reaching the repair service would panic, so a
// clean 400 also proves no work was launched.
func TestFixBrokenRejectsExplicitAllFalseActions(t *testing.T) {
	setupRepairConfigTest(t, repairFixtureJSON)

	s := &Server{}
	body := `{"actions":{"repair":false,"prune":false,"regrab":false}}`
	req := httptest.NewRequest(http.MethodPost, "/api/repair/fix", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleFixBroken(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body: %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "no repair action selected") {
		t.Fatalf("unexpected error message: %q", rec.Body.String())
	}
}

// TestFixBrokenAbsentActionsStillReachesTheService: "actions" absent must keep
// its documented meaning (fall back to the configured knobs), i.e. the request
// is NOT rejected by the all-false guard. With a nil manager it panics on the
// repair lookup, which is exactly the signal that the guard let it through.
func TestFixBrokenAbsentActionsStillReachesTheService(t *testing.T) {
	setupRepairConfigTest(t, repairFixtureJSON)

	rec := httptest.NewRecorder()
	func() {
		// The nil manager makes the repair lookup panic; swallowing it is fine —
		// getting that far is the point.
		defer func() { _ = recover() }()
		s := &Server{}
		req := httptest.NewRequest(http.MethodPost, "/api/repair/fix", strings.NewReader(`{"names":["x"]}`))
		s.handleFixBroken(rec, req)
	}()

	if rec.Code == http.StatusBadRequest && strings.Contains(rec.Body.String(), "no repair action selected") {
		t.Fatal("an ABSENT actions object must keep falling back to the configured knobs, not be rejected as an explicit no-op")
	}
}

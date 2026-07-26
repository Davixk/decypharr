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

func putRepairConfig(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	// A bare Server is enough: the handler only needs config.Get(), and the
	// manager lookup is nil-guarded.
	s := &Server{}
	req := httptest.NewRequest(http.MethodPut, "/api/repair/config", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleUpdateRepairConfig(rec, req)
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

// TestUpdateRepairConfigPartialPutPreservesSafetyKnobs is the regression test
// for the silent safety-knob reset: PUT /api/repair/config assigned the decoded
// body wholesale, so a partial PUT reset max_deletions_per_run to 0,
// stop_schedule to "" (stop schedule disabled) and prune/regrab to false while
// answering 200.
func TestUpdateRepairConfigPartialPutPreservesSafetyKnobs(t *testing.T) {
	cfgFile := setupRepairConfigTest(t, repairFixtureJSON)

	rec := putRepairConfig(t, `{"workers":8}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	for _, got := range []config.RepairConfig{config.Get().Repair, readSavedRepairConfig(t, cfgFile)} {
		if got.Workers != 8 {
			t.Fatalf("posted workers not applied: %+v", got)
		}
		if got.MaxDeletionsPerRun != 5 {
			t.Fatalf("partial PUT wiped max_deletions_per_run: %d", got.MaxDeletionsPerRun)
		}
		if got.StopSchedule != "06:00" {
			t.Fatalf("partial PUT wiped stop_schedule: %q", got.StopSchedule)
		}
		if !got.Prune {
			t.Fatalf("partial PUT wiped prune")
		}
		if got.Regrab {
			t.Fatalf("partial PUT turned regrab on")
		}
		if !got.Enabled || got.Schedule != "0 3 * * *" || got.Source != config.RepairSourceManaged {
			t.Fatalf("partial PUT wiped scheduling fields: %+v", got)
		}
		if !got.SkipNZBRepair || len(got.Arrs) != 1 || got.Arrs[0] != "sonarr" {
			t.Fatalf("partial PUT wiped the remaining fields: %+v", got)
		}
		// Repair *bool tri-state: the fixture's explicit false must survive.
		if got.Repair == nil || *got.Repair {
			t.Fatalf("partial PUT lost the explicit repair:false tri-state: %v", got.Repair)
		}
		if got.RepairEnabled() {
			t.Fatalf("RepairEnabled() = true after a partial PUT that never mentioned repair")
		}
	}
}

// TestUpdateRepairConfigExplicitValuesStillApply: presence of a key, even with
// a zero/false value, is a real instruction and must overwrite.
func TestUpdateRepairConfigExplicitValuesStillApply(t *testing.T) {
	cfgFile := setupRepairConfigTest(t, repairFixtureJSON)

	rec := putRepairConfig(t, `{"max_deletions_per_run":0,"stop_schedule":"","prune":false,"regrab":true}`)
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

// TestUpdateRepairConfigRepairTriState pins every transition of the Repair
// *bool: nil (unset ⇒ defaults true), explicit true and explicit false, each
// preserved when omitted and overwritten when posted.
func TestUpdateRepairConfigRepairTriState(t *testing.T) {
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
		{name: "posted false overwrites unset", current: ``, body: `{"repair":false}`, wantPtr: boolPtrTest(false), wantEff: false},
		{name: "posted true overwrites false", current: `"repair": false,`, body: `{"repair":true}`, wantPtr: boolPtrTest(true), wantEff: true},
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
				if got.MaxDeletionsPerRun != 5 {
					t.Fatalf("max_deletions_per_run lost: %d", got.MaxDeletionsPerRun)
				}
			}
		})
	}
}

func boolPtrTest(v bool) *bool { return &v }

// TestUpdateRepairConfigRejectsEmptyAndMalformedBody: the merge must not turn a
// missing or broken body into "keep everything, return 200" silently — those are
// client errors.
func TestUpdateRepairConfigRejectsEmptyAndMalformedBody(t *testing.T) {
	for _, body := range []string{``, `   `, `{`, `not json`} {
		t.Run("body="+body, func(t *testing.T) {
			setupRepairConfigTest(t, repairFixtureJSON)
			if rec := putRepairConfig(t, body); rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 for body %q", rec.Code, body)
			}
		})
	}
}

// TestUpdateRepairConfigValidatesMergedResult: validation sees the merged
// config, so a body that only flips "enabled" is accepted when the stored
// schedule is valid, and a posted invalid schedule is still rejected.
func TestUpdateRepairConfigValidatesMergedResult(t *testing.T) {
	setupRepairConfigTest(t, repairFixtureJSON)
	if rec := putRepairConfig(t, `{"enabled":true}`); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (stored schedule is valid): %s", rec.Code, rec.Body.String())
	}

	setupRepairConfigTest(t, repairFixtureJSON)
	if rec := putRepairConfig(t, `{"schedule":"not a schedule"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for an invalid posted schedule", rec.Code)
	}
	if got := config.Get().Repair.Schedule; got != "0 3 * * *" {
		t.Fatalf("rejected PUT still mutated the live schedule: %q", got)
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

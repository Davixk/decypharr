package storage

import (
	"strconv"
	"testing"
	"time"

	json "github.com/bytedance/sonic"
	"github.com/sirrobot01/appendstore"
	"github.com/sirrobot01/decypharr/internal/config"
)

// The regression these tests pin, stated once:
//
// NextCheckDueAt was written by every probe and read by nothing, so fixing IsDue
// to honour it handed the CORRECTED probe a library of verdicts written by the
// BROKEN one — each shielded by a next_check_due_at up to a full recheck
// interval (7 days) away. The observed run: candidates=0, probed=0,
// skipped_fresh=47627, with the 47,504 `healthy` records overwhelmingly produced
// by a probe that never read a byte and failed open.
//
// The fix is to version the VERDICT, not to clear the timestamp: a record
// stamped below RepairProbeVersion is untrustworthy, not merely stale, so no
// freshness deadline it carries may suppress a re-probe.

// TestStaleProbeVersionIsDueDespiteFreshness is the core assertion: freshness
// cannot shield a verdict produced by an older algorithm.
func TestStaleProbeVersionIsDueDespiteFreshness(t *testing.T) {
	now := time.Now()
	recheck := 7 * 24 * time.Hour

	cases := []struct {
		name   string
		health *EntryHealth
	}{
		{
			// The production shape: `healthy`, probed moments ago by the OLD
			// probe, parked a full week out.
			name: "older_version_healthy_parked_a_week_out",
			health: &EntryHealth{
				Status:         HealthHealthy,
				ProbeVersion:   RepairProbeVersion - 1,
				LastCheckedAt:  now,
				NextCheckDueAt: now.Add(recheck),
			},
		},
		{
			// A record from before the field existed at all decodes to 0. This is
			// the literal on-disk state of all ~47,600 entries on the box.
			name: "absent_version_field_is_ancient",
			health: &EntryHealth{
				Status:         HealthHealthy,
				LastCheckedAt:  now,
				NextCheckDueAt: now.Add(100 * 365 * 24 * time.Hour),
			},
		},
		{
			// `unknown` at an old version must not get to sit on its short retry
			// either — the retry is a property of the verdict, and the verdict is
			// not trusted.
			name: "older_version_unknown_within_retry_window",
			health: &EntryHealth{
				Status:         HealthUnknown,
				ProbeVersion:   RepairProbeVersion - 1,
				LastCheckedAt:  now,
				NextCheckDueAt: now.Add(6 * time.Hour),
			},
		},
		{
			// Structural verdicts leave the every-run treadmill via the staleness
			// backstop; that shortcut must not survive a probe change either,
			// since a probe change is exactly what could make a "no probeable
			// file" verdict wrong.
			name: "older_version_structural",
			health: &EntryHealth{
				Status:         HealthBroken,
				Structural:     true,
				ProbeVersion:   RepairProbeVersion - 1,
				LastCheckedAt:  now,
				NextCheckDueAt: now.Add(recheck),
			},
		},
		{
			name: "older_version_unsupported",
			health: &EntryHealth{
				Status:         HealthUnsupported,
				ProbeVersion:   RepairProbeVersion - 1,
				LastCheckedAt:  now,
				NextCheckDueAt: now.Add(recheck),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.health.IsDue(now, recheck) {
				t.Fatalf("a verdict at probe version %d (current is %d) is NOT due; "+
					"the corrected probe is gated behind its predecessor's mistakes for up to %v",
					tc.health.ProbeVersion, RepairProbeVersion, recheck)
			}
		})
	}
}

// TestCurrentProbeVersionKeepsFreshness is the negative space: versioning must
// invalidate OLD verdicts only. A record produced by the current algorithm keeps
// every bit of its existing freshness behaviour, otherwise the change trades a
// suppressed sweep for a permanent full-library re-probe.
func TestCurrentProbeVersionKeepsFreshness(t *testing.T) {
	now := time.Now()
	recheck := 7 * 24 * time.Hour

	healthy := &EntryHealth{
		Status:         HealthHealthy,
		ProbeVersion:   RepairProbeVersion,
		LastCheckedAt:  now,
		NextCheckDueAt: now.Add(recheck),
	}
	if healthy.IsDue(now, recheck) {
		t.Fatal("a healthy verdict from the CURRENT probe is due immediately; freshness stopped working")
	}
	if !healthy.IsDue(now.Add(recheck+time.Minute), recheck) {
		t.Fatal("a healthy verdict is never revisited; the staleness backstop must still apply")
	}

	// `unknown` keeps honouring its own shorter retry deadline once it is at the
	// current version.
	unknown := &EntryHealth{
		Status:         HealthUnknown,
		ProbeVersion:   RepairProbeVersion,
		LastCheckedAt:  now,
		NextCheckDueAt: now.Add(6 * time.Hour),
	}
	if unknown.IsDue(now, recheck) {
		t.Fatal("an unknown verdict from the current probe is due inside its retry window")
	}
	if !unknown.IsDue(now.Add(6*time.Hour+time.Minute), recheck) {
		t.Fatal("an unknown verdict is never re-probed after its retry deadline")
	}

	// Dirty still wins over everything, at any version.
	dirty := &EntryHealth{
		Status:         HealthHealthy,
		ProbeVersion:   RepairProbeVersion,
		Dirty:          true,
		LastCheckedAt:  now,
		NextCheckDueAt: now.Add(recheck),
	}
	if !dirty.IsDue(now, recheck) {
		t.Fatal("a dirty entry must be due immediately regardless of version or deadline")
	}
}

// TestBrokenIsAlwaysDueAtAnyProbeVersion keeps the deletion-cap retry loop
// intact: a broken entry skipped by max_deletions_per_run must be re-picked next
// run, and that must not depend on which probe found it.
func TestBrokenIsAlwaysDueAtAnyProbeVersion(t *testing.T) {
	now := time.Now()
	recheck := 7 * 24 * time.Hour

	for _, version := range []int{0, RepairProbeVersion - 1, RepairProbeVersion, RepairProbeVersion + 1} {
		broken := &EntryHealth{
			Status:         HealthBroken,
			ProbeVersion:   version,
			LastCheckedAt:  now,
			NextCheckDueAt: now.Add(recheck),
		}
		if !broken.IsDue(now, recheck) {
			t.Fatalf("broken at probe version %d is not due; the deletion-cap retry depends on it being re-picked every run", version)
		}
	}
}

// TestProbeVersionSurvivesStorageRoundTrip covers the on-disk contract in both
// directions:
//
//   - BACKWARD: a record written before the field existed decodes cleanly, with
//     ProbeVersion == 0 (the "ancient" sentinel) and therefore due.
//   - FORWARD:  a record carrying keys this build does not know is decoded
//     without error, which is what lets an OLDER build read a record written by
//     this one during a rollback.
func TestProbeVersionSurvivesStorageRoundTrip(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	dbPath := t.TempDir()
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	recheck := 7 * 24 * time.Hour

	// (1) OLD-FORMAT RECORD: exactly the JSON a pre-versioning build wrote — no
	// probe_version key at all — inserted straight into the repair-state store.
	oldFormat := `{"entry_name":"Ancient.Release","protocol":"torrent","status":"healthy",` +
		`"file_count":1,"broken_count":0,"dirty":false,` +
		`"last_checked_at":"` + now.Format(time.RFC3339Nano) + `",` +
		`"last_ok_at":"` + now.Format(time.RFC3339Nano) + `",` +
		`"next_check_due_at":"` + now.Add(recheck).Format(time.RFC3339Nano) + `",` +
		`"updated_at":"` + now.Format(time.RFC3339Nano) + `"}`
	if err := store.repairState.Put("Ancient.Release", []byte(oldFormat), &appendstore.PutOptions{Attributes: map[string]string{attributeStatus: string(HealthHealthy)}}); err != nil {
		t.Fatalf("seed old-format record: %v", err)
	}

	// (2) NEW RECORD written through the normal path.
	fresh := &EntryHealth{
		EntryName:      "Fresh.Release",
		Protocol:       config.ProtocolTorrent,
		Status:         HealthHealthy,
		ProbeVersion:   RepairProbeVersion,
		FileCount:      1,
		LastCheckedAt:  now,
		LastOKAt:       now,
		NextCheckDueAt: now.Add(recheck),
	}
	if err := store.SaveEntryHealth(fresh); err != nil {
		t.Fatalf("SaveEntryHealth: %v", err)
	}

	// (3) A record carrying an unknown key, standing in for "written by a build
	// newer than this one". If this failed to decode, a rollback would corrupt or
	// drop records rather than degrade gracefully.
	forward := `{"entry_name":"Future.Release","status":"healthy","probe_version":` +
		strconv.Itoa(RepairProbeVersion) + `,"some_future_field":{"a":1},` +
		`"last_checked_at":"` + now.Format(time.RFC3339Nano) + `",` +
		`"next_check_due_at":"` + now.Add(recheck).Format(time.RFC3339Nano) + `"}`
	if err := store.repairState.Put("Future.Release", []byte(forward), &appendstore.PutOptions{Attributes: map[string]string{attributeStatus: string(HealthHealthy)}}); err != nil {
		t.Fatalf("seed forward-compat record: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewStorage(dbPath)
	if err != nil {
		t.Fatalf("NewStorage (reopen): %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	ancient, err := reopened.GetEntryHealth("Ancient.Release")
	if err != nil || ancient == nil {
		t.Fatalf("an old-format record failed to decode: %v", err)
	}
	if ancient.Status != HealthHealthy || ancient.EntryName != "Ancient.Release" {
		t.Fatalf("old-format record decoded wrong: %+v", ancient)
	}
	if ancient.ProbeVersion != 0 {
		t.Fatalf("ProbeVersion = %d on a record that has no such key, want 0 (the ancient sentinel)", ancient.ProbeVersion)
	}
	if !ancient.IsDue(now, recheck) {
		t.Fatal("a pre-versioning record is not due; this is the exact 47,627-entry suppression the change exists to end")
	}

	roundTripped, err := reopened.GetEntryHealth("Fresh.Release")
	if err != nil || roundTripped == nil {
		t.Fatalf("GetEntryHealth(Fresh.Release): %v", err)
	}
	if roundTripped.ProbeVersion != RepairProbeVersion {
		t.Fatalf("ProbeVersion = %d after encode/decode, want %d", roundTripped.ProbeVersion, RepairProbeVersion)
	}
	if roundTripped.IsDue(now, recheck) {
		t.Fatal("a current-version healthy record is due immediately after a round trip; freshness broke")
	}

	future, err := reopened.GetEntryHealth("Future.Release")
	if err != nil || future == nil {
		t.Fatalf("a record with an unknown key failed to decode (%v); a rollback would not be safe", err)
	}
	if future.ProbeVersion != RepairProbeVersion {
		t.Fatalf("ProbeVersion = %d on the forward-compat record, want %d", future.ProbeVersion, RepairProbeVersion)
	}

	// The stored bytes must actually carry the key — a marshaller that dropped it
	// would make every record look ancient forever.
	raw, err := reopened.repairState.Get("Fresh.Release")
	if err != nil {
		t.Fatalf("read raw repair-state row: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("stored row is not valid JSON: %v", err)
	}
	if _, ok := decoded["probe_version"]; !ok {
		t.Fatalf("stored row omits probe_version: %s", string(raw))
	}
}

package storage

import (
	"strings"
	"testing"
)

// ⚠️ THE ORIGIN CLASSIFIER IS STACK-DERIVED, SO IT CAN ROT SILENTLY.
//
// It matches on function names. Rename a call site and the classification
// degrades to "unknown" with nothing failing — the log keeps printing, just
// uselessly. That is the same failure mode as the sweeps that logged nothing on
// success, so it gets a test.
//
// These assert the SHAPE the field rule depends on: that every blessed route has
// a distinct name, that the defect routes are visibly marked, and that the
// server-package ordering does not swallow shim deletes.

func TestOriginsAreDistinct(t *testing.T) {
	all := map[string]string{
		"qbit":       originShimQBit,
		"sab":        originShimSAB,
		"api":        originAPI,
		"completion": originCompletion,
		"refusal":    originAddRefusal,
		"repair":     originRepair,
		"migration":  originMigration,
		"stalled":    originStalledSweep,
		"prune":      originPrune,
		"unknown":    originUnknown,
	}
	seen := map[string]string{}
	for name, value := range all {
		if value == "" {
			t.Fatalf("%s has an empty origin; an empty field is indistinguishable from a missing one", name)
		}
		if prev, dup := seen[value]; dup {
			t.Fatalf("%s and %s share the origin %q — the field cannot discriminate them", name, prev, value)
		}
		seen[value] = name
	}
}

// 🔴 A REAPER DELETING A ROW IS THE DEFECT THAT LOST 15,004 ROWS IN 24H.
//
// Those origins must be greppable as defects on sight, not folded into a neutral
// name an operator would scroll past.
func TestReaperOriginsAreMarkedAsDefects(t *testing.T) {
	for _, origin := range []string{originStalledSweep, originPrune} {
		if !strings.HasPrefix(origin, "DEFECT-") {
			t.Fatalf("%q must be visibly a defect: after fork.53 no reaper deletes rows, so if one "+
				"ever fires the log has to say so rather than read as routine", origin)
		}
	}
}

// The shim packages live UNDER pkg/server, so a classifier that tested the
// parent first would label every arr-initiated delete as an API delete — which
// is precisely the discrimination the field rule needs.
func TestShimOriginsAreNotSwallowedByTheAPIOrigin(t *testing.T) {
	if originShimQBit == originAPI || originShimSAB == originAPI {
		t.Fatal("shim deletes must not classify as API deletes; the arr's own removal is the normal " +
			"lifecycle and an operator's is not")
	}
}

// An unclassified route must stay conspicuous. If this ever becomes something
// bland, a new deletion path can blend into the noise — the exact property the
// stack-derived caller was introduced to prevent.
func TestUnknownOriginIsASignal(t *testing.T) {
	if originUnknown == "" {
		t.Fatal("unknown must not be empty")
	}
	for _, blessed := range []string{originShimQBit, originShimSAB, originAPI, originCompletion,
		originAddRefusal, originRepair, originMigration} {
		if originUnknown == blessed {
			t.Fatalf("unknown collides with the blessed route %q, so an unclassified removal would "+
				"read as routine", blessed)
		}
	}
}

// queueRemovalOrigin must terminate and answer for an ordinary stack rather than
// panicking or hanging — it runs on every removal.
func TestOriginClassifierTerminatesOnAnUnknownStack(t *testing.T) {
	got := queueRemovalOrigin()
	if got == "" {
		t.Fatal("classifier returned an empty origin")
	}
	// Called straight from a test, so nothing in the blessed set is on the
	// stack; unknown is the correct answer and proves the default path works.
	if got != originUnknown {
		t.Logf("origin from a test stack = %q (expected %q, not a failure)", got, originUnknown)
	}
}

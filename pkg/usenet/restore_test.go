package usenet

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// failedRestorableNZB is the shape markAsFailed leaves behind when a false
// verdict hits an NZB that completed in an earlier lifecycle: Status flipped
// to failed, FailMessage stamped, Files (with segments) fully intact.
func failedRestorableNZB(id, generation string) *storage.NZB {
	nzb := lifecycleTestNZB(id, "movie.mkv", 4)
	nzb.Generation = generation
	nzb.Status = NZBStatusFailed
	nzb.FailMessage = "no valid files found in NZB"
	return nzb
}

func mustAddNZB(t *testing.T, store *NZBStorage, nzb *storage.NZB) {
	t.Helper()
	if err := store.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB(%s): %v", nzb.ID, err)
	}
}

func TestRestoreCompletedUnflipsFailedMeta(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id  = "restore-happy"
		gen = "gen-restore-happy"
	)
	mustAddNZB(t, store, failedRestorableNZB(id, gen))

	if err := store.RestoreCompleted(id, gen); err != nil {
		t.Fatalf("RestoreCompleted: %v", err)
	}

	got, err := store.GetNZB(id)
	if err != nil {
		t.Fatalf("GetNZB after restore: %v", err)
	}
	if got.Status != NZBStatusCompleted {
		t.Errorf("Status = %q, want %q", got.Status, NZBStatusCompleted)
	}
	if got.FailMessage != "" {
		t.Errorf("FailMessage = %q, want cleared", got.FailMessage)
	}
	if got.Generation != gen {
		t.Errorf("Generation = %q, want unchanged %q", got.Generation, gen)
	}
	if len(got.Files) != 1 || len(got.Files[0].Segments) != 1 || got.Files[0].Segments[0].MessageID != id+"-segment" {
		t.Errorf("segment map not preserved: %+v", got.Files)
	}
	if got.TotalSize != 4 || got.Files[0].Size != 4 {
		t.Errorf("sizes not preserved: total=%d file=%d", got.TotalSize, got.Files[0].Size)
	}

	// A second restore refuses (no longer failed) and changes nothing.
	if err := store.RestoreCompleted(id, gen); err == nil || !strings.Contains(err.Error(), "not \"failed\"") {
		t.Errorf("second restore error = %v, want status refusal", err)
	}
	again, err := store.GetNZB(id)
	if err != nil || again.Status != NZBStatusCompleted {
		t.Errorf("meta changed by refused restore: status=%q err=%v", again.Status, err)
	}
}

func TestRestoreCompletedGenerationSemantics(t *testing.T) {
	store := newTestNZBStorage(t)

	// Mismatching generation: strict refusal with ErrStaleNZBGeneration.
	const idMismatch = "restore-gen-mismatch"
	mustAddNZB(t, store, failedRestorableNZB(idMismatch, "gen-current"))
	if err := store.RestoreCompleted(idMismatch, "gen-other"); !errors.Is(err, ErrStaleNZBGeneration) {
		t.Errorf("mismatch error = %v, want ErrStaleNZBGeneration", err)
	}
	if got, _ := store.GetNZB(idMismatch); got == nil || got.Status != NZBStatusFailed {
		t.Errorf("mismatched restore mutated the meta: %+v", got)
	}

	// Blank stored token adopts the caller's generation. AddNZB always mints a
	// token, so seed the pre-lifecycle shape by writing the encoded blob
	// directly (exactly what pre-fence writers left on disk).
	const idAdopt = "restore-gen-adopt"
	preLifecycle := failedRestorableNZB(idAdopt, "")
	blob, err := encodeNZBV2(preLifecycle)
	if err != nil {
		t.Fatalf("encode pre-lifecycle fixture: %v", err)
	}
	if err := os.WriteFile(store.metaFilePath(idAdopt), blob, 0o644); err != nil {
		t.Fatalf("write pre-lifecycle fixture: %v", err)
	}
	if err := store.RestoreCompleted(idAdopt, "gen-adopted"); err != nil {
		t.Fatalf("RestoreCompleted adopt: %v", err)
	}
	got, err := store.GetNZB(idAdopt)
	if err != nil || got.Status != NZBStatusCompleted || got.Generation != "gen-adopted" {
		t.Errorf("adoption failed: status=%q generation=%q err=%v", got.Status, got.Generation, err)
	}

	// Blank caller generation skips the ownership check entirely.
	const idBlank = "restore-gen-blank-caller"
	mustAddNZB(t, store, failedRestorableNZB(idBlank, "gen-kept"))
	if err := store.RestoreCompleted(idBlank, ""); err != nil {
		t.Fatalf("RestoreCompleted blank caller generation: %v", err)
	}
	if got, _ := store.GetNZB(idBlank); got == nil || got.Generation != "gen-kept" {
		t.Errorf("stored generation not kept: %+v", got)
	}
}

func TestRestoreCompletedRefusals(t *testing.T) {
	store := newTestNZBStorage(t)

	if err := store.RestoreCompleted("restore-missing", ""); !errors.Is(err, ErrNZBNotFound) {
		t.Errorf("missing meta error = %v, want ErrNZBNotFound", err)
	}

	cases := []struct {
		name       string
		mutate     func(nzb *storage.NZB)
		wantSubstr string
	}{
		{
			name:       "durably-bad",
			mutate:     func(nzb *storage.NZB) { nzb.IsBad = true },
			wantSubstr: "durably bad",
		},
		{
			name:       "empty-files",
			mutate:     func(nzb *storage.NZB) { nzb.Files = nil },
			wantSubstr: "no parsed files",
		},
		{
			name:       "deleted-file",
			mutate:     func(nzb *storage.NZB) { nzb.Files[0].IsDeleted = true },
			wantSubstr: "permanently failed",
		},
		{
			name:       "segmentless-file",
			mutate:     func(nzb *storage.NZB) { nzb.Files[0].Segments = nil },
			wantSubstr: "has no segments",
		},
		{
			name: "zero-size",
			mutate: func(nzb *storage.NZB) {
				nzb.TotalSize = 0
				nzb.Files[0].Size = 0
			},
			wantSubstr: "no positive size",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := "restore-refuse-" + tc.name
			nzb := failedRestorableNZB(id, "")
			tc.mutate(nzb)
			mustAddNZB(t, store, nzb)

			err := store.RestoreCompleted(id, "")
			if err == nil || !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("error = %v, want substring %q", err, tc.wantSubstr)
			}
			got, gerr := store.GetNZB(id)
			if gerr != nil || got.Status != NZBStatusFailed || got.FailMessage == "" {
				t.Errorf("refused restore mutated the meta: %+v (err=%v)", got, gerr)
			}
		})
	}
}

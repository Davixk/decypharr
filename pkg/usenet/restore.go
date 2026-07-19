package usenet

import (
	"fmt"
)

// RestoreCompleted flips a durably failed NZB's metadata back to
// Status=completed and clears its FailMessage. Nothing else is touched: Files,
// segment maps, sizes, Path, and Generation (unless adopted, see below) are
// preserved exactly as stored.
//
// It exists solely for OFFLINE RECOVERY tooling (cmd/revive-entries). The
// daemon's markAsFailed only flips Status and FailMessage on the durable meta
// — it never clears Files — so an NZB that completed in an earlier lifecycle
// and was later stamped failed by a false verdict still carries its full
// parsed segment map inside the failed meta. Un-flipping the status restores
// it with zero network access. The daemon itself never calls this; its forward
// path is markAsCompleted.
//
// Refusal semantics mirror markAsCompleted's, inverted for the restore
// direction. It refuses when:
//   - the meta does not exist (ErrNZBNotFound) or is not currently failed,
//   - the meta is durably bad (IsBad) or any file was permanently failed
//     (IsDeleted) — those are genuine content verdicts, never un-flipped,
//   - there is no streamable content: empty Files, a file without segments,
//     or no positive size (TotalSize and the sum of file sizes both <= 0).
//
// generation, when non-empty, must match the stored lifecycle token exactly
// like the daemon's guarded writes (a pre-lifecycle meta with a blank token
// adopts it). Pass "" to skip the ownership check; the write then keeps the
// stored token unchanged.
//
// The whole load -> validate -> flip -> save sequence runs under the per-NZB
// lifecycle lock and the storage mutex, so it cannot interleave with any other
// guarded metadata write.
func (s *NZBStorage) RestoreCompleted(id, generation string) error {
	if id == "" {
		return fmt.Errorf("NZB ID is required")
	}
	unlock := s.lockNZBLifecycle(id)
	defer unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	nzb, exists, err := s.readNZBIfPresentLocked(id)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: %s", ErrNZBNotFound, id)
	}
	if generation != "" {
		// Adoption (blank stored token) is persisted by the final write below.
		if _, err := adoptOrRequireNZBGeneration(nzb, generation); err != nil {
			return err
		}
	}
	if nzb.Status != NZBStatusFailed {
		return fmt.Errorf("refusing to restore NZB %s: status is %q, not %q", id, nzb.Status, NZBStatusFailed)
	}
	if nzb.IsBad {
		return fmt.Errorf("refusing to restore durably bad NZB %s: %s", id, nzb.FailMessage)
	}
	if len(nzb.Files) == 0 {
		return fmt.Errorf("refusing to restore NZB %s: no parsed files in metadata", id)
	}
	var summedSize int64
	for i := range nzb.Files {
		if nzb.Files[i].IsDeleted {
			return fmt.Errorf("refusing to restore NZB %s: file %q permanently failed", id, nzb.Files[i].Name)
		}
		if len(nzb.Files[i].Segments) == 0 {
			return fmt.Errorf("refusing to restore NZB %s: file %q has no segments", id, nzb.Files[i].Name)
		}
		summedSize += nzb.Files[i].Size
	}
	if nzb.TotalSize <= 0 && summedSize <= 0 {
		return fmt.Errorf("refusing to restore NZB %s: no positive size in metadata", id)
	}

	nzb.Status = NZBStatusCompleted
	nzb.FailMessage = ""
	return s.writeNZBLocked(nzb)
}

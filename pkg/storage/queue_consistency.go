package storage

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
)

// The queue is read two different ways, and the whole "ghost grab" class of
// failure is those two ways disagreeing:
//
//   - QueueExists -> a bare index lookup. This is what Queue.Add consults to
//     reject a duplicate reservation.
//   - FilterQueued -> a full scan (ForEach), which walks the index, reads each
//     record from the log and decodes it. This is what every listing endpoint
//     and therefore every arr sees.
//
// When the lookup resolves a key the scan never yields, the entry is
// simultaneously "already exists" on re-add and invisible everywhere — with no
// way to observe that from outside except by inference. These types measure the
// disagreement directly, so index membership can be established without going
// through debrid submission (which confounds the answer with provider cache
// state).

// QueueKeyMismatch is an index key whose stored record belongs to a different
// entry. This decodes cleanly, so it is silent: the listing shows the other
// entry twice and never shows this key.
type QueueKeyMismatch struct {
	IndexKey       string `json:"index_key"`
	RecordInfoHash string `json:"record_infohash"`
	RecordName     string `json:"record_name"`
}

// QueueOrphan is an index key that a full scan did not yield.
//
// Confirmed is the field that matters. This reconcile takes three separate
// snapshots — the key list, the scan, then a per-key re-read — and on a busy
// queue an entry can legitimately be deleted between any two of them. That
// produces an apparent orphan which is really just a concurrent delete, and it
// has exactly the signature of the defect being hunted: transient, self-healing,
// and indexed-but-not-scanned. Only an orphan that is still indexed, still
// readable by key, and still absent from a fresh scan is a genuine
// contradiction.
type QueueOrphan struct {
	IndexKey      string `json:"index_key"`
	Confirmed     bool   `json:"confirmed"`
	StillIndexed  bool   `json:"still_indexed"`
	DirectReadOK  bool   `json:"direct_read_ok"`
	DirectReadErr string `json:"direct_read_error,omitempty"`
	Name          string `json:"name,omitempty"`
	Category      string `json:"category,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
	Status        string `json:"status,omitempty"`
}

// QueueConsistencyReport is a whole-store reconcile of index membership against
// what a scan actually yields.
type QueueConsistencyReport struct {
	IndexCount        int                `json:"index_count"`
	ScanCount         int                `json:"scan_count"`
	Consistent        bool               `json:"consistent"`
	IndexedNotScanned []QueueOrphan      `json:"indexed_not_scanned"`
	ScannedNotIndexed []string           `json:"scanned_not_indexed"`
	KeyRecordMismatch []QueueKeyMismatch `json:"key_record_mismatch"`
	Undecodable       []string           `json:"undecodable"`
}

// QueueKeyDiagnosis answers, for one infohash, the question a re-add attempt
// only answers indirectly: is this key in the index, is its record readable,
// and does a scan actually yield it?
type QueueKeyDiagnosis struct {
	InfoHash      string `json:"infohash"`
	InIndex       bool   `json:"in_index"`
	DirectReadOK  bool   `json:"direct_read_ok"`
	DirectReadErr string `json:"direct_read_error,omitempty"`
	ScanYields    bool   `json:"scan_yields"`
	// Poisoned is the signature under investigation: present to the duplicate
	// check that rejects a re-add, absent from every listing an arr polls.
	Poisoned bool   `json:"poisoned"`
	Name     string `json:"name,omitempty"`
	Category string `json:"category,omitempty"`
	Protocol string `json:"protocol,omitempty"`
	Status   string `json:"status,omitempty"`
}

// ConfirmedOrphanCount counts only orphans that survived re-verification. An
// unconfirmed orphan is a snapshot artefact of an entry deleted mid-reconcile,
// not evidence of anything.
func (r *QueueConsistencyReport) ConfirmedOrphanCount() int {
	n := 0
	for _, orphan := range r.IndexedNotScanned {
		if orphan.Confirmed {
			n++
		}
	}
	return n
}

// QueueConsistency reconciles queue index membership against a full scan.
func (s *Storage) QueueConsistency() (*QueueConsistencyReport, error) {
	report := &QueueConsistencyReport{
		IndexedNotScanned: []QueueOrphan{},
		ScannedNotIndexed: []string{},
		KeyRecordMismatch: []QueueKeyMismatch{},
		Undecodable:       []string{},
	}

	indexed := make(map[string]struct{})
	for _, key := range s.queue.Keys() {
		indexed[key] = struct{}{}
	}
	report.IndexCount = len(indexed)

	scanned := make(map[string]struct{}, len(indexed))
	if err := s.queue.ForEach(func(key string, value []byte) error {
		scanned[key] = struct{}{}
		var pb EntryProto
		if err := proto.Unmarshal(value, &pb); err != nil {
			report.Undecodable = append(report.Undecodable, key)
			return nil
		}
		entry := ProtoToEntry(&pb)
		if !strings.EqualFold(entry.InfoHash, key) {
			report.KeyRecordMismatch = append(report.KeyRecordMismatch, QueueKeyMismatch{
				IndexKey:       key,
				RecordInfoHash: entry.InfoHash,
				RecordName:     entry.Name,
			})
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan queue for consistency report: %w", err)
	}
	report.ScanCount = len(scanned)

	// Capture each candidate's evidence IMMEDIATELY, before anything else runs.
	//
	// Ordering is the whole design here. Deferring this until after a
	// re-verification pass would mean a genuinely poisoned entry that gets
	// deleted in the meantime is dismissed — trading the original false
	// positive for a false negative on exactly the rare event being hunted.
	// The distinguishing evidence is available at the moment of detection: a
	// benign concurrent delete is already unreadable by key, while a real
	// orphan is still indexed and still readable. Take it then, and let the
	// re-scan only dismiss, never gate.
	type candidate struct {
		key    string
		orphan QueueOrphan
	}
	var candidates []candidate
	for key := range indexed {
		if _, ok := scanned[key]; ok {
			continue
		}
		orphan := QueueOrphan{IndexKey: key}
		orphan.StillIndexed = s.queue.Exists(key)
		if _, err := s.queue.Get(key); err != nil {
			orphan.DirectReadErr = err.Error()
		} else {
			orphan.DirectReadOK = true
		}
		if meta, err := s.queue.GetMetadata(key); err == nil && meta != nil {
			orphan.Name = meta.Attribute(attributeName)
			orphan.Category = meta.Attribute(attributeCategory)
			orphan.Protocol = meta.Attribute(attributeProtocol)
			orphan.Status = meta.Attribute(attributeStatus)
		}
		candidates = append(candidates, candidate{key: key, orphan: orphan})
	}

	// Re-scan only to dismiss keys that were mid-flight during the first walk.
	// A candidate that appears here was being written while the first scan
	// passed it, which is not a divergence.
	rescanned := make(map[string]struct{})
	if len(candidates) > 0 {
		if err := s.queue.ForEach(func(key string, _ []byte) error {
			rescanned[key] = struct{}{}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("re-scan queue for consistency report: %w", err)
		}
	}

	for _, c := range candidates {
		if _, ok := rescanned[c.key]; ok {
			continue
		}
		// Judged on the evidence as it stood at detection, so a later deletion
		// cannot retroactively explain away a real contradiction.
		c.orphan.Confirmed = c.orphan.StillIndexed && c.orphan.DirectReadOK
		report.IndexedNotScanned = append(report.IndexedNotScanned, c.orphan)
	}

	// The mirror case: a key the scan yielded that the earlier key snapshot did
	// not list is almost always a concurrent add. Only report it if the key is
	// genuinely absent from the index now.
	for key := range scanned {
		if _, ok := indexed[key]; ok {
			continue
		}
		if !s.queue.Exists(key) {
			report.ScannedNotIndexed = append(report.ScannedNotIndexed, key)
		}
	}

	report.Consistent = report.ConfirmedOrphanCount() == 0 &&
		len(report.ScannedNotIndexed) == 0 &&
		len(report.KeyRecordMismatch) == 0 &&
		len(report.Undecodable) == 0

	return report, nil
}

// QueueKeyState reports index membership and scan visibility for a single
// infohash, with no debrid interaction.
func (s *Storage) QueueKeyState(infohash string) (*QueueKeyDiagnosis, error) {
	key := strings.ToLower(strings.TrimSpace(infohash))
	if key == "" {
		return nil, fmt.Errorf("infohash is required")
	}

	diagnosis := &QueueKeyDiagnosis{InfoHash: key}
	diagnosis.InIndex = s.queue.Exists(key)

	if _, err := s.queue.Get(key); err != nil {
		diagnosis.DirectReadErr = err.Error()
	} else {
		diagnosis.DirectReadOK = true
	}

	if err := s.queue.ForEach(func(scanKey string, _ []byte) error {
		if strings.EqualFold(scanKey, key) {
			diagnosis.ScanYields = true
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan queue for key %s: %w", key, err)
	}

	if meta, err := s.queue.GetMetadata(key); err == nil && meta != nil {
		diagnosis.Name = meta.Attribute(attributeName)
		diagnosis.Category = meta.Attribute(attributeCategory)
		diagnosis.Protocol = meta.Attribute(attributeProtocol)
		diagnosis.Status = meta.Attribute(attributeStatus)
	}

	diagnosis.Poisoned = diagnosis.InIndex && !diagnosis.ScanYields
	return diagnosis, nil
}

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

// QueueOrphan is an index key that a full scan did not yield. DirectReadOK
// distinguishes the real contradiction (the record is readable by key, so the
// scan simply skipped it) from a key that was concurrently deleted mid-scan.
type QueueOrphan struct {
	IndexKey      string `json:"index_key"`
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

	for key := range indexed {
		if _, ok := scanned[key]; ok {
			continue
		}
		// Confirm before reporting. Keys() and ForEach are separate snapshots,
		// so a key deleted between them is an expected miss, not a defect. A
		// key whose record still reads back by direct lookup is the real
		// contradiction.
		orphan := QueueOrphan{IndexKey: key}
		if _, err := s.queue.Get(key); err != nil {
			orphan.DirectReadErr = err.Error()
		} else {
			orphan.DirectReadOK = true
		}
		if meta, err := s.queue.GetMeta(key); err == nil && meta != nil {
			orphan.Name = meta.Name
			orphan.Category = meta.Category
			orphan.Protocol = meta.Protocol
			orphan.Status = meta.Status
		}
		report.IndexedNotScanned = append(report.IndexedNotScanned, orphan)
	}

	for key := range scanned {
		if _, ok := indexed[key]; !ok {
			report.ScannedNotIndexed = append(report.ScannedNotIndexed, key)
		}
	}

	report.Consistent = len(report.IndexedNotScanned) == 0 &&
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

	if meta, err := s.queue.GetMeta(key); err == nil && meta != nil {
		diagnosis.Name = meta.Name
		diagnosis.Category = meta.Category
		diagnosis.Protocol = meta.Protocol
		diagnosis.Status = meta.Status
	}

	diagnosis.Poisoned = diagnosis.InIndex && !diagnosis.ScanYields
	return diagnosis, nil
}

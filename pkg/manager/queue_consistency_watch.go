package manager

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

const (
	queueConsistencyIntervalEnv     = "DECYPHARR_QUEUE_CONSISTENCY_INTERVAL"
	defaultQueueConsistencyInterval = 5 * time.Minute
)

// queueConsistencyInterval resolves the sampling interval. "0", "off" and
// "false" disable the watcher entirely.
func queueConsistencyInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv(queueConsistencyIntervalEnv))
	if raw == "" {
		return defaultQueueConsistencyInterval
	}
	switch strings.ToLower(raw) {
	case "0", "off", "false", "no":
		return 0
	}
	parsed, err := utils.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return defaultQueueConsistencyInterval
	}
	return parsed
}

// watchQueueConsistency samples queue index membership against what a full scan
// actually yields, and reports the moment they diverge.
//
// The divergence is the "ghost grab" signature: Queue.Add rejects a re-add
// based on a bare index lookup, while every listing an arr polls comes from a
// scan that never yields the key. The entry is then both "already exists" and
// invisible, with no outward symptom beyond an arr silently re-grabbing forever.
//
// It has to be caught in the live process. The index is rebuilt from the log at
// startup, so a restart erases the evidence -- which means anyone who notices
// the symptom and then restarts to inspect has already destroyed what they came
// to look at. Sampling on an interval instead bounds when the condition
// appeared, which is the one thing after-the-fact inspection can never recover.
//
// Cost is one scan of the queue store, which holds queued and active downloads
// rather than the whole library. Silent while healthy.
func (m *Manager) watchQueueConsistency(ctx context.Context) {
	interval := queueConsistencyInterval()
	if interval <= 0 {
		m.logger.Debug().Msg("Queue consistency watcher disabled")
		return
	}

	m.logger.Info().
		Dur("interval", interval).
		Msgf("Queue consistency watcher started; set %s=0 to disable", queueConsistencyIntervalEnv)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var divergedSince time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report, err := m.Storage().QueueConsistency()
			if err != nil {
				m.logger.Error().Err(err).Msg("Queue consistency check failed")
				continue
			}
			if report.Consistent {
				if !divergedSince.IsZero() {
					m.logger.Warn().
						Time("diverged_since", divergedSince).
						Dur("diverged_for", time.Since(divergedSince)).
						Msg("Queue index and scan agree again; the divergence cleared without a restart")
					divergedSince = time.Time{}
				}
				// Healthy and quiet is the normal case, but "a raw orphan was
				// seen and dismissed" is not the same event as "nothing
				// happened", and silence cannot distinguish them. Say so once,
				// so a reader can tell the detector ran and cleared something
				// from the detector not running at all.
				if raw := len(report.IndexedNotScanned); raw > 0 {
					m.logger.Info().
						Int("raw_orphans", raw).
						Int("confirmed_orphans", 0).
						Strs("confirmed_orphan_keys", []string{}).
						Msg("Queue reconcile saw entries removed mid-scan and dismissed them as snapshot artefacts; index and scan agree")
				}
				continue
			}

			first := divergedSince.IsZero()
			if first {
				divergedSince = time.Now()
			}
			event := m.logger.Error().
				Bool("first_detection", first).
				Time("diverged_since", divergedSince).
				Int("index_count", report.IndexCount).
				Int("scan_count", report.ScanCount).
				Int("confirmed_orphans", report.ConfirmedOrphanCount()).
				Int("indexed_not_scanned", len(report.IndexedNotScanned)).
				Int("scanned_not_indexed", len(report.ScannedNotIndexed)).
				Int("key_record_mismatch", len(report.KeyRecordMismatch)).
				Int("undecodable", len(report.Undecodable))
			// Emit the key lists UNCONDITIONALLY, empty included. A field that
			// appears only when non-empty reads as "not logged" rather than
			// "none", and that exact ambiguity already cost this investigation
			// a round: a conditional field's absence was the answer, and was
			// filed as a missing feature instead.
			event = event.
				Strs("confirmed_orphan_keys", confirmedOrphanKeys(report.IndexedNotScanned, 20)).
				Strs("scanned_not_indexed_keys", capKeys(report.ScannedNotIndexed, 20))
			event.Msg("Queue index disagrees with a full scan: entries are rejected as duplicates on re-add while absent from every listing")
		}
	}
}

// confirmedOrphanKeys returns up to limit orphan keys that survived
// re-verification: still indexed, still readable by key, and still absent from
// a second independent scan.
//
// Unconfirmed orphans are deliberately excluded. An entry deleted partway
// through the reconcile produces an apparent orphan with exactly the signature
// of the real defect — transient, self-healing, indexed-but-not-scanned — so
// reporting those would manufacture the very finding this is meant to detect.
func confirmedOrphanKeys(orphans []storage.QueueOrphan, limit int) []string {
	keys := make([]string, 0, limit)
	for _, orphan := range orphans {
		if !orphan.Confirmed {
			continue
		}
		if len(keys) == limit {
			break
		}
		keys = append(keys, orphan.IndexKey)
	}
	return keys
}

func capKeys(keys []string, limit int) []string {
	if len(keys) <= limit {
		return keys
	}
	return keys[:limit]
}

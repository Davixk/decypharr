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
				Int("indexed_not_scanned", len(report.IndexedNotScanned)).
				Int("scanned_not_indexed", len(report.ScannedNotIndexed)).
				Int("key_record_mismatch", len(report.KeyRecordMismatch)).
				Int("undecodable", len(report.Undecodable))
			if keys := poisonedKeys(report.IndexedNotScanned, 10); len(keys) > 0 {
				event = event.Strs("poisoned_keys", keys)
			}
			event.Msg("Queue index disagrees with a full scan: entries are rejected as duplicates on re-add while absent from every listing")
		}
	}
}

// poisonedKeys returns up to limit orphan keys whose record is still readable
// by direct lookup — the confirmed contradiction, as opposed to a key deleted
// between the index snapshot and the scan.
func poisonedKeys(orphans []storage.QueueOrphan, limit int) []string {
	keys := make([]string, 0, limit)
	for _, orphan := range orphans {
		if !orphan.DirectReadOK {
			continue
		}
		if len(keys) == limit {
			break
		}
		keys = append(keys, orphan.IndexKey)
	}
	return keys
}

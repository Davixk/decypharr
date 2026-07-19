package manager

import (
	"context"
	"strings"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// minActionGate is the floor for the post-download action gate so restore
// backlogs keep draining even when max_active_downloads is configured very low.
const minActionGate = 4

// actionGateSize returns the capacity of the post-download action gate:
// max(minActionGate, maxActiveDownloads).
func actionGateSize(maxActiveDownloads int) int {
	if maxActiveDownloads > minActionGate {
		return maxActiveDownloads
	}
	return minActionGate
}

// acquireActionSlot blocks until a post-download action slot is free or the
// manager shuts down. Managers constructed without init (tests) may have no
// gate and acquire trivially.
func (m *Manager) acquireActionSlot() bool {
	if m.actionSem == nil {
		return true
	}
	ctx := m.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case m.actionSem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (m *Manager) releaseActionSlot() {
	if m.actionSem == nil {
		return
	}
	<-m.actionSem
}

// beginActionInflight registers infohash as having a pending/running
// post-download action in this process. It returns false when another
// goroutine already owns the action for this hash.
func (m *Manager) beginActionInflight(infohash string) bool {
	if m.actionInflight == nil {
		return true
	}
	_, loaded := m.actionInflight.LoadOrStore(strings.ToLower(infohash), struct{}{})
	return !loaded
}

func (m *Manager) endActionInflight(infohash string) {
	if m.actionInflight == nil {
		return
	}
	m.actionInflight.Delete(strings.ToLower(infohash))
}

func (m *Manager) isActionInflight(infohash string) bool {
	if m.actionInflight == nil {
		return false
	}
	_, ok := m.actionInflight.Load(strings.ToLower(infohash))
	return ok
}

// submitResumeAction resumes a durably claimed post-download action on a
// detached goroutine, bounded by the action gate. Restore-time recovery (boot)
// funnels through here, so a reboot backlog of claimed entries drains at gate
// width instead of stampeding the mount and the completed-entry persist mutex.
func (m *Manager) submitResumeAction(entry *storage.Entry) {
	if entry == nil {
		return
	}
	if !m.beginActionInflight(entry.InfoHash) {
		m.logger.Debug().
			Str("infohash", entry.InfoHash).
			Msg("Post-download action already pending in this process")
		return
	}
	// Copy before detaching: restore loops keep refreshing their own Entry
	// pointers, and the resumed action must not share one with them.
	snapshot := *entry
	go func() {
		defer m.endActionInflight(snapshot.InfoHash)
		if !m.acquireActionSlot() {
			return
		}
		defer m.releaseActionSlot()
		m.resumeClaimedAction(&snapshot)
	}()
}

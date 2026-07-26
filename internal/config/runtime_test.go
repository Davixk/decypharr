package config

import (
	"sync"
	"testing"
)

// TestRequiresRestartMaxActiveDownloads pins the restart classification of
// max_active_downloads: the manager sizes its worker pool and action gate
// from it once at construction, so a change must route config saves through
// the restart branch (applyConfigUpdate) instead of the silent live-apply.
func TestRequiresRestartMaxActiveDownloads(t *testing.T) {
	current := &Config{}
	current.setDefaults()

	unchanged := *current
	if current.RequiresRestart(&unchanged) {
		t.Fatal("identical configs flagged restart-needed")
	}

	updated := *current
	updated.MaxActiveDownloads = current.MaxActiveDownloads + 3
	if !current.RequiresRestart(&updated) {
		t.Fatal("MaxActiveDownloads change was not flagged restart-needed")
	}

	// A hot field alone must still apply live.
	hotOnly := *current
	hotOnly.RefreshInterval = "42m"
	if current.RequiresRestart(&hotOnly) {
		t.Fatal("hot-only change (RefreshInterval) was flagged restart-needed")
	}
}

func TestSnapshotQueueCleanupCopiesRules(t *testing.T) {
	c := &Config{QueueCleanup: QueueCleanup{
		Rules: []QueueCleanupRule{{ID: "failed_download", Action: "blacklist"}},
	}}

	snapshot := c.SnapshotQueueCleanup()
	c.QueueCleanup.Rules[0].Action = "import"

	if got := snapshot.Rules[0].Action; got != "blacklist" {
		t.Fatalf("snapshot shared the live rules backing array: action=%q", got)
	}
}

func TestSnapshotQueueCleanupConcurrentApplyRuntime(t *testing.T) {
	first := &Config{QueueCleanup: QueueCleanup{
		Rules: []QueueCleanupRule{{ID: "first", Action: "blacklist"}},
	}}
	second := &Config{QueueCleanup: QueueCleanup{
		Rules: []QueueCleanupRule{{ID: "second", Action: "import"}},
	}}
	current := &Config{}
	current.ApplyRuntime(first)

	failures := make(chan string, 1)
	report := func(message string) {
		select {
		case failures <- message:
		default:
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			if i%2 == 0 {
				current.ApplyRuntime(first)
			} else {
				current.ApplyRuntime(second)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			snapshot := current.SnapshotQueueCleanup()
			if len(snapshot.Rules) != 1 {
				report("snapshot observed an incomplete rules slice")
				return
			}
			rule := snapshot.Rules[0]
			if (rule.ID != "first" || rule.Action != "blacklist") &&
				(rule.ID != "second" || rule.Action != "import") {
				report("snapshot combined fields from different runtime policies")
				return
			}
		}
	}()
	wg.Wait()

	select {
	case failure := <-failures:
		t.Fatal(failure)
	default:
	}
}

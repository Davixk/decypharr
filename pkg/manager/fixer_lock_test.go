package manager

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/customerror"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// TestStuckProviderStatusPollDoesNotBlockFileOperations is the regression test
// for the wedge this file's changes exist to remove.
//
// Fixer.MoveTorrent used to hold Manager.copyEntryMu — the GLOBAL mutex every
// WebDAV DELETE, COPY and MOVE takes — across SubmitMagnet and CheckStatus.
// CheckStatus re-polls the provider, so a single account stuck in
// "waiting_files_selection" froze every file operation in the process for as
// long as the provider stayed stuck.
//
// The negative control is the whole point: with the provider calls moved back
// under copyEntryMu, the DELETE below never returns and this test fails on its
// own deadline rather than hanging the suite.
func TestStuckProviderStatusPollDoesNotBlockFileOperations(t *testing.T) {
	m := newProviderLifecycleManager(t)
	repairHash := strings.Repeat("c", 39) + "1"
	deleteHash := strings.Repeat("d", 39) + "2"

	repairSnapshot := persistLifecycleEntry(t, m, lifecycleEntry(repairHash, "target", "target-old"))
	persistLifecycleEntry(t, m, lifecycleEntry(deleteHash, "bystander", "bystander-id"))

	polling := make(chan struct{})
	release := make(chan struct{})
	target := &lifecycleDebridClient{name: "target"}
	target.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		return completedRemote(repairHash, "target", "target-new"), nil
	}
	// Stands in for a provider that keeps answering "waiting_files_selection":
	// the poll is alive, it simply never concludes.
	target.check = func(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
		close(polling)
		<-release
		return remote, nil
	}
	bystander := &lifecycleDebridClient{name: "bystander"}
	m.clients.Store("target", target)
	m.clients.Store("bystander", bystander)

	moveDone := make(chan struct{})
	go func() {
		defer close(moveDone)
		_, _ = m.fixer.MoveTorrent(repairSnapshot, "target", true)
	}()

	select {
	case <-polling:
	case <-time.After(10 * time.Second):
		t.Fatal("premise check failed: the provider status poll was never reached")
	}

	// THE ACCEPTANCE CRITERION. A file operation must not wait on a provider
	// status poll — not briefly, not at all.
	deleted := make(chan error, 1)
	go func() { deleted <- m.DeleteEntry(deleteHash, true) }()

	select {
	case err := <-deleted:
		if err != nil {
			t.Fatalf("DeleteEntry during a stuck provider poll: %v", err)
		}
	case <-time.After(5 * time.Second):
		close(release)
		<-moveDone
		t.Fatal("DELETE blocked on a stuck provider status poll: the file-operation mutex is still held across a provider call")
	}

	close(release)
	<-moveDone

	// The repair itself must still land correctly once the provider answers —
	// narrowing the lock must not have cost the commit its meaning.
	current, err := m.storage.Get(repairHash)
	if err != nil {
		t.Fatalf("Get repaired entry: %v", err)
	}
	if current.ActiveProvider != "target" || current.Providers["target"].ID != "target-new" {
		t.Fatalf("re-insertion did not commit after the poll finished: %+v", current.Providers["target"])
	}
	if exists, _ := m.storage.Exists(deleteHash); exists {
		t.Fatal("the concurrent DELETE reported success but left the entry behind")
	}
}

// TestInconclusiveReinsertionDoesNotCondemnTheEntry pins the constraint that a
// ceiling firing is not a verdict.
//
// Entry.Bad short-circuits every later read of an entry before any provider is
// contacted, and only a successful re-insertion clears it. If a status-poll
// ceiling could set it, one slow minute on a provider would blank content that
// is perfectly healthy the moment the provider recovers — and the per-debrid
// failure marker would exclude that provider from repairing the entry for the
// rest of the process's life, since nothing ever deletes those keys.
func TestInconclusiveReinsertionDoesNotCondemnTheEntry(t *testing.T) {
	m := newProviderLifecycleManager(t)
	hash := strings.Repeat("e", 39) + "3"
	snapshot := persistLifecycleEntry(t, m, lifecycleEntry(hash, "target", "target-old"))

	target := &lifecycleDebridClient{name: "target"}
	target.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		return completedRemote(hash, "target", "target-new"), nil
	}
	target.check = func(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
		return remote, customerror.NewBackendTimeoutError(fmt.Errorf("status poll ceiling fired"))
	}
	m.clients.Store("target", target)
	// The fixture config declares no debrids, so NewFixer built an empty attempt
	// order. Name the one provider this test cares about.
	m.fixer.providerOrder = []string{"target"}

	result, err := m.fixer.FixTorrent(context.Background(), snapshot, false)
	if err == nil || result == nil || result.Success {
		t.Fatalf("FixTorrent = (%+v, %v), want a failed repair", result, err)
	}
	if !customerror.IsBackendTimeout(err) {
		t.Fatalf("FixTorrent error = %v; a ceiling firing must stay a typed transient failure", err)
	}
	if customerror.IsContentPermanentlyGone(err) {
		t.Fatal("a status-poll ceiling was reported as a permanent content verdict")
	}

	current, getErr := m.storage.Get(hash)
	if getErr != nil {
		t.Fatalf("Get entry: %v", getErr)
	}
	if current.Bad {
		t.Fatal("an inconclusive re-insertion marked the entry Bad; a provider that did not answer said nothing about the content")
	}
	if m.fixer.IsFailedToReinsert(hash, "target") {
		t.Fatal("an inconclusive re-insertion blacklisted the debrid; that marker is never cleared")
	}
	// The placement created before the ceiling fired must still be compensated
	// away — declining to reach a verdict is not a licence to leak a slot.
	if got := target.deleted(); len(got) != 1 || got[0] != "target-new" {
		t.Fatalf("compensation after the ceiling fired = %v, want [target-new]", got)
	}
}

package manager

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A TARGETED CHECK RETRIES A NON-VERDICT; A SWEEP DOES NOT.
//
// A forced check answering `unknown` tells an operator nothing actionable. It
// cannot always resolve — when a provider is unreachable there is no true answer
// and manufacturing one means calling dead content healthy or live content
// broken at library scale — but it can stop reporting a momentary blip as the
// provider's considered answer.
//
// The boundary is what makes it affordable: one entry can spend seconds, a sweep
// probing ~47,000 files cannot spend them three thousand times.
type flakyProbeClient struct {
	probeClient
	calls     atomic.Int32
	failFirst int32
}

func (f *flakyProbeClient) CheckFile(ctx context.Context, infohash, link string) error {
	n := f.calls.Add(1)
	if n <= f.failFirst {
		return debridTypes.ErrAvailabilityIndeterminate
	}
	return nil
}

func targetedItem(t *testing.T, m *Manager, hash string) *storage.EntryItem {
	t.Helper()
	entry := probeTorrentEntry(hash, hash)
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	return &storage.EntryItem{
		Name:  hash,
		Files: map[string]*storage.File{"file.mkv": {Name: "file.mkv", InfoHash: hash, Size: 1024}},
	}
}

func TestTargetedCheckRetriesUntilItResolves(t *testing.T) {
	client := &flakyProbeClient{failFirst: 1}
	m, r := newProbeFixture(t, client)
	item := targetedItem(t, m, "flaky")

	res := r.probeFileWithRetry(context.Background(), item, "file.mkv",
		RepairRunOptions{Targeted: true}, false, storage.HealthHealthy)

	if !res.healthy {
		t.Fatalf("a blip that cleared on the second attempt still reported %+v. A forced check that answers "+
			"`unknown` on one transient 429 is exactly the report the operator called unactionable", res)
	}
	if got := client.calls.Load(); got != 2 {
		t.Fatalf("made %d probe attempts, want 2 (one failure, one success)", got)
	}
}

// 🛑 THE SWEEP MUST NOT RETRY. Without this boundary the fix costs ~47,000
// probes' worth of extra attempts and backoff per run — a different order of
// expense than the one that was authorised.
func TestScheduledSweepDoesNotRetry(t *testing.T) {
	client := &flakyProbeClient{failFirst: 1}
	m, r := newProbeFixture(t, client)
	item := targetedItem(t, m, "sweepflaky")

	res := r.probeFileWithRetry(context.Background(), item, "file.mkv",
		RepairRunOptions{}, false, storage.HealthHealthy)

	if res.healthy || res.broken {
		t.Fatalf("the sweep reached a verdict %+v; with one indeterminate answer it should report a non-verdict", res)
	}
	if got := client.calls.Load(); got != 1 {
		t.Fatalf("the scheduled sweep made %d attempts, want exactly 1. Retrying here multiplies a 47,000-file "+
			"run by the retry count", got)
	}
}

// A SUSTAINED OUTAGE STILL REPORTS unknown, and that is the honest outcome. This
// is the half of the ruling deliberately not implemented: resolving an outage as
// broken is how a provider's bad afternoon deletes a library.
func TestSustainedOutageStillReportsUnknown(t *testing.T) {
	client := &flakyProbeClient{failFirst: 99}
	m, r := newProbeFixture(t, client)
	item := targetedItem(t, m, "downprovider")

	res := r.probeFileWithRetry(context.Background(), item, "file.mkv",
		RepairRunOptions{Targeted: true}, false, storage.HealthHealthy)

	if res.broken {
		t.Fatal("a provider that never answered was condemned as BROKEN. An answer we could not get is not " +
			"evidence the content is gone, and acting on it at library scale is the mass-deletion case")
	}
	if res.healthy {
		t.Fatal("a provider that never answered was reported HEALTHY")
	}
	if got := client.calls.Load(); got != 3 {
		t.Fatalf("made %d attempts, want 3 (the bound). Unbounded retry would hang a forced check for as long "+
			"as the provider stays down", got)
	}
}

// A VERDICT IS NEVER RETRIED. Re-asking a question already answered only creates
// a chance to disagree with it.
func TestTargetedCheckDoesNotRetryAVerdict(t *testing.T) {
	client := &flakyProbeClient{failFirst: 0}
	m, r := newProbeFixture(t, client)
	item := targetedItem(t, m, "decided")

	res := r.probeFileWithRetry(context.Background(), item, "file.mkv",
		RepairRunOptions{Targeted: true}, false, storage.HealthHealthy)

	if !res.healthy {
		t.Fatalf("probed %+v, want healthy", res)
	}
	if got := client.calls.Load(); got != 1 {
		t.Fatalf("a healthy verdict was re-probed %d times", got)
	}
}

// AND A STRUCTURAL NON-VERDICT IS NOT RETRIED EITHER. A missing infohash is
// still missing on the next attempt; retrying it just adds the full backoff to
// every hopeless entry.
func TestStructuralNonVerdictIsNotRetried(t *testing.T) {
	for _, reason := range []string{
		"missing_infohash", "entry_not_found", "protocol_skipped",
		"provider_client_not_found", "provider_check_unsupported", "placement_not_found",
	} {
		if probeReasonIsRetryable(reason) {
			t.Errorf("%q is treated as retryable; nothing about it can change between attempts", reason)
		}
	}
	for _, reason := range []string{
		"provider_probe_indeterminate", "provider_payload_indeterminate", "unrestrict_link_error",
	} {
		if !probeReasonIsRetryable(reason) {
			t.Errorf("%q is not retryable, so a transient provider failure still reports unknown on first look", reason)
		}
	}
	_ = config.ProtocolTorrent
}

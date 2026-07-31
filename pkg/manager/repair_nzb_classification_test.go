package manager

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/nntp"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// These tests fix the boundary between "this content is gone" and "we could not
// tell", for usenet, at the layer where PRUNE reads the answer.
//
// The production shape: an entry served an EXISTING BUT EMPTY directory over
// WebDAV — handlePropfind dropped every child because preparing it returned a
// permanent 410 from the durable IsDeleted flag — while the repair probe
// recorded `unknown` / `usenet_probe_error` / file_count 1 for the very same
// entry. 123 entries sat in that state: definitively unserveable, and
// permanently non-actionable because the probe could not name what it saw.
//
// The trap on the other side is that `usenet_probe_error` ALSO covered a
// missing meta file — a lost segment map, which says nothing about the content
// and must never be deletable. Both cases now have their own reason code.

// newNZBProbeFixture builds a Repair wired to a REAL usenet client over a
// temp-dir meta store, so CheckFile exercises the actual decode/classify path
// rather than a stub of it. No NNTP connection is ever made: every case here
// returns a verdict from the durable metadata before any STAT is issued.
func newNZBProbeFixture(t *testing.T) (*Manager, *Repair, *usenet.Usenet) {
	t.Helper()
	m, r := newProbeFixture(t, nil)

	cfg := config.Get()
	cfg.Usenet.Providers = []config.UsenetProvider{{Host: "127.0.0.1", Port: 119, MaxConnections: 1}}
	u, err := usenet.New()
	if err != nil {
		t.Fatalf("usenet.New: %v", err)
	}
	t.Cleanup(func() { _ = u.Close() })
	m.usenet = u
	return m, r, u
}

func nzbProbeEntry(hash, name string) *storage.Entry {
	now := time.Unix(1_700_000_000, 0).UTC()
	return &storage.Entry{
		Protocol:   config.ProtocolNZB,
		InfoHash:   hash,
		Name:       name,
		Status:     debridTypes.TorrentStatusDownloaded,
		IsComplete: true,
		AddedOn:    now,
		Files: map[string]*storage.File{
			"movie.mkv": {Name: "movie.mkv", InfoHash: hash, Size: 4096, AddedOn: now},
		},
	}
}

func deadUsenetMeta(id string) *storage.NZB {
	return &storage.NZB{
		ID:          id,
		TotalSize:   4096,
		Status:      usenet.NZBStatusFailed,
		IsBad:       true,
		FailMessage: "articles missing on provider",
		Files: []storage.NZBFile{{
			Name:      "movie.mkv",
			Size:      4096,
			IsDeleted: true,
			Segments:  []storage.NZBSegment{{Number: 1, MessageID: id + "-seg", Bytes: 4096}},
		}},
	}
}

// TestProbeNZBFilePermanentlyFailedIsBroken: the entry whose directory renders
// EMPTY to every WebDAV client must be recorded as broken, not `unknown`.
func TestProbeNZBFilePermanentlyFailedIsBroken(t *testing.T) {
	_, r, u := newNZBProbeFixture(t)
	const hash = "nzb-dead"
	if err := u.NZBStorage().AddNZB(deadUsenetMeta(hash)); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}

	entry := nzbProbeEntry(hash, "DeadRelease")
	res := r.probeNZBFile(context.Background(), entry, "movie.mkv", fileResult{name: "movie.mkv"}, false)

	if !res.broken {
		t.Fatalf("a file the serve path answers 410 Gone for probed broken=false (reason=%q, healthy=%v); it stays non-actionable forever",
			res.reason, res.healthy)
	}
	if res.healthy {
		t.Fatal("dead content probed healthy")
	}
	if res.reason != "usenet_article_missing" {
		t.Fatalf("reason = %q, want %q so the verdict names WHICH condition condemned it", res.reason, "usenet_article_missing")
	}
}

// TestProbeNZBMissingMetaIsUnknownNotBroken is the safety half. A lost segment
// map is decypharr's own bookkeeping failure. It must be diagnosable and it must
// be inert.
func TestProbeNZBMissingMetaIsUnknownNotBroken(t *testing.T) {
	_, r, _ := newNZBProbeFixture(t)

	// Deliberately no AddNZB: the meta file does not exist.
	entry := nzbProbeEntry("nzb-no-meta", "MetaLostRelease")
	res := r.probeNZBFile(context.Background(), entry, "movie.mkv", fileResult{name: "movie.mkv"}, false)

	if res.broken {
		t.Fatal("A MISSING SEGMENT MAP WAS CLASSIFIED AS BROKEN. That is destructive-eligible under PRUNE; losing a local index file must never delete content.")
	}
	if res.healthy {
		t.Fatal("a missing meta file probed healthy; nothing was verified")
	}
	if res.reason != "usenet_meta_missing" {
		t.Fatalf("reason = %q, want %q — it must be distinguishable from dead content, not share usenet_probe_error with it", res.reason, "usenet_meta_missing")
	}
}

// TestProbeNZBMissingMetaCannotTriggerDestructiveAction proves the consequence,
// not just the label: run the whole entry-level verdict and assert no
// destructive component can act on it.
func TestProbeNZBMissingMetaCannotTriggerDestructiveAction(t *testing.T) {
	m, r, _ := newNZBProbeFixture(t)
	const (
		hash = "nzb-no-meta-entry"
		name = "MetaLostRelease"
	)
	entry := nzbProbeEntry(hash, name)
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	item, err := m.storage.GetEntryItem(name)
	if err != nil || item == nil {
		t.Fatalf("GetEntryItem(%s): %v", name, err)
	}

	h, _ := r.probeEntry(context.Background(), "run-meta-missing", &candidate{name: name, item: item}, newHealCache(), RepairRunOptions{}, false)
	if h == nil {
		t.Fatal("probeEntry returned nil")
	}
	if h.Status != storage.HealthUnknown {
		t.Fatalf("status = %q, want %q — a lost segment map is not a content verdict", h.Status, storage.HealthUnknown)
	}
	if h.BrokenCount != 0 {
		t.Fatalf("broken_count = %d, want 0", h.BrokenCount)
	}
	if pruneEligible(h) {
		t.Fatal("PRUNE WOULD DELETE AN ENTRY WHOSE ONLY FAULT IS A MISSING LOCAL INDEX FILE")
	}
	if entryHealthHasArrLink(h) {
		t.Fatal("ARR-DELETE would act on a missing-meta entry")
	}
	if h.FailureReason != "usenet_meta_missing" {
		t.Fatalf("failure_reason = %q, want %q so the condition is diagnosable without a re-run", h.FailureReason, "usenet_meta_missing")
	}
}

// TestProbeNZBEmptySegmentListStaysUnchanged: a file with no segments is not a
// file the provider deleted. Nobody proved anything about it, so it keeps the
// historical non-verdict.
func TestProbeNZBEmptySegmentListStaysUnchanged(t *testing.T) {
	_, r, u := newNZBProbeFixture(t)
	const hash = "nzb-empty-segments"
	if err := u.NZBStorage().AddNZB(&storage.NZB{
		ID:        hash,
		TotalSize: 4096,
		Files:     []storage.NZBFile{{Name: "movie.mkv", Size: 4096}},
	}); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}

	entry := nzbProbeEntry(hash, "EmptySegmentsRelease")
	res := r.probeNZBFile(context.Background(), entry, "movie.mkv", fileResult{name: "movie.mkv"}, false)

	if res.broken {
		t.Fatal("an empty segment list was condemned as dead content; no provider ever said these articles are gone")
	}
	if res.healthy {
		t.Fatal("an empty segment list probed healthy")
	}
	if res.reason != "usenet_probe_error" {
		t.Fatalf("reason = %q, want the unchanged %q", res.reason, "usenet_probe_error")
	}
}

// TestOnlyContentVerdictsAreDeadContent is the safety statement of this change,
// written as an assertion instead of a claim: it enumerates the failure classes
// that a probe can encounter and pins that ONLY definitive content verdicts are
// deletable. Everything an outage, a rate limit, an expired token or a lost
// local file produces must fall on the non-dead side.
func TestOnlyContentVerdictsAreDeadContent(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		dead bool
	}{
		// --- NOT dead: the machinery failed, the content did not ---
		{"nzb meta file missing (lost segment map)", fmt.Errorf("failed to sample file segments: %w: id", usenet.ErrNZBNotFound), false},
		{"availability indeterminate", fmt.Errorf("%w for %q: dial refused", usenet.ErrAvailabilityIndeterminate, "movie.mkv"), false},
		{"nntp connection failure", nntp.NewConnectionError(errors.New("connection reset")), false},
		{"nntp timeout", nntp.NewTimeoutError(errors.New("i/o timeout")), false},
		{"nntp no connection available", nntp.NewNoAvailableConnectionError("pools saturated", errors.New("no conn")), false},
		{"context canceled", context.Canceled, false},
		{"context deadline exceeded", context.DeadlineExceeded, false},
		{"provider probe indeterminate", debridTypes.ErrAvailabilityIndeterminate, false},
		{"unclassified error", errors.New("boom"), false},
		{"empty segment list", errors.New("file has no Segments: movie.mkv"), false},
		{"invalid usenet metadata (permanent, but 500)", func() error {
			e := customerror.NewPermanentError(errors.New("invalid usenet file metadata"))
			e.Code = "usenet_metadata_invalid"
			return e
		}(), false},

		// --- dead: a provider stated the content is gone ---
		{"durable IsDeleted / articles missing", customerror.NewArticleNotFoundError(errors.New("articles missing on provider")), true},
		{"sampled segments definitively missing", customerror.UsenetSegmentMissingError, true},
		{"debrid content gone (404/410)", customerror.NewContentGoneError(errors.New("410")), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDeadContentVerdict(tc.err); got != tc.dead {
				verb := "was NOT"
				if got {
					verb = "WAS"
				}
				t.Fatalf("isDeadContentVerdict(%v) = %v, want %v — this condition %s classified as deletable content",
					tc.err, got, tc.dead, verb)
			}
			// The serve path must agree with the probe on the same predicate.
			if tc.dead != customerror.IsContentPermanentlyGone(tc.err) &&
				!errors.Is(tc.err, debridTypes.EmptyDownloadLinkError) &&
				!nntp.IsContentMissingError(tc.err) {
				t.Fatalf("serve path and probe disagree about %v: probe=%v serve=%v",
					tc.err, tc.dead, customerror.IsContentPermanentlyGone(tc.err))
			}
		})
	}
}

// TestNNTPAuthAndPermissionFailuresCannotSetTheDeletedFlag guards the OTHER end
// of the chain — the one that makes the IsDeleted subset safe to act on at all.
// The durable flag has a single writer, reachable only behind
// nntp.IsContentMissingError / IsArticleNotFoundError, and internal/nntp
// deliberately keeps every substrate failure out of that class.
func TestNNTPAuthAndPermissionFailuresCannotSetTheDeletedFlag(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"connection", nntp.NewConnectionError(errors.New("reset"))},
		{"timeout", nntp.NewTimeoutError(errors.New("i/o timeout"))},
		{"no available connection", nntp.NewNoAvailableConnectionError("saturated", errors.New("x"))},
		{"server busy (503/400)", nntp.NewProtocolError(503, "service temporarily unavailable")},
		{"unclassified", errors.New("boom")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if nntp.IsContentMissingError(tc.err) {
				t.Fatalf("%v is treated as content-missing; it would durably flag a file IsDeleted and make the entry deletable", tc.err)
			}
			if nntp.IsArticleNotFoundError(tc.err) {
				t.Fatalf("%v is treated as article-not-found; it would durably flag a file IsDeleted", tc.err)
			}
		})
	}
}

// TestProbeNZBDeletedFileIsFullyBrokenAndPrunable states the consequence of the
// fix out loud: this narrow, provable subset — and only it — becomes actionable.
func TestProbeNZBDeletedFileIsFullyBrokenAndPrunable(t *testing.T) {
	m, r, u := newNZBProbeFixture(t)
	const (
		hash = "nzb-dead-entry"
		name = "DeadRelease"
	)
	if err := u.NZBStorage().AddNZB(deadUsenetMeta(hash)); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	entry := nzbProbeEntry(hash, name)
	if err := m.storage.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	item, err := m.storage.GetEntryItem(name)
	if err != nil || item == nil {
		t.Fatalf("GetEntryItem(%s): %v", name, err)
	}

	h, _ := r.probeEntry(context.Background(), "run-dead", &candidate{name: name, item: item}, newHealCache(), RepairRunOptions{}, false)
	if h == nil {
		t.Fatal("probeEntry returned nil")
	}
	if h.Status != storage.HealthBroken {
		t.Fatalf("status = %q, want %q", h.Status, storage.HealthBroken)
	}
	if h.Structural {
		t.Fatal("this is a CONTENT verdict, not a structural one; file_count must stay 1 so the entry is genuinely actionable")
	}
	if h.FileCount != 1 || h.BrokenCount != 1 {
		t.Fatalf("file_count=%d broken_count=%d, want 1/1 — the entry is fully dead", h.FileCount, h.BrokenCount)
	}
	if h.FailureReason != "usenet_article_missing" {
		t.Fatalf("failure_reason = %q, want %q", h.FailureReason, "usenet_article_missing")
	}
	if !pruneEligible(h) {
		t.Fatalf("a fully-dead entry is not prune-eligible (%q); the fix would change nothing in production", pruneIneligibleReason(h))
	}
	if h.ProbeVersion != storage.RepairProbeVersion {
		t.Fatalf("ProbeVersion = %d, want %d", h.ProbeVersion, storage.RepairProbeVersion)
	}
}

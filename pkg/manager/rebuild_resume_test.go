package manager

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// writePreFenceCompletedMeta persists a COMPLETED NZB meta the way pre-fork.1
// builds left them on disk: the legacy protobuf codec with no generation
// token. Written directly (not through AddNZB) because every current guarded
// write path mints a generation — exactly what pre-fence metadata lacks.
func writePreFenceCompletedMeta(t *testing.T, id string) string {
	t.Helper()
	pb := &usenet.NZBProto{
		Id:        id,
		Name:      "movie.nzb",
		Status:    usenet.NZBStatusCompleted,
		TotalSize: 4096,
		Files: []*usenet.NZBFileProto{{
			NzbId: id,
			Name:  "movie.mkv",
			Size:  4096,
			Segments: []*usenet.NZBSegmentProto{{
				Number:    1,
				MessageId: "segment-one@test",
				Bytes:     4096,
			}},
		}},
	}
	data, err := proto.Marshal(pb)
	if err != nil {
		t.Fatalf("marshal pre-fence meta: %v", err)
	}
	metaPath := filepath.Join(config.GetMainPath(), "usenet", "meta", id+".meta")
	if err := os.MkdirAll(filepath.Dir(metaPath), 0o755); err != nil {
		t.Fatalf("mkdir meta dir: %v", err)
	}
	if err := os.WriteFile(metaPath, data, 0o644); err != nil {
		t.Fatalf("write pre-fence meta: %v", err)
	}
	return metaPath
}

// assertMetaCompletedWithSegments fails the test when the durable meta no
// longer carries the completed status and full segment map it was seeded with.
func assertMetaCompletedWithSegments(t *testing.T, m *Manager, id string) *storage.NZB {
	t.Helper()
	meta, err := m.usenet.NZBStorage().GetNZB(id)
	if err != nil {
		t.Fatalf("GetNZB(%s): %v", id, err)
	}
	if meta.Status != usenet.NZBStatusCompleted {
		t.Fatalf("durable meta status = %q, want %q (completed meta was overwritten)", meta.Status, usenet.NZBStatusCompleted)
	}
	if len(meta.Files) != 1 || len(meta.Files[0].Segments) != 1 || meta.Files[0].Segments[0].MessageID != "segment-one@test" {
		t.Fatalf("durable segment map destroyed: %+v", meta.Files)
	}
	return meta
}

// A queued rebuild over a COMPLETED pre-fence meta (blank generation, i.e.
// written before the lifecycle fence existed) must resume from the durable
// metadata, never fall through to a re-parse. The fall-through is the
// 2026-07-19 data-loss mechanism: rebuildQueuedNZBJob minted a fresh queue
// generation, the blank-generation completed meta failed the equality gate,
// and ParseWithGeneration's quick-parse persist then REPLACED the completed
// meta with an empty-file parsing shape (parser.Parse builds
// `Files: []storage.NZBFile{}`), destroying the parsed segment map before
// markAsFailed ever ran. This test FAILS on that behavior and pins the fix.
func TestRebuildQueuedNZBJobResumesCompletedPreFenceMeta(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true) // healthy: a re-parse would succeed and wipe
	host, port := server.hostPort(t)
	m, _ := newVerdictTestManager(t, host, port)

	entry := newQueuedNZBEntry(t, m, "prefence-completed") // blank NZBGeneration + staged source
	writePreFenceCompletedMeta(t, entry.InfoHash)

	job, err := m.rebuildQueuedNZBJob(entry)
	if err != nil {
		t.Fatalf("rebuildQueuedNZBJob: %v", err)
	}
	if job == nil || !job.ResumeExisting {
		t.Fatalf("job = %+v, want ResumeExisting for a completed meta (re-parse path would wipe it)", job)
	}
	if job.NZBMeta == nil || job.NZBMeta.Status != usenet.NZBStatusCompleted {
		t.Fatalf("job.NZBMeta = %+v, want the completed metadata", job.NZBMeta)
	}

	meta := assertMetaCompletedWithSegments(t, m, entry.InfoHash)
	if entry.NZBGeneration == "" {
		t.Fatal("entry.NZBGeneration still blank after rebuild")
	}
	if meta.Generation != entry.NZBGeneration {
		t.Fatalf("meta generation %q not adopted to queue generation %q", meta.Generation, entry.NZBGeneration)
	}
}

// Same hole, other half: the ENTRY already owns a generation (persisted by an
// earlier restore) while the completed meta is still pre-fence blank. The
// blank meta must adopt the queue's token and resume, not re-parse.
func TestRebuildQueuedNZBJobAdoptsEntryGenerationIntoBlankCompletedMeta(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	m, _ := newVerdictTestManager(t, host, port)

	entry := newQueuedNZBEntry(t, m, "prefence-entry-owns")
	entry.NZBGeneration = "gen-entry-owns"
	if err := m.queue.Update(entry); err != nil {
		t.Fatalf("persist entry generation: %v", err)
	}
	writePreFenceCompletedMeta(t, entry.InfoHash)

	job, err := m.rebuildQueuedNZBJob(entry)
	if err != nil {
		t.Fatalf("rebuildQueuedNZBJob: %v", err)
	}
	if job == nil || !job.ResumeExisting {
		t.Fatalf("job = %+v, want ResumeExisting for a completed meta", job)
	}

	meta := assertMetaCompletedWithSegments(t, m, entry.InfoHash)
	if meta.Generation != "gen-entry-owns" {
		t.Fatalf("meta generation = %q, want the queue's token adopted (%q)", meta.Generation, "gen-entry-owns")
	}
	if entry.NZBGeneration != "gen-entry-owns" {
		t.Fatalf("entry generation changed to %q", entry.NZBGeneration)
	}
}

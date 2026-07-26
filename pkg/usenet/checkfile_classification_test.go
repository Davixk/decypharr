package usenet

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"google.golang.org/protobuf/proto"
)

// CheckFile's error is a CLASSIFICATION consumed by a probe that can delete
// content. These tests pin the three in-meta outcomes apart from each other,
// because production had them collapsed into one:
//
//	file flagged IsDeleted   -> typed permanent article-missing (DEAD CONTENT)
//	meta file missing        -> ErrNZBNotFound  (lost index, NOT a verdict)
//	empty segment list       -> plain error     (no verdict either way)
//
// Before this, the first case returned a bare fmt.Errorf("file has no
// Segments") indistinguishable from the third, while the SERVE path was already
// answering 410 Gone for the very same file and hiding it from its parent
// listing. The two paths disagreed about one condition.

// newClassificationUsenet builds a Usenet over a temp meta store with a usable
// config singleton. CheckFile reads config.Get().Usenet.AvailabilitySamplePercent,
// which os.Exit(1)s when no config path is set.
func newClassificationUsenet(t *testing.T, store *NZBStorage) *Usenet {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	_ = config.Get()
	return newTestUsenet(store)
}

func deletedFileNZB(id, filename string) *storage.NZB {
	nzb := lifecycleTestNZB(id, filename, 4096)
	nzb.Files[0].IsDeleted = true
	nzb.IsBad = true
	nzb.Status = NZBStatusFailed
	return nzb
}

// TestCheckFileDeletedFileIsTypedDeadContent is the core of the fix: the
// durable IsDeleted flag must surface as a TYPED permanent verdict the repair
// probe already recognises, not as an unclassifiable generic error.
func TestCheckFileDeletedFileIsTypedDeadContent(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id       = "dead-content"
		filename = "movie.mkv"
	)
	if err := store.AddNZB(deletedFileNZB(id, filename)); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	u := newClassificationUsenet(t, store)

	err := u.CheckFile(context.Background(), id, filename)
	if err == nil {
		t.Fatal("CheckFile on a permanently-failed file returned nil; the serve path answers 410 Gone for it")
	}
	if !customerror.IsContentPermanentlyGone(err) {
		t.Fatalf("CheckFile error = %v; want a definitive dead-content verdict (customerror.IsContentPermanentlyGone)", err)
	}
	var custom *customerror.Error
	if !errors.As(err, &custom) {
		t.Fatalf("CheckFile error %v is not a *customerror.Error", err)
	}
	if custom.Code != "usenet_article_missing" {
		t.Fatalf("code = %q, want %q — the probe classifies on this code", custom.Code, "usenet_article_missing")
	}
	if !custom.IsPermanent() {
		t.Fatal("a durable IsDeleted verdict must be permanent; a retryable one would be re-probed forever")
	}
	// It must NOT masquerade as the lost-index condition.
	if errors.Is(err, ErrNZBNotFound) {
		t.Fatal("dead content reported as ErrNZBNotFound; those are opposite verdicts")
	}
}

// TestCheckFileDeletedFileLegacyProtoIsTypedDeadContent covers the OTHER codec.
// SampleFileMessageIDs has two decode paths, and the legacy one reached the
// deleted file through GetFileByName, which silently skips deleted files — the
// same (nil, nil) hole as the v2 path. A meta file that has not yet been
// migrated must classify identically.
func TestCheckFileDeletedFileLegacyProtoIsTypedDeadContent(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id       = "legacy-dead-content"
		filename = "movie.mkv"
	)
	blob, err := proto.Marshal(&NZBProto{
		Id:     id,
		Name:   "legacy.nzb",
		Status: NZBStatusFailed,
		IsBad:  true,
		Files: []*NZBFileProto{{
			Name:      filename,
			Size:      4096,
			IsDeleted: true,
			Segments:  []*NZBSegmentProto{{Number: 1, MessageId: id + "-segment", Bytes: 4096}},
		}},
	})
	if err != nil {
		t.Fatalf("marshal legacy meta: %v", err)
	}
	if err := os.WriteFile(store.metaFilePath(id), blob, 0o644); err != nil {
		t.Fatalf("write legacy meta: %v", err)
	}

	u := newClassificationUsenet(t, store)
	checkErr := u.CheckFile(context.Background(), id, filename)
	if !customerror.IsContentPermanentlyGone(checkErr) {
		t.Fatalf("legacy-proto CheckFile error = %v; want a definitive dead-content verdict", checkErr)
	}
}

// TestCheckFileMissingMetaIsNeverDeadContent is the trap this whole change
// exists to avoid. A missing meta file means decypharr LOST THE SEGMENT MAP. It
// says nothing about whether the content is alive, and it must never be able to
// present itself as a dead-content verdict — that verdict is deletable.
func TestCheckFileMissingMetaIsNeverDeadContent(t *testing.T) {
	store := newTestNZBStorage(t)
	u := newClassificationUsenet(t, store)

	err := u.CheckFile(context.Background(), "no-such-nzb", "movie.mkv")
	if err == nil {
		t.Fatal("CheckFile with no meta file on disk returned nil")
	}
	if !errors.Is(err, ErrNZBNotFound) {
		t.Fatalf("error = %v; want ErrNZBNotFound so the probe can classify it as non-actionable", err)
	}
	if customerror.IsContentPermanentlyGone(err) {
		t.Fatalf("A LOST SEGMENT MAP WAS CLASSIFIED AS DEAD CONTENT (%v). That verdict is destructive-eligible under PRUNE.", err)
	}
	if errors.Is(err, ErrFilePermanentlyFailed) {
		t.Fatalf("missing meta reported as ErrFilePermanentlyFailed: %v", err)
	}
}

// TestCheckFileEmptySegmentListStaysIndeterminate pins that the genuinely-empty
// case is UNCHANGED. It is a different thing from a file the provider deleted:
// nobody ever proved its articles are gone.
func TestCheckFileEmptySegmentListStaysIndeterminate(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id       = "no-segments"
		filename = "movie.mkv"
	)
	if err := store.AddNZB(&storage.NZB{
		ID:        id,
		TotalSize: 4096,
		Files:     []storage.NZBFile{{Name: filename, Size: 4096}},
	}); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	u := newClassificationUsenet(t, store)

	err := u.CheckFile(context.Background(), id, filename)
	if err == nil {
		t.Fatal("CheckFile on a segment-less file returned nil (would score healthy)")
	}
	if !strings.Contains(err.Error(), "file has no Segments") {
		t.Fatalf("error = %v; want the historical segment-less message", err)
	}
	if customerror.IsContentPermanentlyGone(err) {
		t.Fatalf("an empty segment list was classified as dead content: %v — nothing proved these articles are gone", err)
	}
	if errors.Is(err, ErrFilePermanentlyFailed) || errors.Is(err, ErrNZBNotFound) {
		t.Fatalf("empty segment list mis-typed: %v", err)
	}
}

// TestCheckFileAbsentFilenameStaysIndeterminate: a filename that is not in the
// meta at all is also not a content verdict.
func TestCheckFileAbsentFilenameStaysIndeterminate(t *testing.T) {
	store := newTestNZBStorage(t)
	const id = "present-nzb"
	if err := store.AddNZB(lifecycleTestNZB(id, "movie.mkv", 4096)); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	u := newClassificationUsenet(t, store)

	err := u.CheckFile(context.Background(), id, "not-in-this-nzb.mkv")
	if err == nil {
		t.Fatal("CheckFile for an unknown filename returned nil")
	}
	if customerror.IsContentPermanentlyGone(err) {
		t.Fatalf("an unknown filename was classified as dead content: %v", err)
	}
}

// TestSampleFileMessageIDsDistinguishesDeletedFromAbsent pins the distinction at
// the layer that must MAKE it. Both cases returned (nil, nil) before, so the
// caller could only guess — and guessing wrong in either direction is either a
// missed deletion or a wrongful one.
func TestSampleFileMessageIDsDistinguishesDeletedFromAbsent(t *testing.T) {
	store := newTestNZBStorage(t)
	const (
		id       = "distinguish"
		filename = "movie.mkv"
	)
	if err := store.AddNZB(deletedFileNZB(id, filename)); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}

	ids, err := store.SampleFileMessageIDs(id, filename, 100)
	if len(ids) != 0 {
		t.Fatalf("a deleted file yielded %d message ids; it must yield none", len(ids))
	}
	if !errors.Is(err, ErrFilePermanentlyFailed) {
		t.Fatalf("deleted file error = %v, want ErrFilePermanentlyFailed", err)
	}

	ids, err = store.SampleFileMessageIDs(id, "never-existed.mkv", 100)
	if len(ids) != 0 || err != nil {
		t.Fatalf("absent filename = (%v, %v); want (nil, nil) — absence is not a verdict", ids, err)
	}
}

// TestSampleFileMessageIDsLiveNamesakeWins guards the widened lookup. Remembering
// a deleted namesake must not condemn a file that is still live under the same
// name — "permanently failed" means nothing of that name can be served.
func TestSampleFileMessageIDsLiveNamesakeWins(t *testing.T) {
	const (
		id       = "namesake"
		filename = "movie.mkv"
	)
	for _, order := range []string{"deleted-first", "live-first"} {
		t.Run(order, func(t *testing.T) {
			store := newTestNZBStorage(t)
			dead := storage.NZBFile{Name: filename, Size: 4096, IsDeleted: true}
			live := storage.NZBFile{
				Name: filename,
				Size: 4096,
				Segments: []storage.NZBSegment{
					{Number: 1, MessageID: "live-segment", Bytes: 4096},
				},
			}
			files := []storage.NZBFile{dead, live}
			if order == "live-first" {
				files = []storage.NZBFile{live, dead}
			}
			if err := store.AddNZB(&storage.NZB{ID: id, TotalSize: 8192, Files: files}); err != nil {
				t.Fatalf("AddNZB: %v", err)
			}

			ids, err := store.SampleFileMessageIDs(id, filename, 100)
			if err != nil {
				t.Fatalf("a live file was reported failed because a deleted namesake exists: %v", err)
			}
			if len(ids) != 1 || ids[0] != "live-segment" {
				t.Fatalf("sampled ids = %v; want the LIVE file's segment", ids)
			}
		})
	}
}

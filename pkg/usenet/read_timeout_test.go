package usenet

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestProgressDeadlineExpiresWithoutProgress(t *testing.T) {
	deadline := newProgressDeadline(context.Background(), 40*time.Millisecond)
	defer deadline.Close()

	select {
	case <-deadline.Done():
		if cause := context.Cause(deadline); !errors.Is(cause, ErrReadTimeout) {
			t.Fatalf("deadline cause = %v, want ErrReadTimeout", cause)
		}
	case <-time.After(time.Second):
		t.Fatal("progress deadline did not expire")
	}
}

func TestProgressDeadlineResetsWhenBytesAreDelivered(t *testing.T) {
	const timeout = 200 * time.Millisecond
	deadline := newProgressDeadline(context.Background(), timeout)
	defer deadline.Close()

	for range 3 {
		time.Sleep(50 * time.Millisecond)
		deadline.Progress()
		select {
		case <-deadline.Done():
			t.Fatalf("deadline expired despite recent progress: %v", context.Cause(deadline))
		default:
		}
	}

	select {
	case <-deadline.Done():
		if cause := context.Cause(deadline); !errors.Is(cause, ErrReadTimeout) {
			t.Fatalf("deadline cause = %v, want ErrReadTimeout", cause)
		}
	case <-time.After(time.Second):
		t.Fatal("progress deadline did not expire after progress stopped")
	}
}

func TestPrepareStreamReconcilesSegmentDerivedSize(t *testing.T) {
	store := newTestNZBStorage(t)
	nzb := &storage.NZB{
		ID:        "size-fix",
		TotalSize: 6,
		Files: []storage.NZBFile{{
			Name: "movie.mkv",
			Size: 6,
			Segments: []storage.NZBSegment{
				{Number: 1, MessageID: "one", Bytes: 3, StartOffset: 0, EndOffset: 2},
				{Number: 2, MessageID: "two", Bytes: 2, StartOffset: 3, EndOffset: 4},
			},
		}},
	}
	if err := store.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}

	u := newTestUsenet(store)
	gotSize, err := u.PrepareStream(nzb.ID, "movie.mkv")
	if err != nil {
		t.Fatalf("PrepareStream: %v", err)
	}
	if gotSize != 5 {
		t.Fatalf("PrepareStream size = %d, want 5", gotSize)
	}

	stored, err := store.GetNZB(nzb.ID)
	if err != nil {
		t.Fatalf("GetNZB: %v", err)
	}
	if stored.Files[0].Size != 5 || stored.TotalSize != 5 {
		t.Fatalf("persisted sizes = file %d, total %d; want 5, 5", stored.Files[0].Size, stored.TotalSize)
	}
}

func TestPrepareStreamDoesNotExpandSmallerAdvertisedSize(t *testing.T) {
	store := newTestNZBStorage(t)
	nzb := &storage.NZB{
		ID:        "cipher-padding",
		TotalSize: 4,
		Files: []storage.NZBFile{{
			Name:        "encrypted.mkv",
			Size:        4,
			IsEncrypted: true,
			Segments: []storage.NZBSegment{
				{Number: 1, MessageID: "one", Bytes: 3, StartOffset: 0, EndOffset: 2},
				{Number: 2, MessageID: "two", Bytes: 2, StartOffset: 3, EndOffset: 4},
			},
		}},
	}
	if err := store.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}

	gotSize, err := newTestUsenet(store).PrepareStream(nzb.ID, "encrypted.mkv")
	if err != nil {
		t.Fatalf("PrepareStream: %v", err)
	}
	if gotSize != 4 {
		t.Fatalf("PrepareStream size = %d, want preserved advertised size 4", gotSize)
	}
	stored, err := store.GetNZB(nzb.ID)
	if err != nil {
		t.Fatalf("GetNZB: %v", err)
	}
	if stored.Files[0].Size != 4 || stored.TotalSize != 4 {
		t.Fatalf("persisted sizes = file %d, total %d; want unchanged 4, 4", stored.Files[0].Size, stored.TotalSize)
	}
}

func TestPrepareStreamRejectsNonContiguousSegmentMap(t *testing.T) {
	store := newTestNZBStorage(t)
	nzb := &storage.NZB{
		ID:        "bad-map",
		TotalSize: 4,
		Files: []storage.NZBFile{{
			Name: "movie.mkv",
			Size: 4,
			Segments: []storage.NZBSegment{
				{Number: 1, MessageID: "one", Bytes: 2, StartOffset: 0, EndOffset: 1},
				{Number: 2, MessageID: "two", Bytes: 2, StartOffset: 3, EndOffset: 4},
			},
		}},
	}
	if err := store.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}

	_, err := newTestUsenet(store).PrepareStream(nzb.ID, "movie.mkv")
	var streamErr *customerror.Error
	if !errors.As(err, &streamErr) || !streamErr.IsPermanent() || streamErr.Code != "usenet_metadata_invalid" {
		t.Fatalf("PrepareStream error = %#v, want permanent usenet_metadata_invalid", err)
	}
}

func TestPermanentArticleFailuresAreAtomicAndDurable(t *testing.T) {
	store := newTestNZBStorage(t)
	nzb := &storage.NZB{
		ID: "missing",
		Files: []storage.NZBFile{
			{Name: "one.mkv", Size: 1, Segments: []storage.NZBSegment{{Number: 1, MessageID: "one", Bytes: 1}}},
			{Name: "two.mkv", Size: 1, Segments: []storage.NZBSegment{{Number: 1, MessageID: "two", Bytes: 1}}},
		},
	}
	if err := store.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(nzb.Files))
	for _, file := range nzb.Files {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()
			errs <- store.markFilePermanentlyFailed(nzb.ID, filename, "articles missing on provider")
		}(file.Name)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("markFilePermanentlyFailed: %v", err)
		}
	}

	stored, err := store.GetNZB(nzb.ID)
	if err != nil {
		t.Fatalf("GetNZB: %v", err)
	}
	if !stored.IsBad || stored.Status != NZBStatusFailed || stored.FailMessage == "" {
		t.Fatalf("NZB failure state not persisted: bad=%v status=%q message=%q", stored.IsBad, stored.Status, stored.FailMessage)
	}
	for _, file := range stored.Files {
		if !file.IsDeleted {
			t.Errorf("file %q was not marked permanently failed", file.Name)
		}
	}

	// A fresh Usenet instance has an empty hot cache and therefore proves the
	// failure is read back from durable metadata before response headers.
	err = newTestUsenet(store).IsFilePermanentlyFailed(nzb.ID, "one.mkv")
	var streamErr *customerror.Error
	if !errors.As(err, &streamErr) {
		t.Fatalf("persistent failure error = %v, want *customerror.Error", err)
	}
	if streamErr.StatusCode() != http.StatusGone || !streamErr.IsPermanent() {
		t.Fatalf("persistent failure status = %d, permanent=%v; want 410, true", streamErr.StatusCode(), streamErr.IsPermanent())
	}
}

func newTestNZBStorage(t *testing.T) *NZBStorage {
	t.Helper()
	return &NZBStorage{metaDir: t.TempDir(), logger: zerolog.Nop()}
}

func newTestUsenet(store *NZBStorage) *Usenet {
	return &Usenet{
		nzbStorage:    store,
		logger:        zerolog.Nop(),
		failedFiles:   xsync.NewMap[string, error](),
		preparedSizes: xsync.NewMap[string, int64](),
	}
}

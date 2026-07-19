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

func TestParseTimeoutSetting(t *testing.T) {
	const def = 30 * time.Second
	cases := []struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{raw: "", want: def},
		{raw: "  ", want: def},
		{raw: "0", want: 0},
		{raw: "0s", want: 0},
		{raw: "0m", want: 0},
		{raw: "off", want: 0},
		{raw: "OFF", want: 0},
		{raw: "Off", want: 0},
		{raw: "none", want: 0},
		{raw: "None", want: 0},
		{raw: " off ", want: 0},
		{raw: "45s", want: 45 * time.Second},
		{raw: "2m", want: 2 * time.Minute},
		{raw: "garbage", want: def, wantErr: true},
		{raw: "-5s", want: def, wantErr: true},
	}
	for _, tc := range cases {
		got, err := parseTimeoutSetting(tc.raw, def)
		if (err != nil) != tc.wantErr {
			t.Errorf("parseTimeoutSetting(%q) error = %v, wantErr %v", tc.raw, err, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("parseTimeoutSetting(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}

func TestProgressDeadlineDisabledIsPassthrough(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	deadline := newProgressDeadline(parent, 0)
	// Disabled: the caller context must be handed through unwrapped — no
	// derived context, no watchdog goroutine.
	if deadline.Context != parent {
		t.Fatalf("disabled deadline wrapped the parent context: %v", deadline.Context)
	}
	deadline.Progress() // must be a safe no-op
	deadline.Close()    // must be a safe no-op
	if parent.Err() != nil {
		t.Fatalf("disabled deadline canceled the parent context: %v", parent.Err())
	}
}

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

func TestPrepareStreamsReconcilesMultipleFilesInOneMetadataPass(t *testing.T) {
	store := newTestNZBStorage(t)
	nzb := &storage.NZB{
		ID:        "batch-prepare",
		TotalSize: 13,
		Files: []storage.NZBFile{
			{
				Name: "oversized.mkv",
				Size: 10,
				Segments: []storage.NZBSegment{
					{Number: 1, MessageID: "one", Bytes: 4},
					{Number: 2, MessageID: "two", Bytes: 4},
				},
			},
			{
				Name:     "encrypted.mkv",
				Size:     3,
				Segments: []storage.NZBSegment{{Number: 1, MessageID: "three", Bytes: 5}},
			},
		},
	}
	if err := store.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}

	u := newTestUsenet(store)
	sizes, fileErrors, err := u.PrepareStreams(nzb.ID, []string{"oversized.mkv", "encrypted.mkv"})
	if err != nil {
		t.Fatalf("PrepareStreams: %v", err)
	}
	if len(fileErrors) != 0 {
		t.Fatalf("PrepareStreams file errors: %v", fileErrors)
	}
	if sizes["oversized.mkv"] != 8 || sizes["encrypted.mkv"] != 3 {
		t.Fatalf("prepared sizes = %v, want oversized=8 encrypted=3", sizes)
	}

	stored, err := store.GetNZB(nzb.ID)
	if err != nil {
		t.Fatalf("GetNZB: %v", err)
	}
	if stored.TotalSize != 11 || stored.Files[0].Size != 8 || stored.Files[1].Size != 3 {
		t.Fatalf("durable sizes = total:%d files:%d,%d, want 11,8,3",
			stored.TotalSize, stored.Files[0].Size, stored.Files[1].Size)
	}
}

func TestPrepareStreamsReturnsPerFilePermanentFailures(t *testing.T) {
	store := newTestNZBStorage(t)
	nzb := &storage.NZB{
		ID: "batch-failure",
		Files: []storage.NZBFile{
			{Name: "missing.mkv", Size: 1, Segments: []storage.NZBSegment{{Number: 1, MessageID: "missing", Bytes: 1}}},
			{Name: "healthy.mkv", Size: 1, Segments: []storage.NZBSegment{{Number: 1, MessageID: "healthy", Bytes: 1}}},
		},
	}
	if err := store.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	if err := store.markFilePermanentlyFailed(nzb.ID, "missing.mkv", "articles missing"); err != nil {
		t.Fatalf("markFilePermanentlyFailed: %v", err)
	}

	sizes, fileErrors, err := newTestUsenet(store).PrepareStreams(nzb.ID, []string{"missing.mkv", "healthy.mkv"})
	if err != nil {
		t.Fatalf("PrepareStreams: %v", err)
	}
	if sizes["healthy.mkv"] != 1 {
		t.Fatalf("healthy size = %d, want 1", sizes["healthy.mkv"])
	}
	var permanent *customerror.Error
	if !errors.As(fileErrors["missing.mkv"], &permanent) || permanent.StatusCode() != http.StatusGone {
		t.Fatalf("missing file error = %v, want HTTP 410", fileErrors["missing.mkv"])
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

func TestPermanentArticleFailureIsCachedOnlyAfterDurableWrite(t *testing.T) {
	store := newTestNZBStorage(t)
	u := newTestUsenet(store)
	const (
		nzoID    = "durability-order"
		filename = "movie.mkv"
	)
	key := fsKey(nzoID, filename)
	u.preparedSizes.Store(key, int64(123))

	err := u.recordPermanentArticleFailure(nzoID, filename, errors.New("430 no such article"))
	var permanent *customerror.Error
	if errors.As(err, &permanent) {
		t.Fatalf("failed persistence returned permanent error: %v", err)
	}
	if _, ok := u.failedFiles.Load(key); ok {
		t.Fatal("failure entered hot cache before metadata was durable")
	}
	if size, ok := u.preparedSizes.Load(key); !ok || size != 123 {
		t.Fatalf("prepared size changed after failed persistence: size=%d present=%v", size, ok)
	}

	nzb := &storage.NZB{
		ID: nzoID,
		Files: []storage.NZBFile{{
			Name: filename,
			Size: 123,
		}},
	}
	if err := store.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}

	err = u.recordPermanentArticleFailure(nzoID, filename, errors.New("430 no such article"))
	if !errors.As(err, &permanent) || permanent.StatusCode() != http.StatusGone {
		t.Fatalf("durable failure error = %v, want permanent HTTP 410", err)
	}
	if _, ok := u.failedFiles.Load(key); !ok {
		t.Fatal("durable failure was not entered into the hot cache")
	}
	if _, ok := u.preparedSizes.Load(key); ok {
		t.Fatal("prepared size remained cached after durable failure")
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

package usenet

import (
	"errors"
	"sync"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// THE CENSUS IS THE WHOLE REASON THIS HOOK CARRIES NUMBERS.
//
// markFilePermanentlyFailed sets IsBad on the FIRST dead file. An NZB with one
// dead episode and twelve live ones therefore carries exactly the same flag as
// one with nothing left, so a handler that condemned on IsBad would delete a
// thirteen-file pack because one episode expired. Only a count of the durable
// per-file IsDeleted flags can tell those apart, and it has to be taken under
// the same lock as the write or a concurrent 430 for a sibling file races it.
func multiFileNZB(id string, filenames ...string) *storage.NZB {
	nzb := &storage.NZB{ID: id, TotalSize: int64(len(filenames)) * 100}
	for _, name := range filenames {
		nzb.Files = append(nzb.Files, storage.NZBFile{
			Name: name,
			Size: 100,
			Segments: []storage.NZBSegment{{
				Number:    1,
				MessageID: id + "-seg-" + name,
				Bytes:     100,
			}},
		})
	}
	return nzb
}

type deadContentCall struct {
	nzoID    string
	filename string
	dead     int
	total    int
}

type deadContentRecorder struct {
	mu    sync.Mutex
	calls []deadContentCall
}

func (r *deadContentRecorder) handler() DeadContentHandler {
	return func(nzoID, filename string, dead, total int, _ error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.calls = append(r.calls, deadContentCall{nzoID, filename, dead, total})
	}
}

func (r *deadContentRecorder) snapshot() []deadContentCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]deadContentCall(nil), r.calls...)
}

func TestDeadContentHandlerReportsAPartialCensus(t *testing.T) {
	store := newTestNZBStorage(t)
	const id = "pack-partial"
	if err := store.AddNZB(multiFileNZB(id, "S01E01.mkv", "S01E02.mkv", "S01E03.mkv")); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	u := newTestUsenet(store)
	rec := &deadContentRecorder{}
	u.SetDeadContentHandler(rec.handler())

	if err := u.recordPermanentArticleFailure(id, "S01E02.mkv", errors.New("430 no such article")); err == nil {
		t.Fatal("recordPermanentArticleFailure returned nil")
	}

	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("handler called %d times, want 1", len(calls))
	}
	got := calls[0]
	if got.nzoID != id || got.filename != "S01E02.mkv" {
		t.Fatalf("handler got (%q, %q), want (%q, %q)", got.nzoID, got.filename, id, "S01E02.mkv")
	}
	if got.dead != 1 || got.total != 3 {
		t.Fatalf("census = %d dead of %d, want 1 of 3. A wrong census here condemns a pack for one dead episode",
			got.dead, got.total)
	}

	// And the durable state agrees — the count is not a parallel bookkeeping
	// scheme that can drift from what is on disk.
	stored, err := store.GetNZB(id)
	if err != nil {
		t.Fatalf("GetNZB: %v", err)
	}
	if !stored.IsBad {
		t.Fatal("the NZB was not marked bad")
	}
	dead := 0
	for _, f := range stored.Files {
		if f.IsDeleted {
			dead++
		}
	}
	if dead != 1 {
		t.Fatalf("durable dead-file count = %d, want 1", dead)
	}
}

// The census reaches dead == total only when the last live file dies, which is
// the moment the entry becomes condemnable. Walking every file proves the count
// accumulates across separate calls rather than being recomputed from one.
func TestDeadContentCensusReachesAllDeadOnTheLastFile(t *testing.T) {
	store := newTestNZBStorage(t)
	const id = "pack-full"
	files := []string{"S01E01.mkv", "S01E02.mkv"}
	if err := store.AddNZB(multiFileNZB(id, files...)); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	u := newTestUsenet(store)
	rec := &deadContentRecorder{}
	u.SetDeadContentHandler(rec.handler())

	for _, name := range files {
		if err := u.recordPermanentArticleFailure(id, name, errors.New("430 no such article")); err == nil {
			t.Fatalf("recordPermanentArticleFailure(%s) returned nil", name)
		}
	}

	calls := rec.snapshot()
	if len(calls) != 2 {
		t.Fatalf("handler called %d times, want 2", len(calls))
	}
	if calls[0].dead != 1 || calls[0].total != 2 {
		t.Fatalf("first census = %d of %d, want 1 of 2 — the entry must NOT be condemnable yet", calls[0].dead, calls[0].total)
	}
	if calls[1].dead != 2 || calls[1].total != 2 {
		t.Fatalf("second census = %d of %d, want 2 of 2 — nothing survives, so the entry is condemnable",
			calls[1].dead, calls[1].total)
	}
}

// 🛑 THE DURABLE RECORD IS NOT CONDITIONAL ON THE HANDLER.
//
// The verdict is a consequence of the record, never a substitute for it. With no
// handler wired — an older config, a manager built without usenet, a panic in
// the wiring — the article must still be permanently marked, still stop serving,
// and still refuse every later read. Only the *arr finding out promptly is lost,
// and the nightly sweep is the backstop for that.
func TestArticleIsRecordedDurablyWithNoHandlerWired(t *testing.T) {
	store := newTestNZBStorage(t)
	const id = "no-handler"
	if err := store.AddNZB(multiFileNZB(id, "movie.mkv")); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	u := newTestUsenet(store) // deliberately no SetDeadContentHandler

	if err := u.recordPermanentArticleFailure(id, "movie.mkv", errors.New("430 no such article")); err == nil {
		t.Fatal("recordPermanentArticleFailure returned nil")
	}

	stored, err := store.GetNZB(id)
	if err != nil {
		t.Fatalf("GetNZB: %v", err)
	}
	if !stored.IsBad || stored.Status != NZBStatusFailed || !stored.Files[0].IsDeleted {
		t.Fatalf("an unwired handler cost the durable mark: bad=%t status=%q deleted=%t",
			stored.IsBad, stored.Status, stored.Files[0].IsDeleted)
	}
}

// A CENSUS THAT DESCRIBES A STATE THAT NEVER LANDED MUST NOT BE ACTED ON.
//
// If the metadata write fails the file is not marked, so the handler must not be
// told the entry is dead — it would condemn and prune on evidence that was
// rolled back, and the next read would find the articles missing all over again
// with nothing recorded.
func TestNoVerdictWhenTheDurableWriteFails(t *testing.T) {
	store := newTestNZBStorage(t)
	u := newTestUsenet(store)
	rec := &deadContentRecorder{}
	u.SetDeadContentHandler(rec.handler())

	// No such NZB, so the read-modify-write cannot complete.
	if err := u.recordPermanentArticleFailure("missing-nzb", "movie.mkv", errors.New("430 no such article")); err == nil {
		t.Fatal("recording a failure for an unknown NZB returned nil")
	}
	if calls := rec.snapshot(); len(calls) != 0 {
		t.Fatalf("the handler was told %d times about a mark that never landed: %+v", len(calls), calls)
	}
}

// The handler must run with the NZB lifecycle lock RELEASED. It prunes the
// entry, which re-enters this package to tear down cached readers for the same
// nzoID — under the lock that deadlocks against the teardown it just asked for.
// Taking the lock inside the handler is the cheapest way to prove it is free.
func TestDeadContentHandlerRunsWithoutTheLifecycleLock(t *testing.T) {
	store := newTestNZBStorage(t)
	const id = "lock-free"
	if err := store.AddNZB(multiFileNZB(id, "movie.mkv")); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	u := newTestUsenet(store)

	reentered := make(chan struct{})
	u.SetDeadContentHandler(func(nzoID, _ string, _, _ int, _ error) {
		// Would block forever if recordPermanentArticleFailure still held it.
		unlock := u.lockNZBLifecycle(nzoID)
		unlock()
		close(reentered)
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = u.recordPermanentArticleFailure(id, "movie.mkv", errors.New("430 no such article"))
	}()

	waitLifecycleSignal(t, reentered, "handler to re-enter the NZB lifecycle lock")
	waitLifecycleSignal(t, done, "recordPermanentArticleFailure to return")
}

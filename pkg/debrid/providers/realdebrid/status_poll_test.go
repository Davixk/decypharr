package realdebrid

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// waitingFilesSelectionInfo is the payload RealDebrid returns for a magnet it
// has accepted but not yet acted on. One allowed file is required: CheckStatus
// treats an empty selection as a hard error before it ever reaches the re-poll.
func statusInfo(status string) map[string]any {
	return map[string]any{
		"id":       "rd-torrent",
		"filename": "Movie.mkv",
		"hash":     strings.Repeat("a", 40),
		"bytes":    2_000_000_000,
		"status":   status,
		"files": []map[string]any{
			{"id": 1, "path": "/Movie.mkv", "bytes": 2_000_000_000, "selected": 1},
		},
	}
}

func statusPollHandler(t *testing.T, statuses func(call int32) string, infoCalls, selectCalls *atomic.Int32) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/torrents/info/"):
			call := infoCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			body, err := json.Marshal(statusInfo(statuses(call)))
			if err != nil {
				t.Errorf("marshal info payload: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, _ = w.Write(body)
		case strings.HasPrefix(r.URL.Path, "/torrents/selectFiles/"):
			selectCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

func setStatusCeiling(t *testing.T, raw string) {
	t.Helper()
	cfg := config.Get()
	previous := cfg.DebridStatusTimeout
	cfg.DebridStatusTimeout = raw
	t.Cleanup(func() { config.Get().DebridStatusTimeout = previous })
}

// TestCheckStatusBoundsAStuckWaitingFilesSelectionPoll covers the only branch of
// CheckStatus that re-polls at all.
//
// That loop had no context, no deadline and no iteration cap. An account that
// never advanced past "waiting_files_selection" held its caller forever — and
// its callers include re-insertion, which used to run under the manager's
// global file-operation mutex.
//
// Two properties are asserted together because either alone would be a bug:
// the poll STOPS, and what it returns is a TRANSIENT failure. A ceiling firing
// means decypharr stopped waiting; it says nothing whatsoever about the
// content, so nothing downstream may read a verdict out of it.
func TestCheckStatusBoundsAStuckWaitingFilesSelectionPoll(t *testing.T) {
	var infoCalls, selectCalls atomic.Int32
	rd := newTestRealDebrid(t, statusPollHandler(t,
		func(int32) string { return "waiting_files_selection" },
		&infoCalls, &selectCalls,
	))
	setStatusCeiling(t, "1ms")

	done := make(chan struct{})
	var got *types.Torrent
	var err error
	go func() {
		defer close(done)
		got, err = rd.CheckStatus(&types.Torrent{Id: "rd-torrent"})
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("CheckStatus never returned: the waiting_files_selection re-poll is still unbounded")
	}

	if err == nil {
		t.Fatal("CheckStatus() = nil error; a provider that never advanced must not read as success")
	}
	if !customerror.IsBackendTimeout(err) {
		t.Fatalf("CheckStatus() error = %v, want a typed backend timeout", err)
	}
	if customerror.IsContentPermanentlyGone(err) {
		t.Fatalf("CheckStatus() error = %v was reported as a permanent content verdict", err)
	}
	if e := customerror.FromError(err); !e.IsRetryable() || e.IsPermanent() || e.StatusCode() != http.StatusServiceUnavailable {
		t.Fatalf("ceiling error is retryable=%v permanent=%v status=%d, want true/false/503",
			e.IsRetryable(), e.IsPermanent(), e.StatusCode())
	}
	if got == nil {
		t.Fatal("CheckStatus returned a nil torrent; the caller needs it to compensate the placement it just created")
	}
	// The ceiling is checked only after a complete pass, so even a 1ms budget
	// buys the provider one genuine attempt including the file selection.
	if infoCalls.Load() < 1 || selectCalls.Load() < 1 {
		t.Fatalf("ceiling truncated the first attempt: info=%d select=%d", infoCalls.Load(), selectCalls.Load())
	}
}

// TestCheckStatusDoesNotCutOffAProviderThatAdvances is the other half of the
// contract: the ceiling must only ever fire on a provider that is genuinely
// stuck. A provider that acts on the file selection on the next pass — the
// normal case — must be allowed to finish.
func TestCheckStatusDoesNotCutOffAProviderThatAdvances(t *testing.T) {
	var infoCalls, selectCalls atomic.Int32
	rd := newTestRealDebrid(t, statusPollHandler(t,
		func(call int32) string {
			if call == 1 {
				return "waiting_files_selection"
			}
			return "downloading"
		},
		&infoCalls, &selectCalls,
	))
	setStatusCeiling(t, "30s")

	// DownloadUncached lets the "downloading" pass return successfully, which is
	// what makes this a test of the loop rather than of the caching policy.
	got, err := rd.CheckStatus(&types.Torrent{Id: "rd-torrent", DownloadUncached: true})
	if err != nil {
		t.Fatalf("CheckStatus() error = %v; the provider advanced within its ceiling", err)
	}
	if got == nil || got.Status != types.TorrentStatusDownloading {
		t.Fatalf("CheckStatus() = %+v, want a downloading torrent", got)
	}
	if infoCalls.Load() < 2 {
		t.Fatalf("premise check failed: the poll did not re-poll after selecting files (info=%d)", infoCalls.Load())
	}
}

package sabnzbd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// testNZB is a structurally valid single-file, single-segment NZB. The media
// filename in the subject lets the quick parser group it without any network
// content-type detection, so the only parse-time network I/O is the STAT probe.
const testNZB = `<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="tester &lt;tester@example.com&gt;" date="1700000000" subject="&quot;movie.mkv&quot; yEnc (1/1)">
    <groups>
      <group>alt.binaries.test</group>
    </groups>
    <segments>
      <segment bytes="4096" number="1">segment-one@test</segment>
    </segments>
  </file>
</nzb>`

// fakeNNTPServer is a minimal NNTP server for exercising the parse-time probe.
// STAT answers 223 (article exists) or 430 (missing) depending on statOK.
// BODY requests block until the server is closed so async processing can
// neither complete nor fail while a test asserts on add-time state.
type fakeNNTPServer struct {
	listener net.Listener
	statOK   bool

	stop      chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	conns     map[net.Conn]struct{}
	wg        sync.WaitGroup
}

func newFakeNNTPServer(t *testing.T, statOK bool) *fakeNNTPServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for fake NNTP server: %v", err)
	}
	server := &fakeNNTPServer{
		listener: listener,
		statOK:   statOK,
		stop:     make(chan struct{}),
		conns:    make(map[net.Conn]struct{}),
	}
	server.wg.Add(1)
	go server.acceptLoop()
	return server
}

func (s *fakeNNTPServer) address(t *testing.T) (string, int) {
	t.Helper()
	host, portText, err := net.SplitHostPort(s.listener.Addr().String())
	if err != nil {
		t.Fatalf("parse fake NNTP address: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse fake NNTP port: %v", err)
	}
	return host, port
}

func (s *fakeNNTPServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serveConnection(conn)
	}
}

func (s *fakeNNTPServer) serveConnection(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		_ = conn.Close()
	}()

	writer := bufio.NewWriter(conn)
	if _, err := writer.WriteString("200 fake NNTP test server ready\r\n"); err != nil {
		return
	}
	if err := writer.Flush(); err != nil {
		return
	}

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		switch strings.ToUpper(fields[0]) {
		case "STAT":
			if s.statOK && len(fields) == 2 {
				_, _ = writer.WriteString("223 0 " + fields[1] + "\r\n")
			} else {
				_, _ = writer.WriteString("430 no such article\r\n")
			}
			if err := writer.Flush(); err != nil {
				return
			}
		case "BODY":
			// Hold the request open so background processing stays in-flight
			// (neither completed nor failed) for the duration of the test.
			<-s.stop
			return
		default:
			_, _ = writer.WriteString("500 unknown command\r\n")
			if err := writer.Flush(); err != nil {
				return
			}
		}
	}
}

func (s *fakeNNTPServer) Close() {
	s.closeOnce.Do(func() {
		close(s.stop)
		_ = s.listener.Close()
		s.mu.Lock()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
}

// newSABTestHarness wires a real Manager (with a usenet client pointed at the
// fake NNTP server) into the SABnzbd shim. Cleanups run LIFO: the fake server
// closes first so any held BODY reads unblock before the manager stops.
func newSABTestHarness(t *testing.T, server *fakeNNTPServer) (*SABnzbd, *manager.Manager) {
	t.Helper()
	host, port := server.address(t)
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()
	cfg.UseAuth = false
	cfg.Usenet.Providers = []config.UsenetProvider{{
		Host:           host,
		Port:           port,
		MaxConnections: 2,
	}}
	m := manager.New()
	t.Cleanup(func() {
		if err := m.Stop(); err != nil {
			t.Errorf("Stop manager: %v", err)
		}
	})
	t.Cleanup(server.Close)
	return New(m), m
}

func postNZBFile(t *testing.T, s *SABnzbd, filename string, content []byte) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("name", filename)
	if err != nil {
		t.Fatalf("create multipart file: %v", err)
	}
	if _, err := part.Write(content); err != nil {
		t.Fatalf("write multipart file: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api?mode=addfile", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	recorder := httptest.NewRecorder()
	s.Routes().ServeHTTP(recorder, req)
	return recorder
}

func fetchHistory(t *testing.T, s *SABnzbd) History {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api?mode=history", nil)
	recorder := httptest.NewRecorder()
	s.Routes().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("history status = %d, body=%q", recorder.Code, recorder.Body.String())
	}
	var response HistoryResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode history response: %v", err)
	}
	return response.History
}

// A structurally valid NZB whose articles are already gone at parse time must
// be accepted (200 + nzo_id) and recorded as a Failed history entry so the arr
// blocklists the release natively, instead of surfacing a raw HTTP 500 that
// Sonarr/Radarr report as "Failed to connect to SABnzbd".
func TestAddFileProbeFailureIsAcceptedAndRecordedAsFailed(t *testing.T) {
	server := newFakeNNTPServer(t, false) // STAT answers 430
	s, m := newSABTestHarness(t, server)

	recorder := postNZBFile(t, s, "movie.nzb", []byte(testNZB))
	if recorder.Code != http.StatusOK {
		t.Fatalf("addfile status = %d, want 200; body=%q", recorder.Code, recorder.Body.String())
	}
	var response AddNZBResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode addfile response: %v", err)
	}
	if !response.Status || len(response.NzoIds) != 1 || response.NzoIds[0] == "" {
		t.Fatalf("addfile response = %+v, want status true with one nzo_id", response)
	}
	nzoID := response.NzoIds[0]

	entry, err := m.Queue().GetTorrent(nzoID)
	if err != nil {
		t.Fatalf("accepted NZB has no queue entry: %v", err)
	}
	if entry.State != storage.EntryStateError {
		t.Fatalf("entry state = %q, want %q", entry.State, storage.EntryStateError)
	}
	if !strings.Contains(entry.LastError, "articles missing on provider") {
		t.Fatalf("entry LastError = %q, want it to mention missing articles", entry.LastError)
	}

	history := fetchHistory(t, s)
	var slot *HistorySlot
	for i := range history.Slots {
		if history.Slots[i].NzoId == nzoID {
			slot = &history.Slots[i]
			break
		}
	}
	if slot == nil {
		t.Fatalf("nzo_id %s missing from history: %+v", nzoID, history.Slots)
	}
	if slot.Status != StatusFailed {
		t.Fatalf("history status = %q, want %q", slot.Status, StatusFailed)
	}
	if !strings.Contains(slot.FailMessage, "articles missing on provider") {
		t.Fatalf("history fail_message = %q, want it to mention missing articles", slot.FailMessage)
	}
}

// An infrastructure-class probe failure (dead provider: connection refused)
// carries no verdict about the articles. The add must be accepted (200 +
// nzo_id) with the entry kept queued for retry, and the release must NOT
// appear in the Failed history — a Failed projection would make the arr
// blocklist a possibly healthy release.
func TestAddFileInfraProbeFailureStaysQueuedAndOutOfFailedHistory(t *testing.T) {
	server := newFakeNNTPServer(t, true)
	s, m := newSABTestHarness(t, server)
	// Collapse the substrate before the add: dialing the provider is refused.
	server.Close()

	recorder := postNZBFile(t, s, "movie.nzb", []byte(testNZB))
	if recorder.Code != http.StatusOK {
		t.Fatalf("addfile status = %d, want 200; body=%q", recorder.Code, recorder.Body.String())
	}
	var response AddNZBResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode addfile response: %v", err)
	}
	if !response.Status || len(response.NzoIds) != 1 || response.NzoIds[0] == "" {
		t.Fatalf("addfile response = %+v, want status true with one nzo_id", response)
	}
	nzoID := response.NzoIds[0]

	entry, err := m.Queue().GetTorrent(nzoID)
	if err != nil {
		t.Fatalf("accepted NZB has no queue entry: %v", err)
	}
	if entry.State != storage.EntryStateDownloading {
		t.Fatalf("entry state = %q, want %q (must NOT be terminal error)", entry.State, storage.EntryStateDownloading)
	}
	if entry.Status != debridTypes.TorrentStatusQueued {
		t.Fatalf("entry status = %q, want %q", entry.Status, debridTypes.TorrentStatusQueued)
	}
	if entry.LastError != "" {
		t.Fatalf("entry LastError = %q, want empty for an infrastructure deferral", entry.LastError)
	}

	history := fetchHistory(t, s)
	for _, slot := range history.Slots {
		if slot.NzoId == nzoID {
			t.Fatalf("infrastructure-deferred NZB appeared in history as %q: %+v", slot.Status, slot)
		}
	}
}

// Malformed uploads are not availability failures; they must keep returning an
// error status and must not leave queue or history entries behind.
func TestAddFileMalformedNZBStillRejected(t *testing.T) {
	server := newFakeNNTPServer(t, false)
	s, m := newSABTestHarness(t, server)

	recorder := postNZBFile(t, s, "garbage.nzb", []byte("this is not an NZB"))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("malformed addfile status = %d, want 500; body=%q", recorder.Code, recorder.Body.String())
	}
	if entries := m.Queue().ListFilter("", config.ProtocolNZB, "", nil, "", false); len(entries) != 0 {
		t.Fatalf("malformed NZB left %d queue entries behind", len(entries))
	}
	if history := fetchHistory(t, s); len(history.Slots) != 0 {
		t.Fatalf("malformed NZB appeared in history: %+v", history.Slots)
	}
}

// addFailedNZBEntry seeds a terminal-error NZB queue entry with a staged
// source file, the way the incident left ~1,794 entries behind.
func addFailedNZBEntry(t *testing.T, m *manager.Manager, infohash, lastError string, errorCount int) *storage.Entry {
	t.Helper()
	stagedPath := filepath.Join(t.TempDir(), infohash+".nzb.queued")
	if err := os.WriteFile(stagedPath, []byte(testNZB), 0o644); err != nil {
		t.Fatalf("write staged NZB: %v", err)
	}
	now := time.Now()
	entry := &storage.Entry{
		InfoHash:         infohash,
		Name:             "failed-" + infohash,
		OriginalFilename: "failed-" + infohash + ".nzb",
		Protocol:         config.ProtocolNZB,
		Magnet:           stagedPath,
		Status:           debridTypes.TorrentStatusDownloading,
		State:            storage.EntryStateError,
		LastError:        lastError,
		ErrorCount:       errorCount,
		CreatedAt:        now,
		UpdatedAt:        now,
		AddedOn:          now,
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}
	if err := m.Queue().Add(entry); err != nil {
		t.Fatalf("seed failed entry %s: %v", infohash, err)
	}
	return entry
}

func doRetryRequest(t *testing.T, s *SABnzbd, url string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	recorder := httptest.NewRecorder()
	s.Routes().ServeHTTP(recorder, req)
	return recorder
}

func historySlotFor(history History, nzoID string) *HistorySlot {
	for i := range history.Slots {
		if history.Slots[i].NzoId == nzoID {
			return &history.Slots[i]
		}
	}
	return nil
}

// mode=retry with value=<nzo_id> revives that failed entry: {"status":true},
// the entry re-enters the active pipeline, and it disappears from the Failed
// history — exactly how real SABnzbd behaves on retry.
func TestRetryRevivesFailedEntryAndClearsFailedHistory(t *testing.T) {
	server := newFakeNNTPServer(t, true) // healthy provider for the re-parse
	s, m := newSABTestHarness(t, server)

	entry := addFailedNZBEntry(t, m, "retry-entry-1", "articles missing on provider: failed to stat segment", 1)

	if slot := historySlotFor(fetchHistory(t, s), entry.InfoHash); slot == nil || slot.Status != StatusFailed {
		t.Fatalf("seeded entry missing from Failed history before retry: %+v", slot)
	}

	recorder := doRetryRequest(t, s, "/api?mode=retry&value=retry-entry-1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("retry status = %d, body=%q", recorder.Code, recorder.Body.String())
	}
	var response StatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode retry response: %v", err)
	}
	if !response.Status {
		t.Fatalf("retry response = %+v, want status true", response)
	}

	revived, err := m.Queue().GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatalf("revived entry missing from queue: %v", err)
	}
	if revived.State != storage.EntryStateDownloading {
		t.Fatalf("revived entry state = %q, want downloading", revived.State)
	}
	if slot := historySlotFor(fetchHistory(t, s), entry.InfoHash); slot != nil {
		t.Fatalf("revived entry still projected in history as %q", slot.Status)
	}
}

// mode=retry with an unknown nzo_id must report failure, not silently succeed.
func TestRetryUnknownNzoIDFails(t *testing.T) {
	server := newFakeNNTPServer(t, true)
	s, _ := newSABTestHarness(t, server)

	recorder := doRetryRequest(t, s, "/api?mode=retry&value=no-such-entry")
	if recorder.Code == http.StatusOK {
		t.Fatalf("retry of unknown nzo_id returned 200: %q", recorder.Body.String())
	}
	var response StatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode retry response: %v", err)
	}
	if response.Status {
		t.Fatal("retry of unknown nzo_id reported status true")
	}
}

// mode=retry_all (and mode=retry without a value) revives every ELIGIBLE
// failed entry: exhausted ones (ErrorCount >= retries) stay Failed.
func TestRetryAllRevivesOnlyEligibleEntries(t *testing.T) {
	server := newFakeNNTPServer(t, true)
	s, m := newSABTestHarness(t, server)

	eligible := addFailedNZBEntry(t, m, "retryall-eligible", "usenet parse failed: availability probe failed: provider connectivity problem", 1)
	exhausted := addFailedNZBEntry(t, m, "retryall-exhausted", "articles missing on provider: failed to stat segment", 3)

	recorder := doRetryRequest(t, s, "/api?mode=retry_all")
	if recorder.Code != http.StatusOK {
		t.Fatalf("retry_all status = %d, body=%q", recorder.Code, recorder.Body.String())
	}
	var response StatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode retry_all response: %v", err)
	}
	if !response.Status {
		t.Fatalf("retry_all response = %+v, want status true", response)
	}

	revived, err := m.Queue().GetTorrent(eligible.InfoHash)
	if err != nil {
		t.Fatalf("eligible entry missing from queue: %v", err)
	}
	if revived.State != storage.EntryStateDownloading {
		t.Fatalf("eligible entry state = %q, want downloading", revived.State)
	}
	stillFailed, err := m.Queue().GetTorrent(exhausted.InfoHash)
	if err != nil {
		t.Fatalf("exhausted entry missing from queue: %v", err)
	}
	if stillFailed.State != storage.EntryStateError {
		t.Fatalf("exhausted entry state = %q, want it to stay failed", stillFailed.State)
	}

	history := fetchHistory(t, s)
	if slot := historySlotFor(history, eligible.InfoHash); slot != nil {
		t.Fatalf("eligible entry still projected in history as %q", slot.Status)
	}
	if slot := historySlotFor(history, exhausted.InfoHash); slot == nil || slot.Status != StatusFailed {
		t.Fatalf("exhausted entry missing from Failed history: %+v", slot)
	}

	// mode=retry without a value routes through the same retry-all path.
	recorder = doRetryRequest(t, s, "/api?mode=retry")
	if recorder.Code != http.StatusOK {
		t.Fatalf("bare retry status = %d, body=%q", recorder.Code, recorder.Body.String())
	}
}

// A healthy NZB (STAT probe succeeds) must keep the existing behavior: the add
// is accepted and the entry enters the downloading queue without any recorded
// failure.
func TestAddFileHealthyNZBUnaffected(t *testing.T) {
	server := newFakeNNTPServer(t, true) // STAT answers 223
	s, m := newSABTestHarness(t, server)

	recorder := postNZBFile(t, s, "movie.nzb", []byte(testNZB))
	if recorder.Code != http.StatusOK {
		t.Fatalf("healthy addfile status = %d, want 200; body=%q", recorder.Code, recorder.Body.String())
	}
	var response AddNZBResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode addfile response: %v", err)
	}
	if !response.Status || len(response.NzoIds) != 1 || response.NzoIds[0] == "" {
		t.Fatalf("addfile response = %+v, want status true with one nzo_id", response)
	}

	entry, err := m.Queue().GetTorrent(response.NzoIds[0])
	if err != nil {
		t.Fatalf("healthy NZB has no queue entry: %v", err)
	}
	if entry.State != storage.EntryStateDownloading {
		t.Fatalf("entry state = %q, want %q", entry.State, storage.EntryStateDownloading)
	}
	if entry.LastError != "" {
		t.Fatalf("healthy NZB recorded an error: %q", entry.LastError)
	}
}

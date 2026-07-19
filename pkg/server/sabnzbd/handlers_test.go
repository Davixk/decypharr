package sabnzbd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
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

package manager

import (
	"bufio"
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// verdictTestNZB is structurally valid with a media filename in the subject,
// so the only parse-time network I/O is the STAT probe.
const verdictTestNZB = `<?xml version="1.0" encoding="UTF-8"?>
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

// verdictFakeNNTPServer answers STAT with 223 (statOK) or 430; BODY blocks
// until close so async processing neither completes nor fails during a test.
type verdictFakeNNTPServer struct {
	listener net.Listener
	statOK   bool

	stop      chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	conns     map[net.Conn]struct{}
	wg        sync.WaitGroup
}

func newVerdictFakeNNTPServer(t *testing.T, statOK bool) *verdictFakeNNTPServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for fake NNTP server: %v", err)
	}
	server := &verdictFakeNNTPServer{
		listener: listener,
		statOK:   statOK,
		stop:     make(chan struct{}),
		conns:    make(map[net.Conn]struct{}),
	}
	server.wg.Add(1)
	go server.acceptLoop()
	t.Cleanup(server.Close)
	return server
}

func (s *verdictFakeNNTPServer) hostPort(t *testing.T) (string, int) {
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

func (s *verdictFakeNNTPServer) acceptLoop() {
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

func (s *verdictFakeNNTPServer) serveConnection(conn net.Conn) {
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

func (s *verdictFakeNNTPServer) Close() {
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

// newVerdictTestManager wires a partial Manager with a real usenet client
// pointed at host:port, real storage/queue, and a job queue that records
// submitted jobs instead of processing them.
func newVerdictTestManager(t *testing.T, host string, port int) (*Manager, chan *Job) {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()
	cfg.Usenet.Providers = []config.UsenetProvider{{
		Host:           host,
		Port:           port,
		MaxConnections: 2,
	}}

	u, err := usenet.New()
	if err != nil {
		t.Fatalf("usenet.New: %v", err)
	}
	t.Cleanup(func() { _ = u.Close() })

	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	jobCh := make(chan *Job, 16)
	jobQueue := NewJobQueue(context.Background(), 1, func(_ context.Context, job *Job) {
		jobCh <- job
	})
	t.Cleanup(jobQueue.Close)

	m := &Manager{
		storage:  store,
		queue:    newQueue(store, ""),
		usenet:   u,
		logger:   zerolog.Nop(),
		config:   cfg,
		arr:      arr.NewStorage(),
		jobQueue: jobQueue,
	}
	return m, jobCh
}

func newQueuedNZBEntry(t *testing.T, m *Manager, infohash string) *storage.Entry {
	t.Helper()
	stagedPath := filepath.Join(t.TempDir(), infohash+".nzb.queued")
	if err := os.WriteFile(stagedPath, []byte(verdictTestNZB), 0o644); err != nil {
		t.Fatalf("write staged NZB: %v", err)
	}
	now := time.Now()
	entry := &storage.Entry{
		InfoHash:         infohash,
		Name:             "movie.nzb",
		OriginalFilename: "movie.nzb",
		Protocol:         config.ProtocolNZB,
		Magnet:           stagedPath,
		Status:           debridTypes.TorrentStatusQueued,
		State:            storage.EntryStateDownloading,
		CreatedAt:        now,
		UpdatedAt:        now,
		AddedOn:          now,
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("queue.Add: %v", err)
	}
	return entry
}

// Restore pass-2 hitting an infrastructure failure must NOT mark the entry as
// a terminal error; it stays queued/eligible and a job-queue retry is
// scheduled so a later pass reparses it.
func TestRestorePass2InfraFailureLeavesEntryEligibleAndRetries(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	server.Close() // dead substrate: dialing the provider is refused

	m, jobCh := newVerdictTestManager(t, host, port)

	prevBase := nzbInfraRetryBaseDelay
	nzbInfraRetryBaseDelay = 5 * time.Millisecond
	t.Cleanup(func() { nzbInfraRetryBaseDelay = prevBase })

	entry := newQueuedNZBEntry(t, m, "infra-restore-entry")
	m.restoreActiveDownloadJobs()

	persisted, err := m.storage.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	if persisted.State == storage.EntryStateError {
		t.Fatalf("entry was marked terminal error (LastError=%q); infrastructure failures must stay retryable", persisted.LastError)
	}
	if persisted.Status != debridTypes.TorrentStatusQueued {
		t.Fatalf("entry status = %q, want %q", persisted.Status, debridTypes.TorrentStatusQueued)
	}
	if persisted.LastError != "" {
		t.Fatalf("entry LastError = %q, want empty", persisted.LastError)
	}

	select {
	case job := <-jobCh:
		if job.ID != entry.InfoHash || !job.RebuildQueued {
			t.Fatalf("retry job = %+v, want RebuildQueued job for %s", job, entry.InfoHash)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no retry job was scheduled for the infrastructure-deferred entry")
	}
}

// A genuine 430 during restore pass-2 keeps its existing terminal behavior:
// the entry is marked error with the missing-articles verdict so the SAB
// history projects it as Failed and the arr blocklists the dead release.
func TestRestorePass2Genuine430StillMarksError(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, false) // STAT answers 430
	host, port := server.hostPort(t)

	m, _ := newVerdictTestManager(t, host, port)
	entry := newQueuedNZBEntry(t, m, "dead-restore-entry")
	m.restoreActiveDownloadJobs()

	persisted, err := m.storage.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	if persisted.State != storage.EntryStateError {
		t.Fatalf("entry state = %q, want %q for a genuine 430", persisted.State, storage.EntryStateError)
	}
	if !strings.Contains(persisted.LastError, "articles missing on provider") {
		t.Fatalf("entry LastError = %q, want the missing-articles verdict", persisted.LastError)
	}
}

// parseVerdictNZBJob parses the test NZB through the real usenet client and
// returns a ready-to-process job for the given queued entry.
func parseVerdictNZBJob(t *testing.T, m *Manager, entry *storage.Entry) *Job {
	t.Helper()
	meta, groups, err := m.usenet.ParseWithGeneration(context.Background(), entry.InfoHash, usenet.NewNZBGeneration(), "movie.nzb", []byte(verdictTestNZB), "")
	if err != nil {
		t.Fatalf("ParseWithGeneration: %v", err)
	}
	return &Job{ID: entry.InfoHash, Type: JobTypeNZB, Entry: entry, NZBMeta: meta, NZBGroups: groups}
}

// An archive-processing (Process-phase) failure caused by a collapsed NNTP
// substrate must NOT terminally error the entry with the generic "no valid
// files" verdict — the exact mechanism that parked 1,891 entries on
// 2026-07-19. The entry stays queued and a rebuild retry is scheduled.
func TestProcessJobArchiveInfraFailureKeepsEntryQueued(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	m, jobCh := newVerdictTestManager(t, host, port)
	m.usenetTimeout = 30 * time.Second

	prevBase := nzbInfraRetryBaseDelay
	nzbInfraRetryBaseDelay = 5 * time.Millisecond
	t.Cleanup(func() { nzbInfraRetryBaseDelay = prevBase })

	entry := newQueuedNZBEntry(t, m, "archive-infra-entry")
	job := parseVerdictNZBJob(t, m, entry)

	server.Close() // substrate collapses between parse and archive processing
	m.processJob(context.Background(), job)

	persisted, err := m.storage.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	if persisted.State == storage.EntryStateError {
		t.Fatalf("entry was marked terminal error (LastError=%q); archive-phase infrastructure failures must stay retryable", persisted.LastError)
	}
	if persisted.Status != debridTypes.TorrentStatusQueued {
		t.Fatalf("entry status = %q, want %q", persisted.Status, debridTypes.TorrentStatusQueued)
	}
	if persisted.LastError != "" {
		t.Fatalf("entry LastError = %q, want empty", persisted.LastError)
	}

	select {
	case retry := <-jobCh:
		if retry.ID != entry.InfoHash || !retry.RebuildQueued {
			t.Fatalf("retry job = %+v, want RebuildQueued job for %s", retry, entry.InfoHash)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no retry job was scheduled for the archive-phase infrastructure failure")
	}
}

// An expired per-job processing deadline is an infrastructure-class outcome,
// not a content verdict: the entry stays queued and retries with backoff.
func TestProcessJobArchiveDeadlineKeepsEntryQueued(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true) // BODY blocks: processing wedges
	host, port := server.hostPort(t)
	m, jobCh := newVerdictTestManager(t, host, port)
	m.usenetTimeout = 100 * time.Millisecond

	prevBase := nzbInfraRetryBaseDelay
	nzbInfraRetryBaseDelay = 5 * time.Millisecond
	t.Cleanup(func() { nzbInfraRetryBaseDelay = prevBase })

	entry := newQueuedNZBEntry(t, m, "archive-deadline-entry")
	job := parseVerdictNZBJob(t, m, entry)
	m.processJob(context.Background(), job)

	persisted, err := m.storage.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	if persisted.State == storage.EntryStateError {
		t.Fatalf("entry was marked terminal error (LastError=%q); processing deadlines must stay retryable", persisted.LastError)
	}
	if persisted.Status != debridTypes.TorrentStatusQueued {
		t.Fatalf("entry status = %q, want %q", persisted.Status, debridTypes.TorrentStatusQueued)
	}

	select {
	case retry := <-jobCh:
		if retry.ID != entry.InfoHash || !retry.RebuildQueued {
			t.Fatalf("retry job = %+v, want RebuildQueued job for %s", retry, entry.InfoHash)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no retry job was scheduled for the expired processing deadline")
	}
}

// Shutdown (job-context cancellation) during archive processing must leave
// the entry exactly as it was: no error marking, no retry scheduling. Boot
// restore owns the pickup after a restart.
func TestProcessJobShutdownCancelLeavesEntryUntouched(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true)
	host, port := server.hostPort(t)
	m, jobCh := newVerdictTestManager(t, host, port)
	m.usenetTimeout = 30 * time.Second

	entry := newQueuedNZBEntry(t, m, "shutdown-cancel-entry")
	job := parseVerdictNZBJob(t, m, entry)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // shutdown before/while the worker runs the job
	m.processJob(ctx, job)

	persisted, err := m.storage.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	if persisted.State == storage.EntryStateError {
		t.Fatalf("entry was marked terminal error (LastError=%q) during shutdown", persisted.LastError)
	}
	if persisted.LastError != "" {
		t.Fatalf("entry LastError = %q, want empty", persisted.LastError)
	}
	if len(jobCh) != 0 {
		t.Fatalf("%d jobs were scheduled during shutdown, want 0", len(jobCh))
	}
}

// nzbInfraRetryDelay grows exponentially from the base and caps at the max.
func TestNZBInfraRetryDelayBackoff(t *testing.T) {
	prevBase, prevMax := nzbInfraRetryBaseDelay, nzbInfraRetryMaxDelay
	nzbInfraRetryBaseDelay = 30 * time.Second
	nzbInfraRetryMaxDelay = 5 * time.Minute
	t.Cleanup(func() { nzbInfraRetryBaseDelay, nzbInfraRetryMaxDelay = prevBase, prevMax })

	want := []time.Duration{
		30 * time.Second,
		time.Minute,
		2 * time.Minute,
		4 * time.Minute,
		5 * time.Minute,
		5 * time.Minute,
	}
	for attempt, expected := range want {
		if got := nzbInfraRetryDelay(attempt); got != expected {
			t.Fatalf("nzbInfraRetryDelay(%d) = %s, want %s", attempt, got, expected)
		}
	}
}

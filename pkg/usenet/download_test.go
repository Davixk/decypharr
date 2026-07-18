package usenet

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	appconfig "github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
	nntpyenc "github.com/sirrobot01/decypharr/internal/nntp/yenc"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type shortNilWriter struct{}

func (shortNilWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

func TestNormalizeDownloadedSegmentRequiresExactDecodedLength(t *testing.T) {
	segment := storage.NZBSegment{Bytes: 4, SegmentDataStart: 2}
	data, err := normalizeDownloadedSegment(segment, []byte("xxdata-extra"))
	if err != nil {
		t.Fatalf("normalize exact segment: %v", err)
	}
	if string(data) != "data" {
		t.Fatalf("normalized data = %q; want data", data)
	}

	_, err = normalizeDownloadedSegment(segment, []byte("xxdat"))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated segment error = %v; want io.ErrUnexpectedEOF", err)
	}
	_, err = normalizeDownloadedSegment(storage.NZBSegment{Bytes: 1, SegmentDataStart: -1}, []byte("x"))
	if err == nil {
		t.Fatal("negative segment offset was accepted")
	}
}

func TestWriteDownloadedSegmentRejectsShortNilWrite(t *testing.T) {
	n, err := writeDownloadedSegment(shortNilWriter{}, []byte("data"))
	if n != 3 || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write = (%d, %v); want (3, io.ErrShortWrite)", n, err)
	}
}

func TestVerifyDownloadCompleteChecksSegmentsAndBytes(t *testing.T) {
	if err := verifyDownloadComplete(2, 2, 8, 8); err != nil {
		t.Fatalf("complete download: %v", err)
	}
	if err := verifyDownloadComplete(1, 2, 8, 8); err == nil {
		t.Fatal("missing segment was accepted")
	}
	if err := verifyDownloadComplete(2, 2, 7, 8); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short final byte count error = %v; want io.ErrUnexpectedEOF", err)
	}
}

func TestDownloadForGenerationRejectsReplacementBeforeNetworkRead(t *testing.T) {
	store := newTestNZBStorage(t)
	const id = "stale-local-download"
	old := lifecycleTestNZB(id, "movie.mkv", 4)
	if err := store.AddNZB(old); err != nil {
		t.Fatalf("AddNZB old: %v", err)
	}
	if err := store.DeleteNZB(id); err != nil {
		t.Fatalf("DeleteNZB old: %v", err)
	}
	if err := store.AddNZB(lifecycleTestNZB(id, "movie.mkv", 8)); err != nil {
		t.Fatalf("AddNZB replacement: %v", err)
	}

	var output bytes.Buffer
	err := newTestUsenet(store).DownloadForGeneration(context.Background(), id, old.Generation, "movie.mkv", &output, nil)
	if !errors.Is(err, ErrStaleNZBGeneration) {
		t.Fatalf("stale download error = %v; want ErrStaleNZBGeneration", err)
	}
	if output.Len() != 0 {
		t.Fatalf("stale download wrote %d bytes", output.Len())
	}
}

func TestDownloadWriterFailureCancelsBlockedArticleRead(t *testing.T) {
	appconfig.SetConfigPath(t.TempDir())
	oldPureGo := nntpyenc.UsePureGo
	nntpyenc.UsePureGo = true
	t.Cleanup(func() { nntpyenc.UsePureGo = oldPureGo })

	server := newDownloadCancelNNTPServer(t)
	t.Cleanup(server.Close)
	host, port := server.address(t)
	client, err := nntp.NewClient(&appconfig.Config{
		Usenet: appconfig.Usenet{Providers: []appconfig.UsenetProvider{{
			Host:           host,
			Port:           port,
			MaxConnections: 2,
		}}},
	})
	if err != nil {
		t.Fatalf("create NNTP client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	const (
		nzoID    = "writer-cancel"
		filename = "movie.mkv"
	)
	store := newTestNZBStorage(t)
	if err := store.AddNZB(&storage.NZB{
		ID:        nzoID,
		TotalSize: 8,
		Files: []storage.NZBFile{{
			Name: filename,
			Size: 8,
			Segments: []storage.NZBSegment{
				{Number: 1, MessageID: downloadFirstMessageID, Bytes: 4},
				{Number: 2, MessageID: downloadBlockedMessageID, Bytes: 4},
			},
		}},
	}); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}

	u := newTestUsenet(store)
	u.nntp = client
	u.processingMaxConnections = 2
	writeFailure := errors.New("destination disappeared")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	downloadDone := make(chan error, 1)
	go func() {
		downloadDone <- u.Download(ctx, nzoID, filename, errorWriter{err: writeFailure}, nil)
	}()

	select {
	case <-server.blockedStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("second article did not reach the deliberately blocked body read")
	}

	select {
	case err := <-downloadDone:
		if !errors.Is(err, writeFailure) {
			t.Fatalf("Download error = %v; want writer failure", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Download did not return promptly after the writer cancelled its blocked fetch")
	}

	select {
	case <-server.blockedClosed:
	case <-time.After(time.Second):
		t.Fatal("cancelled article connection was not closed")
	}

	stats := client.Stats()
	poolStats, ok := stats["pool"].(map[string]any)
	if !ok {
		t.Fatalf("NNTP pool stats = %#v; want pool map", stats)
	}
	if active, ok := poolStats["active"].(int); !ok || active != 0 {
		t.Fatalf("active NNTP connections after Download = %#v; want 0", poolStats["active"])
	}
}

type errorWriter struct {
	err error
}

func (w errorWriter) Write([]byte) (int, error) {
	return 0, w.err
}

const (
	downloadFirstMessageID   = "download-first@test"
	downloadBlockedMessageID = "download-blocked@test"
)

type downloadCancelNNTPServer struct {
	listener net.Listener

	blockedStarted chan struct{}
	blockedClosed  chan struct{}
	stop           chan struct{}

	blockedStartOnce sync.Once
	blockedCloseOnce sync.Once
	closeOnce        sync.Once
	mu               sync.Mutex
	connections      map[net.Conn]struct{}
	wg               sync.WaitGroup
}

func newDownloadCancelNNTPServer(t *testing.T) *downloadCancelNNTPServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for fake NNTP server: %v", err)
	}
	server := &downloadCancelNNTPServer{
		listener:       listener,
		blockedStarted: make(chan struct{}),
		blockedClosed:  make(chan struct{}),
		stop:           make(chan struct{}),
		connections:    make(map[net.Conn]struct{}),
	}
	server.wg.Add(1)
	go server.acceptLoop()
	return server
}

func (s *downloadCancelNNTPServer) address(t *testing.T) (string, int) {
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

func (s *downloadCancelNNTPServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.connections[conn] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serveConnection(conn)
	}
}

func (s *downloadCancelNNTPServer) serveConnection(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.connections, conn)
		s.mu.Unlock()
		_ = conn.Close()
	}()

	writer := bufio.NewWriter(conn)
	if _, err := writer.WriteString("200 download cancellation test ready\r\n"); err != nil {
		return
	}
	if err := writer.Flush(); err != nil {
		return
	}

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 || !strings.EqualFold(fields[0], "BODY") {
			_, _ = writer.WriteString("500 unsupported command\r\n")
			_ = writer.Flush()
			continue
		}
		messageID := strings.Trim(fields[1], "<>")
		switch messageID {
		case downloadFirstMessageID:
			select {
			case <-s.blockedStarted:
			case <-s.stop:
				return
			}
			if _, err := fmt.Fprintf(writer, "222 0 <%s>\r\n%s.\r\n", messageID, downloadCancelYEnc([]byte("ABCD"))); err != nil {
				return
			}
			if err := writer.Flush(); err != nil {
				return
			}
		case downloadBlockedMessageID:
			if _, err := fmt.Fprintf(writer, "222 0 <%s>\r\n=ybegin line=128 size=4 name=blocked.bin\r\n", messageID); err != nil {
				return
			}
			if err := writer.Flush(); err != nil {
				return
			}
			s.blockedStartOnce.Do(func() { close(s.blockedStarted) })
			var one [1]byte
			_, _ = conn.Read(one[:])
			s.blockedCloseOnce.Do(func() { close(s.blockedClosed) })
			return
		default:
			_, _ = writer.WriteString("430 no such article\r\n")
			_ = writer.Flush()
		}
	}
}

func downloadCancelYEnc(data []byte) string {
	var encoded bytes.Buffer
	_, _ = fmt.Fprintf(&encoded, "=ybegin line=128 size=%d name=first.bin\r\n", len(data))
	for _, value := range data {
		value += 42
		if value == 0 || value == '\n' || value == '\r' || value == '=' || value == '\t' || value == ' ' || value == '.' {
			encoded.WriteByte('=')
			encoded.WriteByte(value + 64)
		} else {
			encoded.WriteByte(value)
		}
	}
	encoded.WriteString("\r\n")
	_, _ = fmt.Fprintf(&encoded, "=yend size=%d\r\n", len(data))
	return encoded.String()
}

func (s *downloadCancelNNTPServer) Close() {
	s.closeOnce.Do(func() {
		close(s.stop)
		_ = s.listener.Close()
		s.mu.Lock()
		for conn := range s.connections {
			_ = conn.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
}

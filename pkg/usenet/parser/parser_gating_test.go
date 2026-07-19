package parser

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// gatingTestNZB is a structurally valid single-file, single-segment NZB whose
// subject carries a media filename, so grouping needs no network content-type
// detection and the only parse-time network I/O is the STAT probe.
const gatingTestNZB = `<?xml version="1.0" encoding="UTF-8"?>
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

// fakeStatServer is a minimal NNTP server whose STAT reply is configurable so
// tests can drive each verdict class through the real client stack.
type fakeStatServer struct {
	listener  net.Listener
	statReply string

	closeOnce sync.Once
	mu        sync.Mutex
	conns     map[net.Conn]struct{}
	wg        sync.WaitGroup
}

func newFakeStatServer(t *testing.T, statReply string) *fakeStatServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for fake NNTP server: %v", err)
	}
	server := &fakeStatServer{
		listener:  listener,
		statReply: statReply,
		conns:     make(map[net.Conn]struct{}),
	}
	server.wg.Add(1)
	go server.acceptLoop()
	t.Cleanup(server.Close)
	return server
}

func (s *fakeStatServer) hostPort(t *testing.T) (string, int) {
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

func (s *fakeStatServer) acceptLoop() {
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

func (s *fakeStatServer) serveConnection(conn net.Conn) {
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
			reply := s.statReply
			if strings.Contains(reply, "%s") && len(fields) == 2 {
				reply = strings.Replace(reply, "%s", fields[1], 1)
			}
			_, _ = writer.WriteString(reply + "\r\n")
			if err := writer.Flush(); err != nil {
				return
			}
		default:
			_, _ = writer.WriteString("500 unknown command\r\n")
			if err := writer.Flush(); err != nil {
				return
			}
		}
	}
}

func (s *fakeStatServer) Close() {
	s.closeOnce.Do(func() {
		_ = s.listener.Close()
		s.mu.Lock()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
}

func newGatingParser(t *testing.T, host string, port int) *NZBParser {
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
	client, err := nntp.NewClient(cfg)
	if err != nil {
		t.Fatalf("nntp.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return NewParser(client, 2, zerolog.Nop())
}

// A provider that genuinely answers 430 for the probe must keep producing the
// "articles missing" verdict — this is the accept-then-fail contract the arrs
// rely on for native blocklisting of dead releases.
func TestParseStatProbe430IsArticlesUnavailable(t *testing.T) {
	server := newFakeStatServer(t, "430 no such article")
	host, port := server.hostPort(t)
	parser := newGatingParser(t, host, port)

	_, _, err := parser.Parse(context.Background(), "movie.nzb", []byte(gatingTestNZB))
	if err == nil {
		t.Fatal("expected Parse to fail when the probe answers 430")
	}
	if !errors.Is(err, ErrArticlesUnavailable) {
		t.Fatalf("Parse error = %v, want ErrArticlesUnavailable", err)
	}
	if errors.Is(err, ErrProbeInfrastructure) {
		t.Fatalf("Parse error = %v, must not also classify as infrastructure", err)
	}
}

// A connection-refused provider carries no verdict about the articles: the
// failure must classify as ErrProbeInfrastructure, never as missing articles.
func TestParseStatProbeConnectionRefusedIsInfrastructure(t *testing.T) {
	server := newFakeStatServer(t, "223 0 %s")
	host, port := server.hostPort(t)
	server.Close() // free the port so dialing it is refused
	parser := newGatingParser(t, host, port)

	_, _, err := parser.Parse(context.Background(), "movie.nzb", []byte(gatingTestNZB))
	if err == nil {
		t.Fatal("expected Parse to fail against a dead provider")
	}
	if !errors.Is(err, ErrProbeInfrastructure) {
		t.Fatalf("Parse error = %v, want ErrProbeInfrastructure", err)
	}
	if errors.Is(err, ErrArticlesUnavailable) {
		t.Fatalf("Parse error = %v, must not classify as missing articles", err)
	}
}

// An unknown/unclassifiable STAT answer defaults to the infrastructure
// verdict (fail-safe: a wrong "infra" verdict retries later; a wrong
// "missing" verdict blocklists a good release).
func TestParseStatProbeUnknownErrorDefaultsToInfrastructure(t *testing.T) {
	server := newFakeStatServer(t, "599 utterly unexpected condition")
	host, port := server.hostPort(t)
	parser := newGatingParser(t, host, port)

	_, _, err := parser.Parse(context.Background(), "movie.nzb", []byte(gatingTestNZB))
	if err == nil {
		t.Fatal("expected Parse to fail when the probe answer is unclassifiable")
	}
	if !errors.Is(err, ErrProbeInfrastructure) {
		t.Fatalf("Parse error = %v, want ErrProbeInfrastructure", err)
	}
	if errors.Is(err, ErrArticlesUnavailable) {
		t.Fatalf("Parse error = %v, must not classify as missing articles", err)
	}
}

// parseGatingGroups drives a successful Parse against a healthy STAT server
// and returns the parsed metadata plus file groups for Process-phase tests.
func parseGatingGroups(t *testing.T, parser *NZBParser) (*storage.NZB, map[string]*FileGroup) {
	t.Helper()
	nzb, groups, err := parser.Parse(context.Background(), "movie.nzb", []byte(gatingTestNZB))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(groups) == 0 {
		t.Fatal("Parse returned no file groups")
	}
	return nzb, groups
}

// Cancellation during archive processing must surface as the context error,
// never as the terminal "no valid files found in NZB" content verdict — that
// bare verdict error-parked 1,891 entries during the 2026-07-19 incident.
func TestProcessCtxCancelBypassesNoValidFilesVerdict(t *testing.T) {
	server := newFakeStatServer(t, "223 0 %s")
	host, port := server.hostPort(t)
	parser := newGatingParser(t, host, port)
	nzb, groups := parseGatingGroups(t, parser)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := parser.Process(ctx, nzb, groups)
	if err == nil {
		t.Fatal("expected Process to fail under a cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Process error = %v, want context.Canceled to pass through", err)
	}
	if strings.Contains(err.Error(), "no valid files") {
		t.Fatalf("Process error = %v, must not carry the generic no-valid-files verdict", err)
	}
}

// A collapsed substrate during archive processing (every header fetch fails
// on the network) carries no verdict about the content: Process must classify
// the failure as ErrProbeInfrastructure, never as missing articles and never
// as the bare "no valid files" content verdict.
func TestProcessInfraFailureClassifiesProbeInfrastructure(t *testing.T) {
	server := newFakeStatServer(t, "223 0 %s")
	host, port := server.hostPort(t)
	parser := newGatingParser(t, host, port)
	nzb, groups := parseGatingGroups(t, parser)

	server.Close() // dead substrate: every subsequent fetch fails

	_, err := parser.Process(context.Background(), nzb, groups)
	if err == nil {
		t.Fatal("expected Process to fail against a dead provider")
	}
	if !errors.Is(err, ErrProbeInfrastructure) {
		t.Fatalf("Process error = %v, want ErrProbeInfrastructure", err)
	}
	if errors.Is(err, ErrArticlesUnavailable) {
		t.Fatalf("Process error = %v, must not classify as missing articles", err)
	}
}

// A genuinely empty NZB (no usable file groups, no network probe failures)
// must keep producing the terminal "no valid files" verdict.
func TestProcessGenuinelyEmptyKeepsNoValidFilesVerdict(t *testing.T) {
	server := newFakeStatServer(t, "223 0 %s")
	host, port := server.hostPort(t)
	parser := newGatingParser(t, host, port)
	nzb, _ := parseGatingGroups(t, parser)

	for _, groups := range []map[string]*FileGroup{
		nil,
		{"empty": {BaseName: "empty", Type: storage.NZBFileTypeMedia}},
	} {
		_, err := parser.Process(context.Background(), nzb, groups)
		if err == nil {
			t.Fatal("expected Process to fail for an empty NZB")
		}
		if !strings.Contains(err.Error(), "no valid files found in NZB") {
			t.Fatalf("Process error = %v, want the no-valid-files verdict", err)
		}
		if errors.Is(err, ErrProbeInfrastructure) || errors.Is(err, ErrArticlesUnavailable) {
			t.Fatalf("Process error = %v, must not carry a probe sentinel", err)
		}
	}
}

// countGroupFailure decides which group failures may soften the "no valid
// files" verdict; pin its table.
func TestCountGroupFailure(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantInfra int
		wantMiss  int
	}{
		{"nil", nil, 0, 0},
		{"430 article not found", &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430}, 0, 1},
		{"articles unavailable sentinel", ErrArticlesUnavailable, 0, 1},
		{"infrastructure sentinel", ErrProbeInfrastructure, 1, 0},
		{"connection", nntp.NewConnectionError(errors.New("refused")), 1, 0},
		{"authentication", &nntp.Error{Type: nntp.ErrorTypeAuthentication, Code: 481}, 1, 0},
		{"context canceled", context.Canceled, 1, 0},
		{"deadline exceeded", context.DeadlineExceeded, 1, 0},
		{"wrapped connection", fmt.Errorf("failed to fetch first segment header: %w", nntp.NewConnectionError(errors.New("reset"))), 1, 0},
		{"unknown content failure", errors.New("unsupported file type"), 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var counts probeFailureCounts
			countGroupFailure(&counts, tc.err)
			if counts.infrastructure != tc.wantInfra || counts.articlesMissing != tc.wantMiss {
				t.Fatalf("countGroupFailure(%v) = infra %d, missing %d; want %d, %d",
					tc.err, counts.infrastructure, counts.articlesMissing, tc.wantInfra, tc.wantMiss)
			}
		})
	}
}

// classifyProbeError is the single choke point for the verdict; pin its table.
func TestClassifyProbeError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want error
	}{
		{"430 article not found", &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430}, ErrArticlesUnavailable},
		{"connection", nntp.NewConnectionError(errors.New("refused")), ErrProbeInfrastructure},
		{"timeout", nntp.NewTimeoutError(errors.New("deadline")), ErrProbeInfrastructure},
		{"no available connection", nntp.NewNoAvailableConnectionError("no eligible providers available", nil), ErrProbeInfrastructure},
		{"authentication", &nntp.Error{Type: nntp.ErrorTypeAuthentication, Code: 481}, ErrProbeInfrastructure},
		{"unknown non-nntp", errors.New("mystery failure"), ErrProbeInfrastructure},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyProbeError(tc.err); !errors.Is(got, tc.want) {
				t.Fatalf("classifyProbeError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

package manager

import (
	"context"
	"sync"
	"testing"
	"time"
)

// gateObserver tracks the number of Process calls currently admitted past the
// concurrency gate and the peak seen, so a test can assert the gate never
// admits more than its configured width.
type gateObserver struct {
	mu     sync.Mutex
	active int
	max    int
}

func (g *gateObserver) apply(delta int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active += delta
	if g.active > g.max {
		g.max = g.active
	}
}

func (g *gateObserver) snapshot() (active, max int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active, g.max
}

// With the gate sized to N, at most N Process calls may be in flight at once;
// additional imports wait at the gate. Process is held open by a BODY-blocking
// fake server (Process fetches article bodies to sniff archive headers), so the
// admitted calls linger long enough to observe the ceiling.
func TestProcessGateBoundsConcurrentProcess(t *testing.T) {
	server := newVerdictFakeNNTPServer(t, true) // STAT 223 for parse; BODY blocks in Process
	host, port := server.hostPort(t)
	m, _ := newVerdictTestManager(t, host, port)
	// Long enough that the admitted Process calls stay in flight through the
	// observation window; closing the server (below) is what actually drains
	// them, so blocked calls never linger into a later test.
	m.usenetTimeout = 10 * time.Second

	const gate = 2
	m.processSem = make(chan struct{}, gate)
	obs := &gateObserver{}
	m.processGateObserver = obs.apply

	const imports = 3
	// Parse all jobs up front while STAT answers (parse never fetches bodies).
	jobs := make([]*Job, imports)
	for i := 0; i < imports; i++ {
		entry := newQueuedNZBEntry(t, m, "gate-entry-"+string(rune('a'+i)))
		jobs[i] = parseVerdictNZBJob(t, m, entry)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Add(1)
		go func(job *Job) {
			defer wg.Done()
			_ = m.processNewNzb(ctx, job.Entry, job.NZBMeta, job.NZBGroups)
		}(job)
	}

	// Wait until the gate is saturated (exactly `gate` calls admitted).
	deadline := time.After(3 * time.Second)
	for {
		active, _ := obs.snapshot()
		if active >= gate {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d Process calls admitted, want gate width %d", active, gate)
		case <-time.After(5 * time.Millisecond):
		}
	}

	// Let any (erroneous) over-admission surface, then assert the ceiling held.
	time.Sleep(250 * time.Millisecond)
	active, maxSeen := obs.snapshot()
	if active != gate {
		t.Fatalf("in-flight Process = %d, want exactly the gate width %d", active, gate)
	}
	if maxSeen != gate {
		t.Fatalf("peak concurrent Process = %d, want it never to exceed the gate width %d", maxSeen, gate)
	}

	// Unblock everything and confirm the waiters drain without ever exceeding
	// the gate. Closing the server releases the blocked BODY reads so every
	// Process call returns promptly (and nothing leaks into later tests); the
	// context cancel is a backstop.
	cancel()
	server.Close()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("processNewNzb goroutines did not drain after unblock")
	}
	if _, maxSeen := obs.snapshot(); maxSeen != gate {
		t.Fatalf("peak concurrent Process = %d after drain, want %d", maxSeen, gate)
	}
}

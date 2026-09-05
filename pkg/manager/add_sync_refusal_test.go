package manager

import (
	"context"
	"errors"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A GRAB THAT FAILS INSIDE ITS OWN REQUEST MUST LEAVE NOTHING BEHIND.
//
// Operator doctrine, verbatim: a synchronous operation (grab, add, submit) that
// fails must report the failure ON THE REQUEST. Accepting the grab and then
// immediately parking the row as `error` hands the arr a successful grab plus a
// corpse — a dead-end warning it can neither retry nor clear — and costs a full
// indexer re-search later, where a synchronous refusal costs nothing because the
// arr is still holding its ranked candidate list.
//
// SubmitJob failing is knowable inside the request, so it belongs on the request.
func newSyncRefusalManager(t *testing.T, client *fakeDebridClient) *Manager {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()

	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	clientMap := xsync.NewMap[string, common.Client]()
	clientMap.Store(client.cfg.Name, client)

	return &Manager{
		storage:           store,
		queue:             newQueue(store, ""),
		clients:           clientMap,
		slotCache:         newProviderSlotCache(),
		fillCache:         newProviderFillCache(),
		config:            cfg,
		logger:            zerolog.Nop(),
		ctx:               context.Background(),
		arr:               arr.NewStorage(),
		processingEntries: xsync.NewMap[string, struct{}](),
		actionInflight:    xsync.NewMap[string, struct{}](),
		// ⚠️ Nil-guarded and silently inert — see the fixture note in
		// action_lifecycle_test.go. Wired here so nothing in the add path
		// no-ops without saying so.
		declines:    newDeclineLedger(),
		pendingAdds: newPendingAddLedger(),
		addPace:     newAddPacer(),
		progress:    newProgressTracker(),
		// jobQueue is deliberately NIL: SubmitJob returns an error when it is
		// unset, which is the failure this test is about.
	}
}

func TestFailedJobSubmissionWithdrawsTheReservation(t *testing.T) {
	client := &fakeDebridClient{
		cfg:      config.Debrid{Name: "primary", Provider: "realdebrid"},
		recorder: &fallbackCallRecorder{},
	}
	m := newSyncRefusalManager(t, client)
	req := fallbackTestRequest("", false, nil)

	err := m.AddNewTorrent(context.Background(), req)

	// 1. THE GRAB FAILS. The arr must learn now, on this request.
	if err == nil {
		t.Fatal("AddNewTorrent returned nil after the job could not be queued; the arr would " +
			"record a successful grab for a download that never starts")
	}

	// 2. NO ROW SURVIVES. This is the assertion that fails if the fix is
	//    reverted to MarkAsError + Update: the row would exist, in `error`.
	hash := req.Magnet.InfoHash
	if entry, getErr := m.queue.GetTorrent(hash); getErr == nil && entry != nil {
		t.Fatalf("a queue row survived a synchronously-failed grab (state=%q status=%q). "+
			"A sync refusal must leave nothing for the arr to trip over.",
			entry.State, entry.Status)
	} else if getErr != nil && !errors.Is(getErr, storage.ErrEntryNotFound) {
		t.Fatalf("unexpected error reading the queue: %v", getErr)
	}

	// 3. THE PLACEMENT GOES WITH IT. The provider had already accepted this
	//    torrent, so dropping only the local row would strand a transfer burning
	//    a provider slot that no local record could ever release — the orphan
	//    leak, re-created one refusal at a time.
	if deleted := client.deleted(); len(deleted) == 0 {
		t.Fatal("the provider placement was never released; withdrawing the reservation without " +
			"releasing the transfer leaks a provider slot as an untrackable orphan")
	}
}

// The reservation must not be withdrawn on the SUCCESS path — that would delete
// the row for every healthy grab. Guards the fix against being written as an
// unconditional cleanup.
func TestSuccessfulAddKeepsItsQueueRow(t *testing.T) {
	client := &fakeDebridClient{
		cfg:      config.Debrid{Name: "primary", Provider: "realdebrid"},
		recorder: &fallbackCallRecorder{},
	}
	m := newSyncRefusalManager(t, client)
	m.jobQueue = NewJobQueue(context.Background(), 1, func(context.Context, *Job) {})
	t.Cleanup(m.jobQueue.Close)

	req := fallbackTestRequest("", false, nil)
	if err := m.AddNewTorrent(context.Background(), req); err != nil {
		t.Fatalf("AddNewTorrent: %v", err)
	}

	entry, err := m.queue.GetTorrent(req.Magnet.InfoHash)
	if err != nil || entry == nil {
		t.Fatalf("a successful add must leave its queue row in place, got entry=%v err=%v", entry, err)
	}
	if len(client.deleted()) != 0 {
		t.Fatalf("a successful add released the provider placement: %v", client.deleted())
	}
}

package arr

import (
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/config"
)

func newSyncTestStorage(arrs ...*Arr) *Storage {
	storage := &Storage{arrs: xsync.NewMap[string, *Arr]()}
	for _, configured := range arrs {
		storage.arrs.Store(configured.Name, configured)
	}
	return storage
}

func TestSyncFromConfigUsesValidEditedHostAndPolicies(t *testing.T) {
	storage := newSyncTestStorage(NewWithOptions("radarr", "http://old.example", "old-token", Options{
		Cleanup: true,
		Source:  SourceManual,
	}))

	storage.SyncFromConfig([]config.Arr{{
		Name:              "radarr",
		Host:              "http://new.example",
		Token:             "new-token",
		Cleanup:           false,
		FallbackOnFailure: true,
		Source:            string(SourceManual),
	}})

	got := storage.Get("radarr")
	if got == nil {
		t.Fatal("edited Arr disappeared")
	}
	if got.Host != "http://new.example" || got.Token != "new-token" {
		t.Fatalf("edited connection not applied: host=%q token=%q", got.Host, got.Token)
	}
	if got.Cleanup || !got.FallbackOnFailure {
		t.Fatalf("edited policies not applied: cleanup=%v fallback=%v", got.Cleanup, got.FallbackOnFailure)
	}
}

func TestSyncFromConfigRemovesDeletedManualArrButKeepsAutoDetected(t *testing.T) {
	manual := NewWithOptions("manual", "http://manual.example", "token", Options{
		Cleanup: true,
		Source:  SourceManual,
	})
	auto := NewWithOptions("auto", "http://auto.example", "token", Options{
		Cleanup: true,
		Source:  SourceAuto,
	})
	storage := newSyncTestStorage(manual, auto)

	storage.SyncFromConfig(nil)

	if got := storage.Get("manual"); got != nil {
		t.Fatalf("deleted manual Arr was retained: %+v", got)
	}
	if got := storage.Get("auto"); got != auto {
		t.Fatalf("auto-detected Arr was not preserved: %+v", got)
	}
}

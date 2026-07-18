package reader

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/nntp"
)

func TestFetchTimeoutIncludesLocalSemaphoreWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := DefaultConfig()
	cfg.MaxConnections = 1
	cfg.PrefetchAhead = 0
	cfg.DownloadTimeout = 50 * time.Millisecond

	stats := &ReaderStats{}
	cache := &SegmentCache{
		segments: []SegmentMeta{{
			MessageID:   "missing",
			Number:      1,
			Bytes:       4,
			StartOffset: 0,
			EndOffset:   3,
		}},
		segCount: 1,
		states:   make([]atomic.Uint32, 1),
		ctx:      ctx,
		stats:    stats,
	}

	fetcher := NewSegmentFetcher(ctx, nil, cache, cfg, stats, zerolog.Nop())
	defer fetcher.Close()

	// Occupy the only reader slot without involving the provider client. The
	// fetch must time out while waiting here and restore Fetching -> Empty.
	fetcher.semaphore <- struct{}{}
	started := time.Now()
	err := fetcher.Fetch(context.Background(), 0)
	elapsed := time.Since(started)
	<-fetcher.semaphore

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Fetch error = %v, want context deadline exceeded", err)
	}
	if elapsed > time.Second {
		t.Fatalf("semaphore wait took %s, want a bounded wait", elapsed)
	}
	if state := cache.GetState(0); state != StateEmpty {
		t.Fatalf("segment state after timeout = %s, want Empty", state)
	}
}

func TestValidateSegmentLengthClassifiesShortBodiesAsMissing(t *testing.T) {
	if err := validateSegmentLength(4, 4); err != nil {
		t.Fatalf("exact segment length rejected: %v", err)
	}

	err := validateSegmentLength(3, 4)
	if !nntp.IsArticleNotFoundError(err) {
		t.Fatalf("short segment error = %v, want article-not-found classification", err)
	}
	if !strings.Contains(err.Error(), "got 3 bytes, expected 4") {
		t.Fatalf("short segment error lacks byte counts: %v", err)
	}
}

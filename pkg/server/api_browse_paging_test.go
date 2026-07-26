package server

import (
	"fmt"
	"net/http/httptest"
	"testing"
)

// /api/browse is the only unfiltered view of the entry set, so it is the tool
// reached for when measuring how many entries a listing bug affects. It was
// unusable for that: `limit` above the ceiling did not CLAMP, it silently RESET
// to the default. Asking for more returned fewer, with nothing in the response
// saying the request had been overridden.
func TestBrowsePageParamsClampInsteadOfResetting(t *testing.T) {
	for _, tc := range []struct {
		query     string
		wantPage  int
		wantLimit int
		why       string
	}{
		{"", 1, browseDefaultLimit, "no params means page 1 at the default size"},
		{"?limit=10", 1, 10, "a small explicit limit is honoured"},
		{"?limit=100", 1, 100, "the old ceiling is still honoured exactly"},
		{"?limit=101", 1, 101, "one over the OLD ceiling used to collapse to 50"},
		{"?limit=1000", 1, browseMaxLimit, "the max is reachable"},
		{"?limit=5000", 1, browseMaxLimit, "above the max CLAMPS; it must never fall back to the default"},
		{"?limit=0", 1, browseDefaultLimit, "zero means unspecified"},
		{"?limit=-7", 1, browseDefaultLimit, "negative means unspecified"},
		{"?limit=abc", 1, browseDefaultLimit, "unparseable means unspecified"},
		{"?page=3&limit=200", 3, 200, "page and limit are independent"},
		{"?page=0", 1, browseDefaultLimit, "page is 1-based"},
		{"?page=-4", 1, browseDefaultLimit, "negative page floors at 1"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/api/browse/__all__"+tc.query, nil)
			page, limit := browsePageParams(r)
			if page != tc.wantPage || limit != tc.wantLimit {
				t.Fatalf("page=%d limit=%d, want page=%d limit=%d (%s)", page, limit, tc.wantPage, tc.wantLimit, tc.why)
			}
		})
	}
}

// TestBrowseLimitAboveCeilingReturnsMoreNotFewer states the regression in the
// terms the operator hit it: a larger requested limit must never yield a smaller
// page than a smaller requested limit.
func TestBrowseLimitAboveCeilingReturnsMoreNotFewer(t *testing.T) {
	entries := make([]BrowseEntry, 500)
	for i := range entries {
		entries[i] = BrowseEntry{Name: fmt.Sprintf("entry-%03d", i)}
	}

	small := httptest.NewRequest("GET", "/?limit=100", nil)
	_, smallLimit := browsePageParams(small)
	smallPage, _ := paginateBrowseEntries(entries, 1, smallLimit)

	big := httptest.NewRequest("GET", "/?limit=5000", nil)
	_, bigLimit := browsePageParams(big)
	bigPage, _ := paginateBrowseEntries(entries, 1, bigLimit)

	if len(bigPage) < len(smallPage) {
		t.Fatalf("limit=5000 returned %d rows but limit=100 returned %d; asking for more must never return fewer",
			len(bigPage), len(smallPage))
	}
	if len(bigPage) != len(entries) {
		t.Fatalf("limit=5000 over %d entries returned %d rows; the clamp should have covered the whole set", len(entries), len(bigPage))
	}
}

// TestPaginateBrowseEntriesWalksTheWholeSet pins that paging is exhaustive and
// non-overlapping — the property an enumeration recipe depends on.
func TestPaginateBrowseEntriesWalksTheWholeSet(t *testing.T) {
	const total = 237
	entries := make([]BrowseEntry, total)
	for i := range entries {
		entries[i] = BrowseEntry{Name: fmt.Sprintf("entry-%03d", i)}
	}

	const limit = 50
	seen := make(map[string]int)
	_, totalPages := paginateBrowseEntries(entries, 1, limit)
	if totalPages != 5 {
		t.Fatalf("total_pages = %d, want 5 for %d entries at %d per page", totalPages, total, limit)
	}
	for page := 1; page <= totalPages; page++ {
		rows, _ := paginateBrowseEntries(entries, page, limit)
		for _, row := range rows {
			seen[row.Name]++
		}
	}
	if len(seen) != total {
		t.Fatalf("walking every page saw %d distinct entries, want %d", len(seen), total)
	}
	for name, count := range seen {
		if count != 1 {
			t.Fatalf("entry %q appeared on %d pages; pages must not overlap", name, count)
		}
	}

	// Past the end is empty, never a panic and never a wrap-around.
	rows, _ := paginateBrowseEntries(entries, totalPages+1, limit)
	if len(rows) != 0 {
		t.Fatalf("page past the end returned %d rows, want 0", len(rows))
	}
}

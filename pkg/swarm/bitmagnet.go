package swarm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// BITMAGNET — a local index, kept as a SECONDARY source.
//
// It is fast (1-3ms) and local, which is what first made it attractive for a
// lookup sitting inside a blocking grab. It is also, as measured on a live
// index, far too stale to condemn anything on its own: median seeder staleness
// 58.3 HOURS, oldest 313h, and not a single record refreshed inside 24h. Used
// as the primary source it would have refused ~18% of grabs on a number with a
// median age of 63.8 hours.
//
// So it stays, behind the interface, for the case where a live scrape cannot
// answer at all — a stale positive ("this swarm had 40 seeders two days ago") is
// weak evidence FOR a torrent, and this gate only ever refuses. It can rescue a
// release the scrape could not reach; it can never be the reason one dies,
// provided it is ordered after a live source.
type Bitmagnet struct {
	// Endpoint is the GraphQL URL. Empty makes the source inert.
	Endpoint string
	Client   *http.Client
}

func (b *Bitmagnet) Name() string { return "bitmagnet" }

type bitmagnetResponse struct {
	Data struct {
		TorrentContent struct {
			Search struct {
				Items []struct {
					InfoHash string `json:"infoHash"`
					Torrent  struct {
						// Pointer because bitmagnet returns null for a torrent
						// it knows but has no swarm reading for. A null is
						// ABSENCE and must not decode to a confident 0 — that
						// would turn "we don't know" into the exact value that
						// triggers a refusal.
						Seeders  *int `json:"seeders"`
						Leechers *int `json:"leechers"`
					} `json:"torrent"`
				} `json:"items"`
			} `json:"search"`
		} `json:"torrentContent"`
	} `json:"data"`
}

func (b *Bitmagnet) Lookup(ctx context.Context, infoHash string, _ []string) (Metadata, bool) {
	if b.Endpoint == "" || !IsInfoHash(infoHash) {
		return Metadata{}, false
	}

	// The hash is interpolated into a GraphQL document and arrives from a magnet
	// somebody else wrote, so IsInfoHash above is the injection guard: only bare
	// hex reaches the query, which makes the interpolation provably inert.
	query := fmt.Sprintf(
		`{"query":"{ torrentContent { search(input:{infoHashes:[\"%s\"], limit:1}) { items { infoHash torrent { seeders leechers } } } } }"}`,
		strings.ToLower(infoHash),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.Endpoint, bytes.NewBufferString(query))
	if err != nil {
		return Metadata{}, false
	}
	req.Header.Set("Content-Type", "application/json")

	client := b.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return Metadata{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Metadata{}, false
	}

	var decoded bitmagnetResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return Metadata{}, false
	}
	for _, item := range decoded.Data.TorrentContent.Search.Items {
		if !strings.EqualFold(item.InfoHash, infoHash) {
			continue
		}
		if item.Torrent.Seeders == nil {
			return Metadata{}, false
		}
		md := Metadata{Seeders: *item.Torrent.Seeders, Source: "bitmagnet"}
		if item.Torrent.Leechers != nil {
			md.Leechers = *item.Torrent.Leechers
		}
		return md, true
	}
	return Metadata{}, false
}

// IsInfoHash reports whether s is a bare 40-character hex infohash.
//
// Shared because it is both a sanity check and a security boundary: an infohash
// arrives from a magnet link written by somebody else, and reaches a query
// language in one implementation and a wire protocol in another. Every rejection
// path allows the grab, so a strange hash costs nothing.
func IsInfoHash(s string) bool {
	if len(s) != 40 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

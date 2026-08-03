package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// THE GRAB-TIME SEEDER GATE.
//
// Refuses an UNCACHED torrent whose swarm is too small to finish, while the arr
// is still blocked inside its add call and still holding its ranked candidate
// list. See config.SeederGateConfig for why that timing is the entire feature
// and why this can never wait for anything.
//
// EVERY UNCERTAINTY ALLOWS. Coverage of the only usable source is ~53%, so
// roughly half of all grabs arrive here with no answer available. Refusing on
// ignorance would silently reject half of everything the arrs ask for, which is
// a far larger harm than letting a dead torrent through — the stall sweep still
// catches those later, just at the async price this gate exists to avoid.

type seederGateSettings struct {
	minSeeders int
	endpoint   string
	timeout    time.Duration
}

func (s seederGateSettings) enabled() bool {
	return s.minSeeders > 0 && s.endpoint != ""
}

func resolveSeederGate(cfg config.SeederGateConfig) seederGateSettings {
	s := seederGateSettings{endpoint: strings.TrimSpace(cfg.BitmagnetURL)}
	// TRI-STATE, and absent means OFF. An earlier version of this feature
	// pointed absent at 1, so an operator who had never heard of it got a live
	// gate. For anything that deletes, silence must mean do nothing.
	if cfg.MinSeeders != nil && *cfg.MinSeeders > 0 {
		s.minSeeders = *cfg.MinSeeders
	}
	if d, err := utils.ParseDuration(cfg.Timeout); err == nil && d > 0 {
		s.timeout = d
	} else if d, err := utils.ParseDuration(config.DefaultSeederGateTimeout); err == nil {
		s.timeout = d
	}
	return s
}

// isInfoHash reports whether s is a bare 40-character hex infohash.
//
// This is also the injection guard. The hash is interpolated into a GraphQL
// document below, and an infohash arrives from a magnet link written by
// somebody else — so it is untrusted input reaching a query language. Rejecting
// anything that is not hex makes the interpolation provably inert, and the
// rejection path allows the grab, so a strange hash costs nothing.
func isInfoHash(s string) bool {
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

type bitmagnetResponse struct {
	Data struct {
		TorrentContent struct {
			Search struct {
				Items []struct {
					InfoHash string `json:"infoHash"`
					Torrent  struct {
						// Pointer because bitmagnet returns null for a torrent
						// it knows but has no swarm reading for. A null is
						// ABSENCE, and must not decode to a confident 0 — that
						// would turn "we don't know" into the exact value that
						// triggers a refusal.
						Seeders *int `json:"seeders"`
					} `json:"torrent"`
				} `json:"items"`
			} `json:"search"`
		} `json:"torrentContent"`
	} `json:"data"`
}

// bitmagnetSeeders returns the indexed swarm size for a hash.
//
// The second return is KNOWN. False never means "zero seeders"; it means the
// question could not be answered, and every caller must read it as allow.
func (m *Manager) bitmagnetSeeders(ctx context.Context, endpoint, infoHash string, timeout time.Duration) (int, bool) {
	if !isInfoHash(infoHash) {
		return 0, false
	}

	query := fmt.Sprintf(
		`{"query":"{ torrentContent { search(input:{infoHashes:[\"%s\"], limit:1}) { items { infoHash torrent { seeders } } } } }"}`,
		strings.ToLower(infoHash),
	)

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBufferString(query))
	if err != nil {
		return 0, false
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false
	}

	var decoded bitmagnetResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return 0, false
	}
	for _, item := range decoded.Data.TorrentContent.Search.Items {
		if !strings.EqualFold(item.InfoHash, infoHash) {
			continue
		}
		if item.Torrent.Seeders == nil {
			return 0, false
		}
		return *item.Torrent.Seeders, true
	}
	return 0, false
}

// seederGateRefusal reports why an uncached grab should be refused, or "" to
// allow it.
//
// providerSeeders is whatever the provider itself reported on the transfer it
// just created. It may CONFIRM and may never CONDEMN: a provider has not had
// time to discover peers on a transfer that is seconds old, so a zero from it
// is ignorance rather than a verdict. Non-zero is real evidence and short-
// circuits the lookup entirely.
func (m *Manager) seederGateRefusal(ctx context.Context, infoHash string, providerSeeders int) string {
	settings := resolveSeederGate(config.Get().SeederGate)
	if !settings.enabled() {
		return ""
	}
	if providerSeeders >= settings.minSeeders {
		return ""
	}

	seeders, known := m.bitmagnetSeeders(ctx, settings.endpoint, infoHash, settings.timeout)
	if !known {
		// FAIL OPEN. ~47% of grabs land here and every one must proceed.
		return ""
	}
	if seeders >= settings.minSeeders {
		return ""
	}
	return fmt.Sprintf("uncached release has %d seeders, below the minimum of %d", seeders, settings.minSeeders)
}

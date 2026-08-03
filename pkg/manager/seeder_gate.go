package manager

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/swarm"
)

// THE GRAB-TIME SEEDER GATE.
//
// Refuses an UNCACHED torrent whose swarm is too small to finish, while the arr
// is still blocked inside its add call and still holding its ranked candidate
// list. See config.SeederGateConfig for why that timing is the entire feature
// and why this can never wait for anything.
//
// This file contains POLICY only — the threshold, the confirm-never-condemn
// rule for provider counts, and the fail-open discipline. WHERE the swarm
// reading comes from lives behind swarm.Source, so the backend can be replaced
// without touching any of it. That separation is deliberate: the first version
// of this feature was welded to one backend, and when that backend's data
// turned out to be 58 hours stale there was no seam to swap it at.

type seederGateSettings struct {
	minSeeders int
	timeout    time.Duration
	source     swarm.Source
}

func (s seederGateSettings) enabled() bool {
	return s.minSeeders > 0 && s.source != nil
}

func resolveSeederGate(cfg config.SeederGateConfig) seederGateSettings {
	s := seederGateSettings{timeout: parseDurationOr(cfg.Timeout, config.DefaultSeederGateTimeout)}

	// TRI-STATE, and absent means OFF. An earlier version pointed absent at 1,
	// so an operator who had never heard of the feature got a live gate that
	// deletes transfers. For anything destructive, silence must mean do nothing.
	if cfg.MinSeeders != nil && *cfg.MinSeeders > 0 {
		s.minSeeders = *cfg.MinSeeders
	}
	if s.minSeeders <= 0 {
		return s
	}

	var chain swarm.Chain
	for _, name := range cfg.Sources {
		switch strings.ToLower(strings.TrimSpace(name)) {
		case config.SwarmSourceUDPScrape:
			chain = append(chain, &swarm.UDPScrape{
				BindAddr:   cfg.ScrapeBindAddr,
				PerTracker: parseDurationOr(cfg.ScrapeTimeout, config.DefaultSeederGateScrapeTimeout),
				Fallback:   cfg.FallbackTrackers,
			})
		case config.SwarmSourceBitmagnet:
			if endpoint := strings.TrimSpace(cfg.BitmagnetURL); endpoint != "" {
				chain = append(chain, &swarm.Bitmagnet{Endpoint: endpoint})
			}
		}
		// An unrecognised name contributes nothing rather than failing the
		// whole gate. A typo should not silently arm a different backend, and
		// it must not turn the gate into a refuse-everything either — with no
		// usable source the gate is simply off.
	}
	if len(chain) > 0 {
		s.source = chain
	}
	return s
}

func parseDurationOr(raw, fallback string) time.Duration {
	if d, err := utils.ParseDuration(raw); err == nil && d > 0 {
		return d
	}
	d, _ := utils.ParseDuration(fallback)
	return d
}

// magnetTrackers pulls the announce list out of a magnet link.
//
// ⚠️ always_rm_tracker_urls strips these on the way in, so with that setting on
// every magnet arrives here with an empty list and the scrape has nobody to ask
// — which is an absence, and therefore allows. That is the safe direction, but
// it does mean the two settings together silently disable the gate unless
// fallback_trackers is populated.
func magnetTrackers(magnetLink string) []string {
	if magnetLink == "" {
		return nil
	}
	parsed, err := url.Parse(magnetLink)
	if err != nil {
		return nil
	}
	return parsed.Query()["tr"]
}

// seederGateRefusal reports why an uncached grab should be refused, or "" to
// allow it.
//
// providerSeeders is whatever the provider itself reported on the transfer it
// just created. It may CONFIRM and may never CONDEMN: a provider has not had
// time to discover peers on a transfer that is seconds old, so a zero from it
// is ignorance rather than a verdict. Non-zero is real evidence and
// short-circuits the lookup entirely.
//
// That rule lives HERE and not inside a source, because it is a policy about
// how much to trust a provider's own reading — not a way of looking up a swarm.
func (m *Manager) seederGateRefusal(ctx context.Context, infoHash, magnetLink string, providerSeeders int) string {
	settings := resolveSeederGate(config.Get().SeederGate)
	if !settings.enabled() {
		return ""
	}
	if providerSeeders >= settings.minSeeders {
		return ""
	}
	if !swarm.IsInfoHash(infoHash) {
		return ""
	}

	// One budget for the whole lookup, every source and every tracker inside
	// it. The caller is an arr blocked on an add; it must not be able to wait
	// longer than this however many backends are configured.
	ctx, cancel := context.WithTimeout(ctx, settings.timeout)
	defer cancel()

	md, known := settings.source.Lookup(ctx, infoHash, magnetTrackers(magnetLink))
	if !known {
		// FAIL OPEN. No source could answer — no record, no tracker, a
		// timeout, a malformed reply. None of those are evidence about the
		// swarm, and refusing on ignorance would reject a large share of
		// everything the arrs ask for.
		return ""
	}
	if md.Seeders >= settings.minSeeders {
		return ""
	}
	return fmt.Sprintf("uncached release has %d seeders per %s, below the minimum of %d",
		md.Seeders, md.Source, settings.minSeeders)
}

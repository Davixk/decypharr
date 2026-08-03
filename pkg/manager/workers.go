package manager

import (
	"context"

	"github.com/go-co-op/gocron/v2"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/netbind"
	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
)

// runInitialCalls performs any initial calls of worker functions
// for example, call the processQueuedEntries function once
func (m *Manager) runInitialCalls(ctx context.Context) {
	go m.refreshDownloadLinks(ctx)
	go m.processQueuedEntries()
	go m.syncAccounts()
}

// logNetworkBindings states, once at startup, where each class of outbound
// traffic will actually leave from.
//
// An operator must be able to READ which class goes where rather than infer it
// from the absence of a complaint — the whole point of the feature is that
// certain traffic takes a specific route, and "it did not error" is not
// evidence of that. A binding that is configured but broken is reported here,
// at startup, instead of surfacing hours later as a failed grab nobody connects
// back to the config.
func (m *Manager) logNetworkBindings() {
	binder := netbind.New(classSpecs(config.Get().NetworkBinding.Bindings()))
	for _, resolved := range binder.Snapshot() {
		event := m.logger.Info()
		if resolved.Err != nil {
			// LOUD. A configured-but-unresolvable binding means every operation
			// in that class will FAIL rather than quietly take the ordinary
			// route. That is the intended behaviour, and the operator needs to
			// know it is happening.
			m.logger.Error().Err(resolved.Err).
				Str("class", string(resolved.Class)).
				Str("configured", resolved.Spec).
				Msg("Network binding cannot be resolved. Traffic in this class will FAIL rather than " +
					"fall back to the default route. If this interface lives on the host, note that a " +
					"bridge-network container cannot see it — that is a deployment change, not a config one.")
			continue
		}
		if resolved.Configured {
			event = event.Str("configured", resolved.Spec)
		}
		event.
			Str("class", string(resolved.Class)).
			Str("egress", resolved.Address).
			Msg("Network binding")
	}
}

func (m *Manager) syncAccounts() {
	// Sync accounts for all debrids
	m.clients.Range(func(debridName string, debridClient debrid.Client) bool {
		if debridClient == nil {
			return true
		}
		debridClient.SyncAccounts()
		return true
	})
}

func (m *Manager) refreshDownloadLinks(ctx context.Context) {
	// Refresh download links for all debrids
	m.clients.Range(func(debridName string, debridClient debrid.Client) bool {
		if debridClient == nil {
			return true
		}
		m.refreshDebridDownloadLinks(ctx, debridName, debridClient)
		return true
	})
}

func (m *Manager) addQueueProcessorJob(ctx context.Context) error {
	// This function is responsible for starting queue processing scheduled tasks

	if jd, err := utils.ConvertToJobDef(m.config.RefreshInterval); err != nil {
		m.logger.Error().Err(err).Msg("Failed to convert queue processing interval to job definition")
	} else {
		// Schedule the job
		if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
			m.processQueuedEntries()
		}), gocron.WithContext(ctx)); err != nil {
			m.logger.Error().Err(err).Msg("Failed to create slots tracking job")
		} else {
			m.logger.Debug().Msgf("Queue processing job scheduled for every %s", m.config.RefreshInterval)
		}
	}

	if m.config.RemoveStalledAfter != "" {
		// Stalled torrents removal job
		if jd, err := utils.ConvertToJobDef("1m"); err != nil {
			m.logger.Error().Err(err).Msg("Failed to convert remove stalled torrents interval to job definition")
		} else {
			// Schedule the job
			if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
				err := m.queue.DeleteStalled()
				if err != nil {
					m.logger.Error().Err(err).Msg("Failed to process remove stalled torrents")
				}
			}), gocron.WithContext(ctx)); err != nil {
				m.logger.Error().Err(err).Msg("Failed to create remove stalled torrents job")
			} else {
				m.logger.Debug().Msgf("Remove stalled torrents job scheduled for every %s", "1m")
			}
		}
	}

	// Orphaned post-download claim reconciler. A claimed action whose goroutine
	// died leaves the entry invisible to the scheduler (IsDownloading) with no
	// recovery path other than a restart; this resubmits such claims through
	// the action gate at runtime.
	if jd, err := utils.ConvertToJobDef("1m"); err != nil {
		m.logger.Error().Err(err).Msg("Failed to convert orphaned claim reconciler interval to job definition")
	} else {
		if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
			m.reconcileOrphanedClaims()
		}), gocron.WithContext(ctx), gocron.WithName("orphaned-claim-reconciler")); err != nil {
			m.logger.Error().Err(err).Msg("Failed to create orphaned claim reconciler job")
		} else {
			m.logger.Debug().Msg("Orphaned claim reconciler job scheduled for every 1m")
		}
	}

	// Failed-entry revival sweep. Entries that failed with an
	// infrastructure/availability signature (and are still below the
	// configured retries) are reset and resubmitted, rate-limited per sweep,
	// so a future substrate incident self-heals at runtime without a reboot.
	// The same bounded budget then re-feeds NZBs parked by the infra-retry cap
	// (resweepParkedInfraNZBs): parked entries retry ONLY on this slow cadence,
	// never on the fast job-queue loop, so a permanent Process-infrastructure
	// failure cannot pin the worker pool.
	if jd, err := utils.ConvertToJobDef(reviveSweepInterval); err != nil {
		m.logger.Error().Err(err).Msg("Failed to convert revival sweep interval to job definition")
	} else {
		if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
			revived := m.reviveErrorEntries(ctx, reviveSweepLimit, true)
			if revived < reviveSweepLimit {
				m.resweepParkedInfraNZBs(ctx, reviveSweepLimit-revived)
			}
		}), gocron.WithContext(ctx), gocron.WithName("error-entry-revival")); err != nil {
			m.logger.Error().Err(err).Msg("Failed to create failed-entry revival job")
		} else {
			m.logger.Debug().Msgf("Failed-entry revival job scheduled for every %s", reviveSweepInterval)
		}
	}

	// Capacity admission controller. Entries accepted at grab time that no
	// provider had room for are normally admitted the instant a slot frees — an
	// event this process witnesses and usually causes. This asks each provider
	// ONCE per interval, however many entries are waiting, to catch capacity
	// that frees from sources we never see: AllDebrid's own 30-minute no-peer
	// prune, the operator deleting on the provider, another client sharing the
	// account, and the daily add allowance resetting (which frees no slot at
	// all, so no event can ever exist for it).
	//
	// One call PER PROVIDER, not one per held entry. That is O(1) against O(N):
	// at a few thousand held entries a per-entry cadence is ~100 provider calls
	// a second and grows with the backlog, where this stays constant.
	if jd, err := utils.ConvertToJobDef(capacityAdmissionInterval); err != nil {
		m.logger.Error().Err(err).Msg("Failed to convert capacity admission interval to job definition")
	} else {
		if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
			m.admitHeldFromProviderCapacity()
		}), gocron.WithContext(ctx), gocron.WithName("capacity-admission")); err != nil {
			m.logger.Error().Err(err).Msg("Failed to create capacity admission job")
		} else {
			m.logger.Debug().Msgf("Capacity admission controller scheduled for every %s", capacityAdmissionInterval)
		}
	}

	// Missing-download recovery sweep. Completed entries whose download folder
	// vanished (e.g. the category-directory data-loss incident) are reset to the
	// claimed shape and resubmitted through the action gate, which rebuilds their
	// symlinks from intact content — rate-limited per sweep so a mass wipe drains
	// progressively instead of stampeding the mount.
	if jd, err := utils.ConvertToJobDef(reviveSweepInterval); err != nil {
		m.logger.Error().Err(err).Msg("Failed to convert missing-download recovery interval to job definition")
	} else {
		if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
			m.reconcileMissingDownloads(ctx, missingDownloadSweepLimit, true)
		}), gocron.WithContext(ctx), gocron.WithName("missing-download-recovery")); err != nil {
			m.logger.Error().Err(err).Msg("Failed to create missing-download recovery job")
		} else {
			m.logger.Debug().Msgf("Missing-download recovery job scheduled for every %s", reviveSweepInterval)
		}
	}

	// Stall prune. Always scheduled; the sweep itself no-ops when
	// stall_prune_after is empty, which is what lets the knob be hot — an
	// operator who set it too aggressively can disable it without a restart,
	// and this pass deletes data so that matters.
	if jd, err := utils.ConvertToJobDef(stallPruneSweepInterval); err != nil {
		m.logger.Error().Err(err).Msg("Failed to convert stall prune interval to job definition")
	} else {
		if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
			settings := resolveStallPruneSettings(config.Get().StallPrune)
			if pruned := m.pruneStalledDownloads(ctx, settings); pruned > 0 {
				m.logger.Info().Int("pruned", pruned).Msg("Stall prune released provider slots held by stalled torrents")
			}
		}), gocron.WithContext(ctx), gocron.WithName("stall-prune")); err != nil {
			m.logger.Error().Err(err).Msg("Failed to create stall prune job")
		} else {
			m.logger.Debug().Msgf("Stall prune job scheduled for every %s", stallPruneSweepInterval)
		}
	}

	// NZB refresh job for pending archives (every 5 minutes)
	if m.usenet != nil {
		if jd, err := utils.ConvertToJobDef("10m"); err != nil {
			m.logger.Error().Err(err).Msg("Failed to convert NZB refresh interval to job definition")
		} else {
			if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
				if err := m.syncNZBs(ctx); err != nil {
					m.logger.Error().Err(err).Msg("Failed to refresh NZBs")
				}
			}), gocron.WithContext(ctx), gocron.WithName("nzb-refresh")); err != nil {
				m.logger.Error().Err(err).Msg("Failed to create NZB refresh job")
			} else {
				m.logger.Debug().Msg("NZB refresh job scheduled for every 5m")
			}
		}
	}
	return nil
}

func (m *Manager) StartWorker(ctx context.Context) error {
	// Stop any existing jobs before starting new ones
	m.scheduler.RemoveByTags("decypharr")

	m.logNetworkBindings()

	// Call the initial calls
	m.runInitialCalls(ctx)

	if err := m.addQueueProcessorJob(ctx); err != nil {
		return err
	}
	// Schedule per-debrid refresh jobs
	m.clients.Range(func(debridName string, debridClient debrid.Client) bool {
		if debridClient == nil {
			return true
		}

		debridConfig := debridClient.Config()

		// Schedule download link refresh job for this debrid
		if jd, err := utils.ConvertToJobDef(debridConfig.DownloadLinksRefreshInterval); err != nil {
			m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to convert download link refresh interval to job definition")
		} else {
			jobName := debridName + "-download-links"
			if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
				m.refreshDebridDownloadLinks(ctx, debridName, debridClient)
			}), gocron.WithContext(ctx), gocron.WithName(jobName)); err != nil {
				m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to create download link refresh job")
			} else {
				m.logger.Debug().Str("debrid", debridName).Msgf("Download link refresh job scheduled for every %s", debridConfig.DownloadLinksRefreshInterval)
			}
		}

		// Schedule torrent refresh job for this debrid
		if jd, err := utils.ConvertToJobDef(debridConfig.TorrentsRefreshInterval); err != nil {
			m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to convert torrent refresh interval to job definition")
		} else {
			jobName := debridName + "-torrents"
			if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
				if err := m.refreshTorrents(ctx, debridName, debridClient); err != nil {
					m.logger.Error().Err(err).Str("debrid", debridName).Msg("Torrent refresh failed")
				}
				m.RefreshEntries(true)
			}), gocron.WithContext(ctx), gocron.WithName(jobName)); err != nil {
				m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to create torrent refresh job")
			} else {
				m.logger.Debug().Str("debrid", debridName).Msgf("Torrent refresh job scheduled for every %s", debridConfig.TorrentsRefreshInterval)
			}
		}

		// Schedule account syncTorrents job for this debrid
		if jd, err := utils.ConvertToJobDef(config.DefaultAccountSyncInterval); err != nil {
			m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to convert account syncTorrents interval to job definition")
		} else {
			jobName := debridName + "-account-syncTorrents"
			if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
				debridClient.SyncAccounts()
			}), gocron.WithContext(ctx), gocron.WithName(jobName)); err != nil {
				m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to create account syncTorrents job")
			} else {
				m.logger.Debug().Str("debrid", debridName).Msgf("Account syncTorrents job scheduled for every %s", config.DefaultAccountSyncInterval)
			}
		}

		return true
	})

	// Schedule the reset invalid links job
	// This job will run every at 00:00 CET
	// and reset the invalid links in the cache
	if jd, err := utils.ConvertToJobDef("00:00"); err != nil {
		m.logger.Error().Err(err).Msg("Failed to convert link reset interval to job definition")
	} else {
		// Schedule the job
		if _, err := m.cetScheduler.NewJob(jd, gocron.NewTask(func() {
			// Reset link cache at midnight CET
			m.linkService.Clear()
			m.logger.Debug().Msg("Cleared link service cache")
		}), gocron.WithContext(ctx)); err != nil {
			m.logger.Error().Err(err).Msg("Failed to create link reset job")
		} else {
			m.logger.Debug().Msgf("Link reset job scheduled for every midnight, CET")
		}
	}

	// Arr monitoring job
	if jd, err := utils.ConvertToJobDef("10s"); err != nil {
		m.logger.Error().Err(err).Msg("Failed to convert arr monitoring interval to job definition")
	} else {
		// Schedule the job
		if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
			// Reset invalid download links map at midnight CET
			m.arr.Monitor()
		}), gocron.WithContext(ctx)); err != nil {
			m.logger.Error().Err(err).Msg("Failed to create arr monitoring job")
		} else {
			m.logger.Debug().Msgf("Arr monitoring job scheduled for every %s", "10s")
		}
	}

	// Register the health checker sweep with the scheduler if enabled.
	if m.repair != nil {
		if err := m.repair.Start(ctx); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to start repair service")
		}
	}

	// Start the scheduler
	m.scheduler.Start()
	m.cetScheduler.Start()
	return nil
}

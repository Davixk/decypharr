package link

import (
	"context"

	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// THE MINT'S INPUT IS OUR OWN CACHED STATE, AND IT GOES STALE SILENTLY.
//
// 🔴 Resolving a download link is not a lookup, it is a POST that hands the
// provider a link WE stored:
//
//	ad.doAccountRequest(account, unlockEndpoint, map[string]string{"link": file.Link})
//	r.doPostFormWithClient(..., "/unrestrict/link/", map[string]string{"link": link})
//
// When `file.Link` rotates on the provider's side, that POST 404s — and the
// content behind it is completely untouched. Measured on two specimens: both
// returned `Input/output error` through the mount at 90% and 99%, and both
// answered HTTP 206 at the same offsets when read directly from the provider
// using a freshly-unlocked link. 3.3% of a 170k-file library on one scan.
//
// NOTHING REFRESHED THAT INPUT. `download_links_refresh_interval` re-mints FROM
// `file.Link`, so it re-runs the failing call on a timer forever. And the hourly
// torrent refresh skips the entry entirely, because ProviderEntry.NeedsUpdate
// returns true only for a changed provider ID, a changed status, or zero files
// — never for a rotated link. A settled library entry is walked past every hour
// and never rewritten, so a stale link is permanent until a read forces it.
//
// ⚠️ AND THE SAME STALENESS COULD CONDEMN LIVE CONTENT. RealDebrid types a mint
// 404/410 as customerror.NewContentGoneError → code `debrid_content_gone` →
// IsContentPermanentlyGone, which that predicate's own doc describes as
// "destructive-eligible under PRUNE", and which WebDAV also uses to drop the
// file from directory listings. Audited against 7 days of production deletions:
// 2 of 157 pruned torrents were still `downloaded` on RealDebrid — content
// deleted on the strength of a link we failed to refresh.
//
// 🔑 THE DISCRIMINATOR IS WHETHER THE LINK ACTUALLY CHANGED.
//
// Re-asking the provider for the entry's current links answers "was our copy
// stale?" definitively and in one call:
//
//	the provider now reports a DIFFERENT link -> our state was stale. Rewrite it
//	                                             and retry the mint.
//	the provider reports the SAME link        -> our state was fine and the mint
//	                                             genuinely refuses. Whatever
//	                                             verdict the provider gave stands,
//	                                             now actually earned.
//
// That is why this runs BEFORE handleBadLink rather than inside it: a verdict
// derived from stale input must never reach the classifier that can act on it.
//
// Deliberately provider-agnostic. It keys on nothing but "the mint failed", so
// it fixes AllDebrid (whose 404 arrives untyped and falls straight through
// reinsertReason) and RealDebrid (whose 404 arrives as a content verdict)
// without either provider needing a new error taxonomy — and a provider added
// later inherits it. Costs one extra call only on a path that has already
// failed, and nothing at all when the link is current.

// retryWithRefreshedInput re-reads the entry's links from the provider after a
// failed mint and retries once if our stored link turns out to have been stale.
//
// The bool reports whether this path handled the failure. False means "not
// stale, or could not tell" and the caller must fall through to its ordinary
// error handling unchanged — the safe direction, and today's behaviour exactly.
func (s *Service) retryWithRefreshedInput(
	ctx context.Context,
	err error,
	entry *storage.Entry,
	filename string,
	attempt int,
) (types.DownloadLink, error, bool) {
	// 🛑 A LEGAL TAKEDOWN IS ABOUT THE RELEASE, NOT ABOUT OUR LINK. Re-reading
	// the provider cannot change a 451, and re-minting against one is the
	// 695-refusals-a-day loop handleTakedown exists to stop. Leave it alone.
	if customerror.IsContentTakedown(err) {
		return emptyDownloadLink, nil, false
	}
	// Bound the same way re-insertion is. A refresh only proceeds when the link
	// actually changed, so this cannot spin on its own — the bound is here for
	// the pathological case of a provider rotating links on every read.
	if attempt >= MaxReinsertionAttempt {
		return emptyDownloadLink, nil, false
	}
	if ctx.Err() != nil {
		return emptyDownloadLink, nil, false
	}

	changed, refreshErr := s.refreshPlacementLink(entry, filename)
	if refreshErr != nil {
		// Could not ask. That is OUR failure and says nothing about the link,
		// so it must not turn into a different answer than we already had.
		s.logger.Debug().Err(refreshErr).
			Str("infohash", entry.InfoHash).
			Str("filename", filename).
			Msg("Could not re-read provider links after a failed link resolution; keeping the original error")
		return emptyDownloadLink, nil, false
	}
	if !changed {
		// The provider handed back the same link it already had. Our state was
		// never the problem, so the original verdict stands and is now earned.
		return emptyDownloadLink, nil, false
	}

	s.logger.Info().
		Str("infohash", entry.InfoHash).
		Str("name", entry.Name).
		Str("filename", filename).
		Msg("Stored provider link was stale; refreshed it from the provider and retrying the download link")

	dl, retryErr := s.fetchAndValidate(ctx, entry, filename, attempt+1)
	return dl, retryErr, true
}

// refreshPlacementLink re-reads the entry's placement from the provider and
// rewrites the stored link for filename if it differs. It reports whether
// anything actually changed.
//
// ⚠️ It rewrites ONLY the link and id for this file. The provider is
// authoritative about how to reach the bytes; it is not authoritative about the
// arr association, category, callback or action that live on our record, and a
// wholesale merge here would orphan the arr's queue row — the same merge
// direction restore reconciliation settled on.
func (s *Service) refreshPlacementLink(entry *storage.Entry, filename string) (bool, error) {
	placement := entry.Providers[entry.ActiveProvider]
	if placement == nil || placement.ID == "" {
		return false, nil
	}
	placementFile := placement.Files[filename]
	if placementFile == nil {
		return false, nil
	}

	client, err := s.getClient(entry.ActiveProvider)
	if err != nil {
		return false, err
	}
	remote, err := client.GetTorrent(placement.ID)
	if err != nil {
		return false, err
	}
	if remote == nil {
		return false, nil
	}

	remoteFile, ok := remote.Files[filename]
	if !ok {
		// The provider no longer lists this file under that name. That may be a
		// genuine change, but it is NOT something to act on from here — the
		// caller's original error is still the better-founded answer.
		return false, nil
	}
	if remoteFile.Link == "" || remoteFile.Link == placementFile.Link {
		return false, nil
	}

	placementFile.Link = remoteFile.Link
	if remoteFile.Id != "" {
		placementFile.Id = remoteFile.Id
	}
	if s.entrySaver != nil {
		if err := s.entrySaver(entry); err != nil {
			// The in-memory entry now carries the fresh link, so the retry will
			// still work; only the persistence failed. Say so rather than
			// failing the read over it.
			s.logger.Warn().Err(err).
				Str("infohash", entry.InfoHash).
				Str("filename", filename).
				Msg("Refreshed a stale provider link but failed to persist it; the next read will refresh again")
		}
	}
	return true, nil
}

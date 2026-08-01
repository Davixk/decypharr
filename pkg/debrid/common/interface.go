package common

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

type Client interface {
	SubmitMagnet(tr *types.Torrent) (*types.Torrent, error)
	CheckStatus(tr *types.Torrent) (*types.Torrent, error)
	GetDownloadLink(torrentID string, file *types.File) (types.DownloadLink, error)
	DeleteTorrent(torrentId string) error
	// IsAvailable reports which infohashes a provider already holds cached.
	//
	// ⚠️ DELIBERATELY UNWIRED. It has no production callers, and that is the
	// CORRECT state — do not "reconnect the dead code". Verified against the
	// live APIs on 2026-08-01:
	//
	//	RealDebrid  GET /torrents/instantAvailability/{hash}
	//	            -> HTTP 403 {"error":"disabled_endpoint","error_code":37}
	//	            on every form, including known-cached hashes. Real-Debrid
	//	            switched it off; there is no documented replacement and no
	//	            supported way to test cache state without adding first.
	//	AllDebrid   never supported it. Returns an empty map by design.
	//	            Every hash therefore reads as NOT cached.
	//
	// So a caller that trusts this gets a confident wrong answer on both
	// providers, which is worse than not asking: an add-time gate built on it
	// would refuse or admit on fabricated cache state. That is why the
	// availability gate was removed in 9bc90d9 rather than repaired.
	//
	// Kept on the interface because Torbox/DebridLink/Premiumize may still
	// answer meaningfully, and because deleting it would erase the record of
	// why it must not be used for RD or AD.
	IsAvailable(infohashes []string) map[string]bool
	UpdateTorrent(torrent *types.Torrent) error
	GetTorrent(torrentId string) (*types.Torrent, error)
	GetTorrents() ([]*types.Torrent, error)
	Config() config.Debrid
	Logger() zerolog.Logger
	RefreshDownloadLinks() error
	CheckFile(ctx context.Context, infohash, fileID string) error // fileID here can link, file id(in the case of torbox), etc.
	AccountManager() *account.Manager                             // Returns the active download account/token
	GetProfile() (*types.Profile, error)
	GetAvailableSlots() (int, error)
	SyncAccounts() // Updates each accounts details(like traffic, username, etc.)
	DeleteLink(dl types.DownloadLink) error
	SpeedTest(ctx context.Context) types.SpeedTestResult
	SupportsCheck() bool
}

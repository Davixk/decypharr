package torbox

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/version"
	"go.uber.org/ratelimit"
)

var planSlots = map[string]int{
	"essential": 3,
	"standard":  5,
	"pro":       10,
}

// downloadPresentTTL bounds how stale the download_present snapshot may be
// before a cache MISS is allowed to mean anything. A miss is only ever reported
// as definitively dead against a snapshot no older than this. One full
// /api/torrents/mylist walk per TTL is shared by every probe in a sweep, which
// keeps the refresh well clear of TorBox's 300 req/min cap.
const downloadPresentTTL = 5 * time.Minute

// downloadPresentSnapshot is an immutable point-in-time view of which torrent
// IDs the account holds and whether their download is present. It is replaced
// wholesale on every reload — never merged — so a torrent removed from the
// account actually disappears instead of lingering as a stale "present" entry.
type downloadPresentSnapshot struct {
	present  map[string]bool
	loadedAt time.Time
}

func (s *downloadPresentSnapshot) fresh() bool {
	return s != nil && time.Since(s.loadedAt) < downloadPresentTTL
}

type Torbox struct {
	Host                  string `json:"host"`
	APIKey                string
	accountsManager       *account.Manager
	autoExpiresLinksAfter time.Duration
	client                *request.Client
	logger                zerolog.Logger
	Profile               *types.Profile
	config                config.Debrid
	downloadPresent       atomic.Pointer[downloadPresentSnapshot]
	downloadPresentMu     sync.Mutex
}

func New(dc config.Debrid, ratelimits map[string]ratelimit.Limiter) (*Torbox, error) {
	cfg := config.Get()
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", dc.APIKey),
	}
	if dc.UserAgent != "" {
		headers["User-Agent"] = dc.UserAgent
	} else {
		headers["User-Agent"] = fmt.Sprintf("Decypharr/%s (%s; %s)", version.GetInfo(), runtime.GOOS, runtime.GOARCH)
	}
	_log := logger.New(dc.Name)

	// TorBox enforces a hard cap of 300 req/min per API key, applied
	// synchronously across all servers since v8.4 (Feb 2026, GAP-002).
	// Default to that limit if the user has not configured one explicitly.
	mainRL := ratelimits["main"]
	if mainRL == nil {
		mainRL = ratelimit.New(300, ratelimit.Per(time.Minute), ratelimit.WithSlack(30))
	}

	opts := []request.ClientOption{
		request.WithHeaders(headers),
		request.WithRateLimiter(mainRL),
		request.WithMaxRetries(cfg.Retries),
		request.WithRetryableStatus(http.StatusTooManyRequests, http.StatusBadGateway),
		request.WithLogger(_log),
	}
	if dc.Proxy != "" {
		opts = append(opts, request.WithProxy(dc.Proxy))
	}

	autoExpiresLinksAfter, err := utils.ParseDuration(dc.AutoExpireLinksAfter)
	if autoExpiresLinksAfter == 0 || err != nil {
		autoExpiresLinksAfter = 48 * time.Hour
	}

	tb := &Torbox{
		Host:                  "https://api.torbox.app/v1",
		APIKey:                dc.APIKey,
		accountsManager:       account.NewManager(dc, ratelimits["download"], _log),
		config:                dc,
		autoExpiresLinksAfter: autoExpiresLinksAfter,
		client:                request.New(opts...),
		logger:                _log,
	}
	return tb, nil
}

func (tb *Torbox) Config() config.Debrid {
	return tb.config
}

func (tb *Torbox) Logger() zerolog.Logger {
	return tb.logger
}

// doGet performs a GET request and unmarshals the response
func (tb *Torbox) doGet(endpoint string, queryParams map[string]string, result any) (*http.Response, error) {
	u, err := url.Parse(tb.Host + endpoint)
	if err != nil {
		return nil, err
	}

	if queryParams != nil {
		q := u.Query()
		for k, v := range queryParams {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()
	}

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := tb.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(resp.Body).Decode(result); err != nil {
			return resp, err
		}
	}

	return resp, nil
}

// doPostForm performs a POST request with form data
func (tb *Torbox) doPostForm(endpoint string, formData map[string]string, result any) (*http.Response, error) {
	form := url.Values{}
	for k, v := range formData {
		form.Set(k, v)
	}

	req, err := http.NewRequest(http.MethodPost, tb.Host+endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := tb.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(resp.Body).Decode(result); err != nil {
			return resp, err
		}
	}

	return resp, nil
}

// doDelete performs a DELETE request
func (tb *Torbox) doDelete(endpoint string, payload any) (*http.Response, error) {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequest(http.MethodDelete, tb.Host+endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := tb.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

func (tb *Torbox) IsAvailable(hashes []string) map[string]bool {
	result := make(map[string]bool)

	for i := 0; i < len(hashes); i += 100 {
		end := min(i+100, len(hashes))

		validHashes := make([]string, 0, end-i)
		for _, hash := range hashes[i:end] {
			if hash != "" {
				validHashes = append(validHashes, hash)
			}
		}

		if len(validHashes) == 0 {
			continue
		}

		hashStr := strings.Join(validHashes, ",")
		var res AvailableResponse

		resp, err := tb.doGet("/api/torrents/checkcached", map[string]string{"hash": hashStr}, &res)
		if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
			continue
		}
		if res.Data == nil {
			return result
		}

		for h, c := range *res.Data {
			if c.Size > 0 {
				result[strings.ToUpper(h)] = true
			}
		}
	}
	return result
}

func (tb *Torbox) SubmitMagnet(torrent *types.Torrent) (*types.Torrent, error) {
	var data AddMagnetResponse

	formData := map[string]string{
		"magnet": torrent.Magnet.Link,
	}
	if !torrent.DownloadUncached {
		formData["add_only_if_cached"] = "true"
	}

	resp, err := tb.doPostForm("/api/torrents/createtorrent", formData, &data)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}
	if data.Data == nil {
		return nil, fmt.Errorf("error adding torrent")
	}
	dt := *data.Data
	torrentId := strconv.Itoa(dt.Id)
	torrent.Id = torrentId
	torrent.Debrid = tb.config.Name
	torrent.Added = time.Now()

	return torrent, nil
}

func (tb *Torbox) getTorboxStatus(status string, finished bool) types.TorrentStatus {
	if finished {
		return types.TorrentStatusDownloaded
	}
	downloading := []string{"paused", "downloading",
		"checkingResumeData", "metaDL", "pausedUP", "queuedUP", "checkingUP",
		"forcedUP", "allocating", "downloading", "metaDL", "pausedDL",
		"queuedDL", "checkingDL", "forcedDL", "checkingResumeData", "moving",
		"incomplete",
	}

	downloaded := []string{
		"completed", "cached", "uploading", "downloaded",
	}

	status = regexp.MustCompile(`\s*\(.*?\)\s*`).ReplaceAllString(status, "")

	switch {
	case utils.Contains(downloading, status):
		return types.TorrentStatusDownloading
	case utils.Contains(downloaded, status):
		return types.TorrentStatusDownloaded
	default:
		return types.TorrentStatusError
	}
}

func (tb *Torbox) GetTorrent(torrentId string) (*types.Torrent, error) {
	var res InfoResponse

	resp, err := tb.doGet("/api/torrents/mylist", map[string]string{"id": torrentId}, &res)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}
	data := res.Data
	if data == nil {
		return nil, fmt.Errorf("error getting torrent")
	}
	t := &types.Torrent{
		Id:               strconv.Itoa(data.Id),
		Name:             data.Name,
		Bytes:            data.Size,
		Progress:         data.Progress * 100,
		Status:           tb.getTorboxStatus(data.DownloadState, data.DownloadFinished),
		Speed:            data.DownloadSpeed,
		Seeders:          data.Seeds,
		Filename:         data.Name,
		OriginalFilename: data.Name,
		Debrid:           tb.config.Name,
		Files:            make(map[string]types.File),
		Added:            data.CreatedAt,
	}
	cfg := config.Get()

	for _, f := range data.Files {
		fileName := filepath.Base(f.Name)
		if err := cfg.IsFileAllowed(f.AbsolutePath, f.Size); err != nil {
			continue
		}

		file := types.File{
			TorrentId: t.Id,
			Id:        strconv.Itoa(f.Id),
			Name:      fileName,
			Size:      f.Size,
			Path:      f.Name,
		}

		if data.DownloadFinished {
			file.Link = fmt.Sprintf("torbox://%s/%d", t.Id, f.Id)
		}

		t.Files[fileName] = file
	}
	var cleanPath string
	if len(t.Files) > 0 {
		cleanPath = path.Clean(data.Files[0].Name)
	} else {
		cleanPath = path.Clean(data.Name)
	}

	t.OriginalFilename = strings.Split(cleanPath, "/")[0]
	t.Debrid = tb.config.Name

	return t, nil
}

// loadDownloadPresent walks the account's torrent list and installs the result
// as a new snapshot. On failure the previous snapshot is left in place — a
// refresh that could not complete must not destroy what we already knew.
// Callers must hold downloadPresentMu.
func (tb *Torbox) loadDownloadPresent() error {
	offset := 0
	present := make(map[string]bool)
	for {
		var res TorrentsListResponse
		resp, err := tb.doGet("/api/torrents/mylist", map[string]string{"offset": fmt.Sprintf("%d", offset)}, &res)
		if err != nil {
			return err
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
		}
		if res.Data == nil || len(*res.Data) == 0 {
			break
		}
		for _, t := range *res.Data {
			present[strconv.Itoa(t.Id)] = t.DownloadPresent
		}
		offset += len(*res.Data)
	}
	tb.downloadPresent.Store(&downloadPresentSnapshot{present: present, loadedAt: time.Now()})
	tb.logger.Info().Int("count", len(present)).Msg("loaded download_present cache for repair")
	return nil
}

// snapshot returns the current snapshot, building one if none exists yet. It
// deliberately does NOT refresh a stale snapshot: staleness only matters on a
// miss, and forcing a reload on every hit would walk the whole torrent list far
// too often.
func (tb *Torbox) snapshot() (*downloadPresentSnapshot, error) {
	if snap := tb.downloadPresent.Load(); snap != nil {
		return snap, nil
	}
	return tb.refreshSnapshot()
}

// refreshSnapshot reloads the snapshot unless a concurrent probe already
// produced a fresh one. Serialized on downloadPresentMu with a re-check inside,
// so a sweep in which many files miss pays for ONE list walk per TTL rather
// than one per file.
func (tb *Torbox) refreshSnapshot() (*downloadPresentSnapshot, error) {
	tb.downloadPresentMu.Lock()
	defer tb.downloadPresentMu.Unlock()

	if current := tb.downloadPresent.Load(); current.fresh() {
		return current, nil
	}
	if err := tb.loadDownloadPresent(); err != nil {
		return nil, err
	}
	return tb.downloadPresent.Load(), nil
}

func (tb *Torbox) UpdateTorrent(t *types.Torrent) error {
	var res InfoResponse

	resp, err := tb.doGet("/api/torrents/mylist", map[string]string{"id": t.Id}, &res)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}
	data := res.Data
	name := data.Name

	t.Name = name
	t.Bytes = data.Size
	t.Progress = data.Progress * 100
	t.Status = tb.getTorboxStatus(data.DownloadState, data.DownloadFinished)
	t.Speed = data.DownloadSpeed
	t.Seeders = data.Seeds
	t.Filename = name
	t.OriginalFilename = name
	if data.Hash != "" {
		t.InfoHash = data.Hash
	}
	t.Debrid = tb.config.Name

	t.Files = make(map[string]types.File)

	cfg := config.Get()

	for _, f := range data.Files {
		fileName := filepath.Base(f.Name)

		if err := cfg.IsFileAllowed(f.AbsolutePath, f.Size); err != nil {
			continue
		}

		file := types.File{
			TorrentId: t.Id,
			Id:        strconv.Itoa(f.Id),
			Name:      fileName,
			Size:      f.Size,
			Path:      fileName,
		}

		if data.DownloadFinished {
			file.Link = fmt.Sprintf("torbox://%s/%s", t.Id, strconv.Itoa(f.Id))
		}

		t.Files[fileName] = file
	}

	var cleanPath string
	if len(t.Files) > 0 {
		cleanPath = path.Clean(data.Files[0].Name)
	} else {
		cleanPath = path.Clean(data.Name)
	}

	t.OriginalFilename = strings.Split(cleanPath, "/")[0]
	t.Debrid = tb.config.Name
	return nil
}

func (tb *Torbox) CheckStatus(torrent *types.Torrent) (*types.Torrent, error) {
	for {
		err := tb.UpdateTorrent(torrent)

		if err != nil || torrent == nil {
			return torrent, err
		}

		switch torrent.Status {
		case types.TorrentStatusDownloaded:
			tb.logger.Info().Msgf("Torrent: %s downloaded", torrent.Name)
			return torrent, nil
		case types.TorrentStatusDownloading:
			if !torrent.DownloadUncached {
				return torrent, fmt.Errorf("torrent: %s not cached", torrent.Name)
			}
			return torrent, nil
		default:
			return torrent, fmt.Errorf("torrent: %s has error", torrent.Name)
		}
	}
}

func (tb *Torbox) DeleteTorrent(torrentId string) error {
	payload := map[string]string{"torrent_id": torrentId, "action": "Delete"}

	resp, err := tb.doDelete(fmt.Sprintf("/api/torrents/controltorrent/%s", torrentId), payload)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	tb.logger.Info().Msgf("Torrent %s deleted from Torbox", torrentId)
	return nil
}

func (tb *Torbox) GetDownloadLink(id string, file *types.File) (types.DownloadLink, error) {
	return tb.accountsManager.GetDownloadLink(id, file, tb.fetchDownloadLink)
}

func (tb *Torbox) fetchDownloadLink(account *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	query := url.Values{}
	query.Set("token", account.Token)
	query.Set("torrent_id", id)
	query.Set("file_id", file.Id)
	query.Set("redirect", "true")

	downloadURL := fmt.Sprintf("%s/api/torrents/requestdl?%s", tb.Host, query.Encode())

	now := time.Now()

	// Always expires
	dl := types.DownloadLink{
		Filename:     file.Name,
		Size:         file.Size,
		Token:        tb.APIKey,
		Link:         file.Link,
		DownloadLink: downloadURL,
		Debrid:       tb.config.Name,
		Id:           file.Id,
		Generated:    now,
		ExpiresAt:    now.Add(tb.autoExpiresLinksAfter),
	}
	return dl, nil
}

// GetAllTorrents delegates, and its completeness is UNVERIFIED for Torbox.
// Callers must only act on positive findings, never on absence.
func (tb *Torbox) GetAllTorrents() ([]*types.Torrent, error) {
	return tb.GetTorrents()
}

func (tb *Torbox) GetTorrents() ([]*types.Torrent, error) {
	offset := 0
	allTorrents := make([]*types.Torrent, 0)

	for {
		torrents, err := tb.getTorrents(offset)
		if err != nil {
			break
		}
		if len(torrents) == 0 {
			break
		}
		allTorrents = append(allTorrents, torrents...)
		offset += len(torrents)
	}
	return allTorrents, nil
}

func (tb *Torbox) getTorrents(offset int) ([]*types.Torrent, error) {
	var res TorrentsListResponse

	resp, err := tb.doGet("/api/torrents/mylist", map[string]string{"offset": fmt.Sprintf("%d", offset)}, &res)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	if !res.Success || res.Data == nil {
		return nil, fmt.Errorf("torbox API error: %v", res.Error)
	}

	torrents := make([]*types.Torrent, 0, len(*res.Data))
	cfg := config.Get()

	for _, data := range *res.Data {
		t := &types.Torrent{
			Id:               strconv.Itoa(data.Id),
			Name:             data.Name,
			Bytes:            data.Size,
			Progress:         data.Progress * 100,
			Status:           tb.getTorboxStatus(data.DownloadState, data.DownloadFinished),
			Speed:            data.DownloadSpeed,
			Seeders:          data.Seeds,
			Filename:         data.Name,
			OriginalFilename: data.Name,
			Debrid:           tb.config.Name,
			Files:            make(map[string]types.File),
			Added:            data.CreatedAt,
			InfoHash:         data.Hash,
		}

		for _, f := range data.Files {
			fileName := filepath.Base(f.Name)
			if err := cfg.IsFileAllowed(f.AbsolutePath, f.Size); err != nil {
				continue
			}
			file := types.File{
				TorrentId: t.Id,
				Id:        strconv.Itoa(f.Id),
				Name:      fileName,
				Size:      f.Size,
				Path:      f.Name,
			}

			if data.DownloadFinished {
				file.Link = fmt.Sprintf("torbox://%s/%d", t.Id, f.Id)
			}

			t.Files[fileName] = file
		}

		var cleanPath string
		if len(t.Files) > 0 {
			cleanPath = path.Clean(data.Files[0].Name)
		} else {
			cleanPath = path.Clean(data.Name)
		}
		t.OriginalFilename = strings.Split(cleanPath, "/")[0]

		torrents = append(torrents, t)
	}

	return torrents, nil
}

func (tb *Torbox) fetchDownloadLinks(account *account.Account) ([]types.DownloadLink, error) {
	return []types.DownloadLink{}, nil
}

func (tb *Torbox) RefreshDownloadLinks() error {
	return tb.accountsManager.RefreshLinks(tb.fetchDownloadLinks)
}

// CheckFile answers from the download_present snapshot of the account's torrent
// list with a three-way verdict: nil (available),
// customerror.HosterUnavailableError (definitively gone), or
// types.ErrAvailabilityIndeterminate (unknown).
//
// If the snapshot itself cannot be built — Torbox unreachable, rate limited,
// credentials rejected — we know nothing about the file, so the probe must
// report indeterminate rather than an error the caller could confuse with a
// verdict. Only a loaded snapshot can produce a definitive answer.
//
// A MISS is the dangerous case. The snapshot was previously loaded once and
// never invalidated, so a torrent added after that load was absent from it and
// was reported definitively dead — a false "dead" on perfectly healthy content,
// which is exactly the verdict that authorises destructive repair. A miss is
// therefore confirmed against a snapshot refreshed within downloadPresentTTL
// before any dead verdict, and if that refresh cannot be completed the answer
// is indeterminate. An absence we cannot currently verify is never death.
func (tb *Torbox) CheckFile(ctx context.Context, infohash, link string) error {
	snap, err := tb.snapshot()
	if err != nil {
		return fmt.Errorf("%w: torbox check: loading download_present snapshot: %w", types.ErrAvailabilityIndeterminate, err)
	}

	torrentID := link
	if after, ok := strings.CutPrefix(link, "torbox://"); ok {
		parts := strings.SplitN(after, "/", 2)
		if len(parts) > 0 {
			torrentID = parts[0]
		}
	}

	if present, ok := snap.present[torrentID]; ok {
		if !present {
			return customerror.HosterUnavailableError
		}
		return nil
	}

	// Missing from a snapshot that may predate the torrent. Re-confirm.
	if !snap.fresh() {
		refreshed, err := tb.refreshSnapshot()
		if err != nil {
			return fmt.Errorf("%w: torbox check: torrent %s absent from a stale snapshot that could not be refreshed: %w",
				types.ErrAvailabilityIndeterminate, torrentID, err)
		}
		snap = refreshed
		if present, ok := snap.present[torrentID]; ok {
			if !present {
				return customerror.HosterUnavailableError
			}
			return nil
		}
	}

	// Absent from a snapshot known to be fresh: genuinely not on the account.
	return customerror.HosterUnavailableError
}

func (tb *Torbox) GetAvailableSlots() (int, error) {
	var accountSlots = 1
	profile, err := tb.GetProfile()
	if err != nil {
		return 0, err
	}

	if slots, ok := planSlots[profile.Type]; ok {
		accountSlots = slots
	}
	return accountSlots, nil
}

func (tb *Torbox) GetProfile() (*types.Profile, error) {
	if tb.Profile != nil {
		return tb.Profile, nil
	}
	var data ProfileResponse

	resp, err := tb.doGet("/api/user/me", map[string]string{"settings": "true"}, &data)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	userData := data.Data
	if userData == nil {
		return nil, fmt.Errorf("error getting user profile")
	}

	expiration, err := time.Parse(time.RFC3339, userData.PremiumExpiresAt)
	if err != nil {
		expiration = time.Time{}
	}

	profile := &types.Profile{
		Name:       tb.config.Name,
		Id:         userData.Id,
		Username:   userData.Email,
		Email:      userData.Email,
		Expiration: expiration,
	}

	switch userData.Plan {
	case 1:
		profile.Type = "essential"
	case 2:
		profile.Type = "pro"
	case 3:
		profile.Type = "standard"
	default:
		profile.Type = "free"
	}

	tb.Profile = profile

	return profile, nil
}

func (tb *Torbox) AccountManager() *account.Manager {
	return tb.accountsManager
}

func (tb *Torbox) syncAccount(account *account.Account) error {
	return nil
}

func (tb *Torbox) SyncAccounts() {
	tb.accountsManager.Sync(tb.syncAccount)
}

func (tb *Torbox) deleteDownloadLink(account *account.Account, downloadLink types.DownloadLink) error {
	return nil
}

func (tb *Torbox) DeleteLink(downloadLink types.DownloadLink) error {
	return tb.accountsManager.DeleteDownloadLink(downloadLink, tb.deleteDownloadLink)
}

// SpeedTest measures API latency and download speed using cached links
func (tb *Torbox) SpeedTest(ctx context.Context) types.SpeedTestResult {
	result := types.SpeedTestResult{
		Provider: tb.config.Name,
		TestedAt: time.Now(),
	}

	start := time.Now()
	resp, err := tb.doGet("/api/user/me", nil, nil)
	latency := time.Since(start)

	if err != nil {
		result.Error = fmt.Sprintf("latency test failed: %v", err)
		return result
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Error = fmt.Sprintf("latency test unexpected status: %d", resp.StatusCode)
		return result
	}
	result.LatencyMs = latency.Milliseconds()

	// Try to measure download speed using a cached link
	current := tb.accountsManager.Current()
	if current == nil {
		return result
	}

	link, found := current.GetRandomLink()
	if !found || link.DownloadLink == "" {
		return result
	}

	// Download first 1MB to measure speed
	const downloadSize = 1 * 1024 * 1024 // 1MB
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link.DownloadLink, nil)
	if err != nil {
		return result
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", downloadSize-1))

	downloadStart := time.Now()
	dlResp, err := current.Client().Do(req)
	if err != nil {
		return result
	}
	defer dlResp.Body.Close()

	data, err := io.ReadAll(dlResp.Body)
	downloadDuration := time.Since(downloadStart)

	if err != nil || len(data) == 0 {
		return result
	}

	result.BytesRead = int64(len(data))
	if downloadDuration.Seconds() > 0 {
		result.SpeedMBps = float64(result.BytesRead) / downloadDuration.Seconds() / (1024 * 1024)
	}

	return result
}

func (tb *Torbox) SupportsCheck() bool {
	return true
}

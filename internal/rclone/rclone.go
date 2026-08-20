package rclone

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
)

type Client struct {
	client   *request.Client
	baseURL  string
	username string
	password string
	logger   zerolog.Logger
}

type Request struct {
	Command string         `json:"command"`
	Args    map[string]any `json:"args,omitempty"`
}

func NewClient(url, username, password string, logger zerolog.Logger) *Client {
	headers := map[string]string{}

	// Add basic auth header if credentials provided
	if username != "" && password != "" {
		auth := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
		headers["Authorization"] = "Basic " + auth
	}

	opts := []request.ClientOption{
		request.WithHeaders(headers),
		request.WithTimeout(60 * time.Second),
		request.WithMaxRetries(3),
	}

	return &Client{
		client:   request.New(opts...),
		baseURL:  url,
		username: username,
		password: password,
		logger:   logger,
	}
}

func (r *Client) Do(ctx context.Context, req Request, res any) error {
	body, err := json.Marshal(req.Args)
	if err != nil {
		return err
	}

	finalURL, err := utils.JoinURL(r.baseURL, req.Command)
	if err != nil {
		return err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, finalURL, bytes.NewBuffer(body))

	if err != nil {
		return err
	}

	httpReq.Header.Set("Content-Type", "application/json")

	response, err := r.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode >= 400 {
		respBody, _ := io.ReadAll(response.Body)
		return fmt.Errorf("rclone error: %s - %s", response.Status, string(respBody))
	}

	if res != nil {
		if err := json.ConfigDefault.NewDecoder(response.Body).Decode(res); err != nil && err != io.EOF {
			return err
		}
	}

	return nil
}

// Forget drops one cached node from rclone's VFS, by path.
//
// 🔴 WHY THE PARENT REFRESH IS NOT ENOUGH — measured, on a live 60,000-symlink
// library. Deleting an entry refreshes the configured group directories
// (default `__all__`), and `vfs/refresh` is NON-RECURSIVE: it re-reads the group
// listing and stops there. The per-entry subdirectory beneath it, and the file
// nodes inside it, are never revisited.
//
// So the group listing correctly loses the entry while the child node lives on,
// still holding attributes decypharr has already stopped backing:
//
//	stat on the full path -> OK, 3,577,947,542 bytes   (stale child node)
//	readdir of the parent -> empty                     (the group refresh worked)
//	read                  -> 0 bytes, no error         (the GET behind it 404s)
//
// Plex renders that as "Error opening input file" and the transcoder exits. The
// node did eventually expire — hours later, well past the mount's nominal 5m
// dir_cache_time — but "eventually" is not a mechanism, and while it lasts
// NOBODY can see the content is gone: decypharr has no entry left to reason
// about, and the arr's dangling-symlink reaper sees a perfectly healthy stat.
//
// ⚠️ AND THE OBVIOUS FIX IS THE WRONG ONE. `vfs/refresh` accepts recursive=true,
// which would walk every entry in the group on EVERY delete — thousands of them,
// precisely when prune waves make deletes arrive in batches. That trades a rare
// ghost for a predictable stampede. Forgetting the one path that actually
// changed costs a single call and does not scale with the library at all.
//
// Forget only, and no refresh: the parent refresh already re-reads the listing,
// and there is nothing to re-read at a path that no longer exists.
func (r *Client) Forget(ctx context.Context, path, fs string) error {
	if path == "" {
		return nil
	}
	args := map[string]any{"dir": path}
	if fs != "" {
		args["fs"] = fs
	}
	if err := r.Do(ctx, Request{Command: "vfs/forget", Args: args}, nil); err != nil {
		return fmt.Errorf("failed to forget path %s for fs %s: %w", path, fs, err)
	}
	return nil
}

func (r *Client) Refresh(ctx context.Context, dirs []string, fs string) error {
	args := map[string]any{}
	if fs != "" {
		args["fs"] = fs
	}
	for i, dir := range dirs {
		if dir != "" {
			if i == 0 {
				args["dir"] = dir
			} else {
				args[fmt.Sprintf("dir%d", i+1)] = dir
			}
		}
	}
	req := Request{
		Command: "vfs/forget",
		Args:    args,
	}

	err := r.Do(ctx, req, nil)
	if err != nil {
		return fmt.Errorf("failed to refresh directory %s for fs %s: %w", dirs, fs, err)
	}
	req = Request{
		Command: "vfs/refresh",
		Args:    args,
	}
	err = r.Do(ctx, req, nil)
	if err != nil {
		return fmt.Errorf("failed to refresh directory %s for fs %s: %w", dirs, fs, err)
	}
	return nil
}

func (r *Client) CheckMountHealth(ctx context.Context, fs string) error {
	req := Request{
		Command: "operations/list",
		Args: map[string]any{
			"fs":     fs,
			"remote": "",
		},
	}
	err := r.Do(ctx, req, nil)
	return err
}

func (r *Client) Ping(ctx context.Context) error {
	req := Request{Command: "core/version"}
	err := r.Do(ctx, req, nil)
	return err
}

func (r *Client) Unmount(ctx context.Context, mountPoint string) error {
	req := Request{
		Command: "mount/unmount",
		Args: map[string]any{
			"mountPoint": mountPoint,
		},
	}
	err := r.Do(ctx, req, nil)
	if err != nil {
		return fmt.Errorf("failed to unmount %s via RC: %w", mountPoint, err)
	}
	return nil
}

func (r *Client) Mount(ctx context.Context, mountArgs map[string]any) error {
	req := Request{
		Command: "mount/mount",
		Args:    mountArgs,
	}
	err := r.Do(ctx, req, nil)
	if err != nil {
		return fmt.Errorf("failed to mount via RC: %w", err)
	}
	return nil
}

func (r *Client) CreateConfig(ctx context.Context, args map[string]any) error {
	req := Request{
		Command: "config/create",
		Args:    args,
	}
	err := r.Do(ctx, req, nil)
	if err != nil {
		return fmt.Errorf("failed to create config: %w", err)
	}
	return nil
}

func (r *Client) GetCoreStats(ctx context.Context) (*CoreStatsResponse, error) {
	req := Request{
		Command: "core/stats",
	}
	var coreStats CoreStatsResponse
	err := r.Do(ctx, req, &coreStats)
	if err != nil {
		return nil, fmt.Errorf("failed to get core stats: %w", err)
	}
	return &coreStats, nil
}

// GetMemoryUsage returns memory usage statistics
func (r *Client) GetMemoryUsage(ctx context.Context) (*MemoryStats, error) {
	req := Request{
		Command: "core/memstats",
	}

	var memStats MemoryStats
	err := r.Do(ctx, req, &memStats)
	if err != nil {
		return nil, fmt.Errorf("failed to get memory stats: %w", err)
	}
	return &memStats, nil
}

// GetBandwidthStats returns bandwidth usage for all transfers
func (r *Client) GetBandwidthStats(ctx context.Context) (*BandwidthStats, error) {
	req := Request{
		Command: "core/bwlimit",
	}

	var bwStats BandwidthStats
	err := r.Do(ctx, req, &bwStats)
	if err != nil {
		return nil, fmt.Errorf("failed to get bandwidth stats: %w", err)
	}
	return &bwStats, nil
}

// GetVersion returns rclone version information
func (r *Client) GetVersion(ctx context.Context) (*VersionResponse, error) {
	req := Request{
		Command: "core/version",
	}
	var versionResp VersionResponse

	err := r.Do(ctx, req, &versionResp)
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}
	return &versionResp, nil
}

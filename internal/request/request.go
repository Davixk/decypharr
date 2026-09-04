package request

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/netbind"
	"go.uber.org/ratelimit"
	"golang.org/x/net/proxy"
)

var (
	once     sync.Once
	instance *Client
)

type ClientOption func(*Client)

// Client represents an HTTP client with additional capabilities
type Client struct {
	client          *retryablehttp.Client
	httpClient      *http.Client // underlying http client
	rateLimiter     ratelimit.Limiter
	headers         map[string]string
	headersMu       sync.RWMutex
	maxRetries      int
	timeout         time.Duration
	skipTLSVerify   bool
	retryableStatus map[int]struct{}
	logger          zerolog.Logger
	proxy           string
}

// WithMaxRetries sets the maximum number of retry attempts
func WithMaxRetries(maxRetries int) ClientOption {
	return func(c *Client) {
		c.maxRetries = maxRetries
	}
}

// WithTimeout sets the request timeout
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// WithRateLimiter sets a rate limiter
func WithRateLimiter(rl ratelimit.Limiter) ClientOption {
	return func(c *Client) {
		c.rateLimiter = rl
	}
}

// WithHeaders sets default headers
func WithHeaders(headers map[string]string) ClientOption {
	return func(c *Client) {
		c.headersMu.Lock()
		c.headers = headers
		c.headersMu.Unlock()
	}
}

func (c *Client) SetHeader(key, value string) {
	c.headersMu.Lock()
	c.headers[key] = value
	c.headersMu.Unlock()
}

func WithLogger(logger zerolog.Logger) ClientOption {
	return func(c *Client) {
		c.logger = logger
	}
}

func WithTransport(transport *http.Transport) ClientOption {
	return func(c *Client) {
		c.httpClient.Transport = transport
	}
}

// WithRetryableStatus adds status codes that should trigger a retry
func WithRetryableStatus(statusCodes ...int) ClientOption {
	return func(c *Client) {
		c.retryableStatus = make(map[int]struct{}) // reset the map
		for _, code := range statusCodes {
			c.retryableStatus[code] = struct{}{}
		}
	}
}

func WithProxy(proxyURL string) ClientOption {
	return func(c *Client) {
		c.proxy = proxyURL
	}
}

// Do performs an HTTP request with retries for certain status codes
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	// Apply headers
	c.headersMu.RLock()
	if c.headers != nil {
		for key, value := range c.headers {
			req.Header.Set(key, value)
		}
	}
	c.headersMu.RUnlock()

	// Apply rate limiting.
	//
	// 🔴 Take() HAS NO CONTEXT AND CANNOT BE CANCELLED, which is why this is not
	// simply a call. The old form checked the context once and then blocked
	// unconditionally, so a caller whose request had already been abandoned —
	// an *arr that hung up, a shutdown in progress — still waited its full turn
	// in a queue it no longer had any reason to be in.
	//
	// That is harmless while demand sits below the limiter's rate and fatal
	// above it: the queue grows without bound, every waiter blocks forever, and
	// because the qBittorrent add path is synchronous the *arr sees a hung
	// connection rather than an answer. Observed in production on fork.77, with
	// the process at 0.28% CPU — nothing was working, everything was waiting.
	//
	// Waiting in a goroutine lets the caller leave. The goroutine still finishes
	// when its turn arrives, so the limiter's pacing is unchanged and no token
	// is skipped; what changes is that an abandoned request stops occupying the
	// caller.
	if c.rateLimiter != nil {
		ctx := req.Context()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ready := make(chan struct{})
		go func() {
			c.rateLimiter.Take()
			close(ready)
		}()
		select {
		case <-ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Convert to retryablehttp request
	retryReq, err := retryablehttp.FromRequest(req)
	if err != nil {
		return nil, fmt.Errorf("creating retryable request: %w", err)
	}

	return c.client.Do(retryReq)
}

// MakeRequest performs an HTTP request and returns the response body as bytes
func (c *Client) MakeRequest(req *http.Request) ([]byte, error) {
	res, err := c.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := res.Body.Close(); err != nil {
			c.logger.Printf("Failed to close response body: %v", err)
		}
	}()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP error %d: %s", res.StatusCode, string(bodyBytes))
	}

	return bodyBytes, nil
}

func (c *Client) Get(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating GET request: %w", err)
	}

	return c.Do(req)
}

// zerologAdapter bridges zerolog to the retryablehttp.Logger interface so that
// retry events (including 429 backoffs) appear in decypharr's structured log.
type zerologAdapter struct{ log zerolog.Logger }

func (z zerologAdapter) Printf(format string, args ...interface{}) {
	z.log.Debug().Msgf(format, args...)
}

// retryAfterBackoff extends DefaultBackoff with Retry-After header support.
// When a 429 response carries a Retry-After header decypharr waits exactly as
// long as the server requests instead of using jittered exponential backoff.
func retryAfterBackoff(min, max time.Duration, attemptNum int, resp *http.Response) time.Duration {
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if secs, err := strconv.Atoi(ra); err == nil && secs > 0 {
				wait := time.Duration(secs) * time.Second
				if wait > max {
					return max
				}
				return wait
			}
			if t, err := http.ParseTime(ra); err == nil {
				if wait := time.Until(t); wait > 0 {
					if wait > max {
						return max
					}
					return wait
				}
			}
		}
	}
	return retryablehttp.DefaultBackoff(min, max, attemptNum, resp)
}

// classSpecs adapts the config's string-keyed bindings to netbind's Class keys.
// The indirection keeps netbind free of a config dependency, so it stays
// testable without a loaded configuration.
func classSpecs(raw map[string]string) map[netbind.Class]string {
	out := make(map[netbind.Class]string, len(raw))
	for name, spec := range raw {
		out[netbind.Class(name)] = spec
	}
	return out
}

// New creates a new HTTP client with the specified options
func New(options ...ClientOption) *Client {
	client := &Client{
		maxRetries:    5,
		skipTLSVerify: true,
		retryableStatus: map[int]struct{}{
			http.StatusTooManyRequests:     {},
			http.StatusInternalServerError: {},
			http.StatusBadGateway:          {},
			http.StatusServiceUnavailable:  {},
			http.StatusGatewayTimeout:      {},
		},
		logger:  logger.New("request"),
		timeout: 60 * time.Second,
		proxy:   "",
		headers: make(map[string]string),
	}

	// Create default http client
	client.httpClient = &http.Client{
		Timeout: client.timeout,
	}

	// Apply options before configuring transport
	for _, option := range options {
		option(client)
	}

	client.httpClient.Timeout = client.timeout

	// Check if transport was set by WithTransport option
	if client.httpClient.Transport == nil {
		// Every HTTP client in decypharr is built here, so binding the dialer
		// at this one point covers all of them. The class is `default`; a
		// dial resolves the binding fresh each time, so a reconnected tunnel is
		// picked up and a vanished one FAILS the dial rather than silently
		// leaving on the ordinary route.
		binder := netbind.New(classSpecs(config.Get().NetworkBinding.Bindings()))
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: client.skipTLSVerify,
			},
			DialContext: binder.DialContext(netbind.ClassDefault, 30*time.Second, 15*time.Second),
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       30 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
		}

		// Configure proxy if needed
		SetProxy(transport, client.proxy)

		// Set the transport to the client
		client.httpClient.Transport = transport
	}

	// Create retryablehttp client
	retryClient := retryablehttp.NewClient()
	retryClient.HTTPClient = client.httpClient
	retryClient.RetryMax = client.maxRetries
	retryClient.RetryWaitMin = 1 * time.Second
	retryClient.RetryWaitMax = 30 * time.Second
	retryClient.Logger = zerologAdapter{log: client.logger}
	retryClient.Backoff = retryAfterBackoff

	// Custom retry policy based on retryable status codes
	retryClient.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		// Don't retry on context errors
		if ctx.Err() != nil {
			return false, ctx.Err()
		}

		// 🔴 THE STATUS LIST IS AUTHORITATIVE. IT USED TO BE ADDITIVE, WHICH
		// MADE IT IMPOSSIBLE TO TURN A RETRY OFF.
		//
		// DefaultRetryPolicy was consulted FIRST and retries 429 and 5xx on its
		// own, so the configured list could only ever ADD codes. Removing 429
		// from a client's list — the obvious way to stop retrying a rate limit —
		// changed nothing at all, and did so silently.
		//
		// That mattered because retrying a 429 is actively harmful on
		// RealDebrid: its limit is global across every endpoint and refused
		// requests count toward it, so each retry spends the budget that decides
		// the next answer. Measured live, an add refusal comes back in ~0.15s
		// while decypharr took a median 205s to record the verdict — six
		// requests separated by a backoff capped at 30s, holding a worker
		// through the short bursts of capacity it was waiting for.
		//
		// A transport failure has no status to consult, so DefaultRetryPolicy
		// still decides those. A response that arrived is judged ONLY by the
		// configured list — which by default still carries 429 and the 5xx set,
		// so every client that does not override it behaves exactly as before.
		if resp == nil || err != nil {
			shouldRetry, defaultErr := retryablehttp.DefaultRetryPolicy(ctx, resp, err)
			if defaultErr != nil {
				return false, defaultErr
			}
			return shouldRetry, nil
		}

		if _, ok := client.retryableStatus[resp.StatusCode]; ok {
			return true, nil
		}
		return false, nil
	}

	// SURFACE THE STATUS THE LIBRARY THROWS AWAY.
	//
	// retryablehttp's default give-up path drains and closes the response and
	// returns `giving up after N attempt(s)` carrying NO status at all. That is
	// by design on its side, and it cost three separate diagnoses here: an
	// operator staring at 12,123 identical warnings had no way to tell a 429
	// (self-inflicted rate limit — the interesting case) from a 503 (provider
	// outage) from a transport failure.
	//
	// The body snippet is bounded because a provider error page can be large
	// and this is a log line, not a payload. What matters is the status and the
	// first line of whatever it said.
	retryClient.ErrorHandler = func(resp *http.Response, err error, attempts int) (*http.Response, error) {
		if resp == nil {
			return nil, fmt.Errorf("request failed after %d attempt(s): %w", attempts, err)
		}
		defer resp.Body.Close()
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		detail := strings.TrimSpace(string(snippet))
		if detail == "" {
			detail = "(empty body)"
		}
		giveUp := fmt.Errorf("%s %s gave up after %d attempt(s): status %d: %s",
			resp.Request.Method, resp.Request.URL, attempts, resp.StatusCode, detail)

		// AND TYPE THE ONE STATUS A CALLER MUST BE ABLE TO ACT ON.
		//
		// 429 is in retryableStatus, so a rate-limited request is retried to
		// exhaustion and only ever reaches its caller through this handler —
		// which means the provider-side switch statements that map status codes
		// to typed errors NEVER SEE IT. Every one of them lands on its default
		// branch and produces an untyped string.
		//
		// That is exactly how a rate limit became a refused grab: the add path
		// classifies a failure by TYPE to decide whether to hold the entry or
		// fail it back to the *arr, the type was absent, and "I could not
		// classify this" means refuse. So a self-inflicted 429 — the most
		// transient condition in the system, clearing in seconds — was answered
		// with a permanent-looking 400 and the release was lost.
		//
		// Typed HERE rather than per provider because this is the only place
		// that still knows the status. Doing it in five provider packages would
		// mean five switches that are all unreachable for this case.
		if resp.StatusCode == http.StatusTooManyRequests {
			return nil, fmt.Errorf("%w: %w", customerror.RateLimitedError, giveUp)
		}
		return nil, giveUp
	}

	client.client = retryClient

	return client
}

func Default() *Client {
	once.Do(func() {
		instance = New()
	})
	return instance
}

func SetProxy(transport *http.Transport, proxyURL string) {
	if proxyURL != "" {
		if strings.HasPrefix(proxyURL, "socks5://") {
			// Handle SOCKS5 proxy
			socksURL, err := url.Parse(proxyURL)
			if err == nil {
				auth := &proxy.Auth{}
				if socksURL.User != nil {
					auth.User = socksURL.User.Username()
					password, _ := socksURL.User.Password()
					auth.Password = password
				}

				dialer, err := proxy.SOCKS5("tcp", socksURL.Host, auth, proxy.Direct)
				if err == nil {
					transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
						return dialer.Dial(network, addr)
					}
				}
			}
		} else {
			_proxy, err := url.Parse(proxyURL)
			if err == nil {
				transport.Proxy = http.ProxyURL(_proxy)
			}
		}
	} else {
		transport.Proxy = http.ProxyFromEnvironment
	}
}

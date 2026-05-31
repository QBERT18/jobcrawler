package httputil

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"time"

	"github.com/applytude/jobcrawler/internal/domain"
	utls "github.com/refraction-networking/utls"
	"golang.org/x/net/http2"
	"golang.org/x/net/publicsuffix"
)

// ErrForbidden is returned when the server responds with HTTP 403.
var ErrForbidden = fmt.Errorf("access forbidden (HTTP 403)")

// CrawlerClient is a browser-mimicking HTTP client with User-Agent rotation,
// automatic retry with exponential backoff, and crawler-friendly headers.
//
// The transport stack:
//   - uTLS (refraction-networking/utls) supplies a Chrome 120 TLS ClientHello,
//     yielding a JA3/JA4 fingerprint Cloudflare-protected origins (Indeed et
//     al.) accept. Stdlib crypto/tls produces a fingerprint Cloudflare flags.
//   - golang.org/x/net/http2 carries the HTTP layer, because real browsers
//     negotiate h2 with these origins and falling back to h1 is itself a
//     fingerprint deviation.
type CrawlerClient struct {
	client     *http.Client
	userAgents []string
}

// NewCrawlerClient builds a CrawlerClient with a uTLS-backed HTTP/2 transport
// and a per-host cookie jar.
//
// The cookie jar is essential for two reasons:
//   - Indeed gates paginated listings (start=10,20,…) behind a session cookie;
//     without one, page 2+ are punted to /auth.
//   - Cloudflare's __cf_bm cookie marks a UA/IP as already-verified for ~30
//     min, reducing the rate at which managed challenges fire.
func NewCrawlerClient(userAgents []string) *CrawlerClient {
	transport := &http2.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return chromeUTLSDialer(ctx, network, addr)
		},
	}

	jar, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})

	return &CrawlerClient{
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
			Jar:       jar,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 5 {
					return fmt.Errorf("too many redirects")
				}
				return nil
			},
		},
		userAgents: userAgents,
	}
}

// chromeUTLSDialer performs a TLS handshake using a Chrome 120 ClientHello,
// negotiating h2 via ALPN. The returned net.Conn satisfies http2.Transport's
// expected post-ALPN h2 connection.
func chromeUTLSDialer(ctx context.Context, network, addr string) (net.Conn, error) {
	rawConn, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}

	cfg := &utls.Config{ServerName: host}
	uconn := utls.UClient(rawConn, cfg, utls.HelloChrome_120)
	if err := uconn.HandshakeContext(ctx); err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("utls handshake %s: %w", host, err)
	}

	if proto := uconn.ConnectionState().NegotiatedProtocol; proto != "h2" {
		_ = uconn.Close()
		return nil, fmt.Errorf("server %s did not negotiate h2 (got %q)", host, proto)
	}
	return uconn, nil
}

// Get fetches url and returns the response body.
// Retries up to 3 times with exponential backoff (1s → 2s → 4s).
// HTTP 429 → returns domain.ErrRateLimited immediately (no retry).
// HTTP 403 → returns ErrForbidden immediately (no retry).
// Any other non-2xx → retried, returns error after all attempts exhausted.
func (c *CrawlerClient) Get(ctx context.Context, url string) ([]byte, error) {
	delays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	var lastErr error

	for attempt, delay := range delays {
		body, err := c.doGet(ctx, url)
		if err == nil {
			return body, nil
		}

		if err == domain.ErrRateLimited || err == ErrForbidden {
			return nil, err
		}

		lastErr = err

		if attempt == len(delays)-1 {
			break
		}

		jitter := time.Duration(rand.Int63n(int64(delay) / 5)) //nolint:gosec
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay + jitter):
		}
	}

	return nil, fmt.Errorf("get %s: all retries exhausted: %w", url, lastErr)
}

// doGet performs a single HTTP GET with browser-like headers, including
// Chrome Client Hints — Cloudflare's `critical-ch` response indicates these
// are now required to pass managed challenges.
func (c *CrawlerClient) doGet(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	ua := c.randomUserAgent()
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7")
	req.Header.Set("Cache-Control", "max-age=0")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-User", "?1")

	// Send Client Hints only when the rotated UA is Chrome- or Edge-based.
	// Firefox/Safari don't send sec-ch-ua, so doing so would itself be a
	// fingerprint mismatch.
	if isChromiumUA(ua) {
		req.Header.Set("Sec-Ch-Ua", `"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"`)
		req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
		req.Header.Set("Sec-Ch-Ua-Platform", `"Windows"`)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		return nil, domain.ErrRateLimited
	case http.StatusForbidden:
		return nil, ErrForbidden
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected status %d for %s", resp.StatusCode, url)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return body, nil
}

// randomUserAgent returns a random User-Agent from the configured list.
func (c *CrawlerClient) randomUserAgent() string {
	if len(c.userAgents) == 0 {
		return "Mozilla/5.0 (compatible; JobCrawlerBot/1.0)"
	}
	return c.userAgents[rand.Intn(len(c.userAgents))] //nolint:gosec
}

// isChromiumUA returns true for Chrome/Chromium/Edge/Opera UAs — i.e., the
// browsers that emit Client Hints. Firefox and Safari are excluded.
func isChromiumUA(ua string) bool {
	low := strings.ToLower(ua)
	if strings.Contains(low, "firefox") {
		return false
	}
	if strings.Contains(low, "safari") && !strings.Contains(low, "chrome") && !strings.Contains(low, "edg") {
		return false
	}
	return strings.Contains(low, "chrome") || strings.Contains(low, "chromium") ||
		strings.Contains(low, "edg") || strings.Contains(low, "opr")
}

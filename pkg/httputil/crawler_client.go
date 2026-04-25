package httputil

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"

	"github.com/applytude/jobcrawler/internal/domain"
)

// ErrForbidden is returned when the server responds with HTTP 403.
var ErrForbidden = fmt.Errorf("access forbidden (HTTP 403)")

// CrawlerClient is a browser-mimicking HTTP client with User-Agent rotation,
// automatic retry with exponential backoff, and crawler-friendly headers.
type CrawlerClient struct {
	client     *http.Client
	userAgents []string
}

// NewCrawlerClient builds a CrawlerClient with sensible timeouts.
func NewCrawlerClient(userAgents []string) *CrawlerClient {
	return &CrawlerClient{
		client: &http.Client{
			Timeout: 30 * time.Second,
			// Do not follow redirects blindly — log them instead.
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

		// Non-retryable errors — return immediately.
		if err == domain.ErrRateLimited || err == ErrForbidden {
			return nil, err
		}

		lastErr = err

		// Don't sleep after the last attempt.
		if attempt == len(delays)-1 {
			break
		}

		// Jitter: ±20 % of the base delay.
		jitter := time.Duration(rand.Int63n(int64(delay) / 5)) //nolint:gosec
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay + jitter):
		}
	}

	return nil, fmt.Errorf("get %s: all retries exhausted: %w", url, lastErr)
}

// doGet performs a single HTTP GET with browser-like headers.
func (c *CrawlerClient) doGet(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("User-Agent", c.randomUserAgent())
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7")
	// Intentionally do NOT set Accept-Encoding: Go's http.Transport only
	// auto-decompresses gzip responses when the caller leaves it unset.
	// Setting it manually would leave the body compressed and downstream
	// parsers would see bytes of noise instead of HTML.
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")

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

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20)) // 10 MB limit
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
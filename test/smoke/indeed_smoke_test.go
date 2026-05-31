//go:build manual_smoke
// +build manual_smoke

// Package smoke holds live, network-touching integration tests that hit real
// crawl targets. Gated behind the `manual_smoke` build tag so they never run
// during `go test ./...` or in CI. Run them explicitly:
//
//	go test -tags=manual_smoke -v ./test/smoke/...
package smoke

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/applytude/jobcrawler/config"
	"github.com/applytude/jobcrawler/pkg/httputil"
)

// TestIndeedSmoke hits a real Indeed listing URL and verifies our uTLS-backed
// client clears Cloudflare's managed challenge.
func TestIndeedSmoke(t *testing.T) {
	c := httputil.NewCrawlerClient(config.DefaultUserAgents)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	url := "https://de.indeed.com/jobs?q=software+developer&l=Deutschland&start=0"
	body, err := c.Get(ctx, url)
	if err != nil {
		if errors.Is(err, httputil.ErrForbidden) {
			t.Fatalf("FAIL: 403 from Indeed (Cloudflare still blocking) — %v", err)
		}
		t.Fatalf("FAIL: %v", err)
	}

	t.Logf("HTTP 200 — body length: %d bytes", len(body))

	bodyStr := strings.ToLower(string(body))
	switch {
	case strings.Contains(bodyStr, "security check") || strings.Contains(bodyStr, "just a moment"):
		t.Fatalf("FAIL: got Cloudflare challenge HTML (status 200 but body is challenge page)")
	case strings.Contains(bodyStr, "jobsearch") || strings.Contains(bodyStr, "jobcards") ||
		strings.Contains(bodyStr, "application/ld+json") || strings.Contains(bodyStr, "data-jk="):
		t.Logf("PASS: body contains job-listing markers")
	default:
		t.Logf("AMBIGUOUS: status 200 but no obvious job markers — first 400 chars:\n%s",
			string(body[:min(400, len(body))]))
	}
}

// TestExistingSourcesSmoke verifies the uTLS+h2 transport still works against
// Stepstone and Xing — guards against regressions whenever the transport is
// touched.
func TestExistingSourcesSmoke(t *testing.T) {
	c := httputil.NewCrawlerClient(config.DefaultUserAgents)
	cases := map[string]string{
		"stepstone": "https://www.stepstone.de/jobs/it-software-entwicklung?page=1",
		"xing":      "https://www.xing.com/jobs/search?keywords=softwareentwickler",
	}
	for name, url := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			body, err := c.Get(ctx, url)
			if err != nil {
				t.Fatalf("FAIL %s: %v", name, err)
			}
			t.Logf("%s: HTTP 200 — %d bytes", name, len(body))
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

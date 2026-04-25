package httputil

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	"github.com/temoto/robotstxt"
)

// RobotsChecker fetches and caches robots.txt per domain.
// Cache is never evicted — robots.txt rarely changes and the process
// is expected to restart periodically (via Kubernetes).
type RobotsChecker struct {
	mu    sync.RWMutex
	cache map[string]*robotstxt.Group // key: scheme+host
}

// NewRobotsChecker creates an empty RobotsChecker.
func NewRobotsChecker() *RobotsChecker {
	return &RobotsChecker{
		cache: make(map[string]*robotstxt.Group),
	}
}

// IsAllowed returns true if the given path may be crawled according to
// the target domain's robots.txt. Fetches and caches on first call per domain.
// If fetching robots.txt fails, IsAllowed returns true and logs the error —
// a missing robots.txt should not block crawling.
func (rc *RobotsChecker) IsAllowed(ctx context.Context, client *CrawlerClient, rawURL string, path string) (bool, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false, fmt.Errorf("parse url %q: %w", rawURL, err)
	}

	origin := parsed.Scheme + "://" + parsed.Host

	// Fast path — already cached.
	rc.mu.RLock()
	group, ok := rc.cache[origin]
	rc.mu.RUnlock()

	if !ok {
		// Slow path — fetch and cache.
		fetchedGroup, err := rc.fetch(ctx, client, origin)
		if err != nil {
			// Non-fatal: assume allowed when robots.txt is unreachable.
			return true, nil
		}
		rc.mu.Lock()
		rc.cache[origin] = fetchedGroup
		rc.mu.Unlock()
		group = fetchedGroup
	}

	if group == nil {
		return true, nil // no restrictions found
	}

	return group.Test(path), nil
}

// fetch downloads robots.txt from origin and returns the "*" agent group.
func (rc *RobotsChecker) fetch(ctx context.Context, client *CrawlerClient, origin string) (*robotstxt.Group, error) {
	robotsURL := origin + "/robots.txt"

	data, err := client.Get(ctx, robotsURL)
	if err != nil {
		return nil, fmt.Errorf("fetch robots.txt from %s: %w", origin, err)
	}

	robots, err := robotstxt.FromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("parse robots.txt from %s: %w", origin, err)
	}

	return robots.FindGroup("*"), nil
}
package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/applytude/jobcrawler/internal/domain"
	pkgredis "github.com/applytude/jobcrawler/pkg/redis"
	"github.com/redis/go-redis/v9"
)

// CachedJobService wraps a JobService with a Redis Cache-Aside layer.
//
// Design Pattern: Decorator
//   CachedJobService implements the same JobService interface as the
//   underlying service. The HTTP handler is unaware that caching exists —
//   it talks to a JobService and gets results. This is textbook Decorator:
//   same interface, extended behaviour (caching), zero change to callers.
//
// Cache-Aside Pattern:
//   1. Check cache → hit: return cached value
//   2. Miss: call inner service (DB/ES)
//   3. Store result in cache with TTL
//   4. Return result
//
//   The application manages the cache explicitly. This gives full control
//   over TTL, invalidation, and fallback behaviour — unlike Write-Through
//   or Read-Through which hide the cache behind the data layer.
type CachedJobService struct {
	inner  JobService
	cache  *pkgredis.Cache
	log    *slog.Logger
}

// NewCachedJobService wraps inner with a Redis caching layer.
func NewCachedJobService(inner JobService, cache *pkgredis.Cache, log *slog.Logger) *CachedJobService {
	return &CachedJobService{
		inner: inner,
		cache: cache,
		log:   log,
	}
}

// Search checks the cache for the given filter combination.
// On a cache miss, delegates to the inner service and caches the result.
//
// Cache key: SHA-256 of the JSON-serialised SearchFilter.
// This produces a unique, stable key for every distinct filter combination
// without needing to manually enumerate every field.
func (s *CachedJobService) Search(ctx context.Context, filter domain.SearchFilter) (*domain.SearchResult, error) {
	key, err := searchCacheKey(filter)
	if err != nil {
		// Key generation failed — skip cache, go straight to inner service.
		s.log.WarnContext(ctx, "cache key generation failed — bypassing cache",
			slog.String("error", err.Error()),
		)
		return s.inner.Search(ctx, filter)
	}

	// ── Cache read ────────────────────────────────────────────────────────────
	var cached domain.SearchResult
	if err := s.cache.Get(ctx, key, &cached); err == nil {
		s.log.DebugContext(ctx, "cache hit", slog.String("key", key))
		return &cached, nil
	} else if !errors.Is(err, redis.Nil) {
		// Redis error (not a miss) — log and fall through to inner service.
		s.log.WarnContext(ctx, "cache get error — bypassing cache",
			slog.String("key", key),
			slog.String("error", err.Error()),
		)
	}

	// ── Cache miss → inner service ────────────────────────────────────────────
	result, err := s.inner.Search(ctx, filter)
	if err != nil {
		return nil, err
	}

	// ── Populate cache ────────────────────────────────────────────────────────
	if err := s.cache.Set(ctx, key, result, pkgredis.TTLCacheSearch); err != nil {
		// Cache write failure is non-fatal — the result is still returned.
		s.log.WarnContext(ctx, "cache set failed",
			slog.String("key", key),
			slog.String("error", err.Error()),
		)
	}

	return result, nil
}

// GetByID checks the per-job cache before calling the inner service.
// Individual job pages are cached for 5 minutes — long enough to absorb
// traffic spikes on a viral listing, short enough to reflect Processor updates.
func (s *CachedJobService) GetByID(ctx context.Context, id string) (*domain.Job, error) {
	key := pkgredis.KeyCacheJob(id)

	// ── Cache read ────────────────────────────────────────────────────────────
	var cached domain.Job
	if err := s.cache.Get(ctx, key, &cached); err == nil {
		s.log.DebugContext(ctx, "cache hit", slog.String("key", key))
		return &cached, nil
	} else if !errors.Is(err, redis.Nil) {
		s.log.WarnContext(ctx, "cache get error",
			slog.String("key", key),
			slog.String("error", err.Error()),
		)
	}

	// ── Cache miss → inner service ────────────────────────────────────────────
	job, err := s.inner.GetByID(ctx, id)
	if err != nil {
		return nil, err // includes domain.ErrNotFound — don't cache errors
	}

	// ── Populate cache ────────────────────────────────────────────────────────
	if err := s.cache.Set(ctx, key, job, pkgredis.TTLCacheJob); err != nil {
		s.log.WarnContext(ctx, "cache set failed",
			slog.String("key", key),
			slog.String("error", err.Error()),
		)
	}

	return job, nil
}

// GetStats delegates to the inner service with a 1-hour cache.
// Stats are computed from aggregate DB queries — expensive to recompute.
func (s *CachedJobService) GetStats(ctx context.Context) (*domain.JobStats, error) {
	key := pkgredis.KeyCacheStats()

	var cached domain.JobStats
	if err := s.cache.Get(ctx, key, &cached); err == nil {
		return &cached, nil
	}

	stats, err := s.inner.GetStats(ctx)
	if err != nil {
		return nil, err
	}

	if err := s.cache.Set(ctx, key, stats, pkgredis.TTLCacheStats); err != nil {
		s.log.WarnContext(ctx, "stats cache set failed", slog.String("error", err.Error()))
	}

	return stats, nil
}

// Invalidate removes the cached entry for a specific job and all search
// cache entries (since any search result set may have contained this job).
//
// Invalidation strategy:
//   - Job detail: deleted by known key ("cache:job:{id}")
//   - Search results: deleted via SCAN + DEL pattern "cache:search:*"
//
// Trade-off: invalidating all search caches on every job update is aggressive
// but correct. A smarter approach (e.g. tagging caches by source or location)
// would reduce churn but add significant complexity. At our scale the simple
// approach is the right call.
func (s *CachedJobService) Invalidate(ctx context.Context, id string) error {
	// Delete specific job detail cache.
	if err := s.cache.Delete(ctx, pkgredis.KeyCacheJob(id)); err != nil {
		s.log.WarnContext(ctx, "invalidate job cache failed",
			slog.String("id", id),
			slog.String("error", err.Error()),
		)
	}

	// Delete all search result caches via SCAN.
	// SCAN is non-blocking — it iterates in small batches without locking Redis.
	if err := s.cache.ScanAndDelete(ctx, "cache:search:*"); err != nil {
		s.log.WarnContext(ctx, "invalidate search caches failed",
			slog.String("error", err.Error()),
		)
	}

	// Also invalidate the stats cache — a new job changes the totals.
	if err := s.cache.Delete(ctx, pkgredis.KeyCacheStats()); err != nil {
		s.log.WarnContext(ctx, "invalidate stats cache failed",
			slog.String("error", err.Error()),
		)
	}

	return nil
}

// searchCacheKey produces a stable, unique cache key for a SearchFilter.
//
// Algorithm:
//  1. JSON-marshal the filter (deterministic field order via struct tags)
//  2. SHA-256 hash the JSON bytes
//  3. Return "cache:search:" + hex(hash)
//
// Why JSON + SHA-256 instead of fmt.Sprintf?
//   Sprintf requires enumerating every filter field and risks missing new fields
//   when the struct grows. JSON marshalling is automatically exhaustive and
//   produces a compact, consistent representation.
func searchCacheKey(filter domain.SearchFilter) (string, error) {
	data, err := json.Marshal(filter)
	if err != nil {
		return "", fmt.Errorf("marshal filter for cache key: %w", err)
	}
	h := sha256.Sum256(data)
	return pkgredis.KeyCacheSearch(fmt.Sprintf("%x", h)), nil
}
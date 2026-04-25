package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache wraps a Redis client with typed JSON get/set/delete helpers.
// It is the foundation of the Cache-Aside pattern used throughout JobCrawler:
// callers check the cache, fall back to the source of truth on a miss,
// then populate the cache for subsequent requests.
type Cache struct {
	client *redis.Client
}

// NewCache creates a Cache backed by client.
func NewCache(client *redis.Client) *Cache {
	return &Cache{client: client}
}

// Get retrieves the value at key and JSON-unmarshals it into dest.
//
// Returns:
//   - nil if the key exists and was successfully unmarshalled into dest.
//   - redis.Nil if the key does not exist (cache miss — caller should fetch from source).
//   - a wrapped error for any other Redis or JSON failure.
//
// Usage pattern:
//
//	var result SearchResult
//	err := cache.Get(ctx, key, &result)
//	if errors.Is(err, redis.Nil) {
//	    // cache miss — fetch from DB/ES
//	}
func (c *Cache) Get(ctx context.Context, key string, dest any) error {
	data, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return redis.Nil // propagate as-is so callers can errors.Is check
		}
		return fmt.Errorf("cache get %q: %w", key, err)
	}

	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("cache get %q: unmarshal: %w", key, err)
	}
	return nil
}

// Set JSON-marshals value and stores it at key with the given TTL.
// A TTL of 0 means the key never expires — use only for non-volatile data.
func (c *Cache) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("cache set %q: marshal: %w", key, err)
	}

	if err := c.client.Set(ctx, key, data, ttl).Err(); err != nil {
		return fmt.Errorf("cache set %q: %w", key, err)
	}
	return nil
}

// Delete removes one or more keys atomically using a single DEL command.
// Non-existent keys are silently ignored by Redis.
func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := c.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("cache delete %v: %w", keys, err)
	}
	return nil
}

// ScanAndDelete finds all keys matching pattern using SCAN (non-blocking,
// cursor-based) and deletes them in batches.
//
// Pattern uses Redis glob syntax: "cache:search:*" deletes all search caches.
//
// SCAN is preferred over KEYS in production because it does not block the
// Redis event loop. It iterates in O(N/cursor_batch) chunks.
func (c *Cache) ScanAndDelete(ctx context.Context, pattern string) error {
	var cursor uint64
	var deleted int

	for {
		keys, nextCursor, err := c.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("cache scan %q: %w", pattern, err)
		}

		if len(keys) > 0 {
			if err := c.client.Del(ctx, keys...).Err(); err != nil {
				return fmt.Errorf("cache scan-delete %q: %w", pattern, err)
			}
			deleted += len(keys)
		}

		cursor = nextCursor
		if cursor == 0 {
			break // SCAN complete
		}
	}

	return nil
}
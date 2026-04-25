package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter implements a sliding window rate limiter using a Redis sorted set.
//
// Algorithm — Sliding Window Log:
//   Each request's Unix millisecond timestamp is stored as a sorted set member.
//   Before adding the current request, old entries outside the window are pruned.
//   The cardinality (ZCard) of the cleaned set is the current request count.
//
// All four operations execute in one Pipeline round trip.
type RateLimiter struct {
	client *redis.Client
}

// NewRateLimiter creates a RateLimiter backed by client.
func NewRateLimiter(client *redis.Client) *RateLimiter {
	return &RateLimiter{client: client}
}

// Allow returns true if the entity identified by key has made fewer than
// `limit` requests within the last `window` duration.
//
// Sliding window pipeline steps:
//  1. ZRemRangeByScore: prune entries older than windowStart
//  2. ZCard: count remaining (in-window) entries
//  3. ZAdd: record this request with score=now
//  4. Expire: reset key TTL so idle keys are cleaned up
func (r *RateLimiter) Allow(ctx context.Context, key string, limit int64, window time.Duration) (bool, error) {
	now := time.Now().UnixMilli()
	windowStart := now - window.Milliseconds()

	// Unique member: timestamp + nanosecond suffix prevents deduplication
	// when two requests arrive within the same millisecond.
	member := strconv.FormatInt(now, 10) + "." +
		strconv.FormatInt(time.Now().UnixNano()%1_000_000, 10)

	pipe := r.client.Pipeline()

	pipe.ZRemRangeByScore(ctx, key,
		"0",
		strconv.FormatInt(windowStart-1, 10),
	)

	zcardCmd := pipe.ZCard(ctx, key)

	pipe.ZAdd(ctx, key, redis.Z{
		Score:  float64(now),
		Member: member,
	})

	// TTL = window + 1s buffer. Without this, a sorted set created by a burst
	// of requests that then goes quiet would sit in Redis indefinitely.
	pipe.Expire(ctx, key, window+time.Second)

	if _, err := pipe.Exec(ctx); err != nil {
		// Fail open: if Redis is unavailable, allow the request.
		// Failing closed would make Redis a hard dependency of every endpoint.
		return true, fmt.Errorf("rate limiter pipeline for %q: %w", key, err)
	}

	// ZCard was read before ZAdd, so it reflects the count of prior requests
	// within the window — not including the current one.
	count := zcardCmd.Val()
	return count < limit, nil
}
package processor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClient is the minimal interface Deduplicator needs from Redis.
// This keeps the dependency narrow and easy to mock in tests.
type RedisClient interface {
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
}

// Deduplicator uses Redis SETNX as a distributed set membership check.
// SETNX (Set if Not eXists) is atomic — safe for multiple concurrent workers.
type Deduplicator struct {
	redis RedisClient
}

// NewDeduplicator creates a Deduplicator backed by the given Redis client.
func NewDeduplicator(redis RedisClient) *Deduplicator {
	return &Deduplicator{redis: redis}
}

// IsDuplicate returns true if a job with the same normalised
// title + company + location has been seen within the last 7 days.
//
// Algorithm:
//  1. Normalise each field (lowercase, trim, collapse whitespace)
//  2. SHA-256 hash the concatenation
//  3. SETNX with 7d TTL
//  4. If the key already existed → duplicate
func (d *Deduplicator) IsDuplicate(ctx context.Context, title, company, location string) (bool, error) {
	fp := Fingerprint(title, company, location)
	key := "dedup:job:" + fp

	set, err := d.redis.SetNX(ctx, key, "1", 7*24*time.Hour).Result()
	if err != nil {
		return false, err
	}
	// SetNX returns true when the key was NEW (not a duplicate).
	// It returns false when the key already existed (IS a duplicate).
	return !set, nil
}

// IsURLCrawled returns true if this exact URL was already successfully
// crawled within the last 24 hours.
func (d *Deduplicator) IsURLCrawled(ctx context.Context, rawURL string) (bool, error) {
	h := sha256.Sum256([]byte(rawURL))
	key := "dedup:url:" + hex.EncodeToString(h[:])

	set, err := d.redis.SetNX(ctx, key, "1", 24*time.Hour).Result()
	if err != nil {
		return false, err
	}
	return !set, nil
}

// Fingerprint returns the hex-encoded SHA-256 of the normalised
// title + "|" + company + "|" + location string.
// Used both by IsDuplicate and to populate Job.Fingerprint before DB insert.
func Fingerprint(title, company, location string) string {
	normalised := normaliseField(title) + "|" + normaliseField(company) + "|" + normaliseField(location)
	h := sha256.Sum256([]byte(normalised))
	return hex.EncodeToString(h[:])
}

// normaliseField lowercases, trims, and collapses internal whitespace.
var multiSpace = regexp.MustCompile(`\s+`)

func normaliseField(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	return multiSpace.ReplaceAllString(s, " ")
}
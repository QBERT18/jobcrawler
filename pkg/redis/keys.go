package redis

// Key builder functions for every Redis namespace used by JobCrawler.
//
// Design choice: functions, not constants.
// Constants like `const KeyDedupJob = "dedup:job:"` require the caller to
// concatenate the parameter themselves, risking typos and inconsistent
// separators. Functions encapsulate the full key construction and are
// just as inlineable by the Go compiler.
//
// Namespace schema:
//   dedup:job:{fingerprint}         — content deduplication (7d TTL)
//   dedup:url:{sha256}              — URL deduplication (24h TTL)
//   cache:search:{queryHash}        — paginated search result cache (60s TTL)
//   cache:job:{id}                  — single job detail cache (5min TTL)
//   cache:stats:market              — aggregated market stats (1h TTL)
//   ratelimit:crawler:{source}      — per-source crawler rate limit (sliding window)
//   ratelimit:api:{ip}              — per-IP API rate limit (sliding window)
//   crawler:last_run:{source}       — ISO8601 timestamp of last successful crawl
//   crawler:job_count:{source}      — running total of crawled jobs per source
//   crawler:status                  — hash: source → "ok"|"error"

// KeyDedupJob returns the Redis key for content-based job deduplication.
// The fingerprint is the SHA-256 hex of normalised title+company+location.
func KeyDedupJob(fingerprint string) string {
	return "dedup:job:" + fingerprint
}

// KeyDedupURL returns the Redis key for URL-level crawl deduplication.
// The urlHash is the SHA-256 hex of the raw URL string.
func KeyDedupURL(urlHash string) string {
	return "dedup:url:" + urlHash
}

// KeyCacheSearch returns the Redis key for a cached paginated search result.
// queryHash should be a deterministic hash of the SearchFilter struct.
func KeyCacheSearch(queryHash string) string {
	return "cache:search:" + queryHash
}

// KeyCacheJob returns the Redis key for a cached single job detail response.
func KeyCacheJob(id string) string {
	return "cache:job:" + id
}

// KeyCacheStats returns the Redis key for the cached market statistics payload.
// No parameter — there is only one global stats object.
func KeyCacheStats() string {
	return "cache:stats:market"
}

// KeyRateLimitCrawler returns the Redis key for the per-source crawler rate
// limit sliding window. The sorted set holds request timestamps.
func KeyRateLimitCrawler(source string) string {
	return "ratelimit:crawler:" + source
}

// KeyRateLimitAPI returns the Redis key for the per-IP API rate limit window.
func KeyRateLimitAPI(ip string) string {
	return "ratelimit:api:" + ip
}

// KeyCrawlerLastRun returns the Redis key storing the RFC3339 timestamp of
// the most recent successful crawl run for a source.
func KeyCrawlerLastRun(source string) string {
	return "crawler:last_run:" + source
}

// KeyCrawlerJobCount returns the Redis key for the running integer counter
// of total jobs crawled from a source since the process started.
func KeyCrawlerJobCount(source string) string {
	return "crawler:job_count:" + source
}

// KeyCrawlerStatus returns the Redis hash key that stores the per-source
// health status. Fields: source name → "ok" | "error".
func KeyCrawlerStatus() string {
	return "crawler:status"
}
package redis

import "time"

// TTL constants define how long each Redis key type should live.
//
// Rationale for each value:
//
//   TTLDedupJob (7 days):
//     A job posting stays live for roughly 1–4 weeks. After 7 days we allow
//     the same job to be re-indexed — the source may have updated the description
//     or salary. Short enough to pick up legitimate updates; long enough to
//     prevent duplicate spam during normal crawl cycles.
//
//   TTLDedupURL (24 hours):
//     A listing page URL changes content at most daily. No need to re-fetch
//     the same URL within one day. After 24h the Scheduler will naturally
//     re-enqueue it and the Crawler should re-process it.
//
//   TTLCacheSearch (60 seconds):
//     Search results are read-heavy and change infrequently. A 60s cache
//     dramatically reduces Elasticsearch load for popular queries (e.g.
//     "golang berlin remote") while keeping results fresh enough for users.
//
//   TTLCacheJob (5 minutes):
//     Individual job detail pages are less frequently requested. 5 minutes
//     is a good balance — long enough to absorb traffic spikes on a viral
//     job listing, short enough to reflect edits made by the Processor.
//
//   TTLCacheStats (1 hour):
//     Market statistics (total jobs, top tags, avg salary) are aggregated
//     across millions of rows. Computing them is expensive. They change
//     slowly — hourly freshness is more than sufficient.
const (
	TTLDedupJob    = 7 * 24 * time.Hour
	TTLDedupURL    = 24 * time.Hour
	TTLCacheSearch = 60 * time.Second
	TTLCacheJob    = 5 * time.Minute
	TTLCacheStats  = 1 * time.Hour
)
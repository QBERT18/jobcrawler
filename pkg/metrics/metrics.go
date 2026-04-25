package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// All metrics are registered against the default Prometheus registry via
// promauto — they are created once at package init and never re-registered.
//
// Naming follows the Prometheus convention:
//   {namespace}_{subsystem}_{name}_{unit}
//
// RED method metrics (Rate, Errors, Duration) cover every service boundary:
//   - HTTP endpoints (httpRequestDuration)
//   - Crawler fetches (jobsCrawledTotal, crawlerDurationSeconds)
//   - Cache reads (cacheHitTotal)
//   - Kafka consumer (processorLag)

var (
	// ── Crawler metrics ───────────────────────────────────────────────────────

	// JobsCrawledTotal counts every crawl attempt, labelled by outcome.
	// status values: "success" | "failed" | "duplicate"
	// Use RecordCrawl() and RecordDuplicate() helpers — never Inc() directly.
	JobsCrawledTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "jobcrawler",
			Subsystem: "crawler",
			Name:      "jobs_crawled_total",
			Help:      "Total number of crawl attempts, partitioned by source and outcome.",
		},
		[]string{"source", "status"},
	)

	// CrawlerDurationSeconds measures how long a single page fetch takes.
	// Buckets cover the expected 1–30 second range for external HTTP calls.
	CrawlerDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "jobcrawler",
			Subsystem: "crawler",
			Name:      "duration_seconds",
			Help:      "Duration of individual crawl operations in seconds.",
			Buckets:   []float64{1, 2, 5, 10, 30},
		},
		[]string{"source"},
	)

	// ── HTTP API metrics ──────────────────────────────────────────────────────

	// HTTPRequestDuration measures API endpoint latency — the most important
	// SLI for the REST API. Buckets are sub-second because the API is backed
	// by a Redis cache and should respond in <100ms for cache hits.
	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "jobcrawler",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "HTTP request latency in seconds, partitioned by method, path, and status code.",
			Buckets:   prometheus.DefBuckets, // .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10
		},
		[]string{"method", "path", "status_code"},
	)

	// ── Processor / Kafka metrics ─────────────────────────────────────────────

	// ProcessorLag tracks how many messages are waiting in each Kafka partition.
	// A growing lag means the Processor can't keep up — scale up workers.
	// Updated externally by a background lag reporter goroutine.
	ProcessorLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "jobcrawler",
			Subsystem: "processor",
			Name:      "kafka_consumer_lag",
			Help:      "Current number of unconsumed messages per Kafka topic/partition.",
		},
		[]string{"topic", "partition"},
	)

	// ── Business metrics ──────────────────────────────────────────────────────

	// ActiveJobs tracks the current count of live job listings in the database.
	// Updated by the Processor after each successful upsert (and by a periodic
	// background reconciliation job). Useful for capacity planning dashboards.
	ActiveJobs = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "jobcrawler",
			Name:      "active_jobs_total",
			Help:      "Current number of active (non-expired) job listings in the database.",
		},
	)

	// ── Cache metrics ─────────────────────────────────────────────────────────

	// CacheHitTotal counts cache lookups by cache name and result.
	// result values: "hit" | "miss"
	// Cache hit rate = hit / (hit + miss) — target >60% for search cache.
	CacheHitTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "jobcrawler",
			Subsystem: "cache",
			Name:      "requests_total",
			Help:      "Total cache lookups partitioned by cache name and hit/miss result.",
		},
		[]string{"cache_name", "result"},
	)
)

// RecordCacheHit increments the hit counter for cacheName.
func RecordCacheHit(cacheName string) {
	CacheHitTotal.WithLabelValues(cacheName, "hit").Inc()
}

// RecordCacheMiss increments the miss counter for cacheName.
func RecordCacheMiss(cacheName string) {
	CacheHitTotal.WithLabelValues(cacheName, "miss").Inc()
}
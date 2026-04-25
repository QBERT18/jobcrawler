package metrics

import "time"

// RecordCrawl observes one completed crawl attempt for the given source.
//
// Parameters:
//   - source:   the JobSource string ("STEPSTONE", "INDEED", etc.)
//   - success:  true if the page was fetched and published successfully
//   - duration: wall-clock time from request start to Kafka publish
//
// This helper exists so CrawlerWorker doesn't import prometheus directly —
// it depends only on this metrics package, keeping the dependency graph clean.
func RecordCrawl(source string, success bool, duration time.Duration) {
	status := "success"
	if !success {
		status = "failed"
	}

	JobsCrawledTotal.
		WithLabelValues(source, status).
		Inc()

	CrawlerDurationSeconds.
		WithLabelValues(source).
		Observe(duration.Seconds())
}

// RecordDuplicate increments the duplicate counter for source.
// Called by the ProcessorWorker when content deduplication identifies
// a job that was already indexed — not a failure, but worth tracking.
// A high duplicate rate may indicate the crawl schedule is too aggressive.
func RecordDuplicate(source string) {
	JobsCrawledTotal.
		WithLabelValues(source, "duplicate").
		Inc()
}

// RecordProcessorLag updates the Kafka consumer lag gauge for a partition.
// Called by a background goroutine that polls Kafka for offset information.
func RecordProcessorLag(topic, partition string, lag float64) {
	ProcessorLag.
		WithLabelValues(topic, partition).
		Set(lag)
}

// SetActiveJobs updates the active jobs gauge to the given absolute count.
// Should be called after each ProcessorWorker upsert cycle and periodically
// by a background reconciliation job that queries the DB directly.
func SetActiveJobs(count float64) {
	ActiveJobs.Set(count)
}
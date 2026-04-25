package kafka

// Topic name constants used across all services.
// Always reference these — never use magic strings in producer/consumer calls.
const (
	TopicCrawlQueue    = "crawl.queue"
	TopicJobsRaw       = "jobs.raw"
	TopicJobsProcessed = "jobs.processed"
	TopicJobsFailed    = "jobs.failed"
)
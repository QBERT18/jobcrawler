package domain

import "time"

// CrawlType distinguishes between a listing-page crawl (discover URLs)
// and a detail-page crawl (extract full job data).
const (
	CrawlTypeListing = "LISTING"
	CrawlTypeDetail  = "DETAIL"
)

// CrawlTask is the Kafka message payload written to crawl.queue by the
// Scheduler and consumed by Crawler Workers.
type CrawlTask struct {
	Source     JobSource `json:"source"`
	URL        string    `json:"url"`
	CrawlType  string    `json:"crawl_type"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// RawCrawlResult is the Kafka message payload written to jobs.raw by the
// Crawler Worker after successfully fetching a page. The raw HTML is kept
// intact so the Processor can re-parse it without re-crawling.
type RawCrawlResult struct {
	Source    JobSource `json:"source"`
	URL       string    `json:"url"`
	HTML      string    `json:"html"`
	CrawledAt time.Time `json:"crawled_at"`
}
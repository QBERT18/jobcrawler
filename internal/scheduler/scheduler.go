package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/applytude/jobcrawler/config"
	"github.com/applytude/jobcrawler/internal/domain"
	jobkafka "github.com/applytude/jobcrawler/pkg/kafka"
	"github.com/robfig/cron/v3"
)

const (
	stepstoneBase = "https://www.stepstone.de"
	indeedBase    = "https://de.indeed.com"
	xingBase      = "https://www.xing.com"
)

// Scheduler enqueues CrawlTask messages into Kafka on a cron schedule.
// It deliberately does no crawling itself — pure coordination only.
type Scheduler struct {
	producer jobkafka.KafkaProducer
	cfg      config.CrawlerConfig
	log      *slog.Logger
}

// New creates a Scheduler with the given dependencies.
func New(producer jobkafka.KafkaProducer, cfg config.CrawlerConfig, log *slog.Logger) *Scheduler {
	return &Scheduler{
		producer: producer,
		cfg:      cfg,
		log:      log,
	}
}

// Start registers a cron job for each source and blocks until ctx is cancelled.
// Schedules are intentionally staggered so sources don't all fire at once:
//   - Stepstone: every 30 minutes
//   - Indeed:    every 60 minutes (stricter rate limiting)
//   - Xing:      every 2 hours
func (s *Scheduler) Start(ctx context.Context) {
	c := cron.New()

	addJob := func(schedule string, source domain.JobSource) {
		_, err := c.AddFunc(schedule, func() {
			if err := s.EnqueueSource(ctx, source); err != nil {
				s.log.ErrorContext(ctx, "enqueue failed",
					slog.String("source", string(source)),
					slog.String("error", err.Error()),
				)
			}
		})
		if err != nil {
			s.log.ErrorContext(ctx, "failed to register cron job",
				slog.String("source", string(source)),
				slog.String("schedule", schedule),
				slog.String("error", err.Error()),
			)
		}
	}

	addJob("*/30 * * * *", domain.SourceStepstone)
	addJob("0 */2 * * *", domain.SourceXing)
	// Indeed sits behind Cloudflare's managed challenge — fetches succeed
	// only via the uTLS-backed transport in pkg/httputil. Hourly cadence
	// keeps us comfortably under any per-IP rate threshold while still
	// surfacing fresh listings.
	addJob("0 * * * *", domain.SourceIndeed)

	c.Start()
	s.log.InfoContext(ctx, "scheduler started",
		slog.Int("jobs", len(c.Entries())),
	)

	// Run each source immediately on startup — don't wait for the first tick.
	for _, source := range []domain.JobSource{
		domain.SourceStepstone,
		domain.SourceXing,
		domain.SourceIndeed,
	} {
		if err := s.EnqueueSource(ctx, source); err != nil {
			s.log.ErrorContext(ctx, "initial enqueue failed",
				slog.String("source", string(source)),
				slog.String("error", err.Error()),
			)
		}
	}

	<-ctx.Done()
	s.log.InfoContext(ctx, "scheduler stopping")
	<-c.Stop().Done() // wait for any running job to finish
}

// EnqueueSource generates listing URLs for source and publishes them as
// CrawlTask messages to the crawl.queue Kafka topic in a single batch.
func (s *Scheduler) EnqueueSource(ctx context.Context, source domain.JobSource) error {
	urls := s.generateURLs(source)
	if len(urls) == 0 {
		return fmt.Errorf("no URLs generated for source %s", source)
	}

	msgs := make([]jobkafka.Message, 0, len(urls))
	for _, u := range urls {
		msgs = append(msgs, jobkafka.Message{
			// Keyed by source so all tasks for a source land on the same partition.
			Key: string(source),
			Payload: domain.CrawlTask{
				Source:    source,
				URL:       u,
				CrawlType: domain.CrawlTypeListing,
			},
		})
	}

	if err := s.producer.PublishBatch(ctx, jobkafka.TopicCrawlQueue, msgs); err != nil {
		return fmt.Errorf("enqueue %s: %w", source, err)
	}

	s.log.InfoContext(ctx, "enqueued crawl tasks",
		slog.String("source", string(source)),
		slog.Int("count", len(msgs)),
	)
	return nil
}

// generateURLs returns the listing page URLs to crawl for a given source.
// URL patterns are hardcoded per source — extend when adding new sources.
func (s *Scheduler) generateURLs(source domain.JobSource) []string {
	switch source {
	case domain.SourceStepstone:
		// 5 pages of IT/software job listings on Stepstone Germany.
		paths := []string{
			"/jobs/it-software-entwicklung?page=1",
			"/jobs/it-software-entwicklung?page=2",
			"/jobs/it-software-entwicklung?page=3",
			"/jobs/it-software-entwicklung?page=4",
			"/jobs/it-software-entwicklung?page=5",
		}
		return prefixed(stepstoneBase, paths)

	case domain.SourceIndeed:
		// Page 1 only. Indeed punts deep-linked paginated requests
		// (start=10,20,…) to a captcha+auth wall: those URLs are only
		// reachable by following the "Next" link from page 1, which carries
		// session-bound `tk` and `pp` parameters we can't synthesize. Page 1
		// yields ~15 detail URLs per run, which is sufficient given hourly
		// cadence.
		return []string{
			fmt.Sprintf("%s/jobs?q=software+developer&l=Deutschland", indeedBase),
		}

	case domain.SourceXing:
		keywords := []string{
			"softwareentwickler",
			"backend+developer",
			"golang+entwickler",
		}
		var urls []string
		for _, kw := range keywords {
			urls = append(urls, fmt.Sprintf(
				"%s/jobs/search?keywords=%s",
				xingBase, kw,
			))
		}
		return urls

	default:
		return nil
	}
}

// prefixed prepends base to every path in paths.
func prefixed(base string, paths []string) []string {
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		out = append(out, strings.TrimRight(base, "/")+p)
	}
	return out
}
package crawler

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/applytude/jobcrawler/internal/domain"
	"github.com/applytude/jobcrawler/pkg/httputil"
	jobkafka "github.com/applytude/jobcrawler/pkg/kafka"
	"github.com/segmentio/kafka-go"
)

// RateLimiter is the minimal interface the worker needs from Redis.
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int64, window time.Duration) (bool, error)
}

// StatsUpdater updates crawler health metrics in Redis after each batch.
type StatsUpdater interface {
	Set(ctx context.Context, key string, value any, ttl time.Duration) error
	Incr(ctx context.Context, key string) error
	HSet(ctx context.Context, key string, field string, value any) error
}

// failedMessage wraps an original Kafka message with error metadata for the DLQ.
type failedMessage struct {
	OriginalTopic string    `json:"original_topic"`
	OriginalKey   string    `json:"original_key"`
	Payload       string    `json:"payload"`
	Error         string    `json:"error"`
	FailedAt      time.Time `json:"failed_at"`
}

// maxListingFanout caps how many detail-URLs one listing page may produce.
// Bounds the blast radius of a broken selector that would otherwise match
// every <a> on the page and enqueue thousands of junk URLs.
const maxListingFanout = 20

// CrawlerWorker consumes CrawlTask messages from crawl.queue, fetches the
// target URL, and either (a) publishes raw detail-page HTML to jobs.raw,
// or (b) parses a listing page and re-enqueues each discovered detail URL
// back to crawl.queue as a CrawlTypeDetail task.
type CrawlerWorker struct {
	reader     jobkafka.KafkaReader
	producer   jobkafka.KafkaProducer
	httpClient *httputil.CrawlerClient
	robots     *httputil.RobotsChecker
	registry   *SourceRegistry
	limiter    RateLimiter
	stats      StatsUpdater
	log        *slog.Logger
}

// NewCrawlerWorker creates a CrawlerWorker with all required dependencies.
func NewCrawlerWorker(
	reader jobkafka.KafkaReader,
	producer jobkafka.KafkaProducer,
	httpClient *httputil.CrawlerClient,
	registry *SourceRegistry,
	limiter RateLimiter,
	stats StatsUpdater,
	log *slog.Logger,
) *CrawlerWorker {
	return &CrawlerWorker{
		reader:     reader,
		producer:   producer,
		httpClient: httpClient,
		robots:     httputil.NewRobotsChecker(),
		registry:   registry,
		limiter:    limiter,
		stats:      stats,
		log:        log,
	}
}

// Start is the main consumer loop. It blocks until ctx is cancelled.
// Each iteration:
//  1. Fetch a message from crawl.queue
//  2. Unmarshal into CrawlTask
//  3. Respect per-source rate limit
//  4. Check robots.txt
//  5. Fetch HTML
//  6. Publish RawCrawlResult to jobs.raw
//  7. Update Redis stats
//  8. Commit the Kafka offset
//
// On any error, the message is sent to jobs.failed (DLQ) and then committed
// so the consumer does not get stuck replaying a permanently broken message.
func (w *CrawlerWorker) Start(ctx context.Context) {
	w.log.InfoContext(ctx, "crawler worker started")

	for {
		msg, err := w.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				w.log.InfoContext(ctx, "crawler worker stopping — context cancelled")
				return
			}
			w.log.ErrorContext(ctx, "fetch message failed",
				slog.String("error", err.Error()),
			)
			continue
		}

		w.processMessage(ctx, msg)
	}
}

// processMessage handles a single Kafka message.
func (w *CrawlerWorker) processMessage(ctx context.Context, msg kafka.Message) {
	var task domain.CrawlTask
	if err := json.Unmarshal(msg.Value, &task); err != nil {
		w.log.ErrorContext(ctx, "unmarshal crawl task failed",
			slog.String("error", err.Error()),
			slog.String("raw", string(msg.Value)),
		)
		w.publishToDLQ(ctx, msg, err)
		w.commit(ctx, msg)
		return
	}

	log := w.log.With(
		slog.String("source", string(task.Source)),
		slog.String("url", task.URL),
	)

	// ── Rate limit ────────────────────────────────────────────────────────────
	if err := w.respectRateLimit(ctx, task.Source); err != nil {
		log.WarnContext(ctx, "rate limit wait failed", slog.String("error", err.Error()))
		w.publishToDLQ(ctx, msg, err)
		w.commit(ctx, msg)
		return
	}

	// ── robots.txt check ──────────────────────────────────────────────────────
	allowed, err := w.robots.IsAllowed(ctx, w.httpClient, task.URL, "/")
	if err != nil {
		log.WarnContext(ctx, "robots.txt check failed", slog.String("error", err.Error()))
	}
	if !allowed {
		log.WarnContext(ctx, "path disallowed by robots.txt — skipping")
		w.commit(ctx, msg)
		return
	}

	// ── Fetch HTML ────────────────────────────────────────────────────────────
	start := time.Now()
	html, err := w.httpClient.Get(ctx, task.URL)
	if err != nil {
		log.ErrorContext(ctx, "http get failed",
			slog.String("error", err.Error()),
			slog.Duration("elapsed", time.Since(start)),
		)
		w.publishToDLQ(ctx, msg, err)
		w.commit(ctx, msg)
		return
	}

	log.InfoContext(ctx, "page fetched",
		slog.Int("bytes", len(html)),
		slog.Duration("elapsed", time.Since(start)),
		slog.String("crawl_type", task.CrawlType),
	)

	// ── Dispatch by crawl type ────────────────────────────────────────────────
	// LISTING → parse out detail URLs and re-enqueue each as a DETAIL task.
	// DETAIL (or empty, for backward compatibility) → forward raw HTML to
	// the processor via jobs.raw.
	if task.CrawlType == domain.CrawlTypeListing {
		if err := w.fanoutListing(ctx, log, task, html); err != nil {
			w.publishToDLQ(ctx, msg, err)
			w.commit(ctx, msg)
			return
		}
	} else {
		result := domain.RawCrawlResult{
			Source:    task.Source,
			URL:       task.URL,
			HTML:      string(html),
			CrawledAt: time.Now().UTC(),
		}
		if err := w.producer.Publish(ctx, jobkafka.TopicJobsRaw, string(task.Source), result); err != nil {
			log.ErrorContext(ctx, "publish to jobs.raw failed",
				slog.String("error", err.Error()),
			)
			w.publishToDLQ(ctx, msg, err)
			w.commit(ctx, msg)
			return
		}
	}

	// ── Update Redis stats ────────────────────────────────────────────────────
	w.updateStats(ctx, task.Source)

	// ── Commit offset ─────────────────────────────────────────────────────────
	w.commit(ctx, msg)
}

// fanoutListing parses a listing page's HTML and re-enqueues each discovered
// detail URL back to crawl.queue as a CrawlTypeDetail task. An empty result
// is not an error — selectors drift; we just log and move on. A missing
// source implementation or a parse error is returned so the caller can DLQ.
func (w *CrawlerWorker) fanoutListing(
	ctx context.Context,
	log *slog.Logger,
	task domain.CrawlTask,
	html []byte,
) error {
	src, err := w.registry.GetSource(task.Source)
	if err != nil {
		log.ErrorContext(ctx, "source not registered — cannot parse listing",
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("listing fanout: %w", err)
	}

	// TEMPORARY: Phase A diagnostic. When CRAWLER_DEBUG_DUMP_DIR is set, write
	// the listing HTML to that directory so we can inspect selectors against
	// the real bytes the crawler receives. Guarded so it's a no-op otherwise.
	dumpListingHTML(ctx, log, task, html)

	urls, err := src.ParseListing(html)
	if err != nil {
		log.ErrorContext(ctx, "parse listing failed", slog.String("error", err.Error()))
		return fmt.Errorf("parse listing: %w", err)
	}

	if len(urls) == 0 {
		log.WarnContext(ctx, "listing produced no detail urls — selectors may have drifted")
		return nil
	}

	trimmed := false
	if len(urls) > maxListingFanout {
		urls = urls[:maxListingFanout]
		trimmed = true
	}

	now := time.Now().UTC()
	msgs := make([]jobkafka.Message, 0, len(urls))
	for _, u := range urls {
		msgs = append(msgs, jobkafka.Message{
			Key: string(task.Source),
			Payload: domain.CrawlTask{
				Source:     task.Source,
				URL:        u,
				CrawlType:  domain.CrawlTypeDetail,
				EnqueuedAt: now,
			},
		})
	}

	if err := w.producer.PublishBatch(ctx, jobkafka.TopicCrawlQueue, msgs); err != nil {
		log.ErrorContext(ctx, "publish detail tasks failed", slog.String("error", err.Error()))
		return fmt.Errorf("publish detail tasks: %w", err)
	}

	log.InfoContext(ctx, "enqueued detail tasks",
		slog.Int("count", len(msgs)),
		slog.Bool("trimmed", trimmed),
	)
	return nil
}

// dumpListingHTML is a TEMPORARY Phase-A diagnostic. When CRAWLER_DEBUG_DUMP_DIR
// is set, it writes the fetched listing HTML to that directory as
// {source}-{sha1(url)[:8]}.html — one file per unique URL so repeated crawls
// don't spam the disk. All failures are best-effort: dumping is never fatal.
// Remove this function and its call site in Phase C once selectors are fixed.
func dumpListingHTML(ctx context.Context, log *slog.Logger, task domain.CrawlTask, html []byte) {
	dir := os.Getenv("CRAWLER_DEBUG_DUMP_DIR")
	if dir == "" {
		return
	}
	sum := sha1.Sum([]byte(task.URL))
	name := fmt.Sprintf("%s-%s.html", task.Source, hex.EncodeToString(sum[:])[:8])
	path := filepath.Join(dir, name)
	if _, err := os.Stat(path); err == nil {
		return // already captured this URL — skip
	}
	if err := os.WriteFile(path, html, 0o644); err != nil {
		log.WarnContext(ctx, "debug dump write failed",
			slog.String("path", path),
			slog.String("error", err.Error()),
		)
		return
	}
	log.InfoContext(ctx, "debug dump written", slog.String("path", path), slog.Int("bytes", len(html)))
}

// respectRateLimit blocks until the sliding-window rate limiter allows the
// request. Returns an error only on context cancellation or Redis failure.
// Target: 0.5 req/s per source = 1 request per 2 seconds.
func (w *CrawlerWorker) respectRateLimit(ctx context.Context, source domain.JobSource) error {
	key := fmt.Sprintf("ratelimit:crawler:%s", source)

	for {
		allowed, err := w.limiter.Allow(ctx, key, 1, 2*time.Second)
		if err != nil {
			return fmt.Errorf("rate limiter error: %w", err)
		}
		if allowed {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
			// Poll again — the window will open shortly.
		}
	}
}

// updateStats writes crawler health metrics to Redis.
// Errors are logged but never fatal — stats are best-effort.
func (w *CrawlerWorker) updateStats(ctx context.Context, source domain.JobSource) {
	src := string(source)

	if err := w.stats.Set(ctx,
		fmt.Sprintf("crawler:last_run:%s", src),
		time.Now().UTC().Format(time.RFC3339),
		0,
	); err != nil {
		w.log.WarnContext(ctx, "stats: set last_run failed", slog.String("error", err.Error()))
	}

	if err := w.stats.Incr(ctx, fmt.Sprintf("crawler:job_count:%s", src)); err != nil {
		w.log.WarnContext(ctx, "stats: incr job_count failed", slog.String("error", err.Error()))
	}

	if err := w.stats.HSet(ctx, "crawler:status", src, "ok"); err != nil {
		w.log.WarnContext(ctx, "stats: hset status failed", slog.String("error", err.Error()))
	}
}

// publishToDLQ sends a failed message to jobs.failed with error metadata.
func (w *CrawlerWorker) publishToDLQ(ctx context.Context, msg kafka.Message, crawlErr error) {
	dlq := failedMessage{
		OriginalTopic: msg.Topic,
		OriginalKey:   string(msg.Key),
		Payload:       string(msg.Value),
		Error:         crawlErr.Error(),
		FailedAt:      time.Now().UTC(),
	}
	if err := w.producer.Publish(ctx, jobkafka.TopicJobsFailed, string(msg.Key), dlq); err != nil {
		w.log.ErrorContext(ctx, "publish to DLQ failed",
			slog.String("error", err.Error()),
		)
	}
}

// commit commits the Kafka offset for msg. Errors are logged — if commit
// fails the message will be reprocessed on restart, which is acceptable.
func (w *CrawlerWorker) commit(ctx context.Context, msg kafka.Message) {
	if err := w.reader.CommitMessages(ctx, msg); err != nil {
		w.log.ErrorContext(ctx, "commit offset failed",
			slog.String("error", err.Error()),
			slog.Int64("offset", msg.Offset),
		)
	}
}
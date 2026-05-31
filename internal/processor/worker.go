package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/applytude/jobcrawler/internal/crawler"
	"github.com/applytude/jobcrawler/internal/domain"
	"github.com/applytude/jobcrawler/internal/repository"
	jobkafka "github.com/applytude/jobcrawler/pkg/kafka"
	"github.com/segmentio/kafka-go"
)

// countRefreshInterval is how often the worker re-syncs its in-memory job
// counter from the database, so the retention cron lowering the count (and the
// drift from upsert-updates) is reflected in cap enforcement.
const countRefreshInterval = 60 * time.Second

// ProcessorWorker consumes RawCrawlResult messages from jobs.raw,
// parses + normalises + deduplicates them, persists to PostgreSQL,
// and triggers alert matching for IMMEDIATE subscribers.
type ProcessorWorker struct {
	reader       jobkafka.KafkaReader
	producer     jobkafka.KafkaProducer
	repo         repository.JobRepository
	dedup        *Deduplicator
	normalizer   *Normalizer
	registry     *crawler.SourceRegistry
	alertMatcher *AlertMatcher // nil-safe: alerts are skipped if not wired
	log          *slog.Logger

	// maxTotalJobs is the hard cap on stored jobs (0 = unlimited). When the live
	// counter reaches it, inserts are paused (soft, self-healing — see processMessage).
	maxTotalJobs int64
	// jobCount is the live approximate count of stored jobs, seeded from the DB
	// and periodically re-synced (see startCountRefresher).
	jobCount atomic.Int64
}

// NewProcessorWorker creates a ProcessorWorker with all required dependencies.
// maxTotalJobs caps the number of stored jobs (0 = unlimited).
func NewProcessorWorker(
	reader jobkafka.KafkaReader,
	producer jobkafka.KafkaProducer,
	repo repository.JobRepository,
	dedup *Deduplicator,
	normalizer *Normalizer,
	registry *crawler.SourceRegistry,
	alertMatcher *AlertMatcher,
	maxTotalJobs int64,
	log *slog.Logger,
) *ProcessorWorker {
	return &ProcessorWorker{
		reader:       reader,
		producer:     producer,
		repo:         repo,
		dedup:        dedup,
		normalizer:   normalizer,
		registry:     registry,
		alertMatcher: alertMatcher,
		maxTotalJobs: maxTotalJobs,
		log:          log,
	}
}

// Start is the main consumer loop. Blocks until ctx is cancelled.
func (w *ProcessorWorker) Start(ctx context.Context) {
	w.log.InfoContext(ctx, "processor worker started")

	if w.maxTotalJobs > 0 {
		w.refreshCount(ctx)
		go w.startCountRefresher(ctx)
		w.log.InfoContext(ctx, "job cap enforced",
			slog.Int64("max_total_jobs", w.maxTotalJobs),
			slog.Int64("current", w.jobCount.Load()),
		)
	}

	for {
		msg, err := w.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				w.log.InfoContext(ctx, "processor worker stopping — context cancelled")
				return
			}
			w.log.ErrorContext(ctx, "fetch message failed", slog.String("error", err.Error()))
			continue
		}

		w.processMessage(ctx, msg)
	}
}

// processMessage handles a single Kafka message end-to-end.
// The Kafka offset is always committed at the end — errors route to DLQ.
func (w *ProcessorWorker) processMessage(ctx context.Context, msg kafka.Message) {
	defer w.commit(ctx, msg)

	// ── 1. Unmarshal ──────────────────────────────────────────────────────────
	var raw domain.RawCrawlResult
	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		w.log.ErrorContext(ctx, "unmarshal failed",
			slog.String("error", err.Error()),
			slog.String("raw", truncate(string(msg.Value), 200)),
		)
		PublishToDLQ(ctx, w.producer, msg, fmt.Errorf("unmarshal: %w", err))
		return
	}

	log := w.log.With(
		slog.String("source", string(raw.Source)),
		slog.String("url", raw.URL),
		slog.String("trace_id", jobkafka.HeaderValue(msg, "trace-id")),
	)

	// ── 2. URL deduplication ──────────────────────────────────────────────────
	isCrawled, err := w.dedup.IsURLCrawled(ctx, raw.URL)
	if err != nil {
		log.WarnContext(ctx, "url dedup check failed — proceeding without dedup",
			slog.String("error", err.Error()),
		)
	}
	if isCrawled {
		log.DebugContext(ctx, "url already processed — skipping")
		return
	}

	// ── 3. Parse HTML ─────────────────────────────────────────────────────────
	source, err := w.registry.GetSource(raw.Source)
	if err != nil {
		log.ErrorContext(ctx, "source not registered", slog.String("error", err.Error()))
		PublishToDLQ(ctx, w.producer, msg, err)
		return
	}

	rawJob, err := source.ParseDetail([]byte(raw.HTML))
	if err != nil {
		log.ErrorContext(ctx, "parse detail failed", slog.String("error", err.Error()))
		PublishToDLQ(ctx, w.producer, msg, fmt.Errorf("parse detail: %w", err))
		return
	}

	// ── 4. Content deduplication ──────────────────────────────────────────────
	isDup, err := w.dedup.IsDuplicate(ctx, rawJob.Title, rawJob.Company, rawJob.Location)
	if err != nil {
		log.WarnContext(ctx, "content dedup check failed — proceeding",
			slog.String("error", err.Error()),
		)
	}
	if isDup {
		log.DebugContext(ctx, "duplicate content — skipping")
		return
	}

	// ── 5. Normalise ──────────────────────────────────────────────────────────
	salMin, salMax, salCur := w.normalizer.ParseSalary(rawJob.SalaryText)

	job := &domain.Job{
		ExternalID: rawJob.ExternalID,
		Source:     raw.Source,
		Title:      w.normalizer.NormalizeTitle(rawJob.Title),
		Company: domain.Company{
			Name: rawJob.Company,
		},
		Location: domain.Location{
			City: w.normalizer.NormalizeLocation(rawJob.Location),
		},
		Remote:         w.normalizer.DetectRemote(rawJob.Description),
		SalaryMin:      salMin,
		SalaryMax:      salMax,
		SalaryCurrency: salCur,
		Description:    rawJob.Description,
		Tags:           w.normalizer.ExtractTags(rawJob.Title, rawJob.Description),
		URL:            raw.URL,
		Fingerprint:    Fingerprint(rawJob.Title, rawJob.Company, rawJob.Location),
	}

	// ── 6. Persist ────────────────────────────────────────────────────────────
	// Hard cap: pause inserts once the stored-job count reaches the limit. The
	// offset is still committed (via the deferred commit) so the queue drains
	// rather than backing up. This is self-healing — once the retention cron
	// drops the count below the cap, inserts resume on the next refresh.
	if w.atCap() {
		log.WarnContext(ctx, "job cap reached — skipping insert",
			slog.Int64("max_total_jobs", w.maxTotalJobs),
			slog.Int64("current", w.jobCount.Load()),
		)
		return
	}

	if err := w.repo.Upsert(ctx, job); err != nil {
		log.ErrorContext(ctx, "upsert failed", slog.String("error", err.Error()))
		PublishToDLQ(ctx, w.producer, msg, fmt.Errorf("upsert: %w", err))
		return
	}

	// Optimistically bump the live counter. Upsert may have been an update
	// rather than an insert, so this can over-count; the periodic refresh from
	// the DB corrects any drift.
	if w.maxTotalJobs > 0 {
		w.jobCount.Add(1)
	}

	log.InfoContext(ctx, "job persisted",
		slog.String("id", job.ID),
		slog.String("title", job.Title),
		slog.String("company", job.Company.Name),
		slog.String("remote", string(job.Remote)),
		slog.Int("tags", len(job.Tags)),
	)

	// ── 7. Publish to jobs.processed ─────────────────────────────────────────
	if err := w.producer.Publish(ctx, jobkafka.TopicJobsProcessed, job.ID, job); err != nil {
		// Not fatal: job is saved in DB. Log and continue to alert matching.
		log.WarnContext(ctx, "publish to jobs.processed failed",
			slog.String("error", err.Error()),
		)
	}

	// ── 8. Alert matching ─────────────────────────────────────────────────────
	if w.alertMatcher != nil {
		matches, err := w.alertMatcher.FindMatchingAlerts(ctx, job)
		if err != nil {
			log.WarnContext(ctx, "alert matching failed — job saved, alerts skipped",
				slog.String("error", err.Error()),
			)
		} else if len(matches) > 0 {
			log.InfoContext(ctx, "alert matches found",
				slog.Int("count", len(matches)),
				slog.String("job_id", job.ID),
			)
			w.alertMatcher.NotifyMatches(ctx, job, matches)
		}
	}
}

// commit advances the Kafka consumer offset past msg.
// Errors are logged — a failed commit means the message will be redelivered
// on restart, which is acceptable (the processor is idempotent via dedup).
func (w *ProcessorWorker) commit(ctx context.Context, msg kafka.Message) {
	if err := w.reader.CommitMessages(ctx, msg); err != nil {
		w.log.ErrorContext(ctx, "commit offset failed",
			slog.Int64("offset", msg.Offset),
			slog.String("error", err.Error()),
		)
	}
}

// atCap reports whether the hard job cap is enabled and has been reached.
func (w *ProcessorWorker) atCap() bool {
	return w.maxTotalJobs > 0 && w.jobCount.Load() >= w.maxTotalJobs
}

// refreshCount re-syncs the in-memory job counter from the database.
func (w *ProcessorWorker) refreshCount(ctx context.Context) {
	n, err := w.repo.Count(ctx)
	if err != nil {
		w.log.WarnContext(ctx, "job count refresh failed — keeping previous value",
			slog.String("error", err.Error()),
		)
		return
	}
	w.jobCount.Store(n)
}

// startCountRefresher periodically re-syncs the job counter until ctx is done.
func (w *ProcessorWorker) startCountRefresher(ctx context.Context) {
	t := time.NewTicker(countRefreshInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.refreshCount(ctx)
		}
	}
}

// truncate shortens s to maxLen characters for safe log output.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "…"
}
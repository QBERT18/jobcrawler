package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/applytude/jobcrawler/internal/crawler"
	"github.com/applytude/jobcrawler/internal/domain"
	"github.com/applytude/jobcrawler/internal/repository"
	jobkafka "github.com/applytude/jobcrawler/pkg/kafka"
	"github.com/segmentio/kafka-go"
)

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
}

// NewProcessorWorker creates a ProcessorWorker with all required dependencies.
func NewProcessorWorker(
	reader jobkafka.KafkaReader,
	producer jobkafka.KafkaProducer,
	repo repository.JobRepository,
	dedup *Deduplicator,
	normalizer *Normalizer,
	registry *crawler.SourceRegistry,
	alertMatcher *AlertMatcher,
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
		log:          log,
	}
}

// Start is the main consumer loop. Blocks until ctx is cancelled.
func (w *ProcessorWorker) Start(ctx context.Context) {
	w.log.InfoContext(ctx, "processor worker started")

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
	if err := w.repo.Upsert(ctx, job); err != nil {
		log.ErrorContext(ctx, "upsert failed", slog.String("error", err.Error()))
		PublishToDLQ(ctx, w.producer, msg, fmt.Errorf("upsert: %w", err))
		return
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

// truncate shortens s to maxLen characters for safe log output.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "…"
}
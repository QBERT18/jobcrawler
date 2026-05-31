// Package janitor enforces data-retention limits by periodically deleting old
// jobs from the repository. It keeps storage bounded on small/self-hosted
// deployments where the crawl pipeline would otherwise grow the dataset forever.
package janitor

import (
	"context"
	"log/slog"
	"time"

	"github.com/applytude/jobcrawler/config"
	"github.com/applytude/jobcrawler/internal/repository"
	"github.com/robfig/cron/v3"
)

// Janitor deletes jobs older than the configured retention window on a cron schedule.
type Janitor struct {
	repo repository.JobRepository
	cfg  config.ProcessorConfig
	log  *slog.Logger
}

// New creates a Janitor with the given dependencies.
func New(repo repository.JobRepository, cfg config.ProcessorConfig, log *slog.Logger) *Janitor {
	return &Janitor{repo: repo, cfg: cfg, log: log}
}

// Start registers the cleanup cron and blocks until ctx is cancelled.
// If cleanup is disabled or retention is non-positive, it returns immediately.
func (j *Janitor) Start(ctx context.Context) {
	if !j.cfg.CleanupEnabled {
		j.log.InfoContext(ctx, "janitor disabled — retention cleanup will not run")
		return
	}
	if j.cfg.CleanupRetentionDays <= 0 {
		j.log.WarnContext(ctx, "janitor enabled but CLEANUP_RETENTION_DAYS <= 0 — skipping to avoid deleting all jobs",
			slog.Int("retention_days", j.cfg.CleanupRetentionDays),
		)
		return
	}

	c := cron.New()
	if _, err := c.AddFunc(j.cfg.CleanupSchedule, func() { j.runOnce(ctx) }); err != nil {
		j.log.ErrorContext(ctx, "failed to register cleanup cron — janitor not running",
			slog.String("schedule", j.cfg.CleanupSchedule),
			slog.String("error", err.Error()),
		)
		return
	}

	c.Start()
	j.log.InfoContext(ctx, "janitor started",
		slog.String("schedule", j.cfg.CleanupSchedule),
		slog.Int("retention_days", j.cfg.CleanupRetentionDays),
	)

	<-ctx.Done()
	j.log.InfoContext(ctx, "janitor stopping")
	<-c.Stop().Done()
}

// runOnce performs a single retention sweep. Exported behaviour is exercised
// directly in tests via RunOnce.
func (j *Janitor) runOnce(ctx context.Context) {
	cutoff := time.Now().Add(-time.Duration(j.cfg.CleanupRetentionDays) * 24 * time.Hour)
	deleted, err := j.repo.DeleteOlderThan(ctx, cutoff)
	if err != nil {
		j.log.ErrorContext(ctx, "retention cleanup failed", slog.String("error", err.Error()))
		return
	}
	j.log.InfoContext(ctx, "retention cleanup complete",
		slog.Int64("deleted", deleted),
		slog.Time("cutoff", cutoff),
	)
}

// RunOnce runs a single retention sweep immediately, independent of the cron
// schedule. Useful for tests and manual triggering.
func (j *Janitor) RunOnce(ctx context.Context) { j.runOnce(ctx) }

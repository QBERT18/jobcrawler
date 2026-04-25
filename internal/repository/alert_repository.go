package repository

import (
	"context"

	"github.com/applytude/jobcrawler/internal/domain"
)

// AlertRepository defines persistence operations for JobAlert entities.
// The interface lives here (not in a concrete package) so the processor
// can depend on the abstraction without importing a database driver.
type AlertRepository interface {
	// Create persists a new JobAlert and sets its ID field.
	Create(ctx context.Context, alert *domain.JobAlert) error

	// ListActive returns all alerts where Active = true.
	// Used by the AlertMatcher to find candidates for every new job.
	ListActive(ctx context.Context) ([]*domain.JobAlert, error)

	// ListByFrequency returns active alerts with the given frequency value.
	// Used by daily/weekly batch digest jobs.
	ListByFrequency(ctx context.Context, freq string) ([]*domain.JobAlert, error)
}
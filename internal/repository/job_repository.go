package repository

import (
	"context"

	"github.com/applytude/jobcrawler/internal/domain"
)

// JobRepository defines all persistence operations for Job entities.
// Implementations may be backed by PostgreSQL, Elasticsearch, or a cache layer.
// The interface lives in the repository package, not in postgres/, so the
// domain and service layers never import a concrete driver.
type JobRepository interface {
	// Upsert inserts a new Job or updates an existing one identified by fingerprint.
	// Returns the persisted Job (with ID set) or an error.
	Upsert(ctx context.Context, job *domain.Job) error

	// GetByID returns the Job with the given UUID.
	// Returns domain.ErrNotFound if no such job exists.
	GetByID(ctx context.Context, id string) (*domain.Job, error)

	// Search returns a filtered, paginated list of jobs and the total match count.
	Search(ctx context.Context, filter domain.SearchFilter) ([]*domain.Job, int, error)

	// GetStats returns aggregate statistics across all indexed jobs.
	GetStats(ctx context.Context) (*domain.JobStats, error)
}
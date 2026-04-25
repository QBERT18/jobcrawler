package service

import (
	"context"

	"github.com/applytude/jobcrawler/internal/domain"
)

// JobService defines the business-logic operations available to HTTP handlers.
// Implementations may be backed by Elasticsearch, PostgreSQL, or a cache layer.
type JobService interface {
	// Search returns a paginated, filtered list of jobs.
	Search(ctx context.Context, filter domain.SearchFilter) (*domain.SearchResult, error)

	// GetByID retrieves a single job by its internal UUID.
	// Returns domain.ErrNotFound if no job exists with that ID.
	GetByID(ctx context.Context, id string) (*domain.Job, error)

	// GetStats returns an aggregated market overview.
	GetStats(ctx context.Context) (*domain.JobStats, error)
}
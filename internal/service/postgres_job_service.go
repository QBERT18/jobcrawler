package service

import (
	"context"

	"github.com/applytude/jobcrawler/internal/domain"
	"github.com/applytude/jobcrawler/internal/repository"
)

// PostgresJobService is a JobService backed directly by a JobRepository
// (no Elasticsearch). It translates the repository's (jobs, total) return
// shape into a paginated SearchResult.
type PostgresJobService struct {
	repo repository.JobRepository
}

func NewPostgresJobService(repo repository.JobRepository) *PostgresJobService {
	return &PostgresJobService{repo: repo}
}

func (s *PostgresJobService) Search(ctx context.Context, filter domain.SearchFilter) (*domain.SearchResult, error) {
	jobs, total, err := s.repo.Search(ctx, filter)
	if err != nil {
		return nil, err
	}
	totalPages := 0
	if filter.PerPage > 0 {
		totalPages = (total + filter.PerPage - 1) / filter.PerPage
	}
	return &domain.SearchResult{
		Jobs:       jobs,
		Total:      total,
		Page:       filter.Page,
		PerPage:    filter.PerPage,
		TotalPages: totalPages,
	}, nil
}

func (s *PostgresJobService) GetByID(ctx context.Context, id string) (*domain.Job, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *PostgresJobService) GetStats(ctx context.Context) (*domain.JobStats, error) {
	return s.repo.GetStats(ctx)
}

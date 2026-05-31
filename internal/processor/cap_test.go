package processor

import (
	"context"
	"testing"
	"time"

	"github.com/applytude/jobcrawler/internal/domain"
)

// capFakeRepo lets a test control the value returned by Count.
type capFakeRepo struct{ count int64 }

func (f *capFakeRepo) Upsert(context.Context, *domain.Job) error            { return nil }
func (f *capFakeRepo) GetByID(context.Context, string) (*domain.Job, error) { return nil, nil }
func (f *capFakeRepo) Search(context.Context, domain.SearchFilter) ([]*domain.Job, int, error) {
	return nil, 0, nil
}
func (f *capFakeRepo) GetStats(context.Context) (*domain.JobStats, error)        { return nil, nil }
func (f *capFakeRepo) Count(context.Context) (int64, error)                      { return f.count, nil }
func (f *capFakeRepo) DeleteOlderThan(context.Context, time.Time) (int64, error) { return 0, nil }

func TestAtCap_DisabledWhenMaxZero(t *testing.T) {
	w := &ProcessorWorker{maxTotalJobs: 0}
	w.jobCount.Store(1_000_000)
	if w.atCap() {
		t.Fatal("cap disabled (max=0) must never report atCap")
	}
}

func TestAtCap_BoundaryAndRefresh(t *testing.T) {
	repo := &capFakeRepo{count: 5}
	w := &ProcessorWorker{repo: repo, maxTotalJobs: 5}

	w.refreshCount(context.Background())
	if got := w.jobCount.Load(); got != 5 {
		t.Fatalf("refreshCount: want 5, got %d", got)
	}
	if !w.atCap() {
		t.Fatal("count==max must report atCap")
	}

	// Simulate the retention cron dropping the count; next refresh re-opens inserts.
	repo.count = 3
	w.refreshCount(context.Background())
	if w.atCap() {
		t.Fatal("count<max must not report atCap after refresh")
	}
}

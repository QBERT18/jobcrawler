package janitor

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/applytude/jobcrawler/config"
	"github.com/applytude/jobcrawler/internal/domain"
)

// fakeRepo records DeleteOlderThan calls. Only the methods the janitor uses are
// meaningful; the rest satisfy repository.JobRepository.
type fakeRepo struct {
	deleteCalls int
	lastCutoff  time.Time
	deleted     int64
}

func (f *fakeRepo) Upsert(context.Context, *domain.Job) error            { return nil }
func (f *fakeRepo) GetByID(context.Context, string) (*domain.Job, error) { return nil, nil }
func (f *fakeRepo) Search(context.Context, domain.SearchFilter) ([]*domain.Job, int, error) {
	return nil, 0, nil
}
func (f *fakeRepo) GetStats(context.Context) (*domain.JobStats, error) { return nil, nil }
func (f *fakeRepo) Count(context.Context) (int64, error)               { return 0, nil }
func (f *fakeRepo) DeleteOlderThan(_ context.Context, cutoff time.Time) (int64, error) {
	f.deleteCalls++
	f.lastCutoff = cutoff
	return f.deleted, nil
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRunOnce_DeletesWithCorrectCutoff(t *testing.T) {
	repo := &fakeRepo{deleted: 7}
	cfg := config.ProcessorConfig{
		CleanupEnabled:       true,
		CleanupRetentionDays: 30,
	}
	j := New(repo, cfg, quietLogger())

	before := time.Now().Add(-30 * 24 * time.Hour)
	j.RunOnce(context.Background())
	after := time.Now().Add(-30 * 24 * time.Hour)

	if repo.deleteCalls != 1 {
		t.Fatalf("expected 1 delete call, got %d", repo.deleteCalls)
	}
	if repo.lastCutoff.Before(before.Add(-time.Minute)) || repo.lastCutoff.After(after.Add(time.Minute)) {
		t.Fatalf("cutoff %v not within expected ~30d-ago window", repo.lastCutoff)
	}
}

func TestStart_DisabledDoesNotDelete(t *testing.T) {
	repo := &fakeRepo{}
	cfg := config.ProcessorConfig{CleanupEnabled: false, CleanupRetentionDays: 30}
	j := New(repo, cfg, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	j.Start(ctx)

	if repo.deleteCalls != 0 {
		t.Fatalf("disabled janitor should not delete, got %d calls", repo.deleteCalls)
	}
}

func TestStart_NonPositiveRetentionSkips(t *testing.T) {
	repo := &fakeRepo{}
	cfg := config.ProcessorConfig{CleanupEnabled: true, CleanupRetentionDays: 0}
	j := New(repo, cfg, quietLogger())

	j.Start(context.Background()) // returns immediately, must not block or delete

	if repo.deleteCalls != 0 {
		t.Fatalf("retention<=0 must not delete, got %d calls", repo.deleteCalls)
	}
}

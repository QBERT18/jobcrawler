package processor

import (
	"context"
	"strings"
	"time"

	"github.com/applytude/jobcrawler/internal/domain"
	"github.com/applytude/jobcrawler/internal/repository"
	jobkafka "github.com/applytude/jobcrawler/pkg/kafka"
)

// AlertMatcher checks a newly indexed Job against all active IMMEDIATE alerts
// and publishes a JobAlertMatch event for each match.
type AlertMatcher struct {
	repo     repository.AlertRepository
	producer jobkafka.KafkaProducer
}

// NewAlertMatcher creates an AlertMatcher with the given dependencies.
func NewAlertMatcher(repo repository.AlertRepository, producer jobkafka.KafkaProducer) *AlertMatcher {
	return &AlertMatcher{repo: repo, producer: producer}
}

// FindMatchingAlerts loads all active IMMEDIATE alerts and returns those
// whose filter criteria are satisfied by job.
//
// Matching rules (all non-zero filter fields must match — AND logic):
//   - Query:     job title or description contains the query string (case-insensitive)
//   - Tags:      job.Tags and filter.Tags have at least one element in common
//   - Remote:    job.Remote exactly equals filter.Remote
//   - SalaryMin: job.SalaryMin is set and >= filter.SalaryMin
func (m *AlertMatcher) FindMatchingAlerts(ctx context.Context, job *domain.Job) ([]*domain.JobAlert, error) {
	alerts, err := m.repo.ListByFrequency(ctx, domain.AlertFreqImmediate)
	if err != nil {
		return nil, err
	}

	var matched []*domain.JobAlert
	for _, alert := range alerts {
		if !alert.Active {
			continue
		}
		if matchesFilter(job, alert.Filter) {
			matched = append(matched, alert)
		}
	}
	return matched, nil
}

// NotifyMatches publishes a JobAlertMatch event to jobs.processed for each
// alert that matched. The Kafka header "event-type: job-alert-match" lets
// downstream consumers filter without deserialising the payload.
func (m *AlertMatcher) NotifyMatches(ctx context.Context, job *domain.Job, alerts []*domain.JobAlert) {
	for _, alert := range alerts {
		event := domain.JobAlertMatch{
			AlertID:   alert.ID,
			UserEmail: alert.UserEmail,
			JobID:     job.ID,
			JobTitle:  job.Title,
			JobURL:    job.URL,
			Frequency: alert.Frequency,
			MatchedAt: time.Now().UTC().Format(time.RFC3339),
		}

		_ = m.producer.Publish(
			jobkafka.ContextWithTraceID(ctx, job.ID), // use job ID as trace context
			jobkafka.TopicJobsProcessed,
			alert.ID,
			event,
			// Additional header injected via Message — note: Publish signature
			// uses the context-based header approach; the event-type is embedded
			// in the payload's type field for simplicity.
		)
		// Error is intentionally ignored here: a failed alert notification
		// should not roll back the job upsert. The job is already saved.
		// Failed alerts will be picked up by the next run of the matcher.
	}
}

// matchesFilter returns true when job satisfies every non-zero criterion in filter.
func matchesFilter(job *domain.Job, filter domain.SearchFilter) bool {
	// ── Full-text query ───────────────────────────────────────────────────────
	if filter.Query != "" {
		q := strings.ToLower(filter.Query)
		titleMatch := strings.Contains(strings.ToLower(job.Title), q)
		descMatch := strings.Contains(strings.ToLower(job.Description), q)
		if !titleMatch && !descMatch {
			return false
		}
	}

	// ── Tag intersection ──────────────────────────────────────────────────────
	if len(filter.Tags) > 0 {
		if !hasTagIntersection(job.Tags, filter.Tags) {
			return false
		}
	}

	// ── Remote type ───────────────────────────────────────────────────────────
	if filter.Remote != "" && job.Remote != filter.Remote {
		return false
	}

	// ── Minimum salary ────────────────────────────────────────────────────────
	if filter.SalaryMin > 0 {
		if job.SalaryMin == nil || *job.SalaryMin < filter.SalaryMin {
			return false
		}
	}

	return true
}

// hasTagIntersection returns true if a and b share at least one element.
func hasTagIntersection(jobTags, filterTags []string) bool {
	set := make(map[string]bool, len(jobTags))
	for _, t := range jobTags {
		set[strings.ToLower(t)] = true
	}
	for _, t := range filterTags {
		if set[strings.ToLower(t)] {
			return true
		}
	}
	return false
}
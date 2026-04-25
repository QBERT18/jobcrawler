package domain

import "time"

// Alert frequency constants define how often a user receives alert emails.
const (
	AlertFreqImmediate = "IMMEDIATE" // Email sent as soon as a matching job is found
	AlertFreqDaily     = "DAILY"     // Batched daily digest at 08:00
	AlertFreqWeekly    = "WEEKLY"    // Batched weekly digest on Monday
)

// JobAlert represents a saved search that notifies a user when new matching
// jobs are indexed. The Filter field reuses SearchFilter so matching logic
// is identical to the API search endpoint.
type JobAlert struct {
	ID        string
	UserEmail string
	Filter    SearchFilter // Same filter struct used by the API — no duplication
	Frequency string       // AlertFreqImmediate | AlertFreqDaily | AlertFreqWeekly
	Active    bool
	CreatedAt time.Time
}

// JobAlertMatch is the event payload published to jobs.processed when a
// new job matches a saved alert. Consumers (e.g. email service) subscribe
// to jobs.processed and filter for this event type using the Kafka header.
type JobAlertMatch struct {
	AlertID   string `json:"alert_id"`
	UserEmail string `json:"user_email"`
	JobID     string `json:"job_id"`
	JobTitle  string `json:"job_title"`
	JobURL    string `json:"job_url"`
	Frequency string `json:"frequency"`
	MatchedAt string `json:"matched_at"` // RFC3339
}
package domain

import "time"

// Job is the central domain entity — a normalised job listing
// aggregated from one or more external sources.
type Job struct {
	ID             string
	ExternalID     string    // ID on the originating source platform
	Source         JobSource // Which platform this job came from
	Title          string
	Company        Company
	Location       Location
	Remote         RemoteType
	SalaryMin      *int   // nil when not advertised
	SalaryMax      *int   // nil when not advertised
	SalaryCurrency string // ISO 4217, e.g. "EUR"
	Description    string
	Tags           []string // Extracted tech keywords, e.g. ["golang","kubernetes"]
	PublishedAt    time.Time
	ExpiresAt      *time.Time // nil when not specified by source
	URL            string     // Canonical URL on the source platform
	Fingerprint    string     // SHA-256 of normalised title+company+location for dedup
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
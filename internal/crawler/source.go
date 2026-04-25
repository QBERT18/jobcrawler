package crawler

import "github.com/applytude/jobcrawler/internal/domain"

// RawJob holds the unprocessed fields extracted from a detail page.
// All fields are strings — the Processor normalises types (dates, salaries, etc.).
type RawJob struct {
	Title       string
	Company     string
	Location    string
	SalaryText  string
	Description string
	URL         string
	ExternalID  string
	PublishedAt string
}

// Source is the interface every site-specific crawler must implement.
// ParseListing discovers job detail URLs from a listing page.
// ParseDetail extracts structured data from a single job page.
type Source interface {
	Name() domain.JobSource
	ParseListing(html []byte) ([]string, error)
	ParseDetail(html []byte) (*RawJob, error)
}
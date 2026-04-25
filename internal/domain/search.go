package domain

// SearchFilter holds every filter and pagination parameter accepted by
// the jobs search endpoint. Zero values mean "no filter applied".
type SearchFilter struct {
	Query     string     // Full-text search term
	Location  string     // City or region
	Remote    RemoteType // FULL | PARTIAL | NONE — empty = all
	Tags      []string   // Tech keywords, AND logic
	SalaryMin int        // 0 = no minimum
	Sources   []JobSource
	Page      int    // 1-based
	PerPage   int    // default 25, max 100
	Sort      string // e.g. "published_at:desc"
}

// SearchResult is the paginated response returned by the jobs search.
type SearchResult struct {
	Jobs       []*Job `json:"jobs"`
	Total      int    `json:"total"`
	Page       int    `json:"page"`
	PerPage    int    `json:"per_page"`
	TotalPages int    `json:"total_pages"`
}

// TagCount holds a tag label and how many jobs carry it.
type TagCount struct {
	Tag   string `json:"tag"`
	Count int    `json:"count"`
}

// JobStats is the market-overview payload returned by GET /jobs/stats.
type JobStats struct {
	TotalJobs int            `json:"total_jobs"`
	BySource  map[string]int `json:"by_source"`
	ByRemote  map[string]int `json:"by_remote"`
	TopTags   []TagCount     `json:"top_tags"`
	AvgSalary *float64       `json:"avg_salary,omitempty"`
}
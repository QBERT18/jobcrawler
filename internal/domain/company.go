package domain

// Company holds the employer information attached to a Job.
type Company struct {
	ID      string // Internal UUID (may be empty until enriched)
	Name    string
	Website string
	LogoURL string
}
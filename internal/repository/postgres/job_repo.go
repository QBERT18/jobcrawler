package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/applytude/jobcrawler/internal/domain"
	"github.com/lib/pq"
)

// PostgresJobRepo implements repository.JobRepository against a PostgreSQL database.
type PostgresJobRepo struct {
	db *sql.DB
}

// NewJobRepo creates a PostgresJobRepo with the given connection pool.
func NewJobRepo(db *sql.DB) *PostgresJobRepo {
	return &PostgresJobRepo{db: db}
}

// Upsert inserts a new job or, on fingerprint conflict, updates only the
// mutable fields (url, updated_at). All other fields are set at first insert
// and never overwritten — preserving the original published_at, description, etc.
func (r *PostgresJobRepo) Upsert(ctx context.Context, job *domain.Job) error {
	const q = `
		INSERT INTO jobs (
			external_id, source, title,
			company_name, company_website,
			location_city, location_country,
			remote_type,
			salary_min, salary_max, salary_currency,
			description, tags, url, fingerprint,
			published_at, expires_at
		) VALUES (
			$1,  $2,  $3,
			$4,  $5,
			$6,  $7,
			$8,
			$9,  $10, $11,
			$12, $13, $14, $15,
			$16, $17
		)
		ON CONFLICT (fingerprint) DO UPDATE SET
			url        = EXCLUDED.url,
			updated_at = NOW()
		RETURNING id, created_at, updated_at`

	var (
		salaryMin *int
		salaryMax *int
	)
	if job.SalaryMin != nil {
		salaryMin = job.SalaryMin
	}
	if job.SalaryMax != nil {
		salaryMax = job.SalaryMax
	}

	row := r.db.QueryRowContext(ctx, q,
		nullString(truncate(job.ExternalID, 255)),
		string(job.Source),
		truncate(job.Title, 500),
		nullString(truncate(job.Company.Name, 255)),
		nullString(truncate(job.Company.Website, 500)),
		nullString(truncate(job.Location.City, 255)),
		nullString(truncate(job.Location.Country, 100)),
		string(job.Remote),
		salaryMin,
		salaryMax,
		nullString(job.SalaryCurrency),
		nullString(job.Description),
		pq.Array(job.Tags),
		truncate(job.URL, 1000),
		job.Fingerprint,
		nullTime(job.PublishedAt),
		job.ExpiresAt,
	)

	return row.Scan(&job.ID, &job.CreatedAt, &job.UpdatedAt)
}

// GetByID retrieves a single job by its internal UUID.
func (r *PostgresJobRepo) GetByID(ctx context.Context, id string) (*domain.Job, error) {
	const q = `
		SELECT
			id, external_id, source, title,
			company_name, company_website,
			location_city, location_country,
			remote_type,
			salary_min, salary_max, salary_currency,
			description, tags, url, fingerprint,
			published_at, expires_at, created_at, updated_at
		FROM jobs
		WHERE id = $1`

	row := r.db.QueryRowContext(ctx, q, id)
	job, err := scanJob(row)
	if err == sql.ErrNoRows {
		return nil, domain.ErrNotFound
	}
	return job, err
}

// Search builds a dynamic WHERE clause from the provided filter and returns
// a paginated result set plus the total count for pagination metadata.
func (r *PostgresJobRepo) Search(ctx context.Context, filter domain.SearchFilter) ([]*domain.Job, int, error) {
	// ── Build WHERE clause dynamically ────────────────────────────────────────
	var (
		conditions []string
		args       []any
		argIdx     = 1
	)

	addArg := func(condition string, value any) {
		conditions = append(conditions, fmt.Sprintf(condition, argIdx))
		args = append(args, value)
		argIdx++
	}

	if filter.Query != "" {
		// Full-text search on title and description using PostgreSQL tsvector.
		addArg("(to_tsvector('german', title || ' ' || COALESCE(description,'')) @@ plainto_tsquery('german', $%d))", filter.Query)
	}
	if filter.Location != "" {
		addArg("location_city ILIKE $%d", "%"+filter.Location+"%")
	}
	if filter.Remote != "" {
		addArg("remote_type = $%d", string(filter.Remote))
	}
	if filter.SalaryMin > 0 {
		addArg("salary_min >= $%d", filter.SalaryMin)
	}
	if len(filter.Sources) > 0 {
		placeholders := make([]string, len(filter.Sources))
		for i, s := range filter.Sources {
			placeholders[i] = fmt.Sprintf("$%d", argIdx)
			args = append(args, string(s))
			argIdx++
		}
		conditions = append(conditions, "source IN ("+strings.Join(placeholders, ",")+")")
	}
	if len(filter.Tags) > 0 {
		addArg("tags @> $%d", pq.Array(filter.Tags))
	}

	where := ""
	if len(conditions) > 0 {
		where = "WHERE " + strings.Join(conditions, " AND ")
	}

	// ── Count query ───────────────────────────────────────────────────────────
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM jobs %s", where)
	var total int
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("search count: %w", err)
	}

	// ── Pagination ────────────────────────────────────────────────────────────
	page := filter.Page
	if page < 1 {
		page = 1
	}
	perPage := filter.PerPage
	if perPage < 1 {
		perPage = 25
	}
	if perPage > 100 {
		perPage = 100
	}

	offset := (page - 1) * perPage

	// ── Sort ──────────────────────────────────────────────────────────────────
	orderBy := "published_at DESC"
	if filter.Sort != "" {
		parts := strings.SplitN(filter.Sort, ":", 2)
		allowedCols := map[string]bool{
			"published_at": true, "created_at": true,
			"salary_min": true, "title": true,
		}
		if allowedCols[parts[0]] {
			dir := "DESC"
			if len(parts) == 2 && strings.ToUpper(parts[1]) == "ASC" {
				dir = "ASC"
			}
			orderBy = parts[0] + " " + dir
		}
	}

	dataQuery := fmt.Sprintf(`
		SELECT
			id, external_id, source, title,
			company_name, company_website,
			location_city, location_country,
			remote_type,
			salary_min, salary_max, salary_currency,
			description, tags, url, fingerprint,
			published_at, expires_at, created_at, updated_at
		FROM jobs
		%s
		ORDER BY %s
		LIMIT $%d OFFSET $%d`,
		where, orderBy, argIdx, argIdx+1,
	)
	args = append(args, perPage, offset)

	rows, err := r.db.QueryContext(ctx, dataQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("search query: %w", err)
	}
	defer rows.Close()

	var jobs []*domain.Job
	for rows.Next() {
		job, err := scanJobRow(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("search scan: %w", err)
		}
		jobs = append(jobs, job)
	}

	return jobs, total, rows.Err()
}

// GetStats returns aggregate statistics across all jobs in the database.
func (r *PostgresJobRepo) GetStats(ctx context.Context) (*domain.JobStats, error) {
	stats := &domain.JobStats{
		BySource: make(map[string]int),
		ByRemote: make(map[string]int),
	}

	// Total count
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs").Scan(&stats.TotalJobs); err != nil {
		return nil, fmt.Errorf("stats total: %w", err)
	}

	// By source
	rows, err := r.db.QueryContext(ctx, "SELECT source, COUNT(*) FROM jobs GROUP BY source")
	if err != nil {
		return nil, fmt.Errorf("stats by source: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var src string
		var cnt int
		if err := rows.Scan(&src, &cnt); err != nil {
			return nil, err
		}
		stats.BySource[src] = cnt
	}

	// By remote type
	rows2, err := r.db.QueryContext(ctx, "SELECT remote_type, COUNT(*) FROM jobs GROUP BY remote_type")
	if err != nil {
		return nil, fmt.Errorf("stats by remote: %w", err)
	}
	defer rows2.Close()
	for rows2.Next() {
		var rt string
		var cnt int
		if err := rows2.Scan(&rt, &cnt); err != nil {
			return nil, err
		}
		stats.ByRemote[rt] = cnt
	}

	// Average salary (only where both min and max are present)
	var avg sql.NullFloat64
	if err := r.db.QueryRowContext(ctx,
		"SELECT AVG((salary_min + salary_max) / 2.0) FROM jobs WHERE salary_min IS NOT NULL AND salary_max IS NOT NULL",
	).Scan(&avg); err != nil {
		return nil, fmt.Errorf("stats avg salary: %w", err)
	}
	if avg.Valid {
		stats.AvgSalary = &avg.Float64
	}

	// Top 10 tags — unnest the tags array and count occurrences
	rows3, err := r.db.QueryContext(ctx, `
		SELECT tag, COUNT(*) AS cnt
		FROM jobs, unnest(tags) AS tag
		GROUP BY tag
		ORDER BY cnt DESC
		LIMIT 10
	`)
	if err != nil {
		return nil, fmt.Errorf("stats top tags: %w", err)
	}
	defer rows3.Close()
	for rows3.Next() {
		var tc domain.TagCount
		if err := rows3.Scan(&tc.Tag, &tc.Count); err != nil {
			return nil, err
		}
		stats.TopTags = append(stats.TopTags, tc)
	}

	return stats, nil
}

// ── Scan helpers ──────────────────────────────────────────────────────────────

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(row *sql.Row) (*domain.Job, error) {
	return scanJobFromScanner(row)
}

func scanJobRow(rows *sql.Rows) (*domain.Job, error) {
	return scanJobFromScanner(rows)
}

func scanJobFromScanner(s scanner) (*domain.Job, error) {
	var (
		job         domain.Job
		externalID  sql.NullString
		companyName sql.NullString
		companyWeb  sql.NullString
		locCity     sql.NullString
		locCountry  sql.NullString
		salMin      sql.NullInt32
		salMax      sql.NullInt32
		salCur      sql.NullString
		description sql.NullString
		publishedAt sql.NullTime
		expiresAt   sql.NullTime
		tags        pq.StringArray
	)

	err := s.Scan(
		&job.ID, &externalID, &job.Source, &job.Title,
		&companyName, &companyWeb,
		&locCity, &locCountry,
		&job.Remote,
		&salMin, &salMax, &salCur,
		&description, &tags, &job.URL, &job.Fingerprint,
		&publishedAt, &expiresAt, &job.CreatedAt, &job.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	job.ExternalID = externalID.String
	job.Company = domain.Company{
		Name:    companyName.String,
		Website: companyWeb.String,
	}
	job.Location = domain.Location{
		City:    locCity.String,
		Country: locCountry.String,
	}
	if salMin.Valid {
		v := int(salMin.Int32)
		job.SalaryMin = &v
	}
	if salMax.Valid {
		v := int(salMax.Int32)
		job.SalaryMax = &v
	}
	job.SalaryCurrency = salCur.String
	job.Description = description.String
	job.Tags = []string(tags)
	if publishedAt.Valid {
		job.PublishedAt = publishedAt.Time
	}
	if expiresAt.Valid {
		job.ExpiresAt = &expiresAt.Time
	}

	return &job, nil
}

// ── SQL null helpers ──────────────────────────────────────────────────────────

func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func nullTime(t time.Time) sql.NullTime {
	return sql.NullTime{Time: t, Valid: !t.IsZero()}
}

// truncate caps s at n runes (not bytes) so UTF-8 multibyte sequences — common
// in German job text — never split mid-character and produce invalid UTF-8.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
-- Individual column indexes for common filter predicates
CREATE INDEX IF NOT EXISTS idx_jobs_source
    ON jobs (source);

CREATE INDEX IF NOT EXISTS idx_jobs_remote_type
    ON jobs (remote_type);

CREATE INDEX IF NOT EXISTS idx_jobs_published_at
    ON jobs (published_at DESC NULLS LAST);

-- Composite index for the most common query pattern: filter by source, order by date
CREATE INDEX IF NOT EXISTS idx_jobs_source_published_at
    ON jobs (source, published_at DESC NULLS LAST);

-- GIN index for array containment queries: tags @> ARRAY['golang','kubernetes']
CREATE INDEX IF NOT EXISTS idx_jobs_tags_gin
    ON jobs USING GIN (tags);

-- Full-text search index using German dictionary
-- Allows: WHERE to_tsvector('german', title || ' ' || description) @@ plainto_tsquery('german', ?)
CREATE INDEX IF NOT EXISTS idx_jobs_fts
    ON jobs USING GIN (
        to_tsvector('german', title || ' ' || COALESCE(description, ''))
    );

-- Partial index for jobs that currently have an expiry set. We can't use
-- NOW() in the predicate — Postgres requires index predicates to be IMMUTABLE
-- and NOW() is STABLE. Callers still do `expires_at IS NULL OR expires_at > NOW()`
-- in the WHERE clause; this index accelerates the non-null case with a plain
-- b-tree on (expires_at, published_at) which the planner combines with the
-- date comparison at query time.
CREATE INDEX IF NOT EXISTS idx_jobs_active
    ON jobs (expires_at, published_at DESC)
    WHERE expires_at IS NOT NULL;
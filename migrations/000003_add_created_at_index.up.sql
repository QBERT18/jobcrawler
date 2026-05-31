-- Index supporting the retention cleanup cron (DELETE FROM jobs WHERE created_at < cutoff).
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at);

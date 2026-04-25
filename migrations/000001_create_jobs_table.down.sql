DROP TRIGGER IF EXISTS jobs_updated_at ON jobs;
DROP FUNCTION IF EXISTS update_updated_at_column();
DROP TABLE IF EXISTS jobs;
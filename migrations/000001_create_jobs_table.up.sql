-- Enable the pgcrypto extension for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS jobs (
    -- Identity
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id     VARCHAR(255),

    -- Source metadata
    source          VARCHAR(50)  NOT NULL
                    CHECK (source IN ('STEPSTONE','INDEED','XING','LINKEDIN')),

    -- Core job data
    title           VARCHAR(500) NOT NULL,
    company_name    VARCHAR(255),
    company_website VARCHAR(500),

    -- Location
    location_city   VARCHAR(255),
    location_country VARCHAR(100),
    remote_type     VARCHAR(20)  NOT NULL DEFAULT 'NONE'
                    CHECK (remote_type IN ('FULL','PARTIAL','NONE')),

    -- Compensation
    salary_min      INTEGER      CHECK (salary_min >= 0),
    salary_max      INTEGER      CHECK (salary_max >= 0),
    salary_currency VARCHAR(3)   DEFAULT 'EUR',

    -- Content
    description     TEXT,
    tags            TEXT[]       NOT NULL DEFAULT '{}',
    url             VARCHAR(1000) NOT NULL,

    -- Deduplication
    fingerprint     VARCHAR(64)  NOT NULL,

    -- Dates
    published_at    TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT jobs_fingerprint_unique UNIQUE (fingerprint),
    CONSTRAINT jobs_salary_range CHECK (
        salary_max IS NULL OR salary_min IS NULL OR salary_max >= salary_min
    )
);

-- Trigger: automatically update updated_at on every row update
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
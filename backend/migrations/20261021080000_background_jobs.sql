-- Add migration script here

CREATE TABLE background_jobs (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id       UUID NOT NULL REFERENCES uploads(id),
    job_type      TEXT NOT NULL CHECK (job_type IN ('verify_apk', 'prepare_distribution')),
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    attempts      INTEGER NOT NULL DEFAULT 0,
    available_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until  TIMESTAMPTZ,
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ
);

CREATE INDEX idx_background_jobs_pending
    ON background_jobs (available_at, created_at)
    WHERE status = 'pending';

-- Optional: index for finding jobs by file_id
CREATE INDEX idx_background_jobs_file_id
    ON background_jobs (file_id);
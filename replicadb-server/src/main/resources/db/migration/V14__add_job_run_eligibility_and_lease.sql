ALTER TABLE job_run
    ADD COLUMN available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN lease_token UUID;

DROP INDEX idx_job_run_pending;

CREATE INDEX idx_job_run_eligible
    ON job_run (available_at, created_at, id)
    WHERE status = 'PENDING';

CREATE TABLE job_run (
    id UUID PRIMARY KEY,
    job_definition_id UUID NOT NULL REFERENCES job_definition(id),
    previous_run_id UUID REFERENCES job_run(id),
    status VARCHAR(32) NOT NULL CHECK (status IN (
        'PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED',
        'CANCEL_REQUESTED', 'CANCELLED', 'RETRY_SCHEDULED'
    )),
    attempt INTEGER NOT NULL DEFAULT 1 CHECK (attempt > 0),
    executor_identity VARCHAR(255),
    lease_until TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    rows_processed BIGINT,
    duration_millis BIGINT,
    committed_watermark TEXT,
    error_message TEXT
);

CREATE INDEX idx_job_run_pending
    ON job_run (created_at)
    WHERE status = 'PENDING';
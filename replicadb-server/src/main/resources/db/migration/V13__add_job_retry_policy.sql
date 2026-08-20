ALTER TABLE job_definition
    ADD COLUMN max_attempts INTEGER,
    ADD COLUMN retry_backoff_seconds BIGINT,
    ADD COLUMN automatic_retry_enabled BOOLEAN;

UPDATE job_definition
SET max_attempts = 3,
    retry_backoff_seconds = 60,
    automatic_retry_enabled = mode IN ('incremental', 'complete-atomic');

ALTER TABLE job_definition
    ALTER COLUMN max_attempts SET DEFAULT 3,
    ALTER COLUMN retry_backoff_seconds SET DEFAULT 60,
    ALTER COLUMN automatic_retry_enabled SET DEFAULT false,
    ALTER COLUMN max_attempts SET NOT NULL,
    ALTER COLUMN retry_backoff_seconds SET NOT NULL,
    ALTER COLUMN automatic_retry_enabled SET NOT NULL,
    ADD CONSTRAINT ck_job_definition_max_attempts CHECK (max_attempts > 0),
    ADD CONSTRAINT ck_job_definition_retry_backoff_seconds CHECK (retry_backoff_seconds >= 0);

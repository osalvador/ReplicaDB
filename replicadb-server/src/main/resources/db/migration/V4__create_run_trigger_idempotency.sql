CREATE TABLE run_trigger_idempotency (
    idempotency_key VARCHAR(255) PRIMARY KEY,
    job_definition_id UUID NOT NULL,
    run_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_run_trigger_idempotency_created_at
    ON run_trigger_idempotency (created_at);

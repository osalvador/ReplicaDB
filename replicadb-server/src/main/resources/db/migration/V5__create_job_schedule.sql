CREATE TABLE job_schedule (
    job_definition_id UUID PRIMARY KEY REFERENCES job_definition(id) ON DELETE CASCADE,
    cron_expression VARCHAR(120) NOT NULL,
    time_zone VARCHAR(64) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
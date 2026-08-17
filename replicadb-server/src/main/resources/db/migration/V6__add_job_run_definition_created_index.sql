CREATE INDEX idx_job_run_definition_created
    ON job_run (job_definition_id, created_at DESC);

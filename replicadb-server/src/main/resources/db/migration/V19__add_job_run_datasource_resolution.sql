ALTER TABLE job_run
    ADD COLUMN resolved_source_datasource_id UUID,
    ADD COLUMN resolved_sink_datasource_id UUID,
    ADD COLUMN datasources_resolved_at TIMESTAMPTZ;

CREATE INDEX idx_job_run_resolved_source_datasource
    ON job_run (resolved_source_datasource_id)
    WHERE resolved_source_datasource_id IS NOT NULL;

CREATE INDEX idx_job_run_resolved_sink_datasource
    ON job_run (resolved_sink_datasource_id)
    WHERE resolved_sink_datasource_id IS NOT NULL;

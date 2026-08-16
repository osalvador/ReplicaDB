CREATE UNIQUE INDEX ux_job_run_one_active_per_definition
    ON job_run (job_definition_id)
    WHERE status IN ('PENDING', 'RUNNING', 'CANCEL_REQUESTED');

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM run_trigger_idempotency idempotency
        LEFT JOIN job_definition definition ON definition.id = idempotency.job_definition_id
        WHERE definition.id IS NULL
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23503',
            MESSAGE = 'Cannot add job deletion cascade: run_trigger_idempotency contains orphaned job references';
    END IF;
END $$;

ALTER TABLE job_run
    DROP CONSTRAINT IF EXISTS job_run_job_definition_id_fkey,
    ADD CONSTRAINT fk_job_run_job_definition
        FOREIGN KEY (job_definition_id) REFERENCES job_definition(id) ON DELETE CASCADE;

ALTER TABLE run_trigger_idempotency
    ADD CONSTRAINT fk_run_trigger_idempotency_job_definition
        FOREIGN KEY (job_definition_id) REFERENCES job_definition(id) ON DELETE CASCADE;

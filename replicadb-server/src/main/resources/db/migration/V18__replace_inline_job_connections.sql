DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM job_definition)
            OR EXISTS (SELECT 1 FROM job_run)
            OR EXISTS (SELECT 1 FROM job_schedule)
            OR EXISTS (SELECT 1 FROM job_permission)
            OR EXISTS (SELECT 1 FROM run_trigger_idempotency) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'Phase 4 requires an empty managed metadata state; reset job_definition and related managed tables before migrating';
    END IF;
END $$;

ALTER TABLE job_definition
    ADD COLUMN source_datasource_id UUID NOT NULL
        REFERENCES managed_datasource(id) ON DELETE RESTRICT,
    ADD COLUMN sink_datasource_id UUID NOT NULL
        REFERENCES managed_datasource(id) ON DELETE RESTRICT,
    ADD COLUMN source_datasource_use_enabled BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN sink_datasource_use_enabled BOOLEAN NOT NULL DEFAULT true,
    DROP COLUMN source_connect,
    DROP COLUMN source_user,
    DROP COLUMN source_password,
    DROP COLUMN source_auth_mode,
    DROP COLUMN source_auth_principal_id,
    DROP COLUMN source_auth_login_hint,
    DROP COLUMN source_auth_client_certificate,
    DROP COLUMN source_auth_client_key,
    DROP COLUMN source_connection_params,
    DROP COLUMN sink_connect,
    DROP COLUMN sink_user,
    DROP COLUMN sink_password,
    DROP COLUMN sink_auth_mode,
    DROP COLUMN sink_auth_principal_id,
    DROP COLUMN sink_auth_login_hint,
    DROP COLUMN sink_auth_client_certificate,
    DROP COLUMN sink_auth_client_key,
    DROP COLUMN sink_connection_params;

CREATE INDEX idx_job_definition_source_datasource
    ON job_definition (source_datasource_id);

CREATE INDEX idx_job_definition_sink_datasource
    ON job_definition (sink_datasource_id);

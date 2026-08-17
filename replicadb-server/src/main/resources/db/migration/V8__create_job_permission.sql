CREATE TABLE job_permission (
    job_definition_id UUID NOT NULL REFERENCES job_definition(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
    permission VARCHAR(20) NOT NULL CHECK (permission IN ('VIEW', 'EDIT', 'EXECUTE', 'CANCEL')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_definition_id, user_id, permission)
);

CREATE INDEX idx_job_permission_user ON job_permission (user_id, permission);

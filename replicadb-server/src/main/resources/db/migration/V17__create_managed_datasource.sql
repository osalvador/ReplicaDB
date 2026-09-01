CREATE TABLE managed_datasource (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    connector_type VARCHAR(64) NOT NULL,
    safe_connect_display TEXT NOT NULL,
    technical_params JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(technical_params) = 'object'),
    encrypted_security BYTEA NOT NULL,
    security_format_version INTEGER NOT NULL CHECK (security_format_version > 0),
    encryption_algorithm VARCHAR(64) NOT NULL,
    key_version VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE datasource_permission (
    datasource_id UUID NOT NULL REFERENCES managed_datasource(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
    permission VARCHAR(20) NOT NULL CHECK (permission IN ('VIEW', 'USE', 'EDIT')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (datasource_id, user_id, permission)
);

CREATE INDEX idx_datasource_permission_user
    ON datasource_permission (user_id, permission);

CREATE TABLE audit_event (
    id UUID PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor_user_id UUID REFERENCES app_user(id) ON DELETE SET NULL,
    actor_username VARCHAR(100) NOT NULL,
    source_address VARCHAR(45),
    action VARCHAR(60) NOT NULL,
    resource_type VARCHAR(30) NOT NULL,
    resource_id VARCHAR(64),
    outcome VARCHAR(10) NOT NULL CHECK (outcome IN ('SUCCESS', 'FAILURE')),
    detail JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_audit_event_occurred_at ON audit_event (occurred_at DESC);
CREATE INDEX idx_audit_event_actor ON audit_event (actor_user_id, occurred_at DESC);
CREATE INDEX idx_audit_event_action ON audit_event (action, occurred_at DESC);
CREATE INDEX idx_audit_event_resource ON audit_event (resource_type, resource_id, occurred_at DESC);

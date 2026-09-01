CREATE TABLE run_log (
    run_id UUID PRIMARY KEY REFERENCES job_run(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    truncated BOOLEAN NOT NULL DEFAULT FALSE,
    captured_size INTEGER NOT NULL CHECK (captured_size >= 0),
    format_version INTEGER NOT NULL CHECK (format_version > 0),
    captured_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT run_log_content_size CHECK (octet_length(content) <= 262144)
);

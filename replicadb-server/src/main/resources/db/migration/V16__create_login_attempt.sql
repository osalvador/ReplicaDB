CREATE TABLE login_attempt
(
    reservation_id UUID        NOT NULL,
    throttle_key   TEXT        NOT NULL,
    status         VARCHAR(16) NOT NULL CHECK (status IN ('PENDING', 'FAILED')),
    attempted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (reservation_id, throttle_key)
);

CREATE INDEX idx_login_attempt_key_time
    ON login_attempt (throttle_key, attempted_at);

CREATE INDEX idx_login_attempt_attempted_at
    ON login_attempt (attempted_at);

CREATE TABLE IF NOT EXISTS phase3_source (
    id BIGINT PRIMARY KEY,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS phase3_sink (
    id BIGINT PRIMARY KEY,
    payload TEXT NOT NULL
);

TRUNCATE TABLE phase3_source, phase3_sink;

INSERT INTO phase3_source (id, payload)
VALUES (1, 'compose-one'), (2, 'compose-two'), (3, 'compose-three');

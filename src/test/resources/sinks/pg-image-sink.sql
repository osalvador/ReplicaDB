-- PostgreSQL BYTEA sink table for Issue #202 reproduction
-- Receives IMAGE data from SQL Server (expected as binary via BYTEA type)

-- Drop table if exists (for repeatable test execution)
DROP TABLE IF EXISTS t_image_sink;

-- Create table with BYTEA column (PostgreSQL binary type)
CREATE TABLE t_image_sink (
    id INTEGER PRIMARY KEY,
    image_col BYTEA,                    -- PostgreSQL binary type for BLOB storage
    image_size INTEGER,
    description VARCHAR(255)
);

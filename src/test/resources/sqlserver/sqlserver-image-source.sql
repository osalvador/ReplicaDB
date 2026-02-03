-- SQL Server IMAGE source table for Issue #202 reproduction
-- Tests deprecated IMAGE type replication to PostgreSQL BYTEA
-- Note: GO statements removed for JDBC compatibility - using separate INSERT statements instead

-- Drop table if exists (for repeatable test execution)
IF OBJECT_ID('t_image_source', 'U') IS NOT NULL DROP TABLE t_image_source;

-- Create table with IMAGE column (deprecated SQL Server binary type)
CREATE TABLE t_image_source (
    id INT PRIMARY KEY IDENTITY(1,1),
    image_col IMAGE,
    image_size INT,
    description VARCHAR(255)
);

-- Test row 1: Small binary (1KB) - using simple INSERT with CAST
INSERT INTO t_image_source (image_col, image_size, description)
VALUES (CAST(REPLICATE(CAST('A' AS VARBINARY(MAX)), 1024) AS IMAGE), 1024, 'Small 1KB image');

-- Test row 2: Medium binary (100KB)
INSERT INTO t_image_source (image_col, image_size, description)
VALUES (CAST(REPLICATE(CAST('TESTDATA' AS VARBINARY(MAX)), 12800) AS IMAGE), 102400, 'Medium 100KB image');

-- Test row 3: Large binary (1.5MB)
INSERT INTO t_image_source (image_col, image_size, description)
VALUES (CAST(REPLICATE(CAST('BINARYDATA' AS VARBINARY(MAX)), 160000) AS IMAGE), 1600000, 'Large 1.5MB image');

-- Test row 4: NULL value (edge case)
INSERT INTO t_image_source (image_col, image_size, description)
VALUES (NULL, 0, 'NULL image test');

-- Test row 5: Binary with special bytes (0x00, 0xFF)
INSERT INTO t_image_source (image_col, image_size, description)
VALUES (CAST(REPLICATE(CAST(0x00FF AS VARBINARY(MAX)), 512000) AS IMAGE), 1024000, 'Special bytes 1MB image');

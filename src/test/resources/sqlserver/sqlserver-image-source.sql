-- SQL Server IMAGE source table for Issue #202 reproduction
-- Tests deprecated IMAGE type replication to PostgreSQL BYTEA

-- Drop table if exists (for repeatable test execution)
IF OBJECT_ID('t_image_source', 'U') IS NOT NULL
DROP TABLE t_image_source;
GO

-- Create table with IMAGE column (deprecated SQL Server binary type)
CREATE TABLE t_image_source (
    id INT PRIMARY KEY IDENTITY(1,1),
    image_col IMAGE,                    -- Deprecated binary type (replaced by varbinary(max) in SQL Server 2005)
    image_size INT,
    description VARCHAR(255)
);
GO

-- Test row 1: Small binary (1KB)
DECLARE @smallBinary VARBINARY(MAX);
SET @smallBinary = REPLICATE(CONVERT(VARBINARY(MAX), 'A'), 1024);
INSERT INTO t_image_source (image_col, image_size, description)
VALUES (CONVERT(IMAGE, @smallBinary), DATALENGTH(@smallBinary), 'Small 1KB image');
GO

-- Test row 2: Medium binary (100KB)
DECLARE @mediumBinary VARBINARY(MAX);
SET @mediumBinary = REPLICATE(CONVERT(VARBINARY(MAX), 'TESTDATA'), 12800);  -- 12800 * 8 bytes = 102,400 bytes
INSERT INTO t_image_source (image_col, image_size, description)
VALUES (CONVERT(IMAGE, @mediumBinary), DATALENGTH(@mediumBinary), 'Medium 100KB image');
GO

-- Test row 3: Large binary (1.5MB)
DECLARE @largeBinary VARBINARY(MAX);
SET @largeBinary = REPLICATE(CONVERT(VARBINARY(MAX), 'BINARYDATA'), 160000);  -- 160000 * 10 bytes = 1,600,000 bytes
INSERT INTO t_image_source (image_col, image_size, description)
VALUES (CONVERT(IMAGE, @largeBinary), DATALENGTH(@largeBinary), 'Large 1.5MB image');
GO

-- Test row 4: NULL value (edge case)
INSERT INTO t_image_source (image_col, image_size, description)
VALUES (NULL, 0, 'NULL image test');
GO

-- Test row 5: Binary with special bytes (0x00, 0xFF)
DECLARE @specialBinary VARBINARY(MAX);
SET @specialBinary = REPLICATE(CONVERT(VARBINARY(MAX), 0x00FF), 512000);  -- 512000 * 2 bytes = 1,024,000 bytes
INSERT INTO t_image_source (image_col, image_size, description)
VALUES (CONVERT(IMAGE, @specialBinary), DATALENGTH(@specialBinary), 'Special bytes 1MB image');
GO

-- Verify data inserted
SELECT id, DATALENGTH(image_col) as actual_size, image_size as expected_size, description
FROM t_image_source
ORDER BY id;
GO

-- LOB test source table for cross-version Oracle replication testing
-- Generates test data with varying LOB sizes to verify ORA-64219 fix

CREATE TABLE t_lob_source (
    id NUMBER PRIMARY KEY,
    blob_col BLOB,
    clob_col CLOB,
    blob_size NUMBER,
    clob_size NUMBER,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Insert test data with varying LOB sizes
-- Small LOBs (1KB)
INSERT INTO t_lob_source (id, blob_col, clob_col, blob_size, clob_size)
SELECT 
    1,
    UTL_RAW.CAST_TO_RAW(DBMS_RANDOM.STRING('A', 1024)),
    DBMS_RANDOM.STRING('A', 1024),
    1024,
    1024
FROM DUAL;

INSERT INTO t_lob_source (id, blob_col, clob_col, blob_size, clob_size)
SELECT 
    2,
    UTL_RAW.CAST_TO_RAW(DBMS_RANDOM.STRING('A', 1024)),
    DBMS_RANDOM.STRING('A', 1024),
    1024,
    1024
FROM DUAL;

-- Medium LOBs (using TO_CLOB/TO_BLOB to avoid RAW size limit)
INSERT INTO t_lob_source (id, blob_col, clob_col, blob_size, clob_size)
SELECT 
    3,
    TO_BLOB(UTL_RAW.CAST_TO_RAW(RPAD('A', 2000, 'B'))),
    TO_CLOB(RPAD('TestData', 4000, ' LOB data for cross-version replication test ')),
    2000,
    4000
FROM DUAL;

-- Null LOBs (edge case)
INSERT INTO t_lob_source (id, blob_col, clob_col, blob_size, clob_size)
VALUES (4, NULL, NULL, 0, 0);

-- Empty LOBs (edge case)
INSERT INTO t_lob_source (id, blob_col, clob_col, blob_size, clob_size)
VALUES (5, EMPTY_BLOB(), EMPTY_CLOB(), 0, 0);

-- Unicode CLOB (multi-byte characters)
INSERT INTO t_lob_source (id, blob_col, clob_col, blob_size, clob_size)
SELECT 
    6,
    UTL_RAW.CAST_TO_RAW('Binary data with special bytes'),
    TO_CLOB('Unicode test: ' || UNISTR('\00E9\00E8\00EA') || ' - ' || UNISTR('\4E2D\6587') || ' - ' || RPAD('X', 500, 'Y')),
    32,
    520
FROM DUAL;

COMMIT;

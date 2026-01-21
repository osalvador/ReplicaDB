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

-- Medium BLOB/CLOB (10KB) - Use DBMS_LOB.WRITE for larger data
DECLARE
    v_blob BLOB;
    v_clob CLOB;
    v_data RAW(2000);
    v_text VARCHAR2(4000);
    v_amount INTEGER := 2000;
    v_offset INTEGER := 1;
BEGIN
    -- Create BLOB
    INSERT INTO t_lob_source (id, blob_col, clob_col, blob_size, clob_size)
    VALUES (3, EMPTY_BLOB(), EMPTY_CLOB(), 10000, 10000)
    RETURNING blob_col, clob_col INTO v_blob, v_clob;
    
    -- Write BLOB in chunks
    FOR i IN 1..5 LOOP
        v_data := UTL_RAW.CAST_TO_RAW(DBMS_RANDOM.STRING('A', 2000));
        DBMS_LOB.WRITE(v_blob, 2000, v_offset, v_data);
        v_offset := v_offset + 2000;
    END LOOP;
    
    -- Write CLOB in chunks
    v_offset := 1;
    FOR i IN 1..3 LOOP
        v_text := DBMS_RANDOM.STRING('A', 3333);
        DBMS_LOB.WRITEAPPEND(v_clob, LENGTH(v_text), v_text);
    END LOOP;
    
    COMMIT;
END;
/

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
    'Unicode test: ' || UNISTR('\00E9\00E8\00EA') || ' - ' || UNISTR('\4E2D\6587') || ' - ' || DBMS_RANDOM.STRING('A', 500),
    32,
    520
FROM DUAL;

COMMIT;

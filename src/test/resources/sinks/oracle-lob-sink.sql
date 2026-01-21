-- LOB test sink table for cross-version Oracle replication testing
-- Used to verify ORA-64219 fix when replicating between different Oracle versions

CREATE TABLE t_lob_sink (
    id NUMBER PRIMARY KEY,
    blob_col BLOB,
    clob_col CLOB,
    blob_size NUMBER,
    clob_size NUMBER,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP
);

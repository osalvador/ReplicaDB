package org.replicadb.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Utility class for Oracle test operations with retry logic
 * to handle ORA-00054 (resource busy) lock errors.
 */
public final class OracleTestUtils {
    
    private static final Logger LOG = LogManager.getLogger(OracleTestUtils.class);
    private static final int MAX_RETRIES = 5;
    private static final long INITIAL_WAIT_MS = 500L;

    private OracleTestUtils() {
        // Utility class
    }

    /**
     * Truncates an Oracle table with retry logic for ORA-00054 lock errors.
     * 
     * @param conn Oracle connection
     * @param tableName Table to truncate
     * @throws SQLException if truncate fails after all retries
     */
    public static void truncateTableWithRetry(Connection conn, String tableName) throws SQLException {
        executeWithRetry(conn, "TRUNCATE TABLE " + tableName);
    }

    /**
     * Executes a SQL statement with retry logic for ORA-00054 lock errors.
     * Uses exponential backoff between retries.
     * 
     * @param conn Oracle connection  
     * @param sql SQL statement to execute
     * @throws SQLException if execution fails after all retries
     */
    public static void executeWithRetry(Connection conn, String sql) throws SQLException {
        int retries = MAX_RETRIES;
        long waitMs = INITIAL_WAIT_MS;
        SQLException lastException = null;

        while (retries > 0) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
                return;
            } catch (SQLException ex) {
                lastException = ex;
                // ORA-00054: resource busy and acquire with NOWAIT specified or timeout expired
                if (ex.getErrorCode() == 54 && retries > 1) {
                    LOG.warn("ORA-00054 lock error, retrying in {} ms. Retries left: {}", waitMs, retries - 1);
                    try {
                        Thread.sleep(waitMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("Interrupted while waiting for retry", ie);
                    }
                    waitMs *= 2; // Exponential backoff
                    retries--;
                } else {
                    throw ex;
                }
            }
        }
        
        throw new SQLException("Failed to execute '" + sql + "' after " + MAX_RETRIES + 
                " retries due to ORA-00054 lock", lastException);
    }
}

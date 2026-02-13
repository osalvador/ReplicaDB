package org.replicadb.postgres;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Debug test to isolate the JDBC behavior with subqueries and OFFSET.
 * This test executes the exact same query sequence as the failing test
 * but in isolation to understand PostgreSQL's behavior.
 */
public class PostgresJdbcDebugTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(PostgresJdbcDebugTest.class);
    private static final ReplicadbPostgresqlContainer postgres = ReplicadbPostgresqlContainer.getInstance();
    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        postgres.start();
        conn = DriverManager.getConnection(
            postgres.getJdbcUrl(),
            postgres.getUsername(),
            postgres.getPassword()
        );
        LOG.info("PostgreSQL version: {}", postgres.getDockerImageName());
        LOG.info("JDBC URL: {}", postgres.getJdbcUrl());
    }

    @AfterAll
    static void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    @Test
    void testDirectQueryExecution() throws SQLException {
        String sourceQuery = "SELECT c_integer, c_character_var, c_numeric, c_date FROM t_source WHERE c_integer < 1000";
        
        LOG.info("=== Test 1: Direct query execution ===");
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sourceQuery);
            int count = 0;
            while (rs.next()) {
                count++;
            }
            LOG.info("Direct query returned {} rows", count);
            assertTrue(count > 0, "Direct query should return rows");
            assertEquals(999, count, "Should return 999 rows for c_integer < 1000");
        }
    }

    @Test
    void testProbeFollowedByWrappedQuery() throws SQLException {
        String sourceQuery = "SELECT c_integer, c_character_var, c_numeric, c_date FROM t_source WHERE c_integer < 1000";
        
        LOG.info("=== Test 2: Probe followed by wrapped query (mimics ReplicaDB flow) ===");
        
        // Step 1: Probe query (what probeSourceMetadata does)
        String probeQuery = "SELECT * FROM (" + sourceQuery + ") tmp WHERE 1=0";
        LOG.info("Executing probe query: {}", probeQuery);
        
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(probeQuery);
            ResultSetMetaData rsmd = rs.getMetaData();
            int colCount = rsmd.getColumnCount();
            LOG.info("Probe returned {} columns", colCount);
            assertEquals(4, colCount, "Probe should return 4 columns");
            
            // Don't iterate - probe query returns 0 rows by design
            assertFalse(rs.next(), "Probe query should return 0 rows");
        }
        
        // Step 2: Actual data query (what readTable does)
        String dataQuery = "SELECT * FROM (" + sourceQuery + ") as T1 OFFSET ?";
        LOG.info("Executing data query: {}", dataQuery);
        
        try (PreparedStatement pstmt = conn.prepareStatement(dataQuery)) {
            pstmt.setObject(1, 0L);  // OFFSET 0
            
            LOG.info("Parameter bound: OFFSET = 0");
            LOG.info("Connection state - autoCommit: {}, transactionIsolation: {}",
                     conn.getAutoCommit(),
                     conn.getTransactionIsolation());
            
            ResultSet rs = pstmt.executeQuery();
            int count = 0;
            while (rs.next()) {
                count++;
                if (count <= 3) {
                    LOG.debug("Row {}: c_integer={}", count, rs.getInt("c_integer"));
                }
            }
            LOG.info("Data query returned {} rows", count);
            assertTrue(count > 0, "Data query should return rows");
            assertEquals(999, count, "Should return 999 rows");
        }
    }

    @Test
    void testWrappedQueryWithDifferentTransactionModes() throws SQLException {
        String sourceQuery = "SELECT c_integer, c_character_var, c_numeric, c_date FROM t_source WHERE c_integer < 1000";
        String wrappedQuery = "SELECT * FROM (" + sourceQuery + ") as T1 OFFSET ?";
        
        LOG.info("=== Test 3: Wrapped query with different transaction modes ===");
        
        // Test with autoCommit = true
        conn.setAutoCommit(true);
        LOG.info("Testing with autoCommit=true");
        int countAutoCommitTrue = executeWrappedQuery(wrappedQuery);
        LOG.info("With autoCommit=true: {} rows", countAutoCommitTrue);
        
        // Test with autoCommit = false
        conn.setAutoCommit(false);
        LOG.info("Testing with autoCommit=false");
        int countAutoCommitFalse = executeWrappedQuery(wrappedQuery);
        conn.commit();
        LOG.info("With autoCommit=false: {} rows", countAutoCommitFalse);
        
        // Both should return same count
        assertEquals(999, countAutoCommitTrue, "Should return 999 rows with autoCommit=true");
        assertEquals(999, countAutoCommitFalse, "Should return 999 rows with autoCommit=false");
    }

    @Test
    void testWrappedQueryWithExplicitLimit() throws SQLException {
        String sourceQuery = "SELECT c_integer, c_character_var, c_numeric, c_date FROM t_source WHERE c_integer < 1000";
        
        LOG.info("=== Test 4: Wrapped query with explicit LIMIT ===");
        
        // With LIMIT
        String queryWithLimit = "SELECT * FROM (" + sourceQuery + ") as T1 OFFSET ? LIMIT ?";
        LOG.info("Query with LIMIT: {}", queryWithLimit);
        
        try (PreparedStatement pstmt = conn.prepareStatement(queryWithLimit)) {
            pstmt.setObject(1, 0L);  // OFFSET 0
            pstmt.setObject(2, Long.MAX_VALUE);  // LIMIT effectively unlimited
            
            ResultSet rs = pstmt.executeQuery();
            int count = 0;
            while (rs.next()) {
                count++;
            }
            LOG.info("Query with LIMIT returned {} rows", count);
            assertEquals(999, count, "Should return 999 rows");
        }
    }

    @Test
    void testQueryWithoutWrapper() throws SQLException {
        String sourceQuery = "SELECT c_integer, c_character_var, c_numeric, c_date FROM t_source WHERE c_integer < 1000";
        
        LOG.info("=== Test 5: Source query without wrapper ===");
        LOG.info("Query: {}", sourceQuery);
        
        try (PreparedStatement pstmt = conn.prepareStatement(sourceQuery)) {
            // No parameters to bind
            ResultSet rs = pstmt.executeQuery();
            int count = 0;
            while (rs.next()) {
                count++;
            }
            LOG.info("Query without wrapper returned {} rows", count);
            assertEquals(999, count, "Should return 999 rows");
        }
    }

    @Test
    void testVerifyDataExists() throws SQLException {
        LOG.info("=== Test 6: Verify source data ===");
        
        try (Statement stmt = conn.createStatement()) {
            // Total rows in t_source
            ResultSet rs1 = stmt.executeQuery("SELECT COUNT(*) FROM t_source");
            rs1.next();
            int totalRows = rs1.getInt(1);
            LOG.info("Total rows in t_source: {}", totalRows);
            assertTrue(totalRows > 0, "t_source should have data");
            
            // Rows matching WHERE clause
            ResultSet rs2 = stmt.executeQuery("SELECT COUNT(*) FROM t_source WHERE c_integer < 1000");
            rs2.next();
            int filteredRows = rs2.getInt(1);
            LOG.info("Rows with c_integer < 1000: {}", filteredRows);
            assertEquals(999, filteredRows, "Should have 999 rows with c_integer < 1000");
            
            // Min and max c_integer values
            ResultSet rs3 = stmt.executeQuery("SELECT MIN(c_integer), MAX(c_integer) FROM t_source");
            rs3.next();
            int minVal = rs3.getInt(1);
            int maxVal = rs3.getInt(2);
            LOG.info("c_integer range: {} to {}", minVal, maxVal);
            assertEquals(1, minVal, "Min c_integer should be 1");
            assertTrue(maxVal > 1000, "Max c_integer should be > 1000");
        }
    }

    private int executeWrappedQuery(String query) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setObject(1, 0L);
            ResultSet rs = pstmt.executeQuery();
            int count = 0;
            while (rs.next()) {
                count++;
            }
            return count;
        }
    }
}

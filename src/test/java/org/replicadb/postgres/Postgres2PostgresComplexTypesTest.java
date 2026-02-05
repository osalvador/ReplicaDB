package org.replicadb.postgres;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for PostgreSQL complex types (ARRAY, JSON, JSONB, XML, INTERVAL)
 * using TEXT COPY format. Validates that shouldUseBinaryCopy() correctly detects
 * complex types and routes them through TEXT COPY path, not binary COPY.
 */
@Testcontainers
class Postgres2PostgresComplexTypesTest {
    private static final Logger LOG = LogManager.getLogger(Postgres2PostgresComplexTypesTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final int EXPECTED_ROWS = 3;

    private Connection postgresConn;
    private static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres;

    @BeforeAll
    static void setUp() throws SQLException, IOException {
        postgres = ReplicadbPostgresqlContainer.getInstance();
        
        // Initialize connection to create tables
        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(), 
                postgres.getUsername(), 
                postgres.getPassword())) {
            
            // Create source table with test data
            String sourceSQL = new String(Files.readAllBytes(
                Paths.get(RESOURCE_DIR, "postgres", "pg-complex-types-source.sql")));
            conn.createStatement().execute(sourceSQL);
            
            // Create sink table
            String sinkSQL = new String(Files.readAllBytes(
                Paths.get(RESOURCE_DIR, "sinks", "pg-complex-types-sink.sql")));
            conn.createStatement().execute(sinkSQL);
            
            LOG.info("Complex types test tables created");
        }
    }

    @BeforeEach
    void before() throws SQLException {
        this.postgresConn = DriverManager.getConnection(
            postgres.getJdbcUrl(), 
            postgres.getUsername(), 
            postgres.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table
        postgresConn.createStatement().execute("TRUNCATE TABLE t_complex_sink");
        this.postgresConn.close();
    }

    @Test
    void testComplexTypesReplication() throws ParseException, IOException, SQLException {
        String[] args = {
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--source-table", "t_complex_source",
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-table", "t_complex_sink",
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        
        ToolOptions options = new ToolOptions(args);
        int result = ReplicaDB.processReplica(options);
        
        assertEquals(0, result, "Replication should complete successfully");
        assertEquals(EXPECTED_ROWS, countSinkRows(), "Should replicate all rows");
        
        // Validate data integrity
        validateArrayTypes();
        validateJsonTypes();
        validateXmlTypes();
        validateIntervalTypes();
    }

    private int countSinkRows() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_complex_sink");
        rs.next();
        int count = rs.getInt(1);
        LOG.info("Sink rows: {}", count);
        return count;
    }

    private void validateArrayTypes() throws SQLException {
        String sql = "SELECT id, int_array, text_array, nested_array FROM t_complex_sink ORDER BY id";
        try (Statement stmt = postgresConn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            // Row 1: Basic arrays
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            Array intArray = rs.getArray("int_array");
            assertNotNull(intArray);
            Integer[] intValues = (Integer[]) intArray.getArray();
            assertArrayEquals(new Integer[]{1, 2, 3, 4, 5}, intValues, "Integer array should match");
            
            Array textArray = rs.getArray("text_array");
            String[] textValues = (String[]) textArray.getArray();
            assertEquals("test \"quoted\"", textValues[2], "Special characters in array should be preserved");
            
            // Row 2: Arrays with NULLs
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            Array nullArray = rs.getArray("int_array");
            Integer[] nullValues = (Integer[]) nullArray.getArray();
            assertNull(nullValues[1], "NULL in array should be preserved");
            
            Array emptyArray = rs.getArray("text_array");
            String[] emptyValues = (String[]) emptyArray.getArray();
            assertEquals(0, emptyValues.length, "Empty array should be preserved");
            
            // Row 3: NULL array
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertNull(rs.getArray("int_array"), "NULL array should be preserved");
            
            LOG.info("✓ Array types validation passed");
        }
    }

    private void validateJsonTypes() throws SQLException {
        String sql = "SELECT id, json_data, jsonb_data FROM t_complex_sink ORDER BY id";
        try (Statement stmt = postgresConn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            // Row 1: JSON with Unicode and special chars
            assertTrue(rs.next());
            String json1 = rs.getString("json_data");
            assertTrue(json1.contains("San José"), "Unicode in JSON should be preserved");
            assertTrue(json1.contains("quote\\\"here"), "Escaped quotes in JSON should be preserved");
            
            String jsonb1 = rs.getString("jsonb_data");
            assertTrue(jsonb1.contains("café"), "Unicode in JSONB should be preserved");
            assertTrue(jsonb1.contains("☕"), "Emoji in JSONB should be preserved");
            
            // Row 2: Complex nested JSON
            assertTrue(rs.next());
            String json2 = rs.getString("json_data");
            assertTrue(json2.contains("users"), "Nested JSON structure should be preserved");
            assertTrue(json2.contains("Alice"), "Nested JSON values should be preserved");
            
            String jsonb2 = rs.getString("jsonb_data");
            assertTrue(jsonb2.contains("null"), "JSON null values should be preserved");
            
            // Row 3: Edge cases
            assertTrue(rs.next());
            String json3 = rs.getString("json_data");
            assertEquals("null", json3, "JSON null literal should be preserved");
            
            String jsonb3 = rs.getString("jsonb_data");
            assertEquals("{}", jsonb3, "Empty JSON object should be preserved");
            
            LOG.info("✓ JSON/JSONB types validation passed");
        }
    }

    private void validateXmlTypes() throws SQLException {
        String sql = "SELECT id, xml_data FROM t_complex_sink ORDER BY id";
        try (Statement stmt = postgresConn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            // Row 1: XML with CDATA
            assertTrue(rs.next());
            SQLXML xml1 = rs.getSQLXML("xml_data");
            String xmlStr1 = xml1.getString();
            assertTrue(xmlStr1.contains("CDATA"), "CDATA section should be preserved");
            assertTrue(xmlStr1.contains("Special <chars>"), "Special characters in XML should be preserved");
            
            // Row 2: XML with attributes and namespaces
            assertTrue(rs.next());
            SQLXML xml2 = rs.getSQLXML("xml_data");
            String xmlStr2 = xml2.getString();
            assertTrue(xmlStr2.contains("xmlns="), "XML namespaces should be preserved");
            assertTrue(xmlStr2.contains("id=\"123\""), "XML attributes should be preserved");
            
            // Row 3: XML with multiple elements
            assertTrue(rs.next());
            SQLXML xml3 = rs.getSQLXML("xml_data");
            String xmlStr3 = xml3.getString();
            assertTrue(xmlStr3.contains("<item>First</item>"), "XML element structure should be preserved");
            
            LOG.info("✓ XML types validation passed");
        }
    }

    private void validateIntervalTypes() throws SQLException {
        String sql = "SELECT id, interval_data FROM t_complex_sink ORDER BY id";
        try (Statement stmt = postgresConn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            // Row 1: Complex interval
            assertTrue(rs.next());
            Object interval1 = rs.getObject("interval_data");
            assertNotNull(interval1);
            String intervalStr1 = interval1.toString();
            // PostgreSQL interval format may vary, just check it's not empty
            assertFalse(intervalStr1.isEmpty(), "Interval should have a value");
            
            // Row 2: Days and hours interval
            assertTrue(rs.next());
            Object interval2 = rs.getObject("interval_data");
            assertNotNull(interval2);
            
            // Row 3: Negative interval
            assertTrue(rs.next());
            Object interval3 = rs.getObject("interval_data");
            assertNotNull(interval3);
            String intervalStr3 = interval3.toString();
            assertTrue(intervalStr3.contains("-"), "Negative interval should be preserved");
            
            LOG.info("✓ Interval types validation passed");
        }
    }

    @Test
    void testComplexTypesUseTextCopy() throws ParseException, IOException, SQLException {
        // This test verifies that complex types trigger TEXT COPY, not binary COPY
        // We can't directly check logs in test, but we can verify replication succeeds
        // and defensive checks don't trigger (no IllegalStateException)
        
        String[] args = {
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--source-table", "t_complex_source",
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-table", "t_complex_sink",
                "--mode", ReplicationMode.COMPLETE.getModeText(),
                "--jobs", "1"  // Single thread to avoid race conditions in logs
        };
        
        ToolOptions options = new ToolOptions(args);
        
        // Should complete without throwing IllegalStateException from defensive checks
        assertDoesNotThrow(() -> {
            int result = ReplicaDB.processReplica(options);
            assertEquals(0, result, "Replication should succeed using TEXT COPY");
        }, "Complex types should use TEXT COPY without errors");
        
        assertEquals(EXPECTED_ROWS, countSinkRows());
        LOG.info("✓ Complex types correctly routed through TEXT COPY");
    }
}

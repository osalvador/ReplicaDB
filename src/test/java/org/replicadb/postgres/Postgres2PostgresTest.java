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
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Postgres2PostgresTest {
    private static final Logger LOG = LogManager.getLogger(Postgres2PostgresTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int TOTAL_SINK_ROWS = 4097;

    private Connection postgresConn;
    private static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres;

    @BeforeAll
    static void setUp() {
        postgres = ReplicadbPostgresqlContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        postgresConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.postgresConn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_sink");
        rs.next();
        int count = rs.getInt(1);
        LOG.info("Sink rows: {}", count);
        return count;
    }

    @Test
    void testPostgresConnection() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        rs.next();
        String result = rs.getString(1);
        LOG.info(result);
        assertTrue(result.contains("1"));
    }

    @Test
    void testPostgres2PostgresComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2PostgresCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-staging-schema", "public",
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2PostgresIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-staging-schema", "public",
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2PostgresCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2PostgresCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-staging-schema", "public",
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testPostgres2PostgresIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-staging-schema", "public",
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

    @Test
    void testBinaryTypes_AllColumnsWithPrecisionValidation() throws ParseException, IOException, SQLException {
        // Replicate all data including new binary-encoded types
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());

        // Validate precision for binary-encoded types (FLOAT, DOUBLE, DATE, TIME, TIMESTAMP, NUMERIC)
        LOG.info("Validating binary type precision across source and sink...");
        
        // Select a sample row (C_INTEGER = 1000) to validate precision
        String query = "SELECT C_INTEGER, C_NUMERIC, C_DECIMAL, C_REAL, C_DOUBLE_PRECISION, C_FLOAT, " +
                      "C_DATE, C_TIME_WITHOUT_TIMEZONE, C_TIMESTAMP_WITHOUT_TIMEZONE " +
                      "FROM {} WHERE C_INTEGER = 1000";
        
        Statement sourceStmt = postgresConn.createStatement();
        Statement sinkStmt = postgresConn.createStatement();
        
        ResultSet sourceRs = sourceStmt.executeQuery(query.replace("{}", "t_source"));
        ResultSet sinkRs = sinkStmt.executeQuery(query.replace("{}", "t_sink"));
        
        assertTrue(sourceRs.next(), "Source should have row with C_INTEGER=1000");
        assertTrue(sinkRs.next(), "Sink should have row with C_INTEGER=1000");
        
        // Validate INTEGER
        assertEquals(sourceRs.getInt("C_INTEGER"), sinkRs.getInt("C_INTEGER"), 
                    "INTEGER values should match exactly");
        
        // Validate NUMERIC with BigDecimal comparison (exact precision)
        if (sourceRs.getBigDecimal("C_NUMERIC") != null && sinkRs.getBigDecimal("C_NUMERIC") != null) {
            assertEquals(0, sourceRs.getBigDecimal("C_NUMERIC").compareTo(sinkRs.getBigDecimal("C_NUMERIC")),
                        "NUMERIC precision should be preserved exactly (no rounding)");
            
            // Also validate string representation matches
            assertEquals(sourceRs.getString("C_NUMERIC"), sinkRs.getString("C_NUMERIC"),
                        "NUMERIC string representation should match");
            
            LOG.info("✓ NUMERIC precision validated: {}", sourceRs.getBigDecimal("C_NUMERIC"));
        }
        
        // Validate DECIMAL with BigDecimal comparison
        if (sourceRs.getBigDecimal("C_DECIMAL") != null && sinkRs.getBigDecimal("C_DECIMAL") != null) {
            assertEquals(0, sourceRs.getBigDecimal("C_DECIMAL").compareTo(sinkRs.getBigDecimal("C_DECIMAL")),
                        "DECIMAL precision should be preserved exactly");
            LOG.info("✓ DECIMAL precision validated: {}", sourceRs.getBigDecimal("C_DECIMAL"));
        }
        
        // Validate FLOAT (REAL) - IEEE 754 precision
        if (sourceRs.getFloat("C_REAL") != 0.0f) {
            assertEquals(sourceRs.getFloat("C_REAL"), sinkRs.getFloat("C_REAL"), 0.0001f,
                        "FLOAT values should match with IEEE 754 precision");
            LOG.info("✓ FLOAT precision validated: {}", sourceRs.getFloat("C_REAL"));
        }
        
        // Validate DOUBLE - IEEE 754 precision
        if (sourceRs.getDouble("C_DOUBLE_PRECISION") != 0.0) {
            assertEquals(sourceRs.getDouble("C_DOUBLE_PRECISION"), sinkRs.getDouble("C_DOUBLE_PRECISION"), 0.000001,
                        "DOUBLE values should match with IEEE 754 precision");
            LOG.info("✓ DOUBLE precision validated: {}", sourceRs.getDouble("C_DOUBLE_PRECISION"));
        }
        
        // Validate DATE - day-level precision
        if (sourceRs.getDate("C_DATE") != null && sinkRs.getDate("C_DATE") != null) {
            assertEquals(sourceRs.getDate("C_DATE"), sinkRs.getDate("C_DATE"),
                        "DATE values should match exactly (days since epoch)");
            LOG.info("✓ DATE precision validated: {}", sourceRs.getDate("C_DATE"));
        }
        
        // Validate TIME - microsecond precision (though JDBC Time is millisecond)
        if (sourceRs.getTime("C_TIME_WITHOUT_TIMEZONE") != null && sinkRs.getTime("C_TIME_WITHOUT_TIMEZONE") != null) {
            // Compare milliseconds (JDBC Time limitation)
            long sourceTimeMs = sourceRs.getTime("C_TIME_WITHOUT_TIMEZONE").getTime();
            long sinkTimeMs = sinkRs.getTime("C_TIME_WITHOUT_TIMEZONE").getTime();
            assertEquals(sourceTimeMs, sinkTimeMs,
                        "TIME values should match at millisecond precision");
            LOG.info("✓ TIME precision validated: {}", sourceRs.getTime("C_TIME_WITHOUT_TIMEZONE"));
        }
        
        // Validate TIMESTAMP - microsecond precision
        if (sourceRs.getTimestamp("C_TIMESTAMP_WITHOUT_TIMEZONE") != null && 
            sinkRs.getTimestamp("C_TIMESTAMP_WITHOUT_TIMEZONE") != null) {
            
            Timestamp sourceTs = sourceRs.getTimestamp("C_TIMESTAMP_WITHOUT_TIMEZONE");
            Timestamp sinkTs = sinkRs.getTimestamp("C_TIMESTAMP_WITHOUT_TIMEZONE");
            
            // Compare milliseconds
            assertEquals(sourceTs.getTime(), sinkTs.getTime(),
                        "TIMESTAMP milliseconds should match exactly");
            
            // Compare nanoseconds for microsecond precision
            assertEquals(sourceTs.getNanos(), sinkTs.getNanos(),
                        "TIMESTAMP microsecond precision should be preserved");
            
            LOG.info("✓ TIMESTAMP microsecond precision validated: {} (nanos: {})", 
                    sourceTs, sourceTs.getNanos());
        }
        
        sourceRs.close();
        sinkRs.close();
        sourceStmt.close();
        sinkStmt.close();
        
        LOG.info("✅ All binary type precision validations passed");
    }
}

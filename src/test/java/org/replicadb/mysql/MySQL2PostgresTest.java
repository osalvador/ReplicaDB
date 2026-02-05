package org.replicadb.mysql;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMysqlContainer;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MySQL2PostgresTest {
    private static final Logger LOG = LogManager.getLogger(MySQL2PostgresTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;
    // Binary and floating-point columns now enabled
    // Note: FLOAT columns use TEXT COPY (no CAST needed), BINARY columns use binary COPY
    private static final String SINK_COLUMNS = "C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL," +
            "C_REAL,C_DOUBLE_PRECISION,C_FLOAT," +
            "C_BOOLEAN," +
            "C_CHARACTER,C_CHARACTER_VAR,C_CHARACTER_LOB," +
            "C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR," +
            "C_BINARY,C_BINARY_VAR,C_BINARY_LOB," +
            "C_DATE,C_TIME_WITHOUT_TIMEZONE,C_TIMESTAMP_WITHOUT_TIMEZONE," +
            "C_TIME_WITH_TIMEZONE,C_TIMESTAMP_WITH_TIMEZONE";
    private static final String SOURCE_COLUMNS = "C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL," +
            "C_REAL,C_DOUBLE_PRECISION,C_FLOAT," +
            "C_BOOLEAN," +
            "C_CHARACTER,C_CHARACTER_VAR,C_CHARACTER_LOB," +
            "C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR," +
            "C_BINARY,C_BINARY_VAR,C_BINARY_LOB," +
            "C_DATE,C_TIME_WITHOUT_TIMEZONE,C_TIMESTAMP_WITHOUT_TIMEZONE," +
            "C_TIME_WITH_TIMEZONE,C_TIMESTAMP_WITH_TIMEZONE";

    private Connection mysqlConn;
    private Connection postgresConn;

    @Rule
    public static MySQLContainer<ReplicadbMysqlContainer> mysql = ReplicadbMysqlContainer.getInstance();

    @Rule
    public static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres = ReplicadbPostgresqlContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.mysqlConn = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        postgresConn.createStatement().execute("TRUNCATE TABLE T_SINK");
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        postgresConn.createStatement().execute("TRUNCATE TABLE T_SINK");
        this.mysqlConn.close();
        this.postgresConn.close();
    }


    public int countSinkRows() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
        rs.next();
        int count = rs.getInt(1);
        LOG.info(count);
        return count;
    }

    /**
     * Validate binary and floating-point data integrity between source and sink.
     * Compares BINARY columns byte-by-byte and FLOAT/DOUBLE with epsilon tolerance.
     */
    private void validateBinaryAndFloatData() throws SQLException {
        String query = "SELECT C_BINARY, C_BINARY_VAR, C_REAL, C_DOUBLE_PRECISION, C_FLOAT " +
                      "FROM t_source ORDER BY C_INTEGER LIMIT 3";
        String sinkQuery = "SELECT C_BINARY, C_BINARY_VAR, C_REAL, C_DOUBLE_PRECISION, C_FLOAT " +
                          "FROM t_sink ORDER BY C_INTEGER LIMIT 3";
        
        try (Statement mysqlStmt = mysqlConn.createStatement();
             Statement pgStmt = postgresConn.createStatement();
             ResultSet mysqlRs = mysqlStmt.executeQuery(query);
             ResultSet pgRs = pgStmt.executeQuery(sinkQuery)) {
            
            int rowCount = 0;
            double floatEpsilon = 0.0001;  // Tolerance for FLOAT/REAL
            double doubleEpsilon = 0.0000001;  // Tolerance for DOUBLE
            
            while (mysqlRs.next() && pgRs.next()) {
                rowCount++;
                
                // Validate BINARY columns byte-by-byte
                byte[] mysqlBinary = mysqlRs.getBytes("C_BINARY");
                byte[] pgBinary = pgRs.getBytes("C_BINARY");
                if (mysqlBinary != null && pgBinary != null) {
                    Assertions.assertArrayEquals(mysqlBinary, pgBinary,
                        "Row " + rowCount + " C_BINARY mismatch");
                }
                
                byte[] mysqlBinaryVar = mysqlRs.getBytes("C_BINARY_VAR");
                byte[] pgBinaryVar = pgRs.getBytes("C_BINARY_VAR");
                if (mysqlBinaryVar != null && pgBinaryVar != null) {
                    Assertions.assertArrayEquals(mysqlBinaryVar, pgBinaryVar,
                        "Row " + rowCount + " C_BINARY_VAR mismatch");
                }
                
                // Validate FLOAT values with epsilon tolerance
                if (!mysqlRs.wasNull() && !pgRs.wasNull()) {
                    float mysqlReal = mysqlRs.getFloat("C_REAL");
                    float pgReal = pgRs.getFloat("C_REAL");
                    assertTrue(Math.abs(mysqlReal - pgReal) < floatEpsilon,
                        String.format("Row %d C_REAL mismatch. MySQL: %f, PG: %f, diff: %f",
                            rowCount, mysqlReal, pgReal, Math.abs(mysqlReal - pgReal)));
                    
                    float mysqlFloat = mysqlRs.getFloat("C_FLOAT");
                    float pgFloat = pgRs.getFloat("C_FLOAT");
                    assertTrue(Math.abs(mysqlFloat - pgFloat) < floatEpsilon,
                        String.format("Row %d C_FLOAT mismatch. MySQL: %f, PG: %f",
                            rowCount, mysqlFloat, pgFloat));
                }
                
                // Validate DOUBLE values with epsilon tolerance
                if (!mysqlRs.wasNull() && !pgRs.wasNull()) {
                    double mysqlDouble = mysqlRs.getDouble("C_DOUBLE_PRECISION");
                    double pgDouble = pgRs.getDouble("C_DOUBLE_PRECISION");
                    assertTrue(Math.abs(mysqlDouble - pgDouble) < doubleEpsilon,
                        String.format("Row %d C_DOUBLE_PRECISION mismatch. MySQL: %.10f, PG: %.10f",
                            rowCount, mysqlDouble, pgDouble));
                }
                
                LOG.info("Row {}: Validated - C_BINARY, C_BINARY_VAR, C_REAL, C_DOUBLE_PRECISION, C_FLOAT", rowCount);
            }
            
            assertEquals(3, rowCount, "Should validate 3 sample rows");
        }
    }


    @Test
    void testMysqlVersion56() throws SQLException {
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT VERSION()");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("5.6"));
    }

    @Test
    void testPostgresConnection() throws SQLException {
        Statement stmt = postgresConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
    }

    @Test
    void testMysqlInit() throws SQLException {
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_source");
        rs.next();
        int rows = rs.getInt(1);
        assertEquals(EXPECTED_ROWS, rows);
    }

    @Test
    void testMySQL2PostgresComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMySQL2PostgresCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());

    }

    @Test
    void testMySQL2PostgresIncremental() throws ParseException, IOException, SQLException {
        // Note: Incremental mode incompatible with --source-query (overrides WHERE clause)
        // Using --source-columns instead, which limits columns but allows TEXT COPY for FLOAT
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());

    }

    @Test
    void testMySQL2PostgresCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--jobs", "4",
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMySQL2PostgresCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4",
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMySQL2PostgresIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mysql.getJdbcUrl(),
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4",
                "--source-columns", SOURCE_COLUMNS,
                "--sink-columns", SINK_COLUMNS
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}

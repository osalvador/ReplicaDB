package org.replicadb.mariadb;

import org.apache.commons.cli.ParseException;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMariaDBContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MariaDB2MariaDBTest {
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4096;

    private Connection mariadbConn;

    @Rule
    public static MariaDBContainer<ReplicadbMariaDBContainer> mariadb = ReplicadbMariaDBContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.mariadbConn = DriverManager.getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        mariadbConn.createStatement().execute("TRUNCATE TABLE t_sink");
        this.mariadbConn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = mariadbConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_sink");
        rs.next();
        int count = rs.getInt(1);
        LOG.info("Sink rows: {}", count);
        return count;
    }

    @Test
    void testMariadbVersion102() throws SQLException {
        Statement stmt = mariadbConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT VERSION()");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("10.2"));
    }

    @Test
    void testMariaDB2MariaDBComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMariaDB2MariaDBCompleteAtomic() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword(),
                "--sink-staging-schema", mariadb.getDatabaseName(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMariaDB2MariaDBIncremental() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword(),
                "--sink-staging-schema", mariadb.getDatabaseName(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMariaDB2MariaDBCompleteParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMariaDB2MariaDBCompleteAtomicParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword(),
                "--sink-staging-schema", mariadb.getDatabaseName(),
                "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }

    @Test
    void testMariaDB2MariaDBIncrementalParallel() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", mariadb.getJdbcUrl(),
                "--source-user", mariadb.getUsername(),
                "--source-password", mariadb.getPassword(),
                "--sink-connect", mariadb.getJdbcUrl(),
                "--sink-user", mariadb.getUsername(),
                "--sink-password", mariadb.getPassword(),
                "--sink-staging-schema", mariadb.getDatabaseName(),
                "--mode", ReplicationMode.INCREMENTAL.getModeText(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(EXPECTED_ROWS, countSinkRows());
    }
}

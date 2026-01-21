package org.replicadb.postgres;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbDuckDBFakeContainer;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class Postgres2DuckDBTest {
    private static final Logger LOG = LogManager.getLogger(Postgres2DuckDBTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb-duckdb.conf";
    private static final int TOTAL_SINK_ROWS = 4097;

    private Connection duckdbConn;
    private Connection postgresConn;

    @Rule
    public static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres = ReplicadbPostgresqlContainer.getInstance();
    @Rule
    public static ReplicadbDuckDBFakeContainer duckdb = ReplicadbDuckDBFakeContainer.getInstance();

    @BeforeAll
    static void setUp() {
    }

    @BeforeEach
    void before() throws SQLException {
        this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        this.duckdbConn = DriverManager.getConnection(duckdb.getJdbcUrl());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        duckdbConn.createStatement().execute("DELETE FROM t_sink");
        this.duckdbConn.close();
        this.postgresConn.close();
    }

    public int countSinkRows() throws SQLException {
        Statement stmt = duckdbConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_sink");
        rs.next();
        int count = rs.getInt(1);
        LOG.info("DuckDB sink rows: {}", count);
        return count;
    }

    @Test
    void testPostgres2DuckDBComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", duckdb.getJdbcUrl()
        };
        ToolOptions options = new ToolOptions(args);
        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }
}

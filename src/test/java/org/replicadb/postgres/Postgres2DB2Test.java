package org.replicadb.postgres;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbDB2Container;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Postgres2DB2Test {
    private static final Logger LOG = LogManager.getLogger(Postgres2DB2Test.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int TOTAL_SINK_ROWS = 4097;

    private Connection db2Conn;
    private Connection postgresConn;

    @Rule
    public static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres = ReplicadbPostgresqlContainer.getInstance();
    @Rule
    public static Db2Container db2 = ReplicadbDB2Container.getInstance();


    @BeforeAll
    static void setUp(){
    }

    @BeforeEach
    void before() throws SQLException {
        this.db2Conn = DriverManager.getConnection(db2.getJdbcUrl(), db2.getUsername(), db2.getPassword());
        this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Truncate sink table and close connections
        db2Conn.createStatement().execute("DELETE t_sink");
        this.db2Conn.close();
        this.postgresConn.close();
    }


    public int countSinkRows() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
        rs.next();
        int count = rs.getInt(1);
        LOG.info(count);
        return count;
    }


    @Test
    void testDb2Connection () throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
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
    void testPostgres2Db2DBComplete() throws ParseException, IOException, SQLException {
        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--sink-connect", db2.getJdbcUrl(),
                "--sink-user", db2.getUsername(),
                "--sink-password", db2.getPassword(),
                //"--sink-disable-truncate", "true",
                "--source-columns","C_INTEGER,C_SMALLINT,C_BIGINT,C_NUMERIC,C_DECIMAL,C_REAL,C_DOUBLE_PRECISION,C_FLOAT,C_BINARY,C_BINARY_VAR,C_BINARY_LOB,C_BOOLEAN,C_CHARACTER,C_CHARACTER_VAR,C_CHARACTER_LOB,C_NATIONAL_CHARACTER,C_NATIONAL_CHARACTER_VAR,C_DATE,C_TIME_WITHOUT_TIMEZONE,C_TIMESTAMP_WITHOUT_TIMEZONE,C_TIME_WITH_TIMEZONE,C_TIMESTAMP_WITH_TIMEZONE"
        };

        ToolOptions options = new ToolOptions(args);

        // Using Standard JDBC Manager
        Properties sinkConnectionParams = new Properties();
        sinkConnectionParams.setProperty("driver", "com.ibm.db2.jcc.DB2Driver");
        options.setSinkConnectionParams(sinkConnectionParams);

        Assertions.assertEquals(0, ReplicaDB.processReplica(options));
        assertEquals(TOTAL_SINK_ROWS, countSinkRows());
    }

}
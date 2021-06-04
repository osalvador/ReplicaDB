package org.replicadb.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MySQLManagerTest {
    private static final Logger LOG = LogManager.getLogger(MySQLManagerTest.class);
    private static final String RESOURECE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String MYSQL_SOURCE_FILE = "/mysql/mysql-source.sql";
    private static final String USER_PASSWD_DB = "replicadb";

    private Connection mysqlConn;

    @ClassRule
    private static final MySQLContainer mysql = new MySQLContainer("mysql:5.5")
            .withDatabaseName(USER_PASSWD_DB)
            .withUsername(USER_PASSWD_DB)
            .withPassword(USER_PASSWD_DB);


    @ClassRule
    public static final OracleContainer oracle = new OracleContainer("oracleinanutshell/oracle-xe-11g")
            ;


    @BeforeAll
    static void setUp() throws SQLException, IOException {

        // Start the mysql container
        mysql.start();
        oracle.start();

        // Create tables
        //Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
        Connection con = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        ScriptRunner runner = new ScriptRunner(con, false, true);
        runner.runScript(new BufferedReader(new FileReader(RESOURECE_DIR + MYSQL_SOURCE_FILE)));
        con.close();

    }

    @BeforeEach
    void before() throws SQLException {
        this.mysqlConn = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    }

    @AfterEach
    void tearDown() {
        try {
            this.mysqlConn.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Test
    void testMysqlVersion55() throws SQLException {
        // Do something with the Connection
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT VERSION()");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("5.5"));
    }

    @Test
    void testOracleConnection() throws SQLException {
        // Do something with the Connection
        Connection oracleConn = DriverManager.getConnection(oracle.getJdbcUrl(), oracle.getUsername(),oracle.getPassword());
        Statement stmt = oracleConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM DUAL");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("1"));
        oracleConn.close();
    }

    @Test
    void testMysqlInit() throws SQLException {
        // Do something with the Connection
        Statement stmt = mysqlConn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t_source");
        rs.next();
        String version = rs.getString(1);
        LOG.info(version);
        assertTrue(version.contains("4096"));
    }

}
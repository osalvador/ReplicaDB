package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MySQLManagerTest {
    private static final Logger LOG = LogManager.getLogger(MySQLManagerTest.class);

    @ClassRule
    private static final MySQLContainer mysql = new MySQLContainer("mysql:5.5")
            .withDatabaseName("replicadb")
            .withUsername("replicadb")
            .withPassword("replicadb");

    @BeforeAll
    static void setUp() {
        // Start the mysql container
        mysql.start();

        // Create tables
    }

    @AfterEach
    void tearDown() {}

    @Test
    void testMysqlVersion55() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            Connection conn = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());

            // Do something with the Connection
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT VERSION()");
            rs.next();
            String version = rs.getString(1);
            LOG.info(version);
            assertTrue(version.contains("5.5"));

        } catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            LOG.error(e);
        }
    }
}
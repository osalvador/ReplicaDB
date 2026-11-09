package org.replicadb.mysql;

import org.apache.commons.cli.ParseException;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbMysqlContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class MySQL2MySQLUtf8mb4Test {
    private static final String SOURCE_TABLE = "t_utf8mb4_source";
    private static final String SINK_TABLE = "t_utf8mb4_sink";
    // Values with 4-byte UTF-8 characters, which utf8mb3 (MySQL alias UTF8) cannot store
    private static final String[] VALUES = {"😀 grin", "🚀 café", "plain text", "𝄞 musical symbol"};

    public static MySQLContainer<ReplicadbMysqlContainer> mysql = ReplicadbMysqlContainer.getInstance();

    private Connection mysqlConn;
    private String mysqlJdbcUrl = "";

    @BeforeEach
    void before() throws SQLException {
        this.mysqlJdbcUrl = mysql.getJdbcUrl();
        this.mysqlConn = DriverManager.getConnection(mysqlJdbcUrl, mysql.getUsername(), mysql.getPassword());

        try (Statement stmt = mysqlConn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE);
            stmt.execute("DROP TABLE IF EXISTS " + SINK_TABLE);
            stmt.execute("CREATE TABLE " + SOURCE_TABLE + " (ID INT PRIMARY KEY, VAL VARCHAR(100)) DEFAULT CHARSET=utf8mb4");
            stmt.execute("CREATE TABLE " + SINK_TABLE + " (ID INT PRIMARY KEY, VAL VARCHAR(100)) DEFAULT CHARSET=utf8mb4");
        }

        // Insert the values as hex-encoded UTF-8 bytes so the test does not depend on the JDBC connection charset
        try (PreparedStatement ps = mysqlConn.prepareStatement(
                "INSERT INTO " + SOURCE_TABLE + " (ID, VAL) VALUES (?, CONVERT(UNHEX(?) USING utf8mb4))")) {
            for (int i = 0; i < VALUES.length; i++) {
                ps.setInt(1, i + 1);
                ps.setString(2, toHex(VALUES[i]));
                ps.executeUpdate();
            }
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Statement stmt = mysqlConn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE);
            stmt.execute("DROP TABLE IF EXISTS " + SINK_TABLE);
        }
        this.mysqlConn.close();
    }

    private static String toHex(String value) {
        StringBuilder hex = new StringBuilder();
        for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
            hex.append(String.format("%02X", b));
        }
        return hex.toString();
    }

    @Test
    void testMySQL2MySQLCompleteUtf8mb4Values() throws ParseException, IOException, SQLException {
        String[] args = {
                "--source-connect", mysqlJdbcUrl,
                "--source-user", mysql.getUsername(),
                "--source-password", mysql.getPassword(),
                "--source-table", SOURCE_TABLE,
                "--sink-connect", mysqlJdbcUrl,
                "--sink-user", mysql.getUsername(),
                "--sink-password", mysql.getPassword(),
                "--sink-table", SINK_TABLE,
                "--mode", ReplicationMode.COMPLETE.getModeText()
        };
        ToolOptions options = new ToolOptions(args);
        assertEquals(0, ReplicaDB.processReplica(options));

        List<String> actualHex = new ArrayList<>();
        try (Statement stmt = mysqlConn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT HEX(VAL) FROM " + SINK_TABLE + " ORDER BY ID")) {
            while (rs.next()) {
                actualHex.add(rs.getString(1));
            }
        }

        assertEquals(VALUES.length, actualHex.size(), "All rows must be replicated");
        for (int i = 0; i < VALUES.length; i++) {
            assertEquals(toHex(VALUES[i]), actualHex.get(i),
                    "Sink value must match the source bytes byte-for-byte for row " + (i + 1));
        }
    }
}

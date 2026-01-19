package org.replicadb.file;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbSqlserverContainer;
import org.replicadb.manager.file.FileFormats;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Csv2SqlserverTest {
   private static final Logger LOG = LogManager.getLogger(Csv2SqlserverTest.class);
   private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
   private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
   private static final int EXPECTED_ROWS = 1024;
   private static final String CSV_SOURCE_FILE = "/csv/source.csv";

   private static final String SOURCE_COLUMNS = "C_VARCHAR,C_CHAR,C_LONGVARCHAR,C_INTEGER,C_BIGINT,C_TINYINT,C_SMALLINT,C_NUMERIC,C_DECIMAL,C_DOUBLE,C_FLOAT,C_DATE,C_TIMESTAMP,C_TIME,C_BOOLEAN";
   private static final String SOURCE_COLUMNS_TYPES ="VARCHAR, CHAR, LONGVARCHAR, INTEGER, BIGINT, TINYINT, SMALLINT, NUMERIC, DECIMAL, DOUBLE, DOUBLE, DATE, TIMESTAMP, TIME, BOOLEAN" ;
   private static final String SINK_COLUMNS = "c_character_var,c_character,c_character_lob,c_integer,c_bigint,c_smallint,c_real,c_numeric,c_decimal,c_double_precision,c_float,c_date,c_timestamp_without_timezone,c_time_without_timezone";

   private Connection sqlserverConn;

   @Rule
   public static MSSQLServerContainer<ReplicadbSqlserverContainer> sqlserver = ReplicadbSqlserverContainer.getInstance();

   @BeforeAll
   static void setUp () {
      // Start the container is not necessary
   }

   @BeforeEach
   void before () throws SQLException {
      this.sqlserverConn = DriverManager.getConnection(sqlserver.getJdbcUrl(), sqlserver.getUsername(), sqlserver.getPassword());
   }

   @AfterEach
   void tearDown () throws SQLException {
      // Truncate sink table and close connections
      sqlserverConn.createStatement().execute("TRUNCATE TABLE t_sink");
      this.sqlserverConn.close();
   }

   public int countSinkRows () throws SQLException {
      Statement stmt = sqlserverConn.createStatement();
      ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
      rs.next();
      int count = rs.getInt(1);
      LOG.info("Total rows in the sink table: {}", count);
      return count;
   }

   @Test
   void testSqlserverVersion2019 () throws SQLException {
      Statement stmt = sqlserverConn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT @@VERSION");
      rs.next();
      String version = rs.getString(1);
      LOG.info(version);
      assertTrue(version.contains("2019"));
   }

   @Test
   void testCsv2SqlserverComplete () throws ParseException, IOException, SQLException {
      String[] args = {
          "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
          "--source-connect", "file://" + RESOURCE_DIR + CSV_SOURCE_FILE,
          "--source-file-format", FileFormats.CSV.getType(),
          "--source-columns", SOURCE_COLUMNS,
          "--sink-connect", sqlserver.getJdbcUrl(),
          "--sink-user", sqlserver.getUsername(),
          "--sink-password", sqlserver.getPassword(),
          "--sink-columns", SINK_COLUMNS
      };
      ToolOptions options = new ToolOptions(args);
      Properties sourceConnectionParams = new Properties();
      sourceConnectionParams.setProperty("columns.types", SOURCE_COLUMNS_TYPES);
      sourceConnectionParams.setProperty("format.firstRecordAsHeader", "true");
      options.setSourceConnectionParams(sourceConnectionParams);

      Assertions.assertEquals(0, ReplicaDB.processReplica(options));
      assertEquals(EXPECTED_ROWS, countSinkRows());
   }

}

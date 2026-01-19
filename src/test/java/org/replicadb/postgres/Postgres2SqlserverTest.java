package org.replicadb.postgres;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Disabled;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbSqlserverContainer;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Postgres2SqlserverTest {
   private static final Logger LOG = LogManager.getLogger(Postgres2SqlserverTest.class);
   private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
   private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
   private static final int TOTAL_SINK_ROWS = 4097;
   private static final String SOURCE_COLUMNS = "c_integer,c_smallint,c_bigint,c_numeric::text,c_decimal::text,c_real,c_double_precision,c_float,encode(c_binary, 'hex'),encode(c_binary_var, 'hex'),encode(c_binary_lob, 'hex'),c_boolean,c_character,c_character_var,c_character_lob,c_national_character,c_national_character_var,c_date,c_time_without_timezone,c_timestamp_without_timezone,c_xml::text";
   private static final String SINK_COLUMNS = "c_integer,c_smallint,c_bigint,c_numeric      ,c_decimal      ,c_real,c_double_precision,c_float,c_binary               ,c_binary_var               ,c_binary_lob               ,c_boolean,c_character,c_character_var,c_character_lob,c_national_character,c_national_character_var,c_date,c_time_without_timezone,c_timestamp_without_timezone,c_xml";

   private Connection postgresConn;
   private Connection sqlserverConn;

   @Rule
   public static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres = ReplicadbPostgresqlContainer.getInstance();
   @Rule
   public static MSSQLServerContainer<ReplicadbSqlserverContainer> sqlserver = ReplicadbSqlserverContainer.getInstance();


   @BeforeAll
   static void setUp () {
   }

   @BeforeEach
   void before () throws SQLException {
      this.sqlserverConn = DriverManager.getConnection(sqlserver.getJdbcUrl(), sqlserver.getUsername(), sqlserver.getPassword());
      this.postgresConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
   }

   @AfterEach
   void tearDown () throws SQLException {
      // Truncate sink table and close connections
      sqlserverConn.createStatement().execute("TRUNCATE TABLE t_sink");
      this.sqlserverConn.close();
      this.postgresConn.close();
   }


   public int countSinkRows () throws SQLException {
      Statement stmt = sqlserverConn.createStatement();
      ResultSet rs = stmt.executeQuery("select count(*) from t_sink");
      rs.next();
      int count = rs.getInt(1);
      LOG.info(count);
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
   void testPostgres2SqlserverComplete () throws ParseException, IOException, SQLException {
      String[] args = {
          "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
          "--source-connect", postgres.getJdbcUrl(),
          "--source-user", postgres.getUsername(),
          "--source-password", postgres.getPassword(),
          "--sink-connect", sqlserver.getJdbcUrl(),
          "--sink-user", sqlserver.getUsername(),
          "--sink-password", sqlserver.getPassword(),
          "--source-columns", SOURCE_COLUMNS,
          "--sink-columns", SINK_COLUMNS
      };
      ToolOptions options = new ToolOptions(args);
      Assertions.assertEquals(0, ReplicaDB.processReplica(options));
      assertEquals(TOTAL_SINK_ROWS, countSinkRows());
   }

   @Disabled("SQL Server sink does not support complete-atomic mode with staging schema - requires schema creation")
   @Test
   void testPostgres2SqlserverCompleteAtomic () throws ParseException, IOException, SQLException {
      String[] args = {
          "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
          "--source-connect", postgres.getJdbcUrl(),
          "--source-user", postgres.getUsername(),
          "--source-password", postgres.getPassword(),
          "--sink-connect", sqlserver.getJdbcUrl(),
          "--sink-user", sqlserver.getUsername(),
          "--sink-password", sqlserver.getPassword(),
          "--sink-staging-schema", sqlserver.getDatabaseName(),
          "--source-columns", SOURCE_COLUMNS,
          "--sink-columns", SINK_COLUMNS,
          "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText()
      };
      ToolOptions options = new ToolOptions(args);
      assertEquals(0, ReplicaDB.processReplica(options));
      assertEquals(TOTAL_SINK_ROWS, countSinkRows());

   }

   @Disabled("SQL Server sink does not support incremental mode with staging schema - requires schema creation")
   @Test
   void testPostgres2SqlserverIncremental () throws ParseException, IOException, SQLException {
      String[] args = {
          "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
          "--source-connect", postgres.getJdbcUrl(),
          "--source-user", postgres.getUsername(),
          "--source-password", postgres.getPassword(),
          "--sink-connect", sqlserver.getJdbcUrl(),
          "--sink-user", sqlserver.getUsername(),
          "--sink-password", sqlserver.getPassword(),
          "--sink-staging-schema", sqlserver.getDatabaseName(),
          "--source-columns", SOURCE_COLUMNS,
          "--sink-columns", SINK_COLUMNS,
          "--mode", ReplicationMode.INCREMENTAL.getModeText()
      };
      ToolOptions options = new ToolOptions(args);
      assertEquals(0, ReplicaDB.processReplica(options));
      assertEquals(TOTAL_SINK_ROWS, countSinkRows());

   }

   @Test
   void testPostgres2SqlserverCompleteParallel () throws ParseException, IOException, SQLException {
      String[] args = {
          "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
          "--source-connect", postgres.getJdbcUrl(),
          "--source-user", postgres.getUsername(),
          "--source-password", postgres.getPassword(),
          "--sink-connect", sqlserver.getJdbcUrl(),
          "--sink-user", sqlserver.getUsername(),
          "--sink-password", sqlserver.getPassword(),
          "--source-columns", SOURCE_COLUMNS,
          "--sink-columns", SINK_COLUMNS,
          "--jobs", "4"
      };
      ToolOptions options = new ToolOptions(args);
      assertEquals(0, ReplicaDB.processReplica(options));
      assertEquals(TOTAL_SINK_ROWS, countSinkRows());
   }

   @Disabled("SQL Server sink does not support complete-atomic mode with staging schema - requires schema creation")
   @Test
   void testPostgres2SqlserverCompleteAtomicParallel () throws ParseException, IOException, SQLException {
      String[] args = {
          "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
          "--source-connect", postgres.getJdbcUrl(),
          "--source-user", postgres.getUsername(),
          "--source-password", postgres.getPassword(),
          "--sink-connect", sqlserver.getJdbcUrl(),
          "--sink-user", sqlserver.getUsername(),
          "--sink-password", sqlserver.getPassword(),
          "--sink-staging-schema", sqlserver.getDatabaseName(),
          "--source-columns", SOURCE_COLUMNS,
          "--sink-columns", SINK_COLUMNS,
          "--mode", ReplicationMode.COMPLETE_ATOMIC.getModeText(),
          "--jobs", "4"
      };
      ToolOptions options = new ToolOptions(args);
      assertEquals(0, ReplicaDB.processReplica(options));
      assertEquals(TOTAL_SINK_ROWS, countSinkRows());
   }

   @Disabled("SQL Server sink does not support incremental mode with staging schema - requires schema creation")
   @Test
   void testSqlserver2PostgresIncrementalParallel () throws ParseException, IOException, SQLException {
      String[] args = {
          "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
          "--source-connect", postgres.getJdbcUrl(),
          "--source-user", postgres.getUsername(),
          "--source-password", postgres.getPassword(),
          "--sink-connect", sqlserver.getJdbcUrl(),
          "--sink-user", sqlserver.getUsername(),
          "--sink-password", sqlserver.getPassword(),
          "--sink-staging-schema", sqlserver.getDatabaseName(),
          "--source-columns", SOURCE_COLUMNS,
          "--sink-columns", SINK_COLUMNS,
          "--mode", ReplicationMode.INCREMENTAL.getModeText(),
          "--jobs", "4"
      };
      ToolOptions options = new ToolOptions(args);
      assertEquals(0, ReplicaDB.processReplica(options));
      assertEquals(TOTAL_SINK_ROWS, countSinkRows());
   }
}

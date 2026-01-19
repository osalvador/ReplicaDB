package org.replicadb.sqlserver;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbSqlserverContainer;
import org.replicadb.manager.file.FileFormats;
import org.replicadb.manager.file.FileManager;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class Sqlserver2CsvFileTest {
   private static final Logger LOG = LogManager.getLogger(Sqlserver2CsvFileTest.class);
   private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
   private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
   private static final int TOTAL_SINK_ROWS = 4097;

   private static final String SINK_FILE_PATH = "file:///tmp/fileSink.csv";
   private static final String SINK_FILE_URI_PATH = "file:///tmp/fileSink.csv";

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
      File sinkFile = new File(URI.create(SINK_FILE_URI_PATH));
      LOG.info("Deleted file: {}", sinkFile.delete());
      this.sqlserverConn.close();

      // Clean the static temFiles HasMap.
      FileManager.setTempFilesPath(new HashMap<>());
   }


   public int countSinkRows () throws IOException {
      Path path = Paths.get(URI.create(SINK_FILE_URI_PATH));
      int count = (int) Files.lines(path).count();
      LOG.info("File total Rows:{}", count);
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
   void testFileComplete () throws ParseException, IOException {
      String[] args = {
          "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
          "--source-connect", sqlserver.getJdbcUrl(),
          "--source-user", sqlserver.getUsername(),
          "--source-password", sqlserver.getPassword(),
          "--sink-connect", SINK_FILE_PATH,
          "--sink-file-format", FileFormats.CSV.getType()
      };
      ToolOptions options = new ToolOptions(args);

      Assertions.assertEquals(0, ReplicaDB.processReplica(options));
      assertEquals(TOTAL_SINK_ROWS, countSinkRows());
   }


   @Test
   void testFileCompleteParallel () throws ParseException, IOException {
      String[] args = {
          "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
          "--source-connect", sqlserver.getJdbcUrl(),
          "--source-user", sqlserver.getUsername(),
          "--source-password", sqlserver.getPassword(),
          "--sink-connect", SINK_FILE_PATH,
          "--sink-file-format", FileFormats.CSV.getType(),
          "--jobs", "4"
      };
      ToolOptions options = new ToolOptions(args);

      Assertions.assertEquals(0, ReplicaDB.processReplica(options));
      assertEquals(TOTAL_SINK_ROWS, countSinkRows());
   }

   @Test
   void testFileIncremental () throws ParseException, IOException {
      String[] args = {
          "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
          "--source-connect", sqlserver.getJdbcUrl(),
          "--source-user", sqlserver.getUsername(),
          "--source-password", sqlserver.getPassword(),
          "--sink-connect", SINK_FILE_PATH,
          "--sink-file-format", FileFormats.CSV.getType(),
          "--mode", ReplicationMode.INCREMENTAL.getModeText()
      };
      ToolOptions options = new ToolOptions(args);
      Assertions.assertEquals(0, ReplicaDB.processReplica(options));
      assertEquals(TOTAL_SINK_ROWS, countSinkRows());
   }

   @Test
   void testFileIncrementalParallel () throws ParseException, IOException {
      String[] args = {
          "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
          "--source-connect", sqlserver.getJdbcUrl(),
          "--source-user", sqlserver.getUsername(),
          "--source-password", sqlserver.getPassword(),
          "--sink-connect", SINK_FILE_PATH,
          "--sink-file-format", FileFormats.CSV.getType(),
          "--mode", ReplicationMode.INCREMENTAL.getModeText(),
          "--jobs", "4"
      };
      ToolOptions options = new ToolOptions(args);
      Assertions.assertEquals(0, ReplicaDB.processReplica(options));
      assertEquals(TOTAL_SINK_ROWS, countSinkRows());
   }

}

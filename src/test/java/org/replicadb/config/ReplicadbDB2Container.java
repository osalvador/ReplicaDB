package org.replicadb.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.Db2Container;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ReplicadbDB2Container extends Db2Container {
   private static final Logger LOG = LogManager.getLogger(ReplicadbDB2Container.class);
   private static final String IMAGE_VERSION = "ibmcom/db2:11.5.0.0a";
   private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
   private static final String DB2_SINK_FILE = "/sinks/db2-sink.sql";
   private static final String DB2_SOURCE_FILE = "/db2/db2-source.sql";
   private static ReplicadbDB2Container container;

   private ReplicadbDB2Container () {
      super(IMAGE_VERSION);
   }

   public static ReplicadbDB2Container getInstance () {
      if (container == null) {
         container = new ReplicadbDB2Container();
         container.addEnv("LICENSE", "accept");
         container.start();
      }
      return container;
   }

   @Override
   public void start () {
      super.start();

      // Creating Database and configuring transaction logs
      try (Connection con = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
         LOG.info("Configuring DB2 transaction log sizes to prevent SQLCODE=-964 errors");
         try (java.sql.Statement stmt = con.createStatement()) {
            // Increase transaction log sizes to avoid "out of space" errors during tests
            // LOGFILSIZ: Size of each log file (in 4KB pages)
            // LOGPRIMARY: Number of primary log files
            // LOGSECOND: Number of secondary log files that can be allocated
            stmt.execute("UPDATE DB CFG FOR testdb USING LOGFILSIZ 10240");
            stmt.execute("UPDATE DB CFG FOR testdb USING LOGPRIMARY 20");
            stmt.execute("UPDATE DB CFG FOR testdb USING LOGSECOND 20");
            LOG.info("DB2 log configuration updated successfully");
         } catch (SQLException e) {
            LOG.warn("Failed to update DB2 log configuration (may not be critical): {}", e.getMessage());
         }

         LOG.info("Creating Db2 tables");
         ScriptRunner runner = new ScriptRunner(con, false, true);
         runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + DB2_SINK_FILE)));
         runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + DB2_SOURCE_FILE)));
      } catch (SQLException | IOException e) {
         throw new RuntimeException(e);
      }

   }

   @Override
   public void stop () {
      //do nothing, JVM handles shut down
   }
}

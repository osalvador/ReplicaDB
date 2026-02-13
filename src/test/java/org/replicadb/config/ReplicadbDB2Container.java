package org.replicadb.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ReplicadbDB2Container extends Db2Container {
   private static final Logger LOG = LogManager.getLogger(ReplicadbDB2Container.class);
   // Use IBM Container Registry (ICR) - Docker Hub images are deprecated
   // Updated from ibmcom/db2:11.5.0.0a to icr.io/db2_community/db2:11.5.9.0
   private static final DockerImageName IMAGE_NAME = selectDockerImage();
   private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
   private static final String DB2_SINK_FILE = "/sinks/db2-sink.sql";
   private static final String DB2_SOURCE_FILE = "/db2/db2-source.sql";
   private static ReplicadbDB2Container container;

   private static DockerImageName selectDockerImage() {
      String arch = System.getProperty("os.arch").toLowerCase();
      String osName = System.getProperty("os.name").toLowerCase();
      
      // Check for ARM64 architecture (native ARM Java)
      boolean isArm64 = arch.contains("aarch64") || arch.contains("arm64");
      
      // Check for macOS (likely Apple Silicon even if running x86_64 via Rosetta)
      if (!isArm64 && osName.contains("mac")) {
         try {
            // Check if we're on Apple Silicon using sysctl
            // This works even when Java runs under Rosetta 2 (x86_64 emulation)
            Process process = Runtime.getRuntime().exec(new String[]{"sysctl", "-n", "hw.optional.arm64"});
            BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(process.getInputStream()));
            String result = reader.readLine();
            reader.close();
            int exitCode = process.waitFor();
            
            if (exitCode == 0 && "1".equals(result)) {
               isArm64 = true;
            }
         } catch (IOException | InterruptedException e) {
            LOG.debug("Could not determine ARM64 architecture via sysctl", e);
         }
      }
      
      if (isArm64) {
         LOG.warn("╔════════════════════════════════════════════════════════════════╗");
         LOG.warn("║ ⚠️  ARM64 ARCHITECTURE DETECTED (Apple Silicon)                 ║");
         LOG.warn("║                                                                ║");
         LOG.warn("║ IBM DB2 does NOT have native ARM64 support.                   ║");
         LOG.warn("║ Tests will run via Rosetta 2 emulation (5-10x slower).        ║");
         LOG.warn("║                                                                ║");
         LOG.warn("║ FIRST STARTUP: ~8-12 minutes (via emulation)                  ║");
         LOG.warn("║ SUBSEQUENT RUNS: ~instant (container reuse enabled)           ║");
         LOG.warn("║                                                                ║");
         LOG.warn("║ TIP: Leave the DB2 container running between test runs        ║");
         LOG.warn("║      to avoid slow startup on every execution.                ║");
         LOG.warn("╚════════════════════════════════════════════════════════════════╝");
      }
      
      return DockerImageName.parse("icr.io/db2_community/db2:11.5.9.0");
   }

   private ReplicadbDB2Container() {
      super(IMAGE_NAME);
      // Accept IBM DB2 license
      this.addEnv("LICENSE", "accept");
      // Startup optimization environment variables to reduce initialization time
      this.addEnv("AUTOCONFIG", "false");        // Skip automatic configuration (saves 1-2 min)
      this.addEnv("ARCHIVE_LOGS", "false");      // Disable archive logging (saves 30-60 sec)
      this.addEnv("TEXT_SEARCH", "false");       // Disable text search indexing (minor improvement)
      this.addEnv("SAMPLEDB", "false");          // Don't create sample database
   }

   public static ReplicadbDB2Container getInstance() {
      if (container == null) {
         try {
            LOG.info("Initializing DB2 container with image: {}", IMAGE_NAME);
            container = new ReplicadbDB2Container();
            // Enable container reuse for faster subsequent test runs (experimental feature)
            container.withReuse(true);
            LOG.info("Starting DB2 container (this may take 3-4 minutes on first run, 8-12 min on ARM64)...");
            container.start();
            LOG.info("DB2 container started successfully");
         } catch (Exception e) {
            LOG.error("Failed to initialize DB2 container", e);
            throw new RuntimeException("Failed to initialize DB2 container", e);
         }
      }
      return container;
   }

   @Override
   public void start() {
      LOG.info("Starting DB2 container initialization...");
      super.start();

      // Creating Database and configuring transaction logs
      try (Connection con = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
         LOG.info("DB2 container ready, configuring transaction logs...");
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

         LOG.info("Creating DB2 test tables from SQL scripts...");
         ScriptRunner runner = new ScriptRunner(con, false, true);
         runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + DB2_SINK_FILE)));
         runner.runScript(new BufferedReader(new FileReader(RESOURCE_DIR + DB2_SOURCE_FILE)));
         LOG.info("DB2 container fully initialized and ready for tests");
      } catch (SQLException | IOException e) {
         LOG.error("Failed to initialize DB2 container", e);
         throw new RuntimeException(e);
      }

   }

   @Override
   public void stop() {
      //do nothing, JVM handles shut down
   }
}

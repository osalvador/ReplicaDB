package org.replicadb.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.utils.ScriptRunner;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.TestcontainersConfiguration;

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
      
      return DockerImageName.parse(System.getenv().getOrDefault(
            "REPLICADB_TEST_DB2_IMAGE", "icr.io/db2_community/db2:11.5.9.0"));
   }

   private ReplicadbDB2Container() {
      super(IMAGE_NAME);
      // Accept IBM DB2 license
      this.addEnv("LICENSE", "accept");
      // Startup optimization environment variables to reduce initialization time
      this.addEnv("AUTOCONFIG", "false");        // Skip automatic configuration (saves 1-2 min)
      this.addEnv("ARCHIVE_LOGS", "false");      // Disable archive logging (saves 30-60 sec)
      this.addEnv("TEXT_SEARCH", "false");       // Disable text search indexing (minor improvement)
      this.addEnv("SAMPLEDB", "false");          // Don't create sample database with sample data
      // Set longer startup timeout for ARM64 emulation (default is 120 seconds, not enough)
      this.withStartupTimeout(java.time.Duration.ofMinutes(20));
   }

   private static class Holder {
      private static final ReplicadbDB2Container INSTANCE = startFixture();
   }

   public static ReplicadbDB2Container getInstance() {
      return Holder.INSTANCE;
   }

   public static void resetSink(Connection testConnection) throws SQLException {
      if (testConnection == null) {
         return;
      }
      // Close any remaining cursors before making TRUNCATE the first statement on a fresh connection.
      testConnection.close();
      ReplicadbDB2Container fixture = getInstance();
      try (Connection cleanup = DriverManager.getConnection(
              fixture.getJdbcUrl(), fixture.getUsername(), fixture.getPassword());
           var statement = cleanup.createStatement()) {
         statement.execute("TRUNCATE TABLE t_sink REUSE STORAGE IMMEDIATE");
      }
   }

   private static ReplicadbDB2Container startFixture() {
      LOG.info("Starting DB2 container with image {}", IMAGE_NAME);
      ReplicadbDB2Container candidate = new ReplicadbDB2Container();
      candidate.withReuse(TestcontainersConfiguration.getInstance().environmentSupportsReuse());
      candidate.start();
      return candidate;
   }

   @Override
   public void start() {
      LOG.info("Starting DB2 container initialization...");
      super.start();

      try {
         // UPDATE DB CFG is a CLP command. Apply the log settings before opening the JDBC fixture connection.
         ExecResult result = execInContainer("su", "-", getUsername(), "-c",
               ". ~/sqllib/db2profile && db2 update db cfg for " + getDatabaseName()
                     + " using LOGFILSIZ 10240 LOGPRIMARY 20 LOGSECOND 20 DEFERRED && db2stop force && db2start");
         if (result.getExitCode() != 0) {
            throw new IllegalStateException("Could not configure DB2 transaction logs: "
                  + result.getStdout() + result.getStderr());
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new IllegalStateException("Interrupted while configuring DB2 transaction logs", e);
      } catch (IOException e) {
         throw new IllegalStateException("Could not configure DB2 transaction logs", e);
      }

      try (Connection con = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword())) {
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

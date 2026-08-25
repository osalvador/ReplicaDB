package org.replicadb.server.job.execution;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuartzClusterIT {

        static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .waitingFor(Wait.forListeningPort());

    private static String schema;

    @BeforeAll
    static void migrateSchema() throws Exception {
        POSTGRES.start();
        schema = PostgresTestcontainersConfig.isolatedSchema();
        PostgresTestcontainersConfig.migrate(POSTGRES, schema);
    }

    @AfterAll
    static void dropSchema() throws Exception {
        if (POSTGRES.isRunning()) {
            PostgresTestcontainersConfig.dropSchema(POSTGRES, schema);
            POSTGRES.stop();
        }
    }

    @Test
    void twoClusteredSchedulersCreateOneDurableRunForOneTrigger() throws Exception {
        UUID jobDefinitionId = UUID.randomUUID();
        insertDefinition(jobDefinitionId);

        Path barrier = Files.createTempDirectory("replicadb-quartz-cluster-");
        Process first = null;
        Process second = null;
        try {
            first = launchNode("api-one", jobDefinitionId, barrier);
            second = launchNode("api-two", jobDefinitionId, barrier);
            awaitReady(barrier.resolve("api-one.ready"), first, "api-one");
            awaitReady(barrier.resolve("api-two.ready"), second, "api-two");
            Files.createFile(barrier.resolve("go"));
            awaitProcess(first, "api-one");
            awaitProcess(second, "api-two");

            assertEquals(1, countFires(jobDefinitionId));
            assertEquals(1, countRuns(jobDefinitionId));
        } finally {
            stop(first);
            stop(second);
            deleteDirectory(barrier);
        }
    }

    private static Process launchNode(String instanceId, UUID jobDefinitionId, Path barrier)
            throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                QuartzNode.class.getName(), jdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword(),
                jobDefinitionId.toString(), instanceId, barrier.toString())
                .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .start();
    }

    private static Scheduler scheduler(String instanceId, String url, String username,
                                       String password) throws SchedulerException {
        Properties properties = new Properties();
        properties.setProperty("org.quartz.scheduler.instanceName", "ReplicaDbScheduler");
        properties.setProperty("org.quartz.scheduler.instanceId", instanceId);
        properties.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
        properties.setProperty("org.quartz.threadPool.threadCount", "1");
        properties.setProperty("org.quartz.threadPool.threadPriority", "5");
        properties.setProperty("org.quartz.jobStore.class", "org.quartz.impl.jdbcjobstore.JobStoreTX");
        properties.setProperty("org.quartz.jobStore.driverDelegateClass",
                "org.quartz.impl.jdbcjobstore.PostgreSQLDelegate");
        properties.setProperty("org.quartz.jobStore.tablePrefix", "QRTZ_");
        properties.setProperty("org.quartz.jobStore.isClustered", "true");
        properties.setProperty("org.quartz.jobStore.clusterCheckinInterval", "1000");
        properties.setProperty("org.quartz.jobStore.misfireThreshold", "60000");
        properties.setProperty("org.quartz.jobStore.dataSource", "metadata");
        properties.setProperty("org.quartz.dataSource.metadata.provider", "hikaricp");
        properties.setProperty("org.quartz.dataSource.metadata.driver", "org.postgresql.Driver");
        properties.setProperty("org.quartz.dataSource.metadata.URL", url);
        properties.setProperty("org.quartz.dataSource.metadata.user", username);
        properties.setProperty("org.quartz.dataSource.metadata.password", password);
        properties.setProperty("org.quartz.dataSource.metadata.maxConnections", "5");
        properties.setProperty("org.quartz.dataSource.metadata.validationQuery", "SELECT 1");
        properties.setProperty("org.quartz.scheduler.skipUpdateCheck", "true");
        properties.setProperty("org.quartz.scheduler.makeSchedulerThreadDaemon", "true");
        properties.setProperty("org.quartz.threadPool.makeThreadsDaemons", "true");
        properties.setProperty("org.quartz.scheduler.batchTriggerAcquisitionMaxCount", "1");
        properties.setProperty("org.quartz.scheduler.rmi.export", "false");
        properties.setProperty("org.quartz.scheduler.rmi.proxy", "false");
        return new org.quartz.impl.StdSchedulerFactory(properties).getScheduler();
    }

    private static void insertDefinition(UUID jobDefinitionId) throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement createTable = connection.createStatement();
             PreparedStatement statement = connection.prepareStatement("""
                     INSERT INTO job_definition
                         (id, name, source_connect, source_table, sink_connect, sink_table, mode, jobs)
                     VALUES (?, ?, 'jdbc:source', 'source_table', 'jdbc:sink', 'sink_table', 'complete', 1)
                     """)) {
            createTable.execute("""
                    CREATE TABLE quartz_cluster_fire (
                        id UUID PRIMARY KEY,
                        job_definition_id UUID NOT NULL
                    )
                    """);
            statement.setObject(1, jobDefinitionId);
            statement.setString(2, "quartz-cluster-" + jobDefinitionId);
            statement.executeUpdate();
        }
    }

    private static void awaitReady(Path readyFile, Process process, String instanceId) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline && !Files.exists(readyFile)) {
            if (!process.isAlive()) {
                throw new AssertionError("Quartz node stopped before becoming ready: " + instanceId);
            }
            Thread.sleep(25);
        }
        assertTrue(Files.exists(readyFile), "Quartz node did not become ready: " + instanceId);
    }

    private static void awaitProcess(Process process, String instanceId) throws Exception {
        assertTrue(process.waitFor(15, TimeUnit.SECONDS),
                "Quartz node did not stop: " + instanceId);
        int exitCode = process.exitValue();
        assertEquals(0, exitCode, "Quartz node failed: " + instanceId);
    }

    private static int countRuns(UUID jobDefinitionId) throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword());
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT COUNT(*) FROM job_run WHERE job_definition_id = ?")) {
            statement.setObject(1, jobDefinitionId);
            try (var resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getInt(1);
            }
        }
    }

    private static int countFires(UUID jobDefinitionId) throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword());
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT COUNT(*) FROM quartz_cluster_fire WHERE job_definition_id = ?")) {
            statement.setObject(1, jobDefinitionId);
            try (var resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getInt(1);
            }
        }
    }

    private static void stop(Process process) {
        if (process != null && process.isAlive()) {
            process.destroyForcibly();
        }
    }

    private static void deleteDirectory(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (var paths = Files.walk(directory)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException exception) {
                    throw new RuntimeException(exception);
                }
            });
        }
    }

    private static String jdbcUrl() {
        return PostgresTestcontainersConfig.jdbcUrl(POSTGRES, schema);
    }

    public static class QuartzNode {

        public static void main(String[] args) throws Exception {
            String url = args[0];
            String username = args[1];
            String password = args[2];
            UUID jobDefinitionId = UUID.fromString(args[3]);
            String instanceId = args[4];
            Path barrier = Path.of(args[5]);
            Scheduler scheduler = scheduler(instanceId, url, username, password);
            try {
                scheduler.start();
                Files.createFile(barrier.resolve(instanceId + ".ready"));
                waitForGo(barrier.resolve("go"));

                JobDetail detail = JobBuilder.newJob(DurableRunJob.class)
                        .withIdentity(jobDefinitionId.toString(), "replicadb-jobs")
                        .usingJobData("jobDefinitionId", jobDefinitionId.toString())
                        .usingJobData("jdbcUrl", url)
                        .usingJobData("username", username)
                        .usingJobData("password", password)
                        .storeDurably(true)
                        .build();
                Trigger trigger = TriggerBuilder.newTrigger()
                        .withIdentity(jobDefinitionId.toString(), "replicadb-jobs")
                        .forJob(detail)
                        .startNow()
                        .withSchedule(SimpleScheduleBuilder.simpleSchedule())
                        .build();
                try {
                    scheduler.scheduleJob(detail, trigger);
                } catch (ObjectAlreadyExistsException ignored) {
                    // The other clustered scheduler won the durable registration race.
                }
                Thread.sleep(3_000);
            } finally {
                scheduler.shutdown(true);
            }
        }

        private static void waitForGo(Path goFile) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline && !Files.exists(goFile)) {
                Thread.sleep(25);
            }
            if (!Files.exists(goFile)) {
                throw new IllegalStateException("Quartz cluster test barrier was not released");
            }
        }
    }

    public static class DurableRunJob implements Job {

        @Override
        public void execute(org.quartz.JobExecutionContext context) throws org.quartz.JobExecutionException {
            JobDataMap data = context.getMergedJobDataMap();
            try (Connection connection = DriverManager.getConnection(data.getString("jdbcUrl"),
                    data.getString("username"), data.getString("password"));
                 PreparedStatement fire = connection.prepareStatement("""
                         INSERT INTO quartz_cluster_fire (id, job_definition_id)
                         VALUES (?, ?)
                         """);
                 PreparedStatement run = connection.prepareStatement("""
                         INSERT INTO job_run (id, job_definition_id, status, attempt, available_at, created_at)
                         VALUES (?, ?, 'PENDING', 1, now(), now())
                         """)) {
                UUID jobDefinitionId = UUID.fromString(data.getString("jobDefinitionId"));
                fire.setObject(1, UUID.randomUUID());
                fire.setObject(2, jobDefinitionId);
                fire.executeUpdate();
                run.setObject(1, UUID.randomUUID());
                run.setObject(2, jobDefinitionId);
                run.executeUpdate();
            } catch (Exception exception) {
                throw new org.quartz.JobExecutionException(
                        "Could not persist clustered Quartz test run", exception);
            }
        }
    }
}

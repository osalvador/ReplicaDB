package org.replicadb.manager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.replicadb.cli.ToolOptions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqliteManagerCancellationTest {

    @TempDir
    Path tempDir;

    @Test
    void stopsBatchInsertAfterCancellationAndPreservesCommittedBatches() throws Exception {
        String sourceUrl = "jdbc:sqlite:" + tempDir.resolve("source.db");
        String sinkUrl = "jdbc:sqlite:" + tempDir.resolve("sink.db");
        createSource(sourceUrl);
        createSink(sinkUrl);

        ToolOptions options = new ToolOptions(new String[]{
                "--mode", "complete",
                "--source-connect", sourceUrl,
                "--source-table", "source_table",
                "--sink-connect", sinkUrl,
                "--sink-table", "sink_table",
                "--fetch-size", "10"
        });
        SqliteManager manager = new SqliteManager(options, DataSourceType.SINK);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch rowLatch = new CountDownLatch(1);
        CountDownLatch releaseLatch = new CountDownLatch(1);
        Future<Integer> future;

        try (Connection source = DriverManager.getConnection(sourceUrl);
             Statement sourceStatement = source.createStatement();
             ResultSet sourceResultSet = sourceStatement.executeQuery("SELECT id, value FROM source_table ORDER BY id")) {
            ResultSet countingResultSet = countAndPauseAtRow(sourceResultSet, rowLatch, releaseLatch);
            future = executor.submit(() -> manager.insertDataToTable(countingResultSet, 0));

            assertTrue(rowLatch.await(10, TimeUnit.SECONDS));
            options.getExecutionContext().requestCancellation();
            releaseLatch.countDown();

            ExecutionException failure = org.junit.jupiter.api.Assertions.assertThrows(
                    ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
            assertInstanceOf(org.replicadb.execution.ReplicationCancelledException.class, failure.getCause());
        } finally {
            releaseLatch.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            manager.close();
        }

        int committedRows = countRows(sinkUrl);
        assertTrue(committedRows > 0 && committedRows < 500);
    }

    @Test
    void checksCancellationBeforeOpeningAConnectionForMerge() throws Exception {
        ToolOptions options = new ToolOptions(new String[]{
                "--mode", "incremental",
                "--source-connect", "jdbc:sqlite::memory:",
                "--source-table", "source_table",
                "--sink-connect", "jdbc:sqlite::memory:",
                "--sink-table", "sink_table"
        });
        SqliteManager manager = new SqliteManager(options, DataSourceType.SINK);
        options.getExecutionContext().requestCancellation();

        try {
            org.junit.jupiter.api.Assertions.assertThrows(
                    org.replicadb.execution.ReplicationCancelledException.class,
                    manager::mergeStagingTable);
        } finally {
            manager.close();
        }
    }

    private static ResultSet countAndPauseAtRow(ResultSet delegate, CountDownLatch rowLatch,
                                                 CountDownLatch releaseLatch) {
        AtomicInteger rowsSeen = new AtomicInteger();
        return (ResultSet) Proxy.newProxyInstance(
                ResultSet.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(delegate, args);
                        if ("next".equals(method.getName()) && Boolean.TRUE.equals(result)
                                && rowsSeen.incrementAndGet() == 50) {
                            rowLatch.countDown();
                            if (!releaseLatch.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError("Cancellation release was not signalled");
                            }
                        }
                        return result;
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static void createSource(String sourceUrl) throws Exception {
        try (Connection connection = DriverManager.getConnection(sourceUrl);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE source_table (id INTEGER PRIMARY KEY, value TEXT)");
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO source_table (id, value) VALUES (?, ?)")) {
                for (int id = 1; id <= 500; id++) {
                    insert.setInt(1, id);
                    insert.setString(2, "value-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    private static void createSink(String sinkUrl) throws Exception {
        try (Connection connection = DriverManager.getConnection(sinkUrl);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE sink_table (id INTEGER PRIMARY KEY, value TEXT)");
        }
    }

    private static int countRows(String sinkUrl) throws Exception {
        try (Connection connection = DriverManager.getConnection(sinkUrl);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM sink_table")) {
            assertTrue(resultSet.next());
            return resultSet.getInt(1);
        }
    }
}
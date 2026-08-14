package org.replicadb.postgres;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.replicadb.execution.ReplicationCancelledException;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.PostgresqlManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Testcontainers
class PostgresqlManagerCancellationTest {

    private static final String SINK_TABLE = "replicadb_cancel_sink";
    private static final int TOTAL_ROWS = 50_000;

    @Container
    static final PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres =
            ReplicadbPostgresqlContainer.getInstance();

    @Test
    void cancelsTextCopyAndDoesNotCommitTheCompleteInput() throws Exception {
        createSinkTable();
        ToolOptions options = options();
        PostgresqlManager manager = new PostgresqlManager(options, DataSourceType.SINK);
        CountDownLatch rowLatch = new CountDownLatch(1);
        CountDownLatch releaseLatch = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        manager.getConnection();
        ResultSet resultSet = resultSet(rowLatch, releaseLatch);
        final Future<Integer> future = executor.submit(() -> manager.insertDataToTable(resultSet, 0));

        try {
            assertTrue(rowLatch.await(20, TimeUnit.SECONDS));
            options.getExecutionContext().requestCancellation();
            releaseLatch.countDown();

            ExecutionException failure = org.junit.jupiter.api.Assertions.assertThrows(
                    ExecutionException.class, () -> future.get(20, TimeUnit.SECONDS));
            assertInstanceOf(ReplicationCancelledException.class, failure.getCause());
        } finally {
            releaseLatch.countDown();
            future.cancel(true);
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(20, TimeUnit.SECONDS));
            manager.close();
        }

        assertTrue(countSinkRows() < TOTAL_ROWS);
        dropSinkTable();
    }

    private static ResultSet resultSet(CountDownLatch rowLatch, CountDownLatch releaseLatch) throws Exception {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.INTEGER);
        when(metadata.getColumnLabel(1)).thenReturn("id");
        when(metadata.getColumnName(1)).thenReturn("id");

        ResultSet resultSet = mock(ResultSet.class);
        AtomicInteger currentRow = new AtomicInteger();
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(resultSet.next()).thenAnswer(invocation -> {
            int row = currentRow.incrementAndGet();
            if (row == 50) {
                rowLatch.countDown();
                if (!releaseLatch.await(20, TimeUnit.SECONDS)) {
                    throw new AssertionError("Cancellation release was not signalled");
                }
            }
            return row <= TOTAL_ROWS;
        });
        when(resultSet.getString(1)).thenAnswer(invocation -> Integer.toString(currentRow.get()));
        when(resultSet.wasNull()).thenReturn(false);
        return resultSet;
    }

    private static ToolOptions options() throws Exception {
        return new ToolOptions(new String[]{
                "--mode", "complete",
                "--source-connect", postgres.getJdbcUrl(),
                "--source-user", postgres.getUsername(),
                "--source-password", postgres.getPassword(),
                "--source-table", "unused_source",
                "--sink-connect", postgres.getJdbcUrl(),
                "--sink-user", postgres.getUsername(),
                "--sink-password", postgres.getPassword(),
                "--sink-table", SINK_TABLE
        });
    }

    private static void createSinkTable() throws Exception {
        try (Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + SINK_TABLE);
            statement.execute("CREATE TABLE " + SINK_TABLE + " (id INTEGER)");
        }
    }

    private static int countSinkRows() throws Exception {
        try (Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword());
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + SINK_TABLE)) {
            assertTrue(resultSet.next());
            return resultSet.getInt(1);
        }
    }

    private static void dropSinkTable() throws Exception {
        try (Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + SINK_TABLE);
        }
    }
}
package org.replicadb.manager.file;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.execution.ReplicationCancelledException;
import org.replicadb.manager.DataSourceType;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CsvFileManagerCancellationTest {

    @Test
    void stopsWritingRowsAfterCancellation() throws Exception {
        ToolOptions options = options();
        CsvFileManager manager = new CsvFileManager(options, DataSourceType.SINK);
        CountDownLatch rowLatch = new CountDownLatch(1);
        CountDownLatch releaseLatch = new CountDownLatch(1);
        final ResultSet resultSet = resultSet(rowLatch, releaseLatch);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Integer> future = executor.submit(() -> manager.writeData(output, resultSet, 1, null));

        try {
            assertTrue(rowLatch.await(10, TimeUnit.SECONDS), () -> workerFailure(future));
            options.getExecutionContext().requestCancellation();
            releaseLatch.countDown();

            ExecutionException failure = org.junit.jupiter.api.Assertions.assertThrows(
                    ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
            assertInstanceOf(ReplicationCancelledException.class, failure.getCause());
        } finally {
            releaseLatch.countDown();
            future.cancel(true);
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        long writtenRows = new String(output.toByteArray(), StandardCharsets.UTF_8).lines().count();
        assertTrue(writtenRows > 0 && writtenRows < 500);
    }

    private static String workerFailure(Future<Integer> future) {
        if (!future.isDone()) {
            return "CSV writer did not reach row 50";
        }
        try {
            future.get();
            return "CSV writer completed before row 50";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "CSV writer wait was interrupted";
        } catch (ExecutionException e) {
            return "CSV writer failed before row 50: " + e.getCause();
        }
    }

    private static ResultSet resultSet(CountDownLatch rowLatch, CountDownLatch releaseLatch) throws Exception {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(metadata.getColumnLabel(1)).thenReturn("value");
        when(metadata.getColumnName(1)).thenReturn("value");

        ResultSet resultSet = mock(ResultSet.class);
        AtomicInteger currentRow = new AtomicInteger();
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(resultSet.next()).thenAnswer(invocation -> {
            int row = currentRow.incrementAndGet();
            if (row == 50) {
                rowLatch.countDown();
                if (!releaseLatch.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("Cancellation release was not signalled");
                }
            }
            return row <= 500;
        });
        when(resultSet.getObject(1)).thenAnswer(invocation -> "value-" + currentRow.get());
        return resultSet;
    }

    private static ToolOptions options() throws Exception {
        ToolOptions options = new ToolOptions(new String[]{
                "--source-connect", "jdbc:postgresql://source",
                "--source-table", "customers",
                "--sink-connect", "file:///tmp/cancel.csv",
                "--sink-table", "customer_copy"
        });
        options.setSinkConnectionParams(new Properties());
        return options;
    }
}
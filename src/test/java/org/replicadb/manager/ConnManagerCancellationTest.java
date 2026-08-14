package org.replicadb.manager;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.execution.ReplicationCancelledException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class ConnManagerCancellationTest {

    @Test
    void checkCancellationReflectsTheRunContext() throws Exception {
        ToolOptions options = options();
        ExposedManager manager = new ExposedManager(options);

        assertDoesNotThrow(manager::check);

        options.getExecutionContext().requestCancellation();

        assertThrows(ReplicationCancelledException.class, manager::check);
    }

    @Test
    void registersAndUnregistersStatementsThroughTheRunContext() throws Exception {
        ToolOptions options = options();
        ExposedManager manager = new ExposedManager(options);
        Statement activeStatement = mock(Statement.class);
        Statement removedStatement = mock(Statement.class);

        manager.register(activeStatement);
        manager.register(removedStatement);
        manager.unregister(removedStatement);
        options.getExecutionContext().requestCancellation();

        verify(activeStatement).cancel();
        verifyNoInteractions(removedStatement);
    }

    private static ToolOptions options() throws Exception {
        return new ToolOptions(new String[]{
                "--source-connect", "jdbc:postgresql://source",
                "--source-table", "customers",
                "--sink-connect", "jdbc:postgresql://sink",
                "--sink-table", "customer_copy"
        });
    }

    private static final class ExposedManager extends ConnManager {

        private ExposedManager(ToolOptions options) {
            this.options = options;
        }

        private void check() throws ReplicationCancelledException {
            checkCancellation();
        }

        private void register(Statement statement) {
            registerActiveStatement(statement);
        }

        private void unregister(Statement statement) {
            unregisterActiveStatement(statement);
        }

        @Override
        public ResultSet readTable(String tableName, String[] columns, int nThread) {
            return null;
        }

        @Override
        public int insertDataToTable(ResultSet resultSet, int taskId) {
            return 0;
        }

        @Override
        public Connection getConnection() {
            return null;
        }

        @Override
        public String getDriverClass() {
            return "";
        }

        @Override
        public void close() {
        }

        @Override
        public void cleanUp() {
        }

        @Override
        public void release() {
        }

        @Override
        public void preSourceTasks() {
        }

        @Override
        public Future<Integer> preSinkTasks(ExecutorService executor) {
            return null;
        }

        @Override
        public void postSourceTasks() {
        }

        @Override
        public void postSinkTasks() {
        }

        @Override
        public String[] getSinkPrimaryKeys(String tableName) {
            return new String[0];
        }
    }
}
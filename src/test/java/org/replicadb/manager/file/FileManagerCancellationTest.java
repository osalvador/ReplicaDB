package org.replicadb.manager.file;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.execution.ReplicationCancelledException;
import org.replicadb.manager.DataSourceType;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileManagerCancellationTest {

    @Test
    void checkCancellationReflectsTheRunContext() throws Exception {
        ToolOptions options = options();
        ExposedFileManager manager = new ExposedFileManager(options);

        assertDoesNotThrow(manager::check);

        options.getExecutionContext().requestCancellation();

        assertThrows(ReplicationCancelledException.class, manager::check);
    }

    private static ToolOptions options() throws Exception {
        return new ToolOptions(new String[]{
                "--source-connect", "jdbc:postgresql://source",
                "--source-table", "customers",
                "--sink-connect", "jdbc:postgresql://sink",
                "--sink-table", "customer_copy"
        });
    }

    private static final class ExposedFileManager extends FileManager {

        private ExposedFileManager(ToolOptions options) {
            super(options, DataSourceType.SINK);
        }

        private void check() throws ReplicationCancelledException {
            checkCancellation();
        }

        @Override
        public int writeData(OutputStream out, ResultSet resultSet, int taskId, File tempFile) {
            return 0;
        }

        @Override
        public void mergeFiles() throws IOException, URISyntaxException {
        }

        @Override
        public void cleanUp() {
        }

        @Override
        public void init() {
        }

        @Override
        public ResultSet readData() {
            return null;
        }
    }
}
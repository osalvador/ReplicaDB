package org.replicadb.server.local;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import javax.sql.DataSource;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class EmbeddedPostgresRuntime implements AutoCloseable {

    private static final Duration PROCESS_EXIT_TIMEOUT = Duration.ofSeconds(5);

    public static final String USERNAME = "postgres";
    public static final String PASSWORD = "postgres";
    public static final String DATABASE = "postgres";

    private final EmbeddedPostgres postgres;
    private boolean closed;

    EmbeddedPostgresRuntime(EmbeddedPostgres postgres) {
        this.postgres = Objects.requireNonNull(postgres, "postgres must not be null");
    }

    public DataSource getDataSource() {
        return postgres.getPostgresDatabase();
    }

    public String getJdbcUrl() {
        return postgres.getJdbcUrl(USERNAME, DATABASE);
    }

    public String getUsername() {
        return USERNAME;
    }

    public String getPassword() {
        return PASSWORD;
    }

    public int getPort() {
        return postgres.getPort();
    }

    public Process getProcess() {
        return postgres.getProcess();
    }

    public synchronized boolean isClosed() {
        return closed;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        try {
            postgres.close();
        } catch (IOException exception) {
            failure = exception;
        }
        Process process = postgres.getProcess();
        if (process.isAlive()) {
            try {
                if (!process.waitFor(PROCESS_EXIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    process.destroy();
                    if (!process.waitFor(PROCESS_EXIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                        process.destroyForcibly();
                        if (!process.waitFor(PROCESS_EXIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                            IOException timeout = new IOException("Embedded PostgreSQL process did not exit");
                            if (failure == null) {
                                failure = timeout;
                            } else {
                                failure.addSuppressed(timeout);
                            }
                        }
                    }
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                IOException interrupted = new IOException(
                        "Interrupted while waiting for embedded PostgreSQL to exit", exception);
                if (failure == null) {
                    failure = interrupted;
                } else {
                    failure.addSuppressed(interrupted);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}

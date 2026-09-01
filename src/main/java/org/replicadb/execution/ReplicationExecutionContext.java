package org.replicadb.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ReplicationExecutionContext {

    private static final Logger LOG = LogManager.getLogger(ReplicationExecutionContext.class.getName());

    private final String runId = UUID.randomUUID().toString();
    private final ReplicationDiagnosticCollector diagnosticCollector = new ReplicationDiagnosticCollector.Bounded();
    private final Map<Integer, String> tempFilesPath = new ConcurrentHashMap<>();
    private final AtomicBoolean cancellationRequested = new AtomicBoolean(false);
    private final Set<Statement> activeStatements = ConcurrentHashMap.newKeySet();
    private volatile String sinkStagingTableName;
    private volatile String watermarkCandidate;
    private volatile long rowsProcessed;
    private volatile long durationMillis;

    public String getRunId() {
        return runId;
    }

    public ReplicationDiagnosticCollector getDiagnosticCollector() {
        return diagnosticCollector;
    }

    public String getSinkStagingTableName() {
        return sinkStagingTableName;
    }

    public void setSinkStagingTableName(String sinkStagingTableName) {
        this.sinkStagingTableName = sinkStagingTableName;
    }

    /**
     * The run-level reduced watermark candidate, set only after a successful merge (Task 2.3).
     */
    public String getWatermarkCandidate() {
        return watermarkCandidate;
    }

    public void setWatermarkCandidate(String watermarkCandidate) {
        this.watermarkCandidate = watermarkCandidate;
    }

    public long getRowsProcessed() {
        return rowsProcessed;
    }

    public void setRowsProcessed(long rowsProcessed) {
        this.rowsProcessed = rowsProcessed;
    }

    public long getDurationMillis() {
        return durationMillis;
    }

    public void setDurationMillis(long durationMillis) {
        this.durationMillis = durationMillis;
    }

    public Map<Integer, String> getTempFilesPath() {
        return tempFilesPath;
    }

    public void setTempFilePath(int taskId, String path) {
        tempFilesPath.put(taskId, path);
    }

    public String getTempFilePath(int taskId) {
        return tempFilesPath.get(taskId);
    }

    public int getTempFilePathSize() {
        return tempFilesPath.size();
    }

    public void requestCancellation() {
        cancellationRequested.set(true);
        cancelActiveStatements();
    }

    public boolean isCancellationRequested() {
        return cancellationRequested.get();
    }

    public void registerActiveStatement(Statement statement) {
        activeStatements.add(statement);
        if (cancellationRequested.get()) {
            cancelStatement(statement);
        }
    }

    public void unregisterActiveStatement(Statement statement) {
        activeStatements.remove(statement);
    }

    private void cancelActiveStatements() {
        for (Statement statement : activeStatements) {
            cancelStatement(statement);
        }
    }

    private void cancelStatement(Statement statement) {
        try {
            statement.cancel();
        } catch (SQLException e) {
            LOG.warn("Failed to cancel statement for run {}: {}", runId, e.getMessage());
        }
    }
}

package org.replicadb.execution;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class ReplicationExecutionContext {

    private final String runId = UUID.randomUUID().toString();
    private final Map<Integer, String> tempFilesPath = new ConcurrentHashMap<>();
    private volatile String sinkStagingTableName;

    public String getRunId() {
        return runId;
    }

    public String getSinkStagingTableName() {
        return sinkStagingTableName;
    }

    public void setSinkStagingTableName(String sinkStagingTableName) {
        this.sinkStagingTableName = sinkStagingTableName;
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
}

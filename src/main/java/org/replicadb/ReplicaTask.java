package org.replicadb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.CredentialRedactor;
import org.replicadb.manager.ConnManager;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.ManagerFactory;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public final class ReplicaTask implements Callable<ReplicaTaskResult> {

    private static final Logger LOG = LogManager.getLogger(ReplicaTask.class.getName());

    private final int taskId;
    private final ToolOptions options;
    private final ManagerFactory managerFactory;


    public ReplicaTask(int id, ToolOptions options) {
        this(id, options, new ManagerFactory());
    }

    ReplicaTask(int id, ToolOptions options, ManagerFactory managerFactory) {
        this.taskId = id;
        this.options = options;
        this.managerFactory = managerFactory;
    }

    @Override
    public ReplicaTaskResult call() throws Exception {
        final long startedAtMillis = System.currentTimeMillis();
        String taskName = "TaskId-" + this.taskId;

        Thread.currentThread().setName(taskName);

        LOG.info("Starting  {}", Thread.currentThread().getName());

        ConnManager sourceDs = null;
        ConnManager sinkDs = null;
        Exception failure = null;
        try {
            sourceDs = managerFactory.accept(options, DataSourceType.SOURCE);
            sinkDs = managerFactory.accept(options, DataSourceType.SINK);

            try {
                sourceDs.getConnection();
            } catch (Exception e) {
                LOG.error("ERROR in {} getting Source connection: {} ", taskName,
                    CredentialRedactor.redactMessage(e.getMessage()));
                throw e;
            }

            try {
                sinkDs.getConnection();
            } catch (Exception e) {
                LOG.error("ERROR in {} getting Sink connection:{} ", taskName,
                    CredentialRedactor.redactMessage(e.getMessage()));
                throw e;
            }

            ResultSet rs;
            try {
                rs = sourceDs.readTable(null, null, taskId);
            } catch (Exception e) {
                LOG.error("ERROR in {} reading source table: {}", taskName,
                    CredentialRedactor.redactMessage(e.getMessage()));
                throw e;
            }

            long processedRows;
            try {
                processedRows = sinkDs.insertDataToTable(rs, taskId);
                // TODO determine the total rows processed in all the managers
                LOG.info("A total of {} rows processed by task {}", processedRows, taskId);
            } catch (Exception e) {
                LOG.error("ERROR in {} inserting data to sink table: {} ", taskName, getExceptionMessageChain(e));
                throw e;
            }

            String watermarkCandidate;
            try {
                watermarkCandidate = sourceDs.resolveWatermarkCandidate(taskId);
            } catch (Exception e) {
                LOG.error("ERROR in {} resolving watermark candidate: {}", taskName,
                    CredentialRedactor.redactMessage(e.getMessage()));
                throw e;
            }

                return new ReplicaTaskResult(this.taskId, processedRows, startedAtMillis,
                    System.currentTimeMillis(), watermarkCandidate);
        } catch (Exception e) {
            failure = e;
            throw e;
        } finally {
            Exception closeFailure = closeManager(sinkDs, failure);
            Exception sourceCloseFailure = closeManager(sourceDs, failure);
            if (closeFailure == null) {
                closeFailure = sourceCloseFailure;
            } else if (sourceCloseFailure != null) {
                closeFailure.addSuppressed(sourceCloseFailure);
            }

            if (failure == null && closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static Exception closeManager(ConnManager manager, Exception failure) {
        if (manager == null) {
            return null;
        }

        try {
            manager.close();
        } catch (Exception closeException) {
            if (failure != null) {
                failure.addSuppressed(closeException);
                return null;
            }
            return closeException;
        }
        return null;
    }

    public static List<String> getExceptionMessageChain(Throwable throwable) {
        List<String> result = new ArrayList<>();
        while (throwable != null) {
            result.add(CredentialRedactor.redactMessage(throwable.getMessage()));
            throwable = throwable.getCause();
        }
        return result;
    }
}

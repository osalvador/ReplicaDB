package org.replicadb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.CredentialRedactor;
import org.replicadb.execution.ReplicationDiagnosticCollector;
import org.replicadb.execution.ReplicationDiagnosticEvent;
import org.replicadb.execution.ReplicationCancelledException;
import org.replicadb.execution.ReplicationLogContext;
import org.replicadb.manager.ConnManager;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.ManagerFactory;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.Map;

public final class ReplicaTask implements Callable<ReplicaTaskResult> {

    private static final Logger LOG = LogManager.getLogger(ReplicaTask.class.getName());

    private final int taskId;
    private final ToolOptions options;
    private final ManagerFactory managerFactory;
    private final Map<String, String> parentLogContext;


    public ReplicaTask(int id, ToolOptions options) {
        this(id, options, new ManagerFactory(), ReplicationLogContext.capture());
    }

    ReplicaTask(int id, ToolOptions options, ManagerFactory managerFactory) {
        this(id, options, managerFactory, ReplicationLogContext.capture());
    }

    ReplicaTask(int id, ToolOptions options, ManagerFactory managerFactory, Map<String, String> parentLogContext) {
        this.taskId = id;
        this.options = options;
        this.managerFactory = managerFactory;
        this.parentLogContext = Map.copyOf(parentLogContext);
    }

    @Override
    public ReplicaTaskResult call() throws Exception {
        try (ReplicationLogContext.Scope ignored = ReplicationLogContext.install(parentLogContext,
                options.getExecutionContext())) {
            return execute();
        }
    }

    private ReplicaTaskResult execute() throws Exception {
        final long startedAtMillis = System.currentTimeMillis();
        String taskName = "TaskId-" + this.taskId;
        ReplicationDiagnosticCollector diagnostics = options.getExecutionContext().getDiagnosticCollector();

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
                diagnostics.record(ReplicationDiagnosticEvent.Stage.SOURCE_CONNECTION,
                    ReplicationDiagnosticEvent.Category.CONNECTION, ReplicationDiagnosticEvent.Severity.ERROR,
                    Integer.toString(taskId), "source", "Source connection failed", e);
                LOG.error("ERROR in {} getting Source connection: {} ", taskName,
                    CredentialRedactor.redactMessage(e.getMessage()));
                throw e;
            }

            try {
                sinkDs.getConnection();
            } catch (Exception e) {
                diagnostics.record(ReplicationDiagnosticEvent.Stage.SINK_CONNECTION,
                    ReplicationDiagnosticEvent.Category.CONNECTION, ReplicationDiagnosticEvent.Severity.ERROR,
                    Integer.toString(taskId), "sink", "Sink connection failed", e);
                LOG.error("ERROR in {} getting Sink connection:{} ", taskName,
                    CredentialRedactor.redactMessage(e.getMessage()));
                throw e;
            }

            ResultSet rs;
            try {
                rs = sourceDs.readTable(null, null, taskId);
            } catch (Exception e) {
                diagnostics.record(ReplicationDiagnosticEvent.Stage.SOURCE_READ,
                    ReplicationDiagnosticEvent.Category.READ, ReplicationDiagnosticEvent.Severity.ERROR,
                    Integer.toString(taskId), "source", "Source table read failed", e);
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
                diagnostics.record(ReplicationDiagnosticEvent.Stage.SINK_WRITE,
                    ReplicationDiagnosticEvent.Category.WRITE, ReplicationDiagnosticEvent.Severity.ERROR,
                    Integer.toString(taskId), "sink", "Sink table write failed", e);
                LOG.error("ERROR in {} inserting data to sink table: {} ", taskName, getExceptionMessageChain(e));
                throw e;
            }

            String watermarkCandidate;
            try {
                watermarkCandidate = sourceDs.resolveWatermarkCandidate(taskId);
            } catch (Exception e) {
                diagnostics.record(ReplicationDiagnosticEvent.Stage.WATERMARK,
                    ReplicationDiagnosticEvent.Category.WATERMARK, ReplicationDiagnosticEvent.Severity.ERROR,
                    Integer.toString(taskId), "source", "Watermark resolution failed", e);
                LOG.error("ERROR in {} resolving watermark candidate: {}", taskName,
                    CredentialRedactor.redactMessage(e.getMessage()));
                throw e;
            }

                return new ReplicaTaskResult(this.taskId, processedRows, startedAtMillis,
                    System.currentTimeMillis(), watermarkCandidate);
        } catch (Exception e) {
            failure = e;
            if (e instanceof ReplicationCancelledException) {
                diagnostics.record(ReplicationDiagnosticEvent.Stage.CANCELLATION,
                    ReplicationDiagnosticEvent.Category.CANCELLATION, ReplicationDiagnosticEvent.Severity.INFO,
                    Integer.toString(taskId), "replication", "Replication task cancelled", e);
            } else if (e instanceof InterruptedException) {
                diagnostics.record(ReplicationDiagnosticEvent.Stage.INTERRUPTION,
                    ReplicationDiagnosticEvent.Category.CANCELLATION, ReplicationDiagnosticEvent.Severity.WARN,
                    Integer.toString(taskId), "replication", "Replication task interrupted", e);
            }
            throw e;
        } finally {
            Exception closeFailure = closeManager(sinkDs, failure);
            Exception sourceCloseFailure = closeManager(sourceDs, failure);
            if (closeFailure != null) {
                diagnostics.record(ReplicationDiagnosticEvent.Stage.CLEANUP,
                    ReplicationDiagnosticEvent.Category.CLEANUP, ReplicationDiagnosticEvent.Severity.ERROR,
                    Integer.toString(taskId), "sink", "Sink task cleanup failed", closeFailure);
            }
            if (sourceCloseFailure != null) {
                diagnostics.record(ReplicationDiagnosticEvent.Stage.CLEANUP,
                    ReplicationDiagnosticEvent.Category.CLEANUP, ReplicationDiagnosticEvent.Severity.ERROR,
                    Integer.toString(taskId), "source", "Source task cleanup failed", sourceCloseFailure);
            }
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

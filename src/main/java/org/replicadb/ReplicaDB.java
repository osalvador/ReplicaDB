package org.replicadb;

import io.sentry.ITransaction;
import io.sentry.Sentry;
import io.sentry.SpanStatus;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.replicadb.cli.ReplicationTable;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.CredentialRedactor;
import org.replicadb.execution.ReplicationCancelledException;
import org.replicadb.execution.ReplicationDiagnosticCollector;
import org.replicadb.execution.ReplicationDiagnosticEvent;
import org.replicadb.execution.ReplicationLogContext;
import org.replicadb.manager.ConnManager;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.ManagerFactory;
import org.replicadb.manager.util.WatermarkBinder;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.replicadb.config.Sentry.SentryInit;

/**
 * ReplicaDB - Enterprise data replication and migration tool.
 *
 * <p>
 * ReplicaDB is a bulk data transfer tool designed to replicate data between
 * heterogeneous databases and data sources. It supports parallel processing,
 * incremental replication, and bandwidth throttling for enterprise
 * environments.
 * </p>
 *
 * <p>
 * Key features:
 * </p>
 * <ul>
 * <li>Bulk data transfer between any two supported data sources</li>
 * <li>Schema-aware replication preserving data types and constraints</li>
 * <li>Parallel processing for large datasets (configurable job count)</li>
 * <li>Incremental replication using timestamp or sequential columns</li>
 * <li>Bandwidth throttling for network-constrained environments</li>
 * </ul>
 *
 * @author ReplicaDB Team
 * @version 0.15.1
 * @since 1.0
 */
public class ReplicaDB {

	private static final Logger LOG = LogManager.getLogger(ReplicaDB.class.getName());
	private static final int SUCCESS = 0;
	private static final int ERROR = 1;
	static final int CANCELLED = 2;

	record ReplicaTaskResultsSummary(long totalRowsProcessed, long maxDurationMillis, int taskCount,
			String watermarkCandidate) {
	}

	/** Holds the executor and result summary of {@link #executeReplicationTasks}, since a caller needs both. */
	private record ReplicationTasksResult(ExecutorService executor, ReplicaTaskResultsSummary summary) {
	}

	static ReplicaTaskResultsSummary summarize(List<ReplicaTaskResult> results) {
		return summarize(results, Types.VARCHAR);
	}

	static ReplicaTaskResultsSummary summarize(List<ReplicaTaskResult> results, int watermarkJdbcType) {
		long totalRowsProcessed = 0;
		long maxDurationMillis = 0;
		String watermarkCandidate = null;

		for (ReplicaTaskResult result : results) {
			totalRowsProcessed += result.rowsProcessed();
			maxDurationMillis = Math.max(maxDurationMillis, result.durationMillis());

			String candidate = result.watermarkCandidate();
			if (candidate != null) {
				watermarkCandidate = watermarkCandidate == null
						|| WatermarkBinder.compareCandidates(candidate, watermarkCandidate, watermarkJdbcType) > 0
								? candidate
								: watermarkCandidate;
			}
		}

		return new ReplicaTaskResultsSummary(totalRowsProcessed, maxDurationMillis, results.size(), watermarkCandidate);
	}

	/** Timeout in milliseconds for pre-sink tasks. */
	private static final int PRE_SINK_TASK_TIMEOUT_MS = 500;

	/** Starting column index for JDBC ResultSet (1-based indexing). */
	private static final int JDBC_COLUMN_START_INDEX = 1;

	/**
	 * Main entry point for ReplicaDB application.
	 *
	 * <p>
	 * Parses command line arguments, processes the data replication, and exits with
	 * appropriate status code.
	 * </p>
	 *
	 * @param args
	 *            command line arguments for configuring the replication
	 */
	public static void main(String[] args) {
		int exitCode;
		final long start = System.nanoTime();

		// Parse Option Arguments
		final ToolOptions options;
		try {
			options = new ToolOptions(args);
			exitCode = processReplica(options);
		} catch (final ParseException | IOException | IllegalArgumentException e) {
			LOG.error("Got exception running ReplicaDB: {} ({})",
					CredentialRedactor.redactMessage(e.getMessage()), e.getClass().getName());
			exitCode = ERROR;
		}

		final long elapsed = (System.nanoTime() - start) / 1000000;
		LOG.info("Total process time: {}ms", elapsed);
		System.exit(exitCode);
	}

	/**
	 * Processes the data replication between source and sink data sources.
	 *
	 * <p>
	 * This method orchestrates the complete replication workflow including:
	 * </p>
	 * <ul>
	 * <li>Creating connection managers for source and sink</li>
	 * <li>Executing pre-tasks and post-tasks</li>
	 * <li>Managing parallel replication jobs</li>
	 * <li>Handling error conditions and cleanup</li>
	 * </ul>
	 *
	 * @param options
	 *            the configuration options for the replication process
	 * @return SUCCESS (0) if replication completed successfully, ERROR (1)
	 *         otherwise
	 */
	public static int processReplica(ToolOptions options) {
		return processReplica(options, new ManagerFactory());
	}

	static int processReplica(ToolOptions options, ManagerFactory managerFactory) {
		LOG.info("Running ReplicaDB version: {}", options.getVersion());
		ReplicaDB.setLogToMode(options.getVerboseLevel());
		LOG.info("Setting verbose mode {}", options.getVerboseLevel());

		if (LOG.isDebugEnabled()) {
			LOG.debug(options.toString());
		}

		final boolean shouldProcess = !options.isHelp() && !options.isVersion();
		if (shouldProcess) {
			if (options.hasReplicationTables()) {
				return executeMultipleReplications(options, managerFactory);
			}
			return executeSingleReplication(options, managerFactory);
		}

		return SUCCESS;
	}

	/**
	 * Executes the actual replication process with proper resource management.
	 *
	 * @param options
	 *            the configuration options for the replication process
	 * @return SUCCESS (0) if replication completed successfully, ERROR (1)
	 *         otherwise
	 */
	static int executeSingleReplication(ToolOptions options, ManagerFactory managerFactory) {
		int exitCode = SUCCESS;
		ConnManager sourceDs = null;
		ConnManager sinkDs = null;
		ExecutorService preSinkTasksExecutor = null;
		ExecutorService replicaTasksService = null;
		ReplicationLogContext.Scope logContext = ReplicationLogContext.bind(options.getExecutionContext());

		try {
			managerFactory.validateAzureAuthenticationConfiguration(options);
		} catch (final IllegalArgumentException e) {
			recordDiagnostic(options, ReplicationDiagnosticEvent.Stage.VALIDATION,
				ReplicationDiagnosticEvent.Category.VALIDATION, "configuration",
				"Replication configuration validation failed", e);
			LOG.error("Invalid Azure authentication configuration: {}", e.getMessage());
			logContext.close();
			return ERROR;
		}

		// Sentry
		SentryInit(options);
		final ITransaction transaction = Sentry.startTransaction("processReplica()", "task");

		try {
			final ReplicationManagers managers = createConnectionManagers(options, managerFactory);
			sourceDs = managers.sourceDs;
			sinkDs = managers.sinkDs;

			preSinkTasksExecutor = Executors.newSingleThreadExecutor();
			final Future<Integer> preSinkTasksFuture = executePreTasks(sourceDs, sinkDs, preSinkTasksExecutor,
				options.getExecutionContext().getDiagnosticCollector());

			final ReplicationTasksResult replicationTasksResult = executeReplicationTasks(options, managerFactory);
			replicaTasksService = replicationTasksResult.executor();
			options.getExecutionContext().setRowsProcessed(replicationTasksResult.summary().totalRowsProcessed());
			options.getExecutionContext().setDurationMillis(replicationTasksResult.summary().maxDurationMillis());

			waitForTaskCompletion(preSinkTasksFuture);
			executePostTasks(sourceDs, sinkDs, options.getExecutionContext().getDiagnosticCollector());
			if (options.getIncrementalWatermarkColumn() != null) {
				options.getExecutionContext().setWatermarkCandidate(replicationTasksResult.summary().watermarkCandidate());
			}
			shutdownExecutors(preSinkTasksExecutor, replicaTasksService);

		} catch (final InterruptedException e) {
			recordDiagnostic(options, ReplicationDiagnosticEvent.Stage.INTERRUPTION,
				ReplicationDiagnosticEvent.Category.CANCELLATION, "replication",
				"Replication was interrupted", e);
			LOG.error("Replication was interrupted: {}", CredentialRedactor.redactMessage(e.getMessage()));
			Thread.currentThread().interrupt(); // Restore interrupted status
			Sentry.captureException(e);
			transaction.setThrowable(e);
			transaction.setStatus(SpanStatus.INTERNAL_ERROR);
			exitCode = ERROR;
		} catch (final ReplicationCancelledException e) {
			recordDiagnostic(options, ReplicationDiagnosticEvent.Stage.CANCELLATION,
				ReplicationDiagnosticEvent.Category.CANCELLATION, "replication",
				"Replication was cancelled", e);
			LOG.info("Replication was cancelled: {}", CredentialRedactor.redactMessage(e.getMessage()));
			exitCode = CANCELLED;
		} catch (final Exception e) {
			if (options.getExecutionContext().isCancellationRequested()) {
				recordDiagnostic(options, ReplicationDiagnosticEvent.Stage.CANCELLATION,
					ReplicationDiagnosticEvent.Category.CANCELLATION, "replication",
					"Replication was cancelled after an interrupted operation", e);
				LOG.info("Replication was cancelled after an operation was interrupted: {}",
						CredentialRedactor.redactMessage(e.getMessage()));
				exitCode = CANCELLED;
			} else {
				recordDiagnostic(options, ReplicationDiagnosticEvent.Stage.AGGREGATION,
					ReplicationDiagnosticEvent.Category.FAILURE, "replication",
					"Replication failed", e);
				LOG.error("Got exception running ReplicaDB: {} ({})",
						CredentialRedactor.redactMessage(e.getMessage()), e.getClass().getName());
				Sentry.captureException(e);
				transaction.setThrowable(e);
				transaction.setStatus(SpanStatus.INTERNAL_ERROR);
				exitCode = ERROR;
			}
		} finally {
			transaction.finish();
			cleanupResources(sourceDs, sinkDs, preSinkTasksExecutor, replicaTasksService,
				options.getExecutionContext().getDiagnosticCollector());
			logContext.close();
		}

		return exitCode;
	}

	static int executeMultipleReplications(ToolOptions baseOptions, ManagerFactory managerFactory) {
		int exitCode = SUCCESS;
		List<ReplicationTable> replicationTables = baseOptions.getReplicationTables();

		for (int index = 0; index < replicationTables.size(); index++) {
			ReplicationTable replicationTable = replicationTables.get(index);
			ToolOptions tableOptions = baseOptions.forReplicationTable(replicationTable);
			LOG.info("Starting replication table {}/{}: {} -> {}", index + 1, replicationTables.size(),
					replicationTable.sourceTable(), replicationTable.sinkTable());

			exitCode = executeSingleReplication(tableOptions, managerFactory);
			if (exitCode != SUCCESS) {
				LOG.error("Replication table {}/{} failed: {} -> {}", index + 1, replicationTables.size(),
						replicationTable.sourceTable(), replicationTable.sinkTable());
				break;
			}
		}

		return exitCode;
	}

	/**
	 * Creates and initializes connection managers for source and sink.
	 *
	 * @param options
	 *            the configuration options
	 * @return ReplicationManagers containing both source and sink managers
	 */
	private static ReplicationManagers createConnectionManagers(ToolOptions options, ManagerFactory managerFactory) {
		final ConnManager sourceDs = managerFactory.accept(options, DataSourceType.SOURCE);
		final ConnManager sinkDs = managerFactory.accept(options, DataSourceType.SINK);
		return new ReplicationManagers(sourceDs, sinkDs);
	}

	/**
	 * Executes pre-tasks for source and sink data sources.
	 *
	 * @param sourceDs
	 *            the source connection manager
	 * @param sinkDs
	 *            the sink connection manager
	 * @param preSinkTasksExecutor
	 *            executor for async pre-sink tasks
	 * @return Future representing the pre-sink task completion
	 * @throws Exception
	 *             if pre-tasks fail
	 */
	private static Future<Integer> executePreTasks(ConnManager sourceDs, ConnManager sinkDs,
			ExecutorService preSinkTasksExecutor, ReplicationDiagnosticCollector diagnostics) throws Exception {
		try {
		sourceDs.preSourceTasks();
		} catch (Exception e) {
			diagnostics.record(ReplicationDiagnosticEvent.Stage.PRE_TASK, ReplicationDiagnosticEvent.Category.LIFECYCLE,
				ReplicationDiagnosticEvent.Severity.ERROR, null, "source", "Source pre-task failed", e);
			throw e;
		}
		try {
		final Future<Integer> preSinkTasksFuture = sinkDs.preSinkTasks(preSinkTasksExecutor);
			return awaitPreSink(preSinkTasksFuture);
		} catch (Exception e) {
			diagnostics.record(ReplicationDiagnosticEvent.Stage.PRE_TASK, ReplicationDiagnosticEvent.Category.LIFECYCLE,
				ReplicationDiagnosticEvent.Severity.ERROR, null, "sink", "Sink pre-task failed", e);
			throw e;
		}
	}

	private static Future<Integer> awaitPreSink(Future<Integer> preSinkTasksFuture) throws Exception {
		// Handle pre-sink task timeout
		if (preSinkTasksFuture != null) {
			try {
				preSinkTasksFuture.get(PRE_SINK_TASK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			} catch (final TimeoutException e) {
				LOG.debug("Pre-sink task is still running, continuing with replication");
			}
		}

		return preSinkTasksFuture;
	}

	/**
	 * Executes the parallel replication tasks.
	 *
	 * @param options
	 *            the configuration options containing job count
	 * @return the ExecutorService used for replication tasks and the aggregated result summary
	 * @throws InterruptedException
	 *             if task execution is interrupted
	 * @throws ExecutionException
	 *             if a task fails
	 */
	private static ReplicationTasksResult executeReplicationTasks(ToolOptions options, ManagerFactory managerFactory)
			throws InterruptedException, ExecutionException, SQLException {
		final List<ReplicaTask> replicaTasks = new ArrayList<>();
		final Map<String, String> parentLogContext = ReplicationLogContext.capture();
		for (int i = 0; i < options.getJobs(); i++) {
			replicaTasks.add(new ReplicaTask(i, options, managerFactory, parentLogContext));
		}

		final ExecutorService replicaTasksService = Executors.newFixedThreadPool(options.getJobs());
		try {
			final List<Future<ReplicaTaskResult>> futures = new ArrayList<>();
			for (final ReplicaTask replicaTask : replicaTasks) {
				futures.add(replicaTasksService.submit(replicaTask));
			}
			final List<ReplicaTaskResult> results = new ArrayList<>();

			for (final Future<ReplicaTaskResult> future : futures) {
				try {
					results.add(future.get());
				} catch (final ExecutionException e) {
					options.getExecutionContext().getDiagnosticCollector().record(
							ReplicationDiagnosticEvent.Stage.AGGREGATION, ReplicationDiagnosticEvent.Category.FAILURE,
							ReplicationDiagnosticEvent.Severity.ERROR, null, "replication",
							"Replication task failed", e.getCause());
					if (e.getCause() instanceof ReplicationCancelledException cancellation) {
						for (final Future<ReplicaTaskResult> sibling : futures) {
							if (sibling != future) {
								sibling.cancel(true);
							}
						}
						throw cancellation;
					}
					if (options.getExecutionContext().isCancellationRequested()) {
						for (final Future<ReplicaTaskResult> sibling : futures) {
							if (sibling != future) {
								sibling.cancel(true);
							}
						}
						throw new ReplicationCancelledException(
								"Replication run " + options.getExecutionContext().getRunId() + " was cancelled", e.getCause());
					}
					throw e;
				}
			}

			final ReplicaTaskResultsSummary summary;
			if (options.getIncrementalWatermarkColumn() != null) {
				int watermarkJdbcType = WatermarkBinder.resolveColumnType(
						options.getSourceColumnDescriptors(), options.getIncrementalWatermarkColumn());
				summary = summarize(results, watermarkJdbcType);
			} else {
				summary = summarize(results);
			}
			LOG.info("Replication tasks completed: {} rows across {} tasks, longest task {}ms",
					summary.totalRowsProcessed(), summary.taskCount(), summary.maxDurationMillis());

			return new ReplicationTasksResult(replicaTasksService, summary);
		} catch (final InterruptedException | ExecutionException | SQLException e) {
			replicaTasksService.shutdownNow();
			throw e;
		}
	}

	/**
	 * Waits for the pre-sink task to complete.
	 *
	 * @param preSinkTasksFuture
	 *            the future representing the pre-sink task
	 * @throws InterruptedException
	 *             if waiting is interrupted
	 * @throws ExecutionException
	 *             if the pre-sink task fails
	 */
	private static void waitForTaskCompletion(Future<Integer> preSinkTasksFuture)
			throws InterruptedException, ExecutionException {
		if (preSinkTasksFuture != null) {
			LOG.info("Waiting for the asynchronous task to be completed...");
			preSinkTasksFuture.get();
		}
	}

	/**
	 * Executes post-tasks for source and sink data sources.
	 *
	 * @param sourceDs
	 *            the source connection manager
	 * @param sinkDs
	 *            the sink connection manager
	 * @throws Exception
	 *             if post-tasks fail
	 */
	private static void executePostTasks(ConnManager sourceDs, ConnManager sinkDs,
			ReplicationDiagnosticCollector diagnostics) throws Exception {
		try {
			sourceDs.postSourceTasks();
		} catch (Exception e) {
			diagnostics.record(ReplicationDiagnosticEvent.Stage.POST_TASK, ReplicationDiagnosticEvent.Category.LIFECYCLE,
				ReplicationDiagnosticEvent.Severity.ERROR, null, "source", "Source post-task failed", e);
			throw e;
		}
		try {
			sinkDs.postSinkTasks();
		} catch (Exception e) {
			diagnostics.record(ReplicationDiagnosticEvent.Stage.POST_TASK, ReplicationDiagnosticEvent.Category.LIFECYCLE,
				ReplicationDiagnosticEvent.Severity.ERROR, null, "sink", "Sink post-task failed", e);
			throw e;
		}
	}

	/**
	 * Gracefully shuts down executor services.
	 *
	 * @param preSinkTasksExecutor
	 *            executor for pre-sink tasks
	 * @param replicaTasksService
	 *            executor for replication tasks
	 */
	private static void shutdownExecutors(ExecutorService preSinkTasksExecutor, ExecutorService replicaTasksService) {
		if (preSinkTasksExecutor != null) {
			preSinkTasksExecutor.shutdown();
		}
		if (replicaTasksService != null) {
			replicaTasksService.shutdown();
		}
	}

	/**
	 * Cleans up resources and closes connections.
	 *
	 * @param sourceDs
	 *            source connection manager
	 * @param sinkDs
	 *            sink connection manager
	 * @param preSinkTasksExecutor
	 *            executor for pre-sink tasks
	 * @param replicaTasksService
	 *            executor for replication tasks
	 */
	private static void cleanupResources(ConnManager sourceDs, ConnManager sinkDs, ExecutorService preSinkTasksExecutor,
			ExecutorService replicaTasksService, ReplicationDiagnosticCollector diagnostics) {
		Exception cleanupFailure = null;

		if (sinkDs != null) {
			try {
				sinkDs.cleanUp();
			} catch (final Exception e) {
				diagnostics.record(ReplicationDiagnosticEvent.Stage.CLEANUP, ReplicationDiagnosticEvent.Category.CLEANUP,
					ReplicationDiagnosticEvent.Severity.ERROR, null, "sink", "Sink cleanup failed", e);
				cleanupFailure = addCleanupFailure(cleanupFailure, e);
			}
			try {
				sinkDs.close();
			} catch (final Exception e) {
				diagnostics.record(ReplicationDiagnosticEvent.Stage.CLEANUP, ReplicationDiagnosticEvent.Category.CLEANUP,
					ReplicationDiagnosticEvent.Severity.ERROR, null, "sink", "Sink close failed", e);
				cleanupFailure = addCleanupFailure(cleanupFailure, e);
			}
		}
		if (sourceDs != null) {
			try {
				sourceDs.close();
			} catch (final Exception e) {
				diagnostics.record(ReplicationDiagnosticEvent.Stage.CLEANUP, ReplicationDiagnosticEvent.Category.CLEANUP,
					ReplicationDiagnosticEvent.Severity.ERROR, null, "source", "Source close failed", e);
				cleanupFailure = addCleanupFailure(cleanupFailure, e);
			}
		}

		if (preSinkTasksExecutor != null) {
			preSinkTasksExecutor.shutdownNow();
		}
		if (replicaTasksService != null) {
			replicaTasksService.shutdownNow();
		}

		if (cleanupFailure != null) {
			LOG.error("Error during cleanup: {} ({})",
					CredentialRedactor.redactMessage(cleanupFailure.getMessage()),
					cleanupFailure.getClass().getName());
		}
	}

	private static void recordDiagnostic(ToolOptions options, ReplicationDiagnosticEvent.Stage stage,
			ReplicationDiagnosticEvent.Category category, String component, String message, Throwable throwable) {
		options.getExecutionContext().getDiagnosticCollector().record(stage, category,
				throwable instanceof ReplicationCancelledException ? ReplicationDiagnosticEvent.Severity.INFO
						: ReplicationDiagnosticEvent.Severity.ERROR,
				null, component, message, throwable);
	}

	private static Exception addCleanupFailure(Exception currentFailure, Exception cleanupFailure) {
		if (currentFailure == null) {
			return cleanupFailure;
		}

		currentFailure.addSuppressed(cleanupFailure);
		return currentFailure;
	}

	/**
	 * Sets the logging level for the application.
	 *
	 * <p>
	 * Dynamically updates the Log4j2 configuration to change the logging level
	 * for ReplicaDB classes only, keeping external libraries at INFO/WARN level
	 * to avoid excessive debug output from ORC, MongoDB, HTTP clients, etc.
	 * </p>
	 *
	 * @param level
	 *            the logging level to set (e.g., DEBUG, INFO, WARN, ERROR)
	 */
	private static void setLogToMode(Level level) {
		final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		final Configuration config = ctx.getConfiguration();
		
		// Set ReplicaDB package to the requested level
		LoggerConfig replicadbLoggerConfig = config.getLoggerConfig("org.replicadb");
		if (replicadbLoggerConfig.getName().equals(LogManager.ROOT_LOGGER_NAME)) {
			// Logger doesn't exist, create it
			replicadbLoggerConfig = new LoggerConfig("org.replicadb", level, true);
			config.addLogger("org.replicadb", replicadbLoggerConfig);
		} else {
			replicadbLoggerConfig.setLevel(level);
		}
		
		// Set root logger level for external libraries:
		// TRACE -> external libs at DEBUG
		// DEBUG -> external libs at INFO
		// Other -> external libs at same level
		final LoggerConfig rootLoggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
		if (level == Level.TRACE) {
			rootLoggerConfig.setLevel(Level.DEBUG);
		} else if (level == Level.DEBUG) {
			rootLoggerConfig.setLevel(Level.INFO);
		} else {
			rootLoggerConfig.setLevel(level);
		}
		
		ctx.updateLoggers();
	}

	/**
	 * Prints a ResultSet to logger instead of standard output for consistency.
	 *
	 * <p>
	 * <strong>Note:</strong> This method is intended for debugging purposes only
	 * and should not be used in production code. It now uses proper logging
	 * mechanisms instead of System.out.println for consistency.
	 * </p>
	 *
	 * @param rs
	 *            the ResultSet to print
	 * @throws SQLException
	 *             if a database access error occurs
	 * @deprecated This method is for debugging only and may be removed in future
	 *             versions
	 */
	@Deprecated
	public static void printResultSet(ResultSet rs) throws SQLException {
		final ResultSetMetaData rsmd = rs.getMetaData();
		final int columnsNumber = rsmd.getColumnCount();
		if (!LOG.isDebugEnabled()) {
			return; // Exit early if debug logging is not enabled
		}

		LOG.debug("Starting to print ResultSet with {} columns", columnsNumber);

		// Build header row
		final StringBuilder headerBuilder = new StringBuilder();
		for (int i = JDBC_COLUMN_START_INDEX; i <= columnsNumber; i++) {
			if (i > JDBC_COLUMN_START_INDEX) {
				headerBuilder.append("\t");
			}
			headerBuilder.append(rsmd.getColumnName(i));
		}
		LOG.debug("ResultSet header: {}", headerBuilder);

		// Build data rows
		int rowCount = 0;
		while (rs.next()) {
			final StringBuilder rowBuilder = new StringBuilder();
			for (int i = JDBC_COLUMN_START_INDEX; i <= columnsNumber; i++) {
				if (i > JDBC_COLUMN_START_INDEX) {
					rowBuilder.append("\t");
				}
				rowBuilder.append(rs.getString(i));
			}
			LOG.debug("ResultSet row {}: {}", ++rowCount, rowBuilder);
		}

		LOG.debug("ResultSet printing completed. Total rows: {}", rowCount);
	}

	/**
	 * Helper class to hold source and sink connection managers.
	 */
	private static class ReplicationManagers {
		final ConnManager sourceDs;
		final ConnManager sinkDs;

		ReplicationManagers(ConnManager sourceDs, ConnManager sinkDs) {
			this.sourceDs = sourceDs;
			this.sinkDs = sinkDs;
		}
	}
}

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
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.ConnManager;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.ManagerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
		} catch (final ParseException | IOException e) {
			LOG.error("Got exception running ReplicaDB:", e);
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
		LOG.info("Running ReplicaDB version: {}", options.getVersion());
		ReplicaDB.setLogToMode(options.getVerboseLevel());
		LOG.info("Setting verbose mode {}", options.getVerboseLevel());

		if (LOG.isDebugEnabled()) {
			LOG.debug(options.toString());
		}

		final boolean shouldProcess = !options.isHelp() && !options.isVersion();
		if (shouldProcess) {
			return executeReplication(options);
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
	private static int executeReplication(ToolOptions options) {
		int exitCode = SUCCESS;
		ConnManager sourceDs = null;
		ConnManager sinkDs = null;
		ExecutorService preSinkTasksExecutor = null;
		ExecutorService replicaTasksService = null;

		// Sentry
		SentryInit(options);
		final ITransaction transaction = Sentry.startTransaction("processReplica()", "task");

		try {
			final ReplicationManagers managers = createConnectionManagers(options);
			sourceDs = managers.sourceDs;
			sinkDs = managers.sinkDs;

			preSinkTasksExecutor = Executors.newSingleThreadExecutor();
			final Future<Integer> preSinkTasksFuture = executePreTasks(sourceDs, sinkDs, preSinkTasksExecutor);

			replicaTasksService = executeReplicationTasks(options);

			waitForTaskCompletion(preSinkTasksFuture);
			executePostTasks(sourceDs, sinkDs);
			shutdownExecutors(preSinkTasksExecutor, replicaTasksService);

		} catch (final InterruptedException e) {
			LOG.error("Replication was interrupted:", e);
			Thread.currentThread().interrupt(); // Restore interrupted status
			Sentry.captureException(e);
			transaction.setThrowable(e);
			transaction.setStatus(SpanStatus.INTERNAL_ERROR);
			exitCode = ERROR;
		} catch (final Exception e) {
			LOG.error("Got exception running ReplicaDB:", e);
			Sentry.captureException(e);
			transaction.setThrowable(e);
			transaction.setStatus(SpanStatus.INTERNAL_ERROR);
			exitCode = ERROR;
		} finally {
			transaction.finish();
			cleanupResources(sourceDs, sinkDs, preSinkTasksExecutor, replicaTasksService);
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
	private static ReplicationManagers createConnectionManagers(ToolOptions options) {
		final ManagerFactory managerFactory = new ManagerFactory();
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
			ExecutorService preSinkTasksExecutor) throws Exception {
		sourceDs.preSourceTasks();
		final Future<Integer> preSinkTasksFuture = sinkDs.preSinkTasks(preSinkTasksExecutor);

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
	 * @return ExecutorService used for replication tasks
	 * @throws InterruptedException
	 *             if task execution is interrupted
	 * @throws ExecutionException
	 *             if a task fails
	 */
	private static ExecutorService executeReplicationTasks(ToolOptions options)
			throws InterruptedException, ExecutionException {
		final List<ReplicaTask> replicaTasks = new ArrayList<>();
		for (int i = 0; i < options.getJobs(); i++) {
			replicaTasks.add(new ReplicaTask(i, options));
		}

		final ExecutorService replicaTasksService = Executors.newFixedThreadPool(options.getJobs());
		final List<Future<Integer>> futures = replicaTasksService.invokeAll(replicaTasks);

		for (final Future<Integer> future : futures) {
			future.get(); // This will throw ExecutionException if any task failed
		}

		return replicaTasksService;
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
	private static void executePostTasks(ConnManager sourceDs, ConnManager sinkDs) throws Exception {
		sourceDs.postSourceTasks();
		sinkDs.postSinkTasks();
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
			ExecutorService replicaTasksService) {
		try {
			if (sinkDs != null) {
				sinkDs.cleanUp();
				sinkDs.close();
			}
			if (sourceDs != null) {
				sourceDs.close();
			}

			if (preSinkTasksExecutor != null) {
				preSinkTasksExecutor.shutdownNow();
			}
			if (replicaTasksService != null) {
				replicaTasksService.shutdownNow();
			}

		} catch (final Exception e) {
			LOG.error("Error during cleanup: {}", e.getMessage(), e);
		}
	}

	/**
	 * Sets the logging level for the application.
	 *
	 * <p>
	 * Dynamically updates the Log4j2 configuration to change the root logger level
	 * based on the verbose level specified in the command line options.
	 * </p>
	 *
	 * @param level
	 *            the logging level to set (e.g., DEBUG, INFO, WARN, ERROR)
	 */
	private static void setLogToMode(Level level) {
		final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		final Configuration config = ctx.getConfiguration();
		final LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
		loggerConfig.setLevel(level);
		ctx.updateLoggers(); // This causes all Loggers to refetch information from their LoggerConfig
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

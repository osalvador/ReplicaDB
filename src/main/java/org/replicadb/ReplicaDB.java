package org.replicadb;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.ConnManager;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.ManagerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * ReplicaDB
 */
public class ReplicaDB {

    private static final Logger LOG = LogManager.getLogger(ReplicaDB.class.getName());
    private static final int SUCCESS = 0;
    private static final int ERROR = 1;

    public static void main(String[] args) {

        boolean exitWithError = false;
        long start = System.nanoTime();
        ConnManager sourceDs = null, sinkDs = null;
        ExecutorService preSinkTasksExecutor = null, replicaTasksService = null;

        try {

            // Parse Option Arguments
            ToolOptions options = new ToolOptions(args);

            LOG.info("Running ReplicaDB version: " + options.getVersion());

            if (options.isVerbose()) {
                setLogToDebugMode();
                LOG.info("Setting verbose mode");
                LOG.debug(options.toString());
            }

            if (!options.isHelp() && !options.isVersion()) {
                // Create Connection Managers
                ManagerFactory managerF = new ManagerFactory();
                sourceDs = managerF.accept(options, DataSourceType.SOURCE);
                sinkDs = managerF.accept(options, DataSourceType.SINK);

                if (options.getMode().equals(ReplicationMode.CDC.getModeText())) {
                    // ReplicaDB in CDC mode is running forever
                    LOG.info("Running ReplicaDB in CDC mode");

                    ReplicaDBCDC cdc = new ReplicaDBCDC(sourceDs, sinkDs);
                    cdc.run();

                } else {

                    // Executor Service for atomic complet refresh replication
                    preSinkTasksExecutor = Executors.newSingleThreadExecutor();

                    // Pre tasks
                    sourceDs.preSourceTasks();
                    Future<Integer> preSinkTasksFuture = sinkDs.preSinkTasks(preSinkTasksExecutor);

                    // Catch exceptions before moving data
                    if (preSinkTasksFuture != null) {
                        try {
                            preSinkTasksFuture.get(500, TimeUnit.MILLISECONDS);
                        } catch (TimeoutException e) {
                            // The preSinkTask is perfoming in the database
                        }
                    }

                    // Prepare Threads for Job processing
                    List<ReplicaTask> replicaTasks = new ArrayList<>();
                    for (int i = 0; i < options.getJobs(); i++) {
                        replicaTasks.add(new ReplicaTask(i, options));
                    }
                    // Run all Replicate Tasks
                    replicaTasksService = Executors.newFixedThreadPool(options.getJobs());
                    List<Future<Integer>> futures = replicaTasksService.invokeAll(replicaTasks);
                    for (Future<Integer> future : futures) {
                        // catch tasks exceptions
                        future.get();
                    }

                    // wait for terminating
                    if (preSinkTasksFuture != null) {
                        LOG.info("Waiting for the asynchronous task to be completed...");
                        preSinkTasksFuture.get();
                    }

                    // Post Tasks
                    sourceDs.postSourceTasks();
                    sinkDs.postSinkTasks();

                    // Shutdown Executor Services
                    preSinkTasksExecutor.shutdown();
                    replicaTasksService.shutdown();
                }
            }
        } catch (Exception e) {
            LOG.error("Got exception running ReplicaDB:", e);
            exitWithError = true;
        } finally {
            //Clean Up environment and close connections
            try {
                if (null != sinkDs) {
                    //aka drop staging table)
                    sinkDs.cleanUp();
                    sinkDs.close();
                }
                if (null != sourceDs) {
                    sourceDs.close();
                }

                if (preSinkTasksExecutor != null) preSinkTasksExecutor.shutdownNow();
                if (replicaTasksService != null) replicaTasksService.shutdownNow();

            } catch (Exception e) {
                LOG.error(e);
            }
        }

        long elapsed = (System.nanoTime() - start) / 1000000;
        LOG.info("Total process time: " + elapsed + "ms");

        if (exitWithError)
            System.exit(ERROR);
        else
            System.exit(SUCCESS);

    }


    private static void setLogToDebugMode() {

        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.valueOf("DEBUG"));
        ctx.updateLoggers();  // This causes all Loggers to refetch information from their LoggerConfi

    }


    public static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        System.out.println("empiezo");

        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) System.out.print("\t");
            System.out.print(rsmd.getColumnName(i));
        }
        System.out.println("");

        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print("\t");
                System.out.print(rs.getString(i));
            }
            System.out.println("");
        }
    }

}

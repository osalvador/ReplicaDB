package org.replicadb;

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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * ReplicaDB
 */
public class ReplicaDB {

    private static final Logger LOG = LogManager.getLogger(ReplicaDB.class.getName());
    private static final int SUCCESS = 0;
    private static final int ERROR = 1;

    public static void main(String[] args) {


        long start = System.nanoTime();
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
                ConnManager sourceDs = managerF.accept(options, DataSourceType.SOURCE);
                ConnManager sinkDs = managerF.accept(options, DataSourceType.SINK);

                // Pre tasks
                sourceDs.preSourceTasks();
                sinkDs.preSinkTasks();

                // Prepare Threads for Job processing
                ExecutorService service = Executors.newFixedThreadPool(options.getJobs());
                List<ReplicaTask> replicaTasks = new ArrayList<>();

                for (int i = 0; i < options.getJobs(); i++) {
                    replicaTasks.add(new ReplicaTask(i, options));
                }

                // Run all Replicate Tasks
                List<Future<Integer>> futures = service.invokeAll(replicaTasks);
                for (Future<Integer> future : futures) {
                    // catch tasks exceptions
                    future.get();
                }

                // Post Tasks
                sourceDs.postSourceTasks();
                sinkDs.postSinkTasks();

                //Close connections
                sourceDs.close();
                sinkDs.close();


                long elapsed = (System.nanoTime() - start) / 1000000;
                LOG.info("Total process time: " + elapsed + "ms");
            }

        } catch (Exception e) {

            LOG.error("Got exception running ReplicaDB:", e);
            long elapsed = (System.nanoTime() - start) / 1000000;
            LOG.info("Total process time: " + elapsed + "ms");
            System.exit(ERROR);

        }

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

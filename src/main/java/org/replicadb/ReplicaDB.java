package org.replicadb;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.*;

import java.sql.*;
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

        //OptionsFile of = new OptionsFile(args[1]);
        //of.printProperties();
        //System.exit(0);


        long start = System.nanoTime();
        try {

            // Parse Option Arguments
            ToolOptions options = new ToolOptions(args);

            if (options.isVerbose()) {
                setLogToDebugMode();
                LOG.info("Setting verbose mode");
                LOG.debug(options.toString());
            }

            if (!options.isHelp()) {

                // Do stuff...
                // Obtener una instancia del DriverManager del Source
                // Obtener una instancia del DriverManager del Sink
                // Mover datos de Source a Sink.
                ManagerFactory managerF = new ManagerFactory();
//                ConnManager sourceDs= managerF.accept(options, DataSourceType.SOURCE);
                ConnManager sinkDs = managerF.accept(options, DataSourceType.SINK);
//
                //                sourceDs.getConnection();
                //sinkDs.getConnection();

                // TODO: delegar el truncate de la tabla al manger del SINK.
                // Truncate Sink table
                Connection con = sinkDs.getConnection();
                Statement stmt = con.createStatement();
                stmt.executeUpdate( "TRUNCATE TABLE " + options.getSinkTable() );
                stmt.close();
                con.commit();
                con.close();


                // Prepare Threads for Job processing
                ExecutorService service = Executors.newFixedThreadPool(options.getJobs());
                List<ReplicaTask> replicaTasks = new ArrayList<>();

                for (int i = 0; i< options.getJobs(); i++){
                    replicaTasks.add(new ReplicaTask(i,options));
                }

                // Run all Replicate Tasks
                List<Future<Integer>> futures = service.invokeAll(replicaTasks);
                for (Future<Integer> future : futures) {
                    // catch tasks exceptions
                    future.get();
                }

                //ResultSet rs = sourceDs.readTable(null, null);
                //sinkDs.insertDataToTable(rs,null,null);

                long elapsed = (System.nanoTime() - start) / 1000000;
                LOG.info("Total process time: " + elapsed + "ms");
            }

            System.exit(SUCCESS);

        } catch (Exception e) {

            LOG.error("Got exception running ReplicaDB:", e);
            long elapsed = (System.nanoTime() - start) / 1000000;
            LOG.info("Total process time: " + elapsed + "ms");
            System.exit(ERROR);

        }
    }


    private static void setLogToDebugMode(){

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

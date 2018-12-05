package org.replicadb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.ConnManager;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.ManagerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

final public class ReplicaTask implements Callable<Integer> {

    private static final Logger LOG = LogManager.getLogger(ReplicaTask.class.getName());

    private int taskId;
    private String taskName;
    private ToolOptions options;


    public ReplicaTask(int id, ToolOptions options) {
        this.taskId = id;
        this.options = options;
    }

    @Override
    public Integer call() throws SQLException {

        //System.out.println("Task ID :" + this.taskId + " performed by " + Thread.currentThread().getName());
        this.taskName = "TaskId-"+this.taskId;

        Thread.currentThread().setName(taskName);

        LOG.info("Starting " + Thread.currentThread().getName());

        // Do stuff...
        // Obtener una instancia del DriverManager del Source
        // Obtener una instancia del DriverManager del Sink
        // Mover datos de Source a Sink.
        ManagerFactory managerF = new ManagerFactory();
        ConnManager sourceDs = managerF.accept(options, DataSourceType.SOURCE);
        ConnManager sinkDs = managerF.accept(options, DataSourceType.SINK);


        try {
            sourceDs.getConnection();
        } catch (Exception e) {
            LOG.error("ERROR in " + this.taskName+ " getting Source connection: " + e.getMessage());
            throw e;
        }

        try {
            sinkDs.getConnection();
        } catch (Exception e) {
            LOG.error("ERROR in " + this.taskName + " getting Sink connection: " + e.getMessage());
            throw e;
        }

        ResultSet rs;
        try {
            rs = sourceDs.readTable(null, null, taskId);
        } catch (Exception e) {
            LOG.error("ERROR in " + this.taskName + " reading source table: " + e.getMessage());
            throw e;
        }

        try {
            sinkDs.insertDataToTable(rs, null, null);
        } catch (Exception e) {
            LOG.error("ERROR in " + this.taskName + " inserting data to sink table: " + e.getMessage());
            throw e;
        }


        //ReplicaDB.printResultSet(rs);
        sourceDs.close();
        sinkDs.close();


        return this.taskId;
    }
}


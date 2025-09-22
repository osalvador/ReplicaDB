package org.replicadb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.ConnManager;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.ManagerFactory;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public final class ReplicaTask implements Callable<Integer> {

    private static final Logger LOG = LogManager.getLogger(ReplicaTask.class.getName());

    private final int taskId;
    private final ToolOptions options;


    public ReplicaTask(int id, ToolOptions options) {
        this.taskId = id;
        this.options = options;
    }

    @Override
    public Integer call() throws Exception {

        String taskName = "TaskId-" + this.taskId;

        Thread.currentThread().setName(taskName);

        LOG.info("Starting  {}", Thread.currentThread().getName());

        ManagerFactory managerF = new ManagerFactory();
        ConnManager sourceDs = managerF.accept(options, DataSourceType.SOURCE);
        ConnManager sinkDs = managerF.accept(options, DataSourceType.SINK);


        try {
            sourceDs.getConnection();
        } catch (Exception e) {
            LOG.error("ERROR in {} getting Source connection: {} ", taskName, e.getMessage());
            throw e;
        }

        try {
            sinkDs.getConnection();
        } catch (Exception e) {
            LOG.error("ERROR in {} getting Sink connection:{} ", taskName,e.getMessage());
            throw e;
        }

        ResultSet rs;
        try {
            rs = sourceDs.readTable(null, null, taskId);
        } catch (Exception e) {
            LOG.error("ERROR in {} reading source table: {}", taskName, e.getMessage());
            throw e;
        }

        try {
            int processedRows = sinkDs.insertDataToTable(rs, taskId);
            // TODO determine the total rows processed in all the managers
            LOG.info("A total of {} rows processed by task {}", processedRows,  taskId);
        } catch (Exception e) {
            LOG.error("ERROR in {} inserting data to sink table: {} ", taskName, getExceptionMessageChain(e));
            throw e;
        }


        //ReplicaDB.printResultSet(rs);
        sourceDs.close();
        sinkDs.close();


        return this.taskId;
    }

    public static List<String> getExceptionMessageChain(Throwable throwable) {
        List<String> result = new ArrayList<>();
        while (throwable != null) {
            result.add(throwable.getMessage());
            throwable = throwable.getCause();
        }
        return result;
    }
}

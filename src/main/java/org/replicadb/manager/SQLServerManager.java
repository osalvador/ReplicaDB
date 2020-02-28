package org.replicadb.manager;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class SQLServerManager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(SQLServerManager.class.getName());

//    private static Long chunkSize;

    /**
     * Constructs the SqlManager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     */
    public SQLServerManager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
    }

    @Override
    public String getDriverClass() {
        return JdbcDrivers.SQLSERVER.getDriverClass();
    }

    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) throws SQLException {

        String tableName;

        // Get table name and columns
        if (options.getMode().equals(ReplicationMode.COMPLETE.getModeText())) {
            tableName = getSinkTableName();
        } else {
            tableName = getQualifiedStagingTableName();
        }

        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();

        SQLServerBulkCopy bulkCopy = null;
        try {
            bulkCopy = new SQLServerBulkCopy(this.getConnection());
            // BulkCopy Options
            SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();
            copyOptions.setBulkCopyTimeout(0);
            bulkCopy.setBulkCopyOptions(copyOptions);

            bulkCopy.setDestinationTableName(tableName);

            // Columns Mapping
            if (this.options.getSinkColumns() != null && !this.options.getSinkColumns().isEmpty()) {
                String sinkColumns = getAllSinkColumns(rsmd);
                String[] sinkColumnsArray = sinkColumns.split(",");
                LOG.debug("Mapping columns: source --> sink");
                for (int i = 1; i <= columnCount; i++) {
                    bulkCopy.addColumnMapping(rsmd.getColumnName(i), sinkColumnsArray[i - 1]);
                    LOG.debug("{} --> {}", rsmd.getColumnName(i), sinkColumnsArray[i - 1]);
                }
            } else {
                for (int i = 1; i <= columnCount; i++) {
                    bulkCopy.addColumnMapping(i, i);
                }
            }


            LOG.info("Perfoming BulkCopy into {} ", tableName);
            // Write from the source to the destination.
            bulkCopy.writeToServer(resultSet);
        } catch (SQLServerException e) {
            e.printStackTrace();
        }

        bulkCopy.close();

        this.getConnection().commit();
        return 0;

    }

    @Override
    protected void createStagingTable() {
        throw new UnsupportedOperationException("SQLServer still not support data insertion");
    }

    @Override
    protected void mergeStagingTable() {
        throw new UnsupportedOperationException("SQLServer still not support data insertion");
    }


    @Override
    public ResultSet readTable(String tableName, String[] columns, int nThread) throws SQLException {

        // If table name parameter is null get it from options
        tableName = tableName == null ? this.options.getSourceTable() : tableName;

        // If columns parameter is null, get it from options
        String allColumns = this.options.getSourceColumns() == null ? "*" : this.options.getSourceColumns();

        String sqlCmd;

        // Read table with source-query option specified
        if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {

            if (options.getJobs() == 1)
                sqlCmd = "SELECT * FROM (" +
                        options.getSourceQuery() + ") as REPDBQUERY where 0 = ?";
            else
                throw new UnsupportedOperationException("ReplicaDB on SQLServer still not support custom query parallel process. Use properties instead: source.table, source.columns and source.where ");
        }
        // Read table with source-where option specified
        else if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {

            sqlCmd = "SELECT  " +
                    allColumns +
                    " FROM " +
                    escapeTableName(tableName);

            if (options.getJobs() == 1)
                sqlCmd = sqlCmd + " where " + options.getSourceWhere() + " AND 0 = ?";
            else
                sqlCmd = sqlCmd + " where " + options.getSourceWhere() + " ABS(CHECKSUM(%% physloc %%)) % " + (options.getJobs()) + " = ?";

        } else {
            // Full table read. Force NO_IDEX and table scan
            sqlCmd = "SELECT  " +
                    allColumns +
                    " FROM " +
                    escapeTableName(tableName);

            if (options.getJobs() == 1)
                sqlCmd = sqlCmd + " where 0 = ?";
            else
                sqlCmd = sqlCmd + " where ABS(CHECKSUM(%% physloc %%)) % " + (options.getJobs()) + " = ?";

        }

        return super.execute(sqlCmd, (Object) nThread);

    }


    @Override
    public void preSourceTasks() {/*Not implemented*/}

    @Override
    public void postSourceTasks() {/*Not implemented*/}
}

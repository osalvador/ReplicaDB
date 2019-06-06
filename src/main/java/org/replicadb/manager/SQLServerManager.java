package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;

import java.sql.ResultSet;
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
    public int insertDataToTable(ResultSet resultSet, int taskId)  {
        throw new UnsupportedOperationException("SQLServer still not support data insertion");
    }

    @Override
    protected void createStagingTable()  {
        throw new UnsupportedOperationException("SQLServer still not support data insertion");
    }

    @Override
    protected void mergeStagingTable()  {
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
                    escapeTableName(tableName) + " WITH (INDEX(0)) where " + options.getSourceWhere() + " AND ABS(CHECKSUM(%% physloc %%)) % "+(options.getJobs())+" = ?";
        } else {
            // Full table read. NO_IDEX and Oracle direct Read
            sqlCmd = "SELECT  " +
                    allColumns +
                    " FROM " +
                    escapeTableName(tableName) + " WITH (INDEX(0)) where ABS(CHECKSUM(%% physloc %%)) % "+(options.getJobs())+" = ?";
        }

        return super.execute(sqlCmd, (Object) nThread);

    }


    @Override
    public void preSourceTasks() throws SQLException {
//
//        /**
//         * Calculating the chunk size for parallel job processing
//         */
//        Statement statement = this.getConnection().createStatement();
//
//        try {
//            String sql = "SELECT " +
//                    " abs(count(*) / " + options.getJobs() + ") chunk_size" +
//                    ", count(*) total_rows" +
//                    " FROM ";
//
//            // Source Query
//            if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
//                sql = sql + "( " + this.options.getSourceQuery() + " )";
//
//            } else {
//
//                sql = sql + this.options.getSourceTable();
//                // Source Where
//                if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
//                    sql = sql + " WHERE " + options.getSourceWhere();
//                }
//            }
//
//            LOG.debug("Calculating the chunks size with this sql: " + sql);
//            ResultSet rs = statement.executeQuery(sql);
//            rs.next();
//            chunkSize = rs.getLong(1);
//            long totalNumberRows = rs.getLong(2);
//            LOG.debug("chunkSize: " + chunkSize + " totalNumberRows: " + totalNumberRows);
//
//            statement.close();
//            this.getConnection().commit();
//        } catch (Exception e) {
//            statement.close();
//            this.connection.rollback();
//            throw e;
//        }

    }

    @Override
    public void postSourceTasks() {/*Not implemented*/}
}

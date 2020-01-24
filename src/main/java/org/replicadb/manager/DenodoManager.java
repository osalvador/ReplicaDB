package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DenodoManager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(DenodoManager.class.getName());

    private static Long chunkSize = 0L;

    /**
     * Constructs the SqlManager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     */
    public DenodoManager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
    }

    @Override
    public String getDriverClass() {
        return JdbcDrivers.DENODO.getDriverClass();
    }

    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) throws SQLException {
        throw new UnsupportedOperationException("Denodo is not supported for data insertion");
    }

    @Override
    protected void createStagingTable() throws SQLException {
        throw new UnsupportedOperationException("Denodo is not supported for data insertion");
    }

    @Override
    protected void mergeStagingTable() throws SQLException {
        throw new UnsupportedOperationException("Denodo is not supported for data insertion");
    }

    /**
     * Denodo needs AutoCommit attribute set to true.
     *
     * @return
     */
    @Override
    public boolean getAutoCommit() {
        return true;
    }

    @Override
    public ResultSet readTable(String tableName, String[] columns, int nThread) throws SQLException {

        // If table name parameter is null get it from options
        tableName = tableName == null ? this.options.getSourceTable() : tableName;

        // If columns parameter is null, get it from options
        String allColumns = this.options.getSourceColumns() == null ? "*" : this.options.getSourceColumns();

        long offset = nThread * chunkSize;
        String sqlCmd;

        // Read table with source-query option specified
        if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
            sqlCmd = "SELECT  * FROM (" +
                    options.getSourceQuery() + ") OFFSET ? ";
        } else {

            sqlCmd = "SELECT " +
                    allColumns +
                    " FROM " +
                    escapeTableName(tableName);

            // Source Where
            if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
                sqlCmd = sqlCmd + " WHERE " + options.getSourceWhere();
            }

            sqlCmd = sqlCmd + " OFFSET ? ";

        }

        String limit = " LIMIT ?";

        if (this.options.getJobs() == nThread + 1) {
            return super.execute(sqlCmd, offset);
        } else {
            sqlCmd = sqlCmd + limit;
            return super.execute(sqlCmd, offset, chunkSize);
        }

    }


    @Override
    public void preSourceTasks() throws SQLException {

        // Only calculate the chunk size when parallel execution is active
        if (this.options.getJobs() != 1) {
            /**
             * Calculating the chunk size for parallel job processing
             */
            Statement statement = this.getConnection().createStatement();
            String sql = "SELECT " +
                    " abs(count(*) / " + options.getJobs() + ") chunk_size" +
                    ", count(*) total_rows" +
                    " FROM ";

            // Source Query
            if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
                sql = sql + "( " + this.options.getSourceQuery() + " )";

            } else {

                sql = sql + this.options.getSourceTable();
                // Source Where
                if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
                    sql = sql + " WHERE " + options.getSourceWhere();
                }
            }

            LOG.debug("Calculating the chunks size with this sql: " + sql);
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            chunkSize = rs.getLong(1);
            long totalNumberRows = rs.getLong(2);
            LOG.debug("chunkSize: " + chunkSize + " totalNumberRows: " + totalNumberRows);

            statement.close();
            this.getConnection().commit();
        }
    }

    @Override
    public void postSourceTasks() {/*Not implemented*/}
}

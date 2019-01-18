package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;

import java.sql.*;

public class PostgresqlManager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(PostgresqlManager.class.getName());

    private static Long chunkSize;

    /**
     * Constructs the SqlManager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     */
    public PostgresqlManager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
    }

    @Override
    public String getDriverClass() {
        return JdbcDrivers.POSTGRES.getDriverClass();
    }

    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) throws SQLException {

        CopyIn copyIn = null;

        try {

            ResultSetMetaData rsmd = resultSet.getMetaData();
            String tableName;

            // Get table name and columns
            if (options.getMode().equals(ReplicationMode.INCREMENTAL.getModeText())) {
                tableName = getQualifiedStagingTableName();
            } else {
                tableName = getSinkTableName();
            }

            String allColumns = getAllSinkColumns(rsmd);

            // Get Postgres COPY meta-command manager
            PgConnection copyOperationConnection = this.connection.unwrap(PgConnection.class);
            CopyManager copyManager = new CopyManager(copyOperationConnection);
            String copyCmd = getCopyCommand(tableName, allColumns);
            copyIn = copyManager.copyIn(copyCmd);

            char unitSeparator = 0x1F;
            int columnsNumber = rsmd.getColumnCount();

            StringBuilder row = new StringBuilder();
            StringBuilder cols = new StringBuilder();

            byte[] bytes;
            String colValue;
            while (resultSet.next()) {

                //Clear el StringBuilders
                row.setLength(0);
                cols.setLength(0);

                // Get Columns values
                // First column
                colValue = resultSet.getString(1);
                if (!resultSet.wasNull() || colValue != null) cols.append(colValue);
                for (int i = 2; i <= columnsNumber; i++) {
                    cols.append(unitSeparator);
                    colValue = resultSet.getString(i);
                    if (!resultSet.wasNull() || colValue != null) cols.append(colValue);
                }

                //Escape special chars
                if (this.options.isSinkDisableEscape())
                    row.append(cols.toString());
                else
                    row.append(cols.toString().replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r"));

                // Row ends with \n
                row.append("\n");

                // Copy data to postgres
                bytes = row.toString().getBytes();
                copyIn.writeToCopy(bytes, 0, bytes.length);
            }

            copyIn.endCopy();
        } catch (Exception e) {
            if (copyIn != null && copyIn.isActive()) {
                copyIn.cancelCopy();
            }
            this.connection.rollback();
            throw e;
        } finally {
            if (copyIn != null && copyIn.isActive()) {
                copyIn.cancelCopy();
            }
        }

        this.getConnection().commit();

        return 0;
    }

    private String getCopyCommand(String tableName, String allColumns) {

        StringBuilder copyCmd = new StringBuilder();

        copyCmd.append("COPY ");
        copyCmd.append(tableName);

        if (allColumns != null) {
            copyCmd.append(" (");
            copyCmd.append(allColumns);
            copyCmd.append(")");
        }

        copyCmd.append(" FROM STDIN WITH DELIMITER e'\\x1f'  NULL '' ENCODING 'UTF-8' ");

        LOG.info("Copying data with this command: " + copyCmd.toString());

        return copyCmd.toString();
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
    protected void createStagingTable() throws SQLException {

        Statement statement = this.getConnection().createStatement();
        String sinkStagingTable = getQualifiedStagingTableName();

        String sql = "CREATE UNLOGGED TABLE IF NOT EXISTS " + sinkStagingTable + " ( LIKE " + this.getSinkTableName() + " INCLUDING DEFAULTS INCLUDING CONSTRAINTS ) WITH (autovacuum_enabled=false)";

        LOG.info("Creating staging table with this command: " + sql);
        statement.executeUpdate(sql);
        statement.close();
        this.getConnection().commit();
    }

    @Override
    protected void mergeStagingTable() throws SQLException {
        Statement statement = this.getConnection().createStatement();

        String[] pks = this.getSinkPrimaryKeys(this.getSinkTableName());
        // Primary key is required
        if (pks == null || pks.length == 0) {
            throw new IllegalArgumentException("Sink table must have at least one primary key column for incremental mode.");
        }

        // options.sinkColumns was set during the insertDataToTable
        String allColls = getAllSinkColumns(null);

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ")
                .append(this.getSinkTableName())
                .append(" SELECT ")
                .append(allColls)
                .append(" FROM ")
                .append(this.getSinkStagingTableName())
                .append(" ON CONFLICT ")
                .append(" (").append(String.join(",", pks)).append(" )")
                .append(" DO UPDATE SET ");

        // Set all columns for DO UPDATE SET statement
        for (String colName : allColls.split(",")) {
            sql.append(" ").append(colName).append(" = excluded.").append(colName).append(" ,");
        }
        // Delete the last comma
        sql.setLength(sql.length() - 1);

        LOG.info("Merging staging table and sink table with this command: " + sql);
        statement.executeUpdate(sql.toString());
        statement.close();
        this.getConnection().commit();
    }

    @Override
    public void preSourceTasks() throws SQLException {

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

    @Override
    public void postSourceTasks() {/*Not implemented*/}
}

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

    private DataSourceType dsType;


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
    public int insertDataToTable(ResultSet resultSet) throws SQLException {

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

        this.connection.commit();

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

        LOG.debug("Copying data with this command: " + copyCmd.toString());

        return copyCmd.toString();
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (null == this.connection) {

            if (dsType == DataSourceType.SOURCE) {
                // TODO
                if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
                    throw new IllegalArgumentException("Source query option for PostgresSQL database is not yet supported");
                }
                this.connection = makeSourceConnection();
            } else if (dsType == DataSourceType.SINK) {
                this.connection = makeSinkConnection();
            } else {
                LOG.error("DataSourceType must be Source or Sink");
            }
        }

        return this.connection;
    }

    @Override
    public ResultSet readTable(String tableName, String[] columns, int nThread) throws SQLException {

        // If table name parameter is null get it from options
        tableName = tableName == null ? this.options.getSourceTable() : tableName;

        // If columns parameter is null, get it from options
        String allColumns = this.options.getSourceColumns() == null ? "ts.*" : this.options.getSourceColumns();

        StringBuilder sqlCmd = new StringBuilder();

        // Full table read. Get table data in buckets
        sqlCmd.append(" WITH int_ctid as (SELECT (('x' || SUBSTR(md5(ctid :: text), 1, 8)) :: bit(32) :: int) ictid  from ");
        sqlCmd.append(escapeTableName(tableName));
        sqlCmd.append("), replicadb_table_stats as (select min(ictid) as min_ictid, max(ictid) as max_ictid from int_ctid )");
        sqlCmd.append("SELECT ").append(allColumns).append(" FROM ").append(escapeTableName(tableName)).append(" as ts, replicadb_table_stats");
        sqlCmd.append(" WHERE ");

        // TODO: source.query with buckets.

        // Read table with source-where option specified
        if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
            sqlCmd.append(options.getSourceWhere());
            sqlCmd.append(" AND ");
        }
        sqlCmd.append(" width_bucket((('x' || substr(md5(ctid :: text), 1, 8)) :: bit(32) :: int), replicadb_table_stats.min_ictid, replicadb_table_stats.max_ictid, ").append(options.getJobs()).append(") ");


        if ((nThread + 1) == options.getJobs())
            sqlCmd.append(" >= ?");
        else
            sqlCmd.append(" = ?");

//        sqlCmd.append(" SELECT ").append(allColumns).append(" FROM ");
//        sqlCmd.append(escapeTableName(tableName)).append(" ts WHERE -1 <> ?");


        return super.execute(sqlCmd.toString(), 5000, nThread + 1);
    }

    @Override
    protected void createStagingTable() throws SQLException {

        Statement statement = this.getConnection().createStatement();
        String sinkStagingTable = getQualifiedStagingTableName();

        String sql = "CREATE UNLOGGED TABLE IF NOT EXISTS " + sinkStagingTable + " ( LIKE " + this.getSinkTableName() + " INCLUDING DEFAULTS INCLUDING CONSTRAINTS )";

        LOG.debug("Creating staging table with this command: " + sql);
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

        LOG.debug("Merging staging table and sink table with this command: " + sql);
        statement.executeUpdate(sql.toString());
        statement.close();
        this.getConnection().commit();
    }

    @Override
    public void preSourceTasks() {/*Not implemented*/}

    @Override
    public void postSourceTasks() {/*Not implemented*/}
}

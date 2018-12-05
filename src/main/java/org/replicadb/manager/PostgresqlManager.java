package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;

import java.sql.*;

public class PostgresqlManager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(PostgresqlManager.class.getName());

    private Connection connection;
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
    public int insertDataToTable(ResultSet resultSet, String tableName, String[] columns) throws SQLException {

        // If table name is null get it from options
        tableName = tableName == null ? this.options.getSinkTable() : tableName;

        String allColumns = this.options.getSinkColumns();

        // Get Postgres COPY meta-command manager
        PgConnection copyOperationConnection = this.connection.unwrap(PgConnection.class);
        CopyManager copyManager = new CopyManager(copyOperationConnection);
        String copyCmd = getCopyCommand(tableName, allColumns);
        CopyIn copyIn = copyManager.copyIn(copyCmd);

        LOG.debug("Coping data with this command: " + copyCmd);

        try {
            ResultSetMetaData rsmd = resultSet.getMetaData();
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
                cols.append(resultSet.getString(1));
                for (int i = 2; i <= columnsNumber; i++) {
                    cols.append(unitSeparator);
                    colValue = resultSet.getString(i);
                    if (colValue != null) cols.append(colValue);
                }

                //Escape special chars
                if (this.options.isSinkDisableEscape())
                    row.append(cols.toString());
                else
                    row.append(cols.toString().replace("\\", "\\\\").replace("\n", "\\n"));

                // Row ends with \n
                row.append("\n");

                // Copy data to postgres
                bytes = row.toString().getBytes();
                copyIn.writeToCopy(bytes, 0, bytes.length);

            }

            copyIn.endCopy();
        } catch (Exception e) {
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

        return copyCmd.toString();
    }

    private String getColumnsFromResultSet(ResultSet resultSet) throws SQLException {

        StringBuilder columnNames = new StringBuilder();

        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();

        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) columnNames.append(", ");
            columnNames.append(rsmd.getColumnName(i));
        }
        LOG.debug("ColumnNames: " + columnNames);

        return columnNames.toString();
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
        String allColumns = this.options.getSourceColumns() == null ? "*" : this.options.getSourceColumns();

        StringBuilder sqlCmd = new StringBuilder();

        // Full table read. Get table data in buckets
        sqlCmd.append(" WITH int_ctid as (" +
                "SELECT (('x' || SUBSTR(md5(ctid :: text), 1, 8)) :: bit(32) :: int) ictid  from ");
        sqlCmd.append(escapeTableName(tableName));
        sqlCmd.append("), replicadb_table_stats as (select min(ictid) as min_ictid, max(ictid) as max_ictid from int_ctid )");
        sqlCmd.append("SELECT ").append(allColumns).append(" FROM ").append(escapeTableName(tableName)).append(", replicadb_table_stats");
        sqlCmd.append(" WHERE ");

        // TODO: source.query with buckets.

        // Read table with source-where option specified
        if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
            sqlCmd.append(options.getSourceWhere());
            sqlCmd.append(" AND " );
        }
        sqlCmd.append(" width_bucket((('x' || substr(md5(ctid :: text), 1, 8)) :: bit(32) :: int), replicadb_table_stats.min_ictid, replicadb_table_stats.max_ictid, ").append(options.getJobs()).append(") ");


        if ((nThread +1) == options.getJobs())
            sqlCmd.append(" >= ?");
        else
            sqlCmd.append(" = ?");

        return super.execute(sqlCmd.toString(), 5000, nThread+1);
    }

}

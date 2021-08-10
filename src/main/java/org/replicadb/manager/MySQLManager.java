package org.replicadb.manager;

import com.mysql.cj.jdbc.JdbcPreparedStatement;
import org.mariadb.jdbc.MariaDbStatement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;

public class MySQLManager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(MySQLManager.class.getName());

    private static Long chunkSize = 0L;

    /**
     * Constructs the SqlManager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     */
    public MySQLManager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
        // In MySQL and MariaDB this properties are required
        if (this.dsType.equals(DataSourceType.SINK)){
            Properties mysqlProps = new Properties();
            mysqlProps.setProperty("characterEncoding", "UTF-8");
            mysqlProps.setProperty("allowLoadLocalInfile", "true");
            mysqlProps.setProperty("rewriteBatchedStatements","true");
            options.setSinkConnectionParams(mysqlProps);
        }
    }

    @Override
    public String getDriverClass() {
        return JdbcDrivers.MYSQL.getDriverClass();
    }

    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) throws SQLException, IOException {

        try {

            ResultSetMetaData rsmd = resultSet.getMetaData();
            String tableName;

            // Get table name and columns
            if (options.getMode().equals(ReplicationMode.COMPLETE.getModeText())) {
                tableName = getSinkTableName();
            } else {
                tableName = getQualifiedStagingTableName();
            }

            String allColumns = getAllSinkColumns(rsmd);

            // Get MySQL LOAD DATA manager
            String loadDataSql = getLoadDataSql(tableName, allColumns);
            PreparedStatement statement = this.connection.prepareStatement(loadDataSql);

            JdbcPreparedStatement mysqlStatement = null;
            MariaDbStatement mariadbStatement = null;
            if (statement.isWrapperFor(MariaDbStatement.class)) {
                mariadbStatement = statement.unwrap(MariaDbStatement.class);
            } else {
                mysqlStatement = statement.unwrap(JdbcPreparedStatement.class);
            }

            char unitSeparator = 0x1F;
            int columnsNumber = rsmd.getColumnCount();

            StringBuilder row = new StringBuilder();
            StringBuilder cols = new StringBuilder();

            byte[] bytes = "".getBytes();
            String colValue;
            int rowCounts = 0;
            int batchSize = options.getFetchSize();

            if (resultSet.next()) {
                // Create Bandwidth Throttling
                bandwidthThrottlingCreate(resultSet, rsmd);

                do {
                    bandwidthThrottlingAcquiere();

                    // Get Columns values
                    for (int i = 1; i <= columnsNumber; i++) {
                        if (i > 1) cols.append(unitSeparator);

                        switch (rsmd.getColumnType(i)) {

                            case Types.CLOB:
                                colValue = clobToString(resultSet.getClob(i));
                                break;
                            //case Types.BINARY:
                            case Types.BLOB:
                                //colValue = blobToPostgresHex(resultSet.getBlob(i));
                                // TODO revisar los BLOB y CLOB
                                colValue = "";
                                break;
                            /*case Types.TIMESTAMP:
                            case Types.TIMESTAMP_WITH_TIMEZONE:
                            case -101:
                            case -102:
                                //colValue = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS XXX").format(resultSet.getTimestamp(i)) ;
                                //colValue= String.valueOf(resultSet.getTimestamp(i).getTime());
                                //colValue= resultSet.getTimestamp(i));
                                colValue = resultSet.getString(i);
                                LOG.debug("colName: {}, colValue: {}, epoch: {}", rsmd.getColumnName(i) ,colValue,resultSet.getTimestamp(i,utc).getTime());
                                break;*/
                            default:
                                colValue = resultSet.getString(i);
                                break;
                        }

                        if (!resultSet.wasNull() || colValue != null) cols.append(colValue);
                    }

                    // Escape special chars
                    if (this.options.isSinkDisableEscape())
                        row.append(cols);
                    else
                        row.append(cols.toString().replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace("\u0000", ""));

                    // Row ends with \n
                    row.append("\n");

                    // Copy data to mysql
                    bytes = row.toString().getBytes(StandardCharsets.UTF_8);

                    if (++rowCounts % batchSize == 0) {
                        if (mysqlStatement instanceof JdbcPreparedStatement) {
                            mysqlStatement.setLocalInfileInputStream(new ByteArrayInputStream(bytes));
                            mysqlStatement.executeUpdate(loadDataSql);
                        } else {
                            mariadbStatement.setLocalInfileInputStream(new ByteArrayInputStream(bytes));
                            mariadbStatement.executeUpdate(loadDataSql);
                        }

                        // Clear StringBuilders
                        row.setLength(0); // set length of buffer to 0
                        row.trimToSize();
                        rowCounts = 0;
                    }

                    // Clear StringBuilders
                    cols.setLength(0); // set length of buffer to 0
                    cols.trimToSize();
                } while (resultSet.next());
            }

            // insert remaining records
            if (rowCounts != 0) {
                if (mysqlStatement instanceof JdbcPreparedStatement) {
                    mysqlStatement.setLocalInfileInputStream(new ByteArrayInputStream(bytes));
                    mysqlStatement.executeUpdate(loadDataSql);
                } else {
                    mariadbStatement.setLocalInfileInputStream(new ByteArrayInputStream(bytes));
                    mariadbStatement.executeUpdate(loadDataSql);
                }
            }

        } catch (Exception e) {
            this.connection.rollback();
            throw e;
        }

        this.getConnection().commit();
        return 0;
    }

    private String getLoadDataSql(String tableName, String allColumns) {
        StringBuilder loadDataSql = new StringBuilder();
        loadDataSql.append("LOAD DATA LOCAL INFILE 'dummy' INTO TABLE ");
        loadDataSql.append(tableName);
        loadDataSql.append(" CHARACTER SET UTF8 FIELDS TERMINATED BY X'1F' ");
        if (allColumns != null) {
            loadDataSql.append(" (");
            loadDataSql.append(allColumns);
            loadDataSql.append(")");
        }

        LOG.info("Loading data with this command: {}", loadDataSql);
        return loadDataSql.toString();
    }

    @Override
    protected void createStagingTable() throws SQLException {
        Statement statement = this.getConnection().createStatement();
        String sinkStagingTable = getQualifiedStagingTableName();

        // Get sink columns.
        String allSinkColumns = null;
        if (this.options.getSinkColumns() != null && !this.options.getSinkColumns().isEmpty()) {
            allSinkColumns = this.options.getSinkColumns();
        } else if (this.options.getSourceColumns() != null && !this.options.getSourceColumns().isEmpty()) {
            allSinkColumns = this.options.getSourceColumns();
        } else {
            allSinkColumns = "*";
        }

        String sql = " CREATE TABLE " + sinkStagingTable + " AS (SELECT " + allSinkColumns + " FROM " + this.getSinkTableName() + " WHERE 1 = 0 ) ";

        LOG.info("Creating staging table with this command: " + sql);
        statement.executeUpdate(sql);
        statement.close();
        this.getConnection().commit();
    }

    @Override
    protected void mergeStagingTable() throws SQLException {
        Statement statement = this.getConnection().createStatement();

        try {
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
                    .append(" (")
                    .append(allColls)
                    .append(" ) ")
                    .append(" SELECT ")
                    .append(allColls)
                    .append(" FROM ")
                    .append(this.getSinkStagingTableName())
                    .append(" as excluded ON DUPLICATE KEY UPDATE ");

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

        } catch (Exception e) {
            statement.close();
            this.connection.rollback();
            throw e;
        }
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
                    options.getSourceQuery() + ") AS REPLICADB_TABLE ";
        } else {

            sqlCmd = "SELECT " +
                    allColumns +
                    " FROM " +
                    escapeTableName(tableName);

            // Source Where
            if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
                sqlCmd = sqlCmd + " WHERE " + options.getSourceWhere();
            }
        }

        if (chunkSize != 0L) {
            sqlCmd = sqlCmd + " LIMIT ? OFFSET ?";
            return super.execute(sqlCmd, chunkSize, offset);
        } else {
            return super.execute(sqlCmd);
        }

    }


    @Override
    public void preSourceTasks() throws SQLException {
        // Because chunkSize is static it's required to initialize it
        // when the unit tests are running
        chunkSize = 0L;

        // Only calculate the chunk size when parallel execution is active
        if (this.options.getJobs() != 1) {
            /**
             * Calculating the chunk size for parallel job processing
             */
            Statement statement = this.getConnection().createStatement();
            String sql = "SELECT " +
                    " CEIL(count(*) / " + options.getJobs() + ") chunk_size" +
                    ", count(*) total_rows" +
                    " FROM ";

            // Source Query
            if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
                sql = sql + "( " + this.options.getSourceQuery() + " ) as REPLICADB_TABLE";

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

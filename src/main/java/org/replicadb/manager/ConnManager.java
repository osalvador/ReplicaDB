package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;

import java.sql.*;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Abstract interface that manages connections to a database.
 * The implementations of this class drive the actual discussion with
 * the database about table formats, etc.
 */
public abstract class ConnManager {

    private static final Logger LOG = LogManager.getLogger(ConnManager.class.getName());

    /**
     * If users are providing their own query, the following string is expected
     * to appear in the WHERE clause, which will be substituted with a pair of
     * conditions on the input to allow input splits to parallelise the import.
     */
    public static final String SUBSTITUTE_TOKEN = "$CONDITIONS";

    protected ToolOptions options;

    private static String randomSinkStagingTableName;

    /**
     * Execute a SQL statement to read the named set of columns from a table.
     * If columns is null, all columns from the table are read. This is a direct
     * (non-parallelized) read of the table back to the current client.
     * The client is responsible for calling ResultSet.close() when done with the
     * returned ResultSet object, and for calling release() after that to free
     * internal state.
     */
    public abstract ResultSet readTable(String tableName, String[] columns, int nThread)
            throws Exception;


    /**
     * Execute a SQL statement to insert the resultset to the table.
     */
    public abstract int insertDataToTable(ResultSet resultSet, int taskId)
            throws Exception;


    /**
     * @return the actual database connection.
     */
    public abstract Connection getConnection() throws Exception;

    /**
     * discard the database connection.
     */
    public void discardConnection(boolean doClose) {
        throw new UnsupportedOperationException("No discard connection support "
                + "for this database");
    }

    /**
     * @return a string identifying the driver class to load for this
     * JDBC connection type.
     */
    public abstract String getDriverClass();

    /**
     * @return default autoCommit connection property
     */
    public boolean getAutoCommit() {
        return false;
    }

    /**
     * When using a column name in a generated SQL query, how (if at all)
     * should we escape that column name? e.g., a column named "table"
     * may need to be quoted with backtiks: "`table`".
     *
     * @param colName the column name as provided by the user, etc.
     * @return how the column name should be rendered in the sql text.
     */
    public String escapeColName(String colName) {
        return colName;
    }

    /**
     * Variant of escapeColName() method that will escape whole column name array.
     *
     * @param colNames Column names as provided by the user, etc.
     * @return
     */
    public String[] escapeColNames(String... colNames) {
        String[] escaped = new String[colNames.length];
        int i = 0;
        for (String colName : colNames) {
            escaped[i++] = escapeColName(colName);
        }
        return escaped;
    }

    /**
     * When using a table name in a generated SQL query, how (if at all)
     * should we escape that column name? e.g., a table named "table"
     * may need to be quoted with backtiks: "`table`".
     *
     * @param tableName the table name as provided by the user, etc.
     * @return how the table name should be rendered in the sql text.
     */
    public String escapeTableName(String tableName) {
        return tableName;
    }

    public String getSchemaFromQualifiedTableName(String tableName) {

        if (tableName.split("\\.").length - 1 > 0) {
            String s = tableName.substring(0, tableName.lastIndexOf("."));
            return s.substring(s.lastIndexOf(".") + 1);
        }
        // No schema defined in the tableName
        return null;
    }

    public String getTableNameFromQualifiedTableName(String tableName) {
        return tableName.substring(tableName.lastIndexOf(".") + 1);
    }


    public String getSinkTableName() {

        if (this.options.getSinkTable() != null && !this.options.getSinkTable().isEmpty()) {
            return this.options.getSinkTable();
        } else if (this.options.getSourceTable() != null && !this.options.getSourceTable().isEmpty()) {
            return this.options.getSourceTable();
        } else {
            throw new IllegalArgumentException("Options source-table and sink-table are null, can't determine the sink table name");
        }

    }

    public String getSinkStagingTableName() {

        if (options.getSinkStagingTable() != null && !options.getSinkStagingTable().isEmpty()) {
            return this.options.getSinkStagingTable();
        } else if (randomSinkStagingTableName != null && !randomSinkStagingTableName.isEmpty()) {
            return randomSinkStagingTableName;
        } else {
            // Create new randomSinkStagingTableName
            Random random = new Random();
            String randomName = "repdb" + random.nextInt(90) + 10;

            String tableName = getSinkTableName();

            // Staging table Alias name.
            // Sometimes the size of the table along with the random name exceeds the maximum size of an object name in some databases
            if (options.getSinkStagingTableAlias() != null && !options.getSinkStagingTableAlias().isEmpty()) {
                tableName = options.getSinkStagingTableAlias();
            }

            randomSinkStagingTableName = getTableNameFromQualifiedTableName(tableName) + randomName;

            return randomSinkStagingTableName;
        }
    }

    public String getQualifiedStagingTableName() {
        if (options.getSinkStagingTable() != null && !options.getSinkStagingTable().isEmpty()) {
            return this.options.getSinkStagingTable();
        } else if (options.getSinkStagingSchema() != null && !options.getSinkStagingSchema().isEmpty()) {
            return options.getSinkStagingSchema() + "." + getTableNameFromQualifiedTableName(getSinkStagingTableName());
        } else {
            return getSinkStagingTableName();
        }
    }


    public String getAllSinkColumns(ResultSetMetaData rsmd) throws SQLException {

        if (this.options.getSinkColumns() != null && !this.options.getSinkColumns().isEmpty()) {
            return this.options.getSinkColumns();
        } else if (this.options.getSourceColumns() != null && !this.options.getSourceColumns().isEmpty()) {
            return this.options.getSourceColumns();
        } else {
            this.options.setSinkColumns(getColumnsFromResultSetMetaData(rsmd));
            LOG.warn("Options source-columns and sink-columns are null, getting from Source ResultSetMetaData: " + this.options.getSinkColumns());
            return this.options.getSinkColumns();
        }
    }


    private String getColumnsFromResultSetMetaData(ResultSetMetaData rsmd) throws SQLException {

        StringBuilder columnNames = new StringBuilder();

        int columnsNumber = rsmd.getColumnCount();

        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) columnNames.append(",");
            if (this.options.getQuotedIdentifiers())
                columnNames.append("\"").append(rsmd.getColumnName(i)).append("\"");
            else
                columnNames.append(rsmd.getColumnName(i));
        }
        return columnNames.toString();
    }


    /**
     * Perform any shutdown operations on the connection.
     *
     * @throws SQLException
     */
    public abstract void close() throws SQLException;


    /**
     * Clean Up the environment. Drop staging table and any other temporal data.
     *
     * @throws SQLException
     */
    public abstract void cleanUp() throws Exception;

    /**
     * If a method of this ConnManager has returned a ResultSet to you,
     * you are responsible for calling release() after you close the
     * ResultSet object, to free internal resources. ConnManager
     * implementations do not guarantee the ability to have multiple
     * returned ResultSets available concurrently. Requesting a new
     * ResultSet from a ConnManager may cause other open ResulSets
     * to close.
     */
    public abstract void release();

    /**
     * Return the current time from the perspective of the database server.
     * Return null if this cannot be accessed.
     */
    public Timestamp getCurrentDbTimestamp() {
        LOG.warn("getCurrentDbTimestamp(): Using local system timestamp.");
        return new Timestamp(System.currentTimeMillis());
    }


    /**
     * Sometimes it is necessary to perform some preliminary tasks before populating the data with multi-threaded jobs.
     */
    public abstract void preSourceTasks() throws Exception;

    public abstract Future<Integer> preSinkTasks(ExecutorService executor) throws Exception;

    /**
     * Sometimes it is necessary to perform some tasks after populating the data with multithreading jobs.
     */
    public abstract void postSourceTasks() throws Exception;

    public abstract void postSinkTasks() throws Exception;


    /**
     * Return the name of the primary keys for a table, or null if there is none.
     *
     * @param tableName
     * @return
     */
    public abstract String[] getSinkPrimaryKeys(String tableName);

}

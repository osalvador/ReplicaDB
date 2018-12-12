package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;

import javax.xml.transform.Result;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

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

    /**
     * Execute a SQL statement to read the named set of columns from a table.
     * If columns is null, all columns from the table are read. This is a direct
     * (non-parallelized) read of the table back to the current client.
     * The client is responsible for calling ResultSet.close() when done with the
     * returned ResultSet object, and for calling release() after that to free
     * internal state.
     */
    public abstract ResultSet readTable(String tableName, String[] columns, int nThread)
            throws SQLException;


    /**
     * Execute a SQL statement to insert the resultset to the table.
     */
    public abstract int insertDataToTable(ResultSet resultSet, String tableName, String[] columns)
            throws SQLException;


    /**
     *  Execute a SQL `TRUNCATE TABLE` statement for the sink table
     */
    public abstract void truncateTable() throws SQLException;


    /**
     * @return the actual database connection.
     */
    public abstract Connection getConnection() throws SQLException;

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


    /**
     * Perform any shutdown operations on the connection.
     */
    public abstract void close() throws SQLException;


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

}



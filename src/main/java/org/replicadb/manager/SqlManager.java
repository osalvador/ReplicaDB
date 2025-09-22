package org.replicadb.manager;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.concurrent.TimedSemaphore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * ConnManager implementation for generic SQL-compliant database.
 * This is an abstract class; it requires a database-specific
 * ConnManager implementation to actually create the connection.
 */
public abstract class SqlManager extends ConnManager {

    private static final Logger LOG = LogManager.getLogger(SqlManager.class.getName());

    protected Connection connection;
    protected DataSourceType dsType;

    private Statement lastStatement;
    // For Bandwidth Throttling
    private TimedSemaphore bandwidthRateLimiter;
    private int rowSize = 0;
    private long fetchs = 0L;

    /**
     * Constructs the SqlManager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     */
    public SqlManager(final ToolOptions opts) {
        this.options = opts;
        initOptionDefaults();
    }

    /**
     * Sets default values for values that were not provided by the user.
     * Only options with database-specific defaults should be configured here.
     */
    protected void initOptionDefaults() {
        //if (options.getFetchSize() == null) {
        //    LOG.info("Using default fetchSize of " + DEFAULT_FETCH_SIZE);
        //    options.setFetchSize(DEFAULT_FETCH_SIZE);
        //}
    }

    @Override
    public ResultSet readTable(String tableName, String[] columns, int nThread)
            throws SQLException {
//        if (columns == null) {
//            columns = getColumnNames(tableName);
//        }

        //        boolean first = true;
//        for (String col : columns) {
//            if (!first) {
//                sb.append(", ");
//            }
//            sb.append(escapeColName(col));
//            first = false;
//        }

        if (tableName == null) tableName = this.options.getSourceTable();

        String allColumns;

        if (this.options.getSourceColumns() == null) allColumns = "*";
        else {
            allColumns = this.options.getSourceColumns();
        }


        String sqlCmd = "SELECT " +
                allColumns +
                " FROM " +
                escapeTableName(tableName);
                /*" AS " +   // needed for hsqldb; doesn't hurt anyone else.
                // Oracle Hurt...
                escapeTableName(tableName);*/
        LOG.debug(Thread.currentThread().getName() + ": Reading table with command: " + sqlCmd);
        return execute(sqlCmd);
    }


    /**
     * Retrieve the actual connection from the outer ConnManager.
     */
    public Connection getConnection() throws SQLException {
        if (null == this.connection) {

            if (dsType == DataSourceType.SOURCE) {
                this.connection = makeSourceConnection();
            } else if (dsType == DataSourceType.SINK) {
                this.connection = makeSinkConnection();
            } else {
                LOG.error("DataSourceType must be Source or Sink");
            }
        }

        return this.connection;
    }

    ;

    /**
     * Executes an arbitrary SQL statement.
     *
     * @param stmt      The SQL statement to execute
     * @param fetchSize Overrides default or parameterized fetch size
     * @return A ResultSet encapsulating the results or null on error
     */
    protected ResultSet execute(String stmt, Integer fetchSize, Object... args)
            throws SQLException {
        // Release any previously-open statement.
        release();

        LOG.info("{}: Executing SQL statement: {}",Thread.currentThread().getName(), stmt);

        PreparedStatement statement = this.getConnection().prepareStatement(stmt, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        if (fetchSize != null) {
            LOG.debug("{}: Using fetchSize for next query: {}",Thread.currentThread().getName() , fetchSize);
            statement.setFetchSize(fetchSize);
        }
        this.lastStatement = statement;
        if (null != args) {
            for (int i = 0; i < args.length; i++) {
                statement.setObject(i + 1, args[i]);
            }
        }

        StringBuilder sb = new StringBuilder();
        for (Object o : args) {
            sb.append(o.toString())
                    .append(", ");
        }
        LOG.info("{}: With args: {}",Thread.currentThread().getName() , sb);

        return statement.executeQuery();
    }

    /**
     * Executes an arbitrary SQL Statement.
     *
     * @param stmt The SQL statement to execute
     * @return A ResultSet encapsulating the results or null on error
     */
    protected ResultSet execute(String stmt, Object... args) throws SQLException {
        return execute(stmt, options.getFetchSize(), args);
    }

    public void close() throws SQLException {
        release();
        // Close connection, ignore exceptions
        if (this.connection != null) {
            try {
                this.getConnection().close();
            } catch (Exception e) {
                LOG.error(e);
            }
        }
    }

    /**
     * Create a connection to the database; usually used only from within
     * getConnection(), which enforces a singleton guarantee around the
     * Connection object.
     */
    protected Connection makeSourceConnection() throws SQLException {

        Connection conn;
        String driverClass = getDriverClass();

        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException("Could not load db driver class: " + driverClass);
        }

        String username = options.getSourceUser();
        String password = options.getSourcePassword();
        String connectString = options.getSourceConnect();

        Properties connectionParams = options.getSourceConnectionParams();
        if (connectionParams != null && connectionParams.size() > 0) {
            LOG.trace("User specified connection params. Using properties specific API for making connection.");

            Properties props = new Properties();
            if (username != null) {
                props.put("user", username);
            }

            if (password != null) {
                props.put("password", password);
            }

            props.putAll(connectionParams);
            conn = DriverManager.getConnection(connectString, props);
        } else {
            LOG.trace("No connection parameters specified. Using regular API for making connection.");
            if (username == null) {
                conn = DriverManager.getConnection(connectString);
            } else {
                conn = DriverManager.getConnection(connectString, username, password);
            }
        }

        conn.setAutoCommit(false);

        return conn;
    }

    protected Connection makeSinkConnection() throws SQLException {

        Connection conn;
        String driverClass = getDriverClass();

        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException("Could not load db driver class: " + driverClass);
        }

        String username = options.getSinkUser();
        String password = options.getSinkPassword();
        String connectString = options.getSinkConnect();

        Properties connectionParams = options.getSinkConnectionParams();
        if (connectionParams != null && connectionParams.size() > 0) {
            LOG.trace("User specified connection params. Using properties specific API for making connection.");

            Properties props = new Properties();
            if (username != null) {
                props.put("user", username);
            }

            if (password != null) {
                props.put("password", password);
            }

            props.putAll(connectionParams);
            conn = DriverManager.getConnection(connectString, props);
        } else {
            LOG.trace("No connection parameters specified. Using regular API for making connection.");
            if (username == null) {
                conn = DriverManager.getConnection(connectString);
            } else {
                conn = DriverManager.getConnection(connectString, username, password);
            }
        }

        conn.setAutoCommit(false);

        return conn;
    }


    public void release() {
        if (null != this.lastStatement) {
            try {
                this.lastStatement.close();
            } catch (SQLException e) {
                LOG.error("Exception closing executed Statement: " + e, e);
            }

            this.lastStatement = null;
        }
    }


    @Override
    public String[] getSinkPrimaryKeys(String tableName) {

        String[] pks;
        String table = getTableNameFromQualifiedTableName(tableName);
        String schema = getSchemaFromQualifiedTableName(tableName);

        pks = getPrimaryKeys(table, schema);

        if (null == pks) {
            LOG.debug("Getting PKs for schema: {} and table: {}. Not found.", schema, table);

            // Trying with uppercase
            table = table != null ? table.toUpperCase() : null;
            schema = schema != null ? schema.toUpperCase() : null;

            pks = getPrimaryKeys(table, schema);

            if (null == pks) {
                LOG.debug("Getting PKs for schema: {} and table: {}. Not found.", schema, table);

                // Trying with lowercase
                table = table != null ? table.toLowerCase() : null;
                schema = schema != null ? schema.toLowerCase() : null;

                pks = getPrimaryKeys(table, schema);
                if (null == pks) {
                    LOG.debug("Getting PKs for schema: {} and table: {}. Not found.", schema, table);
                    return null;
                }
            }
        }

        LOG.info("Getting PKs for schema: {} and table: {}. Found.", schema, table);

        return pks;
    }

    public String[] getPrimaryKeys(String table, String schema) {
        try {
            DatabaseMetaData metaData = this.getConnection().getMetaData();

            ResultSet results = metaData.getPrimaryKeys(null, schema, table);

            if (null == results) {
                return null;
            }

            try {
                ArrayList<String> pks = new ArrayList<>();
                while (results.next()) {
                    String pkName = results.getString("COLUMN_NAME");
                    if (this.options.getQuotedIdentifiers())
                        pks.add("\"" + pkName + "\"");
                    else
                        pks.add(pkName);
                }

                if (pks.isEmpty())
                    return null;
                else
                    return pks.toArray(new String[0]);

            } finally {
                results.close();
                getConnection().commit();
            }
        } catch (SQLException sqlException) {
            LOG.error("Error reading primary key metadata: " + sqlException.toString(), sqlException);
            return null;
        }
    }

    /**
     * Truncate sink table
     *
     * @throws SQLException
     */
    protected void truncateTable() throws SQLException {
        truncateTable("TRUNCATE TABLE ");
    }
    protected void truncateTable(String sqlCommand) throws SQLException {
        String tableName;
        // Get table name
        if (options.getMode().equals(ReplicationMode.INCREMENTAL.getModeText())
                || options.getMode().equals(ReplicationMode.COMPLETE_ATOMIC.getModeText())) {
            tableName = getQualifiedStagingTableName();
        } else {
            tableName = getSinkTableName();
        }
        String sql = sqlCommand + tableName;
        LOG.info("Truncating sink table with this command: {}", sql);
        Statement statement = this.getConnection().createStatement();
        statement.executeUpdate(sql);
        statement.close();
        this.getConnection().commit();
    }

    /**
     * Delete all rows of a sink table within a transaction asynchronously, in a complete-atomic mode
     *
     * @param executor the executor service for delete rows asynchronously in a task.
     * @return the Future of the task.
     */
    public Future<Integer> atomicDeleteSinkTable(ExecutorService executor) {
        String sql = " DELETE FROM " + this.getSinkTableName();
        LOG.info("Atomic and asynchronous deletion of all data from the sink table with this command: " + sql);

        return executor.submit(() -> {
            Statement statement = this.getConnection().createStatement();
            statement.executeUpdate(sql);
            statement.close();
            // Do not commit this transaction
            return 0;
        });
    }

    /**
     * Insert all rows from staging table to the sink table within a transaction
     *
     * @throws SQLException
     */
    public void atomicInsertStagingTable() throws SQLException {
        Statement statement = this.getConnection().createStatement();
        StringBuilder sql = new StringBuilder();
        String allColls = null;
        try {
            allColls = getAllSinkColumns(null);
        } catch (NullPointerException e) {
            // Ignore this exception
        }

        if (allColls != null) {
            sql.append(" INSERT INTO ")
                    .append(this.getSinkTableName())
                    .append(" (" + allColls + ")")
                    .append(" SELECT ")
                    .append(allColls)
                    .append(" FROM ")
                    .append(getQualifiedStagingTableName());
        } else {
            sql.append(" INSERT INTO ")
                    .append(this.getSinkTableName())
                    .append(" SELECT * ")
                    .append(" FROM ")
                    .append(getQualifiedStagingTableName());
        }

        LOG.info("Inserting data from staging table to sink table within a transaction: {}", sql);
        statement.executeUpdate(sql.toString());
        statement.close();
        this.getConnection().commit();
    }

    /**
     * Create staging table on sink database.
     * When the mode is incremental, some DBMS need to create a staging table in order to populate it
     * before merge data between the final table and the staging table.
     *
     * @throws SQLException
     */
    protected abstract void createStagingTable() throws Exception;


    /**
     * Merge staging table and sink table.
     *
     * @throws SQLException
     */
    protected abstract void mergeStagingTable() throws Exception;

    /**
     * Drop staging table.
     *
     * @throws SQLException
     */
    public void dropStagingTable() throws SQLException {
        // TODO: Do not drop stagging table if it's defined by user.
        Statement statement = this.getConnection().createStatement();
        String sql = "DROP TABLE " + getQualifiedStagingTableName();
        LOG.info("Dropping staging table with this command: {}", sql);

        statement.executeUpdate(sql);
        statement.close();
        this.getConnection().commit();
    }

    @Override
    public Future<Integer> preSinkTasks(ExecutorService executor) throws Exception {

        // Create staging table
        // If mode is not COMPLETE
        if (!options.getMode().equals(ReplicationMode.COMPLETE.getModeText())) {

            // Only create staging table if it is not defined by the user
            if (options.getSinkStagingTable() == null || options.getSinkStagingTable().isEmpty()) {

                // If the staging parameters have not been defined then the table is created in the public schema
                if (options.getSinkStagingSchema() == null || options.getSinkStagingSchema().isEmpty()) {
                    // TODO: This is only valid for PostgreSQL
                    LOG.warn("No staging schema is defined, setting it as PUBLIC");
                    options.setSinkStagingSchema("public");
                }
                this.createStagingTable();
            }
        }

        // On COMPLETE_ATOMIC mode
        if (options.getMode().equals(ReplicationMode.COMPLETE_ATOMIC.getModeText())) {
            return atomicDeleteSinkTable(executor);
        } else {
            // Truncate sink table if it is enabled
            if (!options.isSinkDisableTruncate()) {
                this.truncateTable();
            }

            return null;
        }

    }

    @Override
    public void postSinkTasks() throws Exception {
        // On INCREMENTAL mode
        if (options.getMode().equals(ReplicationMode.INCREMENTAL.getModeText())) {
            // Merge Data
            this.mergeStagingTable();
        } else if (options.getMode().equals(ReplicationMode.COMPLETE_ATOMIC.getModeText())) {
            this.atomicInsertStagingTable();
        }
    }

    @Override
    public void cleanUp() throws Exception {

        // Complete-atomic and incremental modes
        if (options.getMode().equals(ReplicationMode.COMPLETE_ATOMIC.getModeText())
                || options.getMode().equals(ReplicationMode.INCREMENTAL.getModeText())) {

            // Only drop staging table if it was created automatically
            if (options.getSinkStagingTable() == null || options.getSinkStagingTable().isEmpty()) {
                // Drop staging table
                this.dropStagingTable();
            }
        }
    }


    /**
     * From java.sql.CLOB to String
     *
     * @return string representation of clob
     * @throws SQLException IOException
     */
    protected String clobToString(Clob clobData) throws SQLException, IOException {

        String returnData = "";

        if (clobData != null) {
            try {
                returnData = IOUtils.toString(clobData.getCharacterStream());
            } finally {
                // The most important thing here is free the CLOB to avoid memory Leaks
                clobData.free();
            }
        }
        return returnData;
    }

    /**
     * From java.sql.SQLXML to String
     *
     * @return string representation of SQLXML
     * @throws SQLException IOException
     */
    protected String sqlxmlToString(SQLXML xmlData) throws SQLException, IOException {

        String returnData = "";

        if (xmlData != null) {
            try {
                returnData = IOUtils.toString(xmlData.getCharacterStream());
            } finally {
                // The most important thing here is free the CLOB to avoid memory Leaks
                xmlData.free();
            }
        }
        return returnData;
    }


    /**
     * Acquires the <code>rowSize</code> number of permits from this <code>bandwidthRateLimiter</code>,
     * blocking until the request can be granted.
     */
    @Deprecated
    protected void bandwidthThrottlingAcquiere() {
        // Wait for Sleeping Stopwatch
        if (rowSize != 0) {
            try {
                ++fetchs;
                if (fetchs == options.getFetchSize()) {
                    bandwidthRateLimiter.acquire();
                    fetchs = 0;
                }
            } catch (InterruptedException e) {
                LOG.error(e);
            }
        }
    }

    /**
     * Create a bandwith cap, estimating the size of the first row returned by the resultset
     * and using it as permits in the rate limit.
     *
     * @param resultSet the resultset cursor moved to the first row (resultSet.next())
     * @param rsmd      the result set metadata object
     * @throws SQLException
     */
    @Deprecated
    protected void bandwidthThrottlingCreate(ResultSet resultSet, ResultSetMetaData rsmd) throws SQLException {
        int kilobytesPerSecond = options.getBandwidthThrottling();

        if (kilobytesPerSecond > 0) {
            // Stimate the Row Size
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {

                if (rsmd.getColumnType(i) != Types.BLOB) {
                    String columnValue = resultSet.getString(i);
                    if (columnValue != null && !resultSet.getString(i).isEmpty())
                        rowSize = rowSize + resultSet.getString(i).length();
                }
            }

            double limit = ((1.0 * kilobytesPerSecond) / rowSize) / (options.getFetchSize() * 1.0 / 1000);
            if (limit == 0) limit = 1;
            this.bandwidthRateLimiter = new TimedSemaphore(1, TimeUnit.SECONDS, (int) Math.round(limit));

            LOG.info("Estimated Row Size: {} KB. Estimated limit of fetchs per second: {} ", rowSize, limit);


        }
    }


    /**
     * Catch the exception SQLFeatureNotSupportedException getting a Blob from some databases
     * @param rs the resultset
     * @param columnIndex the column index
     * @return
     * @throws SQLException
     */
    protected Blob getBlob (ResultSet rs, Integer columnIndex) throws SQLException {
        try {
            return rs.getBlob(columnIndex);
        } catch (SQLFeatureNotSupportedException e) {
            return null;
        }
    }

}

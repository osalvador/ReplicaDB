package org.replicadb.manager;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.concurrent.TimedSemaphore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.util.ColumnDescriptor;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
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
    private static final String DRIVER_PARAM_KEY = "driver";

    protected Connection connection;
    protected DataSourceType dsType;

    private Statement lastStatement;
    // For Bandwidth Throttling
    private TimedSemaphore bandwidthRateLimiter;
    private int rowSize = 0;
    private long fetchs = 0L;
    private boolean sinkTableJustCreated = false;

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
    @SuppressWarnings("null") // Java varargs guarantee args is never null (empty array when no args provided)
    protected ResultSet execute(String stmt, Integer fetchSize, Object... args)
            throws SQLException {
        // Release any previously-open statement.
        release();

        LOG.info("{}: Executing SQL statement: {}",Thread.currentThread().getName(), stmt);

        // NEW: Add connection state logging
        Connection conn = this.getConnection();
        LOG.debug("{}: Connection state - autoCommit: {}, transactionIsolation: {}, isClosed: {}", 
                  Thread.currentThread().getName(),
                  conn.getAutoCommit(),
                  conn.getTransactionIsolation(),
                  conn.isClosed());

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

        // NEW: Enhanced parameter logging with types
        if (null != args && args.length > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("Parameters bound [count=").append(args.length).append("]: ");
            for (int i = 0; i < args.length; i++) {
                Object arg = args[i];
                sb.append("param").append(i+1).append("=")
                  .append(arg == null ? "NULL" : arg.toString())
                  .append(" (type=")
                  .append(arg == null ? "null" : arg.getClass().getSimpleName())
                  .append(")");
                if (i < args.length - 1) sb.append(", ");
            }
            LOG.info("{}: {}", Thread.currentThread().getName(), sb);
        } else {
            LOG.info("{}: No parameters to bind", Thread.currentThread().getName());
        }

        ResultSet resultSet = statement.executeQuery();
        
        // NEW: Log ResultSet metadata immediately after execution
        try {
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int colCount = rsmd.getColumnCount();
            LOG.debug("{}: ResultSet returned with {} columns", Thread.currentThread().getName(), colCount);
            
            // Log column names for debugging
            StringBuilder colNames = new StringBuilder("Columns: ");
            for (int i = 1; i <= colCount; i++) {
                colNames.append(rsmd.getColumnName(i));
                if (i < colCount) colNames.append(", ");
            }
            LOG.debug("{}: {}", Thread.currentThread().getName(), colNames);
        } catch (SQLException e) {
            LOG.warn("{}: Could not retrieve ResultSet metadata: {}", Thread.currentThread().getName(), e.getMessage());
        }
        
        return resultSet;
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
                if (DataSourceType.SOURCE.equals(this.dsType) && !this.getConnection().getAutoCommit()) {
                    this.getConnection().rollback();
                }
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
            // Filter driver parameter - used for Class.forName() only, not JDBC connection
            props.remove(DRIVER_PARAM_KEY);
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
            // Filter driver parameter - used for Class.forName() only, not JDBC connection
            props.remove(DRIVER_PARAM_KEY);
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
        String truncateSql = sqlCommand + tableName;
        LOG.info("Truncating sink table with this command: {}", truncateSql);
        
        Statement statement = this.getConnection().createStatement();
        try {
            statement.executeUpdate(truncateSql);
            statement.close();
            this.getConnection().commit();
        } catch (SQLException e) {
            LOG.warn("TRUNCATE failed ({}), falling back to DELETE FROM: {}", e.getMessage(), tableName);
            statement.close();
            
            // Fallback to DELETE FROM for databases with TRUNCATE issues (DB2, etc.)
            String deleteSql = "DELETE FROM " + tableName;
            LOG.info("Deleting all rows from sink table with this command: {}", deleteSql);
            Statement deleteStatement = this.getConnection().createStatement();
            try {
                deleteStatement.executeUpdate(deleteSql);
                deleteStatement.close();
                this.getConnection().commit();
                LOG.info("DELETE FROM succeeded as fallback for table: {}", tableName);
            } catch (SQLException deleteEx) {
                deleteStatement.close();
                LOG.error("Both TRUNCATE and DELETE FROM failed for table: {}", tableName, deleteEx);
                throw deleteEx;
            }
        }
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
        // Only drop staging table if it was created automatically
        if (options.getSinkStagingTable() != null && !options.getSinkStagingTable().isEmpty()) {
            LOG.info("Skipping staging table drop because it is user-defined: {}", getQualifiedStagingTableName());
            return;
        }
        
        Statement statement = this.getConnection().createStatement();
        String sql = "DROP TABLE " + getQualifiedStagingTableName();
        LOG.info("Dropping staging table with this command: {}", sql);

        statement.executeUpdate(sql);
        statement.close();
        this.getConnection().commit();
    }

    @Override
    public Future<Integer> preSinkTasks(ExecutorService executor) throws Exception {

        // Auto-create sink table if enabled and table doesn't exist
        if (options.isSinkAutoCreate()) {
            autoCreateSinkTable();
        }

        // Create staging table
        // If mode is not COMPLETE
        if (!options.getMode().equals(ReplicationMode.COMPLETE.getModeText())) {

            // Only create staging table if it is not defined by the user
            if (options.getSinkStagingTable() == null || options.getSinkStagingTable().isEmpty()) {

                // If the staging parameters have not been defined then use the sink table's schema
                if (options.getSinkStagingSchema() == null || options.getSinkStagingSchema().isEmpty()) {
                    String sinkSchema = getSchemaFromQualifiedTableName(getSinkTableName());
                    if (sinkSchema != null && !sinkSchema.isEmpty()) {
                        LOG.debug("No staging schema defined, using sink table schema: {}", sinkSchema);
                        options.setSinkStagingSchema(sinkSchema);
                    } else {
                        LOG.debug("No staging schema defined and no schema in sink table name, letting JDBC driver use default");
                    }
                }
                this.createStagingTable();
            }
        }

        // On COMPLETE_ATOMIC mode
        if (options.getMode().equals(ReplicationMode.COMPLETE_ATOMIC.getModeText())) {
            return atomicDeleteSinkTable(executor);
        } else {
            // Truncate sink table if it is enabled and the table was not just created
            if (!options.isSinkDisableTruncate() && !sinkTableJustCreated) {
                this.truncateTable();
            } else if (sinkTableJustCreated) {
                LOG.debug("Skipping truncate because table was just created");
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

    /**
     * Default implementation of preSourceTasks that probes source metadata when auto-create is enabled.
     * Concrete managers can override this and call super.preSourceTasks() if needed.
     */
    @Override
    public void preSourceTasks() throws Exception {
        if (options.isSinkAutoCreate()) {
            probeSourceMetadata();
        }
    }

    /**
     * Probes source metadata by executing a lightweight query (SELECT * WHERE 1=0)
     * and extracts column metadata and primary keys.
     */
    protected void probeSourceMetadata() throws SQLException {
        LOG.info("Probing source metadata for auto-create...");
        
        String probeQuery;
        // Determine columns to probe - respect --source-columns if specified
        String columnsToProbe = "*";
        if (options.getSourceColumns() != null && !options.getSourceColumns().isEmpty()) {
            columnsToProbe = options.getSourceColumns();
            LOG.info("Using specified source-columns for metadata probing: {}", columnsToProbe);
        }
        
        // Prioritize source-query over source-table for metadata probing
        // (both can be set: query for data, table for PK metadata)
        if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
            // Query-based source
            probeQuery = "SELECT " + columnsToProbe + " FROM (" + options.getSourceQuery() + ") tmp WHERE 1=0";
        } else if (options.getSourceTable() != null && !options.getSourceTable().isEmpty()) {
            // Table-based source
            probeQuery = "SELECT " + columnsToProbe + " FROM " + escapeTableName(options.getSourceTable()) + " WHERE 1=0";
        } else {
            throw new IllegalArgumentException("Either source-table or source-query must be specified for auto-create");
        }

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = this.getConnection().createStatement();
            rs = stmt.executeQuery(probeQuery);
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            
            List<ColumnDescriptor> columnDescriptors = new ArrayList<>();
            Set<String> columnNames = new HashSet<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = rsmd.getColumnName(i);
                int jdbcType = rsmd.getColumnType(i);
                int precision = rsmd.getPrecision(i);
                int scale = rsmd.getScale(i);
                int nullable = rsmd.isNullable(i);
                
                columnDescriptors.add(new ColumnDescriptor(columnName, jdbcType, precision, scale, nullable));
                columnNames.add(columnName.toUpperCase());
            }
            
            options.setSourceColumnDescriptors(columnDescriptors);
            LOG.info("Captured {} column descriptors from source", columnCount);
            
            // Get primary keys (only for table-based source)
            if (options.getSourceTable() != null && !options.getSourceTable().isEmpty()) {
                String table = getTableNameFromQualifiedTableName(options.getSourceTable());
                String schema = getSchemaFromQualifiedTableName(options.getSourceTable());
                
                LOG.debug("Probing primary keys for schema='{}', table='{}'", schema, table);
                
                DatabaseMetaData metaData = this.getConnection().getMetaData();
                ResultSet pkResults = null;
                boolean foundPKs = false;
                String effectiveSchema = schema;
                String effectiveTable = table;
                
                // Try as-is first
                try {
                    pkResults = metaData.getPrimaryKeys(null, schema, table);
                    if (pkResults != null && pkResults.next()) {
                        foundPKs = true;
                        LOG.debug("Found PKs using original case: schema='{}', table='{}'", schema, table);
                    }
                    if (pkResults != null) pkResults.close();
                } catch (SQLException e) {
                    LOG.debug("Error querying PKs with original case: {}", e.getMessage());
                    if (pkResults != null) try { pkResults.close(); } catch (SQLException ex) { /* ignore */ }
                }
                
                // Try uppercase if not found
                if (!foundPKs) {
                    String tableUpper = table != null ? table.toUpperCase() : null;
                    String schemaUpper = schema != null ? schema.toUpperCase() : null;
                    try {
                        pkResults = metaData.getPrimaryKeys(null, schemaUpper, tableUpper);
                        if (pkResults != null && pkResults.next()) {
                            foundPKs = true;
                            effectiveSchema = schemaUpper;
                            effectiveTable = tableUpper;
                            LOG.debug("Found PKs using uppercase: schema='{}', table='{}'", schemaUpper, tableUpper);
                        }
                        if (pkResults != null) pkResults.close();
                    } catch (SQLException e) {
                        LOG.debug("Error querying PKs with uppercase: {}", e.getMessage());
                        if (pkResults != null) try { pkResults.close(); } catch (SQLException ex) { /* ignore */ }
                    }
                }
                
                // Try lowercase if still not found
                if (!foundPKs) {
                    String tableLower = table != null ? table.toLowerCase() : null;
                    String schemaLower = schema != null ? schema.toLowerCase() : null;
                    try {
                        pkResults = metaData.getPrimaryKeys(null, schemaLower, tableLower);
                        if (pkResults != null && pkResults.next()) {
                            foundPKs = true;
                            effectiveSchema = schemaLower;
                            effectiveTable = tableLower;
                            LOG.debug("Found PKs using lowercase: schema='{}', table='{}'", schemaLower, tableLower);
                        }
                        if (pkResults != null) pkResults.close();
                    } catch (SQLException e) {
                        LOG.debug("Error querying PKs with lowercase: {}", e.getMessage());
                        if (pkResults != null) try { pkResults.close(); } catch (SQLException ex) { /* ignore */ }
                    }
                }
                
                // If found, re-query with the effective case variant and process results
                if (foundPKs) {
                    pkResults = metaData.getPrimaryKeys(null, effectiveSchema, effectiveTable);
                    try {
                        ArrayList<String> pkList = new ArrayList<>();
                        while (pkResults.next()) {
                            pkList.add(pkResults.getString("COLUMN_NAME"));
                        }
                        
                        if (!pkList.isEmpty()) {
                            // Sort by KEY_SEQ - re-query with ordering
                            pkResults.close();
                            pkResults = metaData.getPrimaryKeys(null, effectiveSchema, effectiveTable);
                            pkList.clear();
                            while (pkResults.next()) {
                                int keySeq = pkResults.getInt("KEY_SEQ");
                                String colName = pkResults.getString("COLUMN_NAME");
                                // Ensure we have the right size
                                while (pkList.size() < keySeq) {
                                    pkList.add(null);
                                }
                                pkList.set(keySeq - 1, colName);
                            }
                            
                            // Filter primary keys to only include columns in the probed column list
                            List<String> filteredPkList = new ArrayList<>();
                            for (String pk : pkList) {
                                if (pk != null && columnNames.contains(pk.toUpperCase())) {
                                    filteredPkList.add(pk);
                                } else if (pk != null) {
                                    LOG.warn("Primary key column '{}' is not in the source-columns list and will be excluded", pk);
                                }
                            }
                            
                            if (!filteredPkList.isEmpty()) {
                                String[] pks = filteredPkList.toArray(new String[0]);
                                options.setSourcePrimaryKeys(pks);
                                LOG.info("Captured {} primary key columns from source (after filtering)", pks.length);
                            } else {
                                LOG.warn("No primary keys remain after filtering by source-columns");
                                if (options.getMode() != null && options.getMode().toLowerCase().contains("incremental")) {
                                    LOG.warn("Incremental mode requires primary keys, but all PK columns were excluded by --source-columns");
                                }
                            }
                        } else {
                            LOG.warn("No primary keys found on source table after trying multiple case variants");
                        }
                    } finally {
                        if (pkResults != null) pkResults.close();
                    }
                } else {
                    LOG.warn("No primary keys found on source table after trying multiple case variants");
                }
            } else {
                LOG.info("Source is a custom query, no primary keys available");
            }
            
        } finally {
            if (rs != null) try { rs.close(); } catch (SQLException e) { /* ignore */ }
            if (stmt != null) try { stmt.close(); } catch (SQLException e) { /* ignore */ }
        }
    }

    /**
     * Checks if the sink table exists using DatabaseMetaData.getTables()
     * with three-case-variant retry (as-is, UPPERCASE, lowercase).
     */
    protected boolean sinkTableExists() throws SQLException {
        String tableName = getSinkTableName();
        String table = getTableNameFromQualifiedTableName(tableName);
        String schema = getSchemaFromQualifiedTableName(tableName);
        
        DatabaseMetaData metaData = this.getConnection().getMetaData();
        
        // Try as-is
        ResultSet rs = metaData.getTables(null, schema, table, new String[]{"TABLE"});
        try {
            if (rs.next()) {
                LOG.debug("Table exists: schema={}, table={}", schema, table);
                return true;
            }
        } finally {
            rs.close();
        }
        
        // Try UPPERCASE
        String tableUpper = table != null ? table.toUpperCase() : null;
        String schemaUpper = schema != null ? schema.toUpperCase() : null;
        rs = metaData.getTables(null, schemaUpper, tableUpper, new String[]{"TABLE"});
        try {
            if (rs.next()) {
                LOG.debug("Table exists (uppercase): schema={}, table={}", schemaUpper, tableUpper);
                return true;
            }
        } finally {
            rs.close();
        }
        
        // Try lowercase
        String tableLower = table != null ? table.toLowerCase() : null;
        String schemaLower = schema != null ? schema.toLowerCase() : null;
        rs = metaData.getTables(null, schemaLower, tableLower, new String[]{"TABLE"});
        try {
            if (rs.next()) {
                LOG.debug("Table exists (lowercase): schema={}, table={}", schemaLower, tableLower);
                return true;
            }
        } finally {
            rs.close();
        }
        
        LOG.debug("Table does not exist: {}", tableName);
        return false;
    }

    /**
     * Abstract method to map JDBC type codes to database-native DDL type fragments.
     * Each manager must implement this with its database-specific type mappings.
     */
    protected abstract String mapJdbcTypeToNativeDDL(String columnName, int jdbcType, int precision, int scale);

    /**
     * Generates CREATE TABLE DDL from column descriptors and primary keys.
     */
    protected String generateCreateTableDDL(String tableName, List<ColumnDescriptor> columns, String[] primaryKeys) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(escapeTableName(tableName)).append(" (\n");
        
        for (int i = 0; i < columns.size(); i++) {
            ColumnDescriptor col = columns.get(i);
            ddl.append("  ").append(escapeColName(col.getColumnName())).append(" ");
            ddl.append(mapJdbcTypeToNativeDDL(col.getColumnName(), col.getJdbcType(), col.getPrecision(), col.getScale()));
            
            // Add NOT NULL if the column is not nullable
            if (col.getNullable() == ResultSetMetaData.columnNoNulls) {
                ddl.append(" NOT NULL");
            }
            
            if (i < columns.size() - 1 || (primaryKeys != null && primaryKeys.length > 0)) {
                ddl.append(",");
            }
            ddl.append("\n");
        }
        
        // Add PRIMARY KEY constraint if present
        if (primaryKeys != null && primaryKeys.length > 0) {
            ddl.append("  PRIMARY KEY (");
            for (int i = 0; i < primaryKeys.length; i++) {
                ddl.append(escapeColName(primaryKeys[i]));
                if (i < primaryKeys.length - 1) {
                    ddl.append(", ");
                }
            }
            ddl.append(")\n");
        }
        
        ddl.append(")");
        return ddl.toString();
    }

    /**
     * Auto-creates the sink table if it doesn't exist, using source metadata.
     */
    protected void autoCreateSinkTable() throws SQLException {
        String tableName = getSinkTableName();
        
        if (sinkTableExists()) {
            LOG.info("Sink table {} already exists, skipping auto-create", tableName);
            return;
        }
        
        LOG.info("Sink table {} does not exist, creating it...", tableName);
        
        List<ColumnDescriptor> sourceColumns = options.getSourceColumnDescriptors();
        String[] sourcePKs = options.getSourcePrimaryKeys();
        
        if (sourceColumns == null || sourceColumns.isEmpty()) {
            throw new IllegalStateException("Source column descriptors not available. Probe may have failed.");
        }
        
        // Warn if incremental mode but no PKs
        if (options.getMode().equals(ReplicationMode.INCREMENTAL.getModeText())) {
            if (sourcePKs == null || sourcePKs.length == 0) {
                LOG.warn("Incremental mode requires primary keys, but none are available. " +
                        "The table will be created without a PRIMARY KEY constraint, and replication will likely fail during merge.");
            }
        }
        
        // Use qualified table name for CREATE TABLE to ensure proper schema placement
        String qualifiedTableName = getQualifiedSinkTableName();
        String ddl = generateCreateTableDDL(qualifiedTableName, sourceColumns, sourcePKs);
        LOG.info("Executing DDL:\n{}", ddl);
        
        Statement stmt = null;
        try {
            stmt = this.getConnection().createStatement();
            stmt.execute(ddl);
            this.getConnection().commit();
            sinkTableJustCreated = true;
            LOG.info("Successfully created sink table {}", qualifiedTableName);
        } finally {
            if (stmt != null) try { stmt.close(); } catch (SQLException e) { /* ignore */ }
        }
    }

}

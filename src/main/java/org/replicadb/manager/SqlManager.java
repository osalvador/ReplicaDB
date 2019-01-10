package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;


/**
 * ConnManager implementation for generic SQL-compliant database.
 * This is an abstract class; it requires a database-specific
 * ConnManager implementation to actually create the connection.
 */
public abstract class SqlManager extends ConnManager {

    private static final Logger LOG = LogManager.getLogger(SqlManager.class.getName());

    protected static final int DEFAULT_FETCH_SIZE = 5000;

    protected Connection connection;
    protected DataSourceType dsType;

    private Statement lastStatement;

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

        PreparedStatement statement = this.getConnection().prepareStatement(stmt, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        if (fetchSize != null) {
            LOG.debug(Thread.currentThread().getName() + ": Using fetchSize for next query: " + fetchSize);
            statement.setFetchSize(fetchSize);
        }
        this.lastStatement = statement;
        if (null != args) {
            for (int i = 0; i < args.length; i++) {
                statement.setObject(i + 1, args[i]);
            }
        }

        LOG.info(Thread.currentThread().getName() + ": Executing SQL statement: " + stmt);

        StringBuilder sb = new StringBuilder();
        for (Object o : args) {
            sb.append(o.toString())
                    .append(", ");
        }
        LOG.info(Thread.currentThread().getName() + ": With args: " + sb.toString());

        return statement.executeQuery();
    }

    /**
     * Executes an arbitrary SQL Statement.
     *
     * @param stmt The SQL statement to execute
     * @return A ResultSet encapsulating the results or null on error
     */
    protected ResultSet execute(String stmt, Object... args) throws SQLException {
        return execute(stmt, /*options.getFetchSize()*/DEFAULT_FETCH_SIZE, args);
        // TODO: FetchSize
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

        Connection connection;
        String driverClass = getDriverClass();

        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException("Could not load db driver class: "
                    + driverClass);
        }

        String username = options.getSourceUser();
        String password = options.getSourcePassword();
        String connectString = options.getSourceConnect();

        Properties connectionParams = options.getSourceConnectionParams();
        if (connectionParams != null && connectionParams.size() > 0) {
            LOG.debug("User specified connection params. Using properties specific API for making connection.");

            Properties props = new Properties();
            if (username != null) {
                props.put("user", username);
            }

            if (password != null) {
                props.put("password", password);
            }

            props.putAll(connectionParams);
            connection = DriverManager.getConnection(connectString, props);
        } else {
            LOG.debug("No connection parameters specified. Using regular API for making connection.");
            if (username == null) {
                connection = DriverManager.getConnection(connectString);
            } else {
                connection = DriverManager.getConnection(connectString, username, password);
            }
        }

        // We only use this for metadata queries. Loosest semantics are okay.
        //connection.setTransactionIsolation(getMetadataIsolationLevel());
        connection.setAutoCommit(false);

        return connection;
    }

//    /**
//     * @return the transaction isolation level to use for metadata queries
//     * (queries executed by the ConnManager itself).
//     */
 /*   protected int getMetadataIsolationLevel() {
        return options.getMetadataTransactionIsolationLevel();
    }
*/


    protected Connection makeSinkConnection() throws SQLException {

        Connection connection;
        String driverClass = getDriverClass();

        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException("Could not load db driver class: "
                    + driverClass);
        }

        String username = options.getSinkUser();
        String password = options.getSinkPassword();
        String connectString = options.getSinkConnect();

        Properties connectionParams = options.getSinkConnectionParams();
        if (connectionParams != null && connectionParams.size() > 0) {
            LOG.debug("User specified connection params. Using properties specific API for making connection.");

            Properties props = new Properties();
            if (username != null) {
                props.put("user", username);
            }

            if (password != null) {
                props.put("password", password);
            }

            props.putAll(connectionParams);
            connection = DriverManager.getConnection(connectString, props);
        } else {
            LOG.debug("No connection parameters specified. Using regular API for making connection.");
            if (username == null) {
                connection = DriverManager.getConnection(connectString);
            } else {
                connection = DriverManager.getConnection(connectString, username, password);
            }
        }

        // We only use this for metadata queries. Loosest semantics are okay.
        //connection.setTransactionIsolation(getMetadataIsolationLevel());
        connection.setAutoCommit(false);

        return connection;
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
            LOG.debug("Getting PKs for schema: " + schema + " and table: " + table + ". Not found.");

            // Trying with uppercase
            table = getTableNameFromQualifiedTableName(tableName).toUpperCase();
            schema = getSchemaFromQualifiedTableName(tableName).toUpperCase();

            pks = getPrimaryKeys(table, schema);

            if (null == pks) {
                LOG.debug("Getting PKs for schema: " + schema + " and table: " + table + ". Not found.");

                // Trying with lowercase
                table = getTableNameFromQualifiedTableName(tableName).toLowerCase();
                schema = getSchemaFromQualifiedTableName(tableName).toLowerCase();

                pks = getPrimaryKeys(table, schema);
                if (null == pks) {
                    LOG.debug("Getting PKs for schema: " + schema + " and table: " + table + ". Not found.");
                    return null;
                }
            }
        }

        LOG.debug("Getting PKs for schema: " + schema + " and table: " + table + ". Found.");

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
        String tableName;
        // Get table name
        if (options.getMode().equals(ReplicationMode.INCREMENTAL.getModeText())) {
            tableName = getQualifiedStagingTableName();
        } else {
            tableName = getSinkTableName();
        }
        String sql = "TRUNCATE TABLE " + tableName;
        LOG.debug("Truncating sink table with this command: " + sql);
        Statement statement = this.getConnection().createStatement();
        statement.executeUpdate(sql);
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
    protected abstract void createStagingTable() throws SQLException;


    /**
     * Merge staging table and sink table.
     *
     * @throws SQLException
     */
    protected abstract void mergeStagingTable() throws SQLException;

    /**
     * Drop staging table.
     *
     * @throws SQLException
     */
    public void dropStagingTable() throws SQLException {
        Statement statement = this.getConnection().createStatement();
        String sql = "DROP TABLE " + getQualifiedStagingTableName();
        LOG.info("Dropping staging table with this command: " + sql);

        statement.executeUpdate(sql);
        statement.close();
        this.getConnection().commit();
    }

    ;

    @Override
    public void preSinkTasks() throws SQLException {

        // On INCREMENTAL mode
        if (options.getMode().equals(ReplicationMode.INCREMENTAL.getModeText())) {

            // Only create staging table if it is not defined by the user
            if (options.getSinkStagingTable() == null || options.getSinkStagingTable().isEmpty()) {

                // If the staging parameters have not been defined then the table is created in the public schema
                if (options.getSinkStagingSchema() == null || options.getSinkStagingSchema().isEmpty()) {
                    // TODO?: This is only valid for PostgreSQL
                    LOG.warn("No staging schema is defined, setting it as PUBLIC");
                    options.setSinkStagingSchema("public");
                }
                this.createStagingTable();
            }
        }

        // Truncate sink table if it is enabled
        if (!options.isSinkDisableTruncate()) {
            this.truncateTable();
        }

    }

    @Override
    public void postSinkTasks() throws SQLException {
        // On INCREMENTAL mode
        if (options.getMode().equals(ReplicationMode.INCREMENTAL.getModeText())) {
            // Merge Data
            this.mergeStagingTable();
        }
    }

    @Override
    public void cleanUp() throws SQLException {
        // Only drop staging table if it was created automatically
        if (options.getSinkStagingTable() == null || options.getSinkStagingTable().isEmpty()) {
            // Drop staging table
            this.dropStagingTable();
        }

    }
}
package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;

import java.io.PrintWriter;
import java.sql.*;
import java.util.Properties;


/**
 * ConnManager implementation for generic SQL-compliant database.
 * This is an abstract class; it requires a database-specific
 * ConnManager implementation to actually create the connection.
 */
public abstract class SqlManager extends ConnManager {

    private static final Logger LOG = LogManager.getLogger(SqlManager.class.getName());

    protected static final int DEFAULT_FETCH_SIZE = 5000;

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
    public abstract Connection getConnection() throws SQLException;

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
        try {
            this.getConnection().close();
        } catch (Exception e) {
            LOG.error(e);
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

//        Properties connectionParams = options.getConnectionParams();
//        if (connectionParams != null && connectionParams.size() > 0) {
//            LOG.debug("User specified connection params. "
//                    + "Using properties specific API for making connection.");
//
//            Properties props = new Properties();
//            if (username != null) {
//                props.put("user", username);
//            }
//
//            if (password != null) {
//                props.put("", password);
//            }
//
//            props.putAll(connectionParams);
//            connection = DriverManager.getConnection(connectString, props);
//        } else {

        // TODO: Especific connection params for Database Vendor

        Properties props = new Properties();
        // Enabling Network Compression in Java
        props.setProperty("oracle.net.networkCompression", "on");
        // Optional configuration for setting the client compression threshold.
        props.setProperty("oracle.net.networkCompressionThreshold", "1024");
        props.put("user", username);
        props.put("password", password);
        connection = DriverManager.getConnection(connectString, props);

        /*
        LOG.debug(Thread.currentThread().getName() + ": No connection parameters specified. "
                + "Using regular API for making connection.");
        if (username == null) {
            connection = DriverManager.getConnection(connectString, props);
        } else {
            connection = DriverManager.getConnection(connectString, username, password);
        }*/
//        }

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

//        Properties connectionParams = options.getConnectionParams();
//        if (connectionParams != null && connectionParams.size() > 0) {
//            LOG.debug("User specified connection params. "
//                    + "Using properties specific API for making connection.");
//
//            Properties props = new Properties();
//            if (username != null) {
//                props.put("user", username);
//            }
//
//            if (password != null) {
//                props.put("password", password);
//            }
//
//            props.putAll(connectionParams);
//            connection = DriverManager.getConnection(connectString, props);
//        } else {
        LOG.debug("No connection parameters specified. "
                + "Using regular API for making connection.");
        if (username == null) {
            connection = DriverManager.getConnection(connectString);
        } else {
            connection = DriverManager.getConnection(connectString, username, password);
        }
//        }

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
                /*LoggingUtils.logAll(LOG, "Exception closing executed Statement: "
                        + e, e);*/
                LOG.error("Exception closing executed Statement: " + e, e);
            }

            this.lastStatement = null;
        }
    }

    @Override
    public void truncateTable() throws SQLException {
        // Truncate Sink table
        Statement statement = this.getConnection().createStatement();
        statement.executeUpdate("TRUNCATE TABLE " + options.getSinkTable());
        statement.close();
        this.getConnection().commit();
        this.getConnection().close();


    }
}
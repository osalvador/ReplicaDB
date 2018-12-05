package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;

import java.io.PrintWriter;
import java.sql.*;


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
     * Offers the ConnManager an opportunity to validate that the
     * options specified in the ImportJobContext are valid.
     * @throws ImportException if the import is misconfigured.
     */
   /* protected void checkTableImportOptions(
            org.apache.sqoop.manager.ImportJobContext context)
            throws IOException, ImportException {
        String tableName = context.getTableName();
        SqoopOptions opts = context.getOptions();

        // Default implementation: check that the split column is set
        // correctly.
        String splitCol = getSplitColumn(opts, tableName);
        if (null == splitCol && opts.getNumMappers() > 1) {
            if (!opts.getAutoResetToOneMapper()) {
                // Can't infer a primary key.
                throw new ImportException("No primary key could be found for table "
                        + tableName + ". Please specify one with --split-by or perform "
                        + "a sequential import with '-m 1'.");
            } else {
                LOG.warn("Split by column not provided or can't be inferred.  Resetting to one mapper");
                opts.setNumMappers(1);
            }
        }
    }*/


    /**
     * Default implementation of importQuery() is to launch a MapReduce job
     * via DataDrivenImportJob to read the table with DataDrivenDBInputFormat,
     * using its free-form query importer.
     */
//    public void importQuery(org.apache.sqoop.manager.ImportJobContext context)
//            throws IOException, ImportException {
//        String jarFile = context.getJarFile();
//        SqoopOptions opts = context.getOptions();
//
//        context.setConnManager(this);
//
//        ImportJobBase importer;
//        if (opts.getHBaseTable() != null) {
//            // Import to HBase.
//            if (!HBaseUtil.isHBaseJarPresent()) {
//                throw new ImportException("HBase jars are not present in classpath,"
//                        + " cannot import to HBase!");
//            }
//            if (!opts.isBulkLoadEnabled()){
//                importer = new HBaseImportJob(opts, context);
//            } else {
//                importer = new HBaseBulkImportJob(opts, context);
//            }
//        } else if (opts.getAccumuloTable() != null) {
//            // Import to Accumulo.
//            if (!AccumuloUtil.isAccumuloJarPresent()) {
//                throw new ImportException("Accumulo jars are not present in classpath,"
//                        + " cannot import to Accumulo!");
//            }
//            importer = new AccumuloImportJob(opts, context);
//        } else {
//            // Import to HDFS.
//            importer = new DataDrivenImportJob(opts, context.getInputFormat(),
//                    context, getParquetJobConfigurator().createParquetImportJobConfigurator());
//        }
//
//        String splitCol = getSplitColumn(opts, null);
//        if (splitCol == null) {
//            String boundaryQuery = opts.getBoundaryQuery();
//            if (opts.getNumMappers() > 1) {
//                // Can't infer a primary key.
//                throw new ImportException("A split-by column must be specified for "
//                        + "parallel free-form query imports. Please specify one with "
//                        + "--split-by or perform a sequential import with '-m 1'.");
//            } else if (boundaryQuery != null && !boundaryQuery.isEmpty()) {
//                // Query import with boundary query and no split column specified
//                throw new ImportException("Using a boundary query for a query based "
//                        + "import requires specifying the split by column as well. Please "
//                        + "specify a column name using --split-by and try again.");
//            }
//        }
//
//        importer.runImport(null, jarFile, splitCol, opts.getConf());
//    }

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
        LOG.info(Thread.currentThread().getName()+ ": With args: " + sb.toString());

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
        try { this.getConnection().close(); } catch (Exception e) { LOG.error(e); }
    }

//    /**
//     * Prints the contents of a ResultSet to the specified PrintWriter.
//     * The ResultSet is closed at the end of this method.
//     *
//     * @param results the ResultSet to print.
//     * @param pw      the location to print the data to.
//     */
//    protected void formatAndPrintResultSet(ResultSet results, PrintWriter pw) {
//        try {
//            try {
//                int cols = results.getMetaData().getColumnCount();
//                pw.println("Got " + cols + " columns back");
//                if (cols > 0) {
//                    ResultSetMetaData rsmd = results.getMetaData();
//                    String schema = rsmd.getSchemaName(1);
//                    String table = rsmd.getTableName(1);
//                    if (null != schema) {
//                        pw.println("Schema: " + schema);
//                    }
//
//                    if (null != table) {
//                        pw.println("Table: " + table);
//                    }
//                }
//            } catch (SQLException sqlE) {
//                LOG.error( "SQLException reading result metadata: ", sqlE);
//            }
//
//            try {
//                new ResultSetPrinter().printResultSet(pw, results);
//            } catch (IOException ioe) {
//                LOG.error("IOException writing results: " + ioe.toString());
//                return;
//            }
//        } finally {
//            try {
//                results.close();
//                getConnection().commit();
//            } catch (SQLException sqlE) {
//                LoggingUtils.logAll(LOG, "SQLException closing ResultSet: "
//                        + sqlE.toString(), sqlE);
//            }
//
//            release();
//        }
//    }

//    /**
//     * Poor man's SQL query interface; used for debugging.
//     *
//     * @param s the SQL statement to execute.
//     */
//    public void execAndPrint(String s) {
//        ResultSet results = null;
//        try {
//            results = execute(s);
//        } catch (SQLException sqlE) {
//            LoggingUtils.logAll(LOG, "Error executing statement: ", sqlE);
//            release();
//            return;
//        }
//
//        PrintWriter pw = new PrintWriter(System.out, true);
//        try {
//            formatAndPrintResultSet(results, pw);
//        } finally {
//            pw.close();
//        }
//    }


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
//                props.put("password", password);
//            }
//
//            props.putAll(connectionParams);
//            connection = DriverManager.getConnection(connectString, props);
//        } else {
        LOG.debug(Thread.currentThread().getName() + ": No connection parameters specified. "
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


//    /**
//     * @return a SQL query to retrieve the current timestamp from the db.
//     */
//    protected String getCurTimestampQuery() {
//        return "SELECT CURRENT_TIMESTAMP()";
//    }
//
//    @Override
//    /**
//     * {@inheritDoc}
//     */
//    public Timestamp getCurrentDbTimestamp() {
//        release(); // Release any previous ResultSet.
//
//        Statement s = null;
//        ResultSet rs = null;
//        try {
//            Connection c = getConnection();
//            s = c.createStatement();
//            rs = s.executeQuery(getCurTimestampQuery());
//            if (rs == null || !rs.next()) {
//                return null; // empty ResultSet.
//            }
//
//            return rs.getTimestamp(1);
//        } catch (SQLException sqlE) {
//            LoggingUtils.logAll(LOG, "SQL exception accessing current timestamp: "
//                    + sqlE, sqlE);
//            return null;
//        } finally {
//            try {
//                if (null != rs) {
//                    rs.close();
//                }
//            } catch (SQLException sqlE) {
//                LoggingUtils.logAll(LOG, "SQL Exception closing resultset: "
//                        + sqlE, sqlE);
//            }
//
//            try {
//                if (null != s) {
//                    s.close();
//                }
//            } catch (SQLException sqlE) {
//                LoggingUtils.logAll(LOG, "SQL Exception closing statement: "
//                        + sqlE, sqlE);
//            }
//        }
//    }
//

}
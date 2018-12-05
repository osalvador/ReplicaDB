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

//    /**
//     * Return a list of column names in a table in the order returned by the db.
//     */
//    public abstract String[] getColumnNames(String tableName);
//
//    /**
//     * Return a list of column names in query in the order returned by the db.
//     */
//    public String[] getColumnNamesForQuery(String query) {
//        LOG.error("This database does not support free-form query column names.");
//        return null;
//    }

//
//    /**
//     * Resolve a database-specific type to the Java type that should contain it.
//     * @param sqlType     sql type
//     * @return the name of a Java type to hold the sql datatype, or null if none.
//     */
//    public String toJavaType(int sqlType) {
//        // Mappings taken from:
//        // http://java.sun.com/j2se/1.3/docs/guide/jdbc/getstart/mapping.html
//        if (sqlType == Types.INTEGER) {
//            return "Integer";
//        } else if (sqlType == Types.VARCHAR) {
//            return "String";
//        } else if (sqlType == Types.CHAR) {
//            return "String";
//        } else if (sqlType == Types.LONGVARCHAR) {
//            return "String";
//        } else if (sqlType == Types.NVARCHAR) {
//            return "String";
//        } else if (sqlType == Types.NCHAR) {
//            return "String";
//        } else if (sqlType == Types.LONGNVARCHAR) {
//            return "String";
//        } else if (sqlType == Types.NUMERIC) {
//            return "java.math.BigDecimal";
//        } else if (sqlType == Types.DECIMAL) {
//            return "java.math.BigDecimal";
//        } else if (sqlType == Types.BIT) {
//            return "Boolean";
//        } else if (sqlType == Types.BOOLEAN) {
//            return "Boolean";
//        } else if (sqlType == Types.TINYINT) {
//            return "Integer";
//        } else if (sqlType == Types.SMALLINT) {
//            return "Integer";
//        } else if (sqlType == Types.BIGINT) {
//            return "Long";
//        } else if (sqlType == Types.REAL) {
//            return "Float";
//        } else if (sqlType == Types.FLOAT) {
//            return "Double";
//        } else if (sqlType == Types.DOUBLE) {
//            return "Double";
//        } else if (sqlType == Types.DATE) {
//            return "java.sql.Date";
//        } else if (sqlType == Types.TIME) {
//            return "java.sql.Time";
//        } else if (sqlType == Types.TIMESTAMP) {
//            return "java.sql.Timestamp";
//        } else if (sqlType == Types.BINARY
//                || sqlType == Types.VARBINARY) {
//            return BytesWritable.class.getName();
//        } else if (sqlType == Types.CLOB) {
//            return ClobRef.class.getName();
//        } else if (sqlType == Types.BLOB
//                || sqlType == Types.LONGVARBINARY) {
//            return BlobRef.class.getName();
//        } else {
//            // TODO(aaron): Support DISTINCT, ARRAY, STRUCT, REF, JAVA_OBJECT.
//            // Return null indicating database-specific manager should return a
//            // java data type if it can find one for any nonstandard type.
//            return null;
//        }
//    }


//    /**
//     * Return an unordered mapping from colname to sqltype for
//     * all columns in a table.
//     * <p>
//     * The Integer type id is a constant from java.sql.Types
//     */
//    public abstract Map<String, Integer> getColumnTypes(String tableName);
//
//
//    /**
//     * Return an unordered mapping from colname to sqltype for
//     * all columns in a table or query.
//     * <p>
//     * The Integer type id is a constant from java.sql.Types
//     *
//     * @param tableName the name of the table
//     * @param sqlQuery  the SQL query to use if tableName is null
//     */
//    public Map<String, Integer> getColumnTypes(String tableName,
//                                               String sqlQuery) throws IOException {
//        Map<String, Integer> columnTypes;
//        if (null != tableName) {
//            // We're generating a class based on a table import.
//            columnTypes = getColumnTypes(tableName);
//        } else {
//            // This is based on an arbitrary query.
//            String query = sqlQuery;
//            if (!query.contains(SUBSTITUTE_TOKEN)) {
//                throw new IOException("Query [" + query + "] must contain '"
//                        + SUBSTITUTE_TOKEN + "' in WHERE clause.");
//            }
//
//            columnTypes = getColumnTypesForQuery(query);
//        }
//        return columnTypes;
//    }

//    /**
//     * Return an unordered mapping from colname to sqltype, precision and scale
//     * for all columns in a table.
//     * <p>
//     * Precision and scale are as defined in the resultset metadata,
//     * <p>
//     * The Integer type id is a constant from java.sql.Types
//     */
//    public Map<String, List<Integer>> getColumnInfo(String tableName) {
//        throw new UnsupportedOperationException(
//                "Get column information is not supported by this manager");
//    }
//
//    /**
//     * Return an unordered mapping from colname to sqltype, precision and scale
//     * for all columns in a query.
//     * <p>
//     * Precision and scale are as defined in the resultset metadata,
//     * <p>
//     * The Integer type id is a constant from java.sql.Types
//     */
//    public Map<String, List<Integer>> getColumnInfoForQuery(String query) {
//        LOG.error("This database does not support free-form query column info.");
//        return null;
//    }
//
//    /**
//     * Return an unordered mapping from colname to sqltype, precision and scale
//     * for all columns in a table or query.
//     * <p>
//     * The Integer type id is a constant from java.sql.Types
//     * Precision and scale are as defined in the resultset metadata,
//     *
//     * @param tableName the name of the table
//     * @param sqlQuery  the SQL query to use if tableName is null
//     */
//    public Map<String, List<Integer>> getColumnInfo(String tableName,
//                                                    String sqlQuery) throws IOException {
//        Map<String, List<Integer>> colInfo;
//        if (null != tableName) {
//            // We're generating a class based on a table import.
//            colInfo = getColumnInfo(tableName);
//        } else {
//            // This is based on an arbitrary query.
//            String query = sqlQuery;
//            if (!query.contains(SUBSTITUTE_TOKEN)) {
//                throw new IOException("Query [" + query + "] must contain '"
//                        + SUBSTITUTE_TOKEN + "' in WHERE clause.");
//            }
//            colInfo = getColumnInfoForQuery(query);
//        }
//        return colInfo;
//    }


//    /**
//     * This method allows various connection managers to indicate if they support
//     * staging data for export jobs. The managers that do support this must
//     * override this method and return <tt>true</tt>.
//     *
//     * @return true if the connection manager supports staging data for export
//     * use-case.
//     */
//    public boolean supportsStagingForExport() {
//        return false;
//    }

//    /**
//     * Returns the count of all rows that exist in the given table.
//     *
//     * @param tableName the name of the table which will be queried.
//     * @return the number of rows present in the given table.
//     * @throws SQLException                  if an error occurs during execution
//     * @throws UnsupportedOperationException if the connection manager does not
//     *                                       support this operation.
//     */
//    public long getTableRowCount(String tableName) throws SQLException {
//        throw new UnsupportedOperationException();
//    }

//    /**
//     * Deletes all records from the given table. This method is invoked during
//     * and export run when a staging table is specified. The staging table is
//     * cleaned before the commencement of export job, and after the data has
//     * been moved to the target table.
//     *
//     * @param tableName name of the table which will be emptied.
//     * @throws SQLException                  if an error occurs during execution
//     * @throws UnsupportedOperationException if the connection manager does not
//     *                                       support this operation.
//     */
//    public void deleteAllRecords(String tableName) throws SQLException {
//        throw new UnsupportedOperationException();
//    }
//
//    /**
//     * Migrates all records from the given <tt>fromTable</tt> to the target
//     * <tt>toTable</tt>. This method is invoked as a last step of an export
//     * run where the staging is used to collect data before pushing it into the
//     * target table.
//     *
//     * @param fromTable the name of the staging table
//     * @param toTable   the name of the target table
//     * @throws SQLException                  if an error occurs during execution
//     * @throws UnsupportedOperationException if the connection manager does not
//     *                                       support this operation.
//     */
//    public void migrateData(String fromTable, String toTable)
//            throws SQLException {
//        throw new UnsupportedOperationException();
//    }


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
     * Configure database output column ordering explicitly for code generator.
     * The code generator should generate the DBWritable.write(PreparedStatement)
     * method with columns exporting in this order.
     */
  /*  public void configureDbOutputColumns(ToolOptions options) {
        // We're in update mode. We need to explicitly set the database output
        // column ordering in the codeGenerator.  The UpdateKeyCol must come
        // last, because the UPDATE-based OutputFormat will generate the SET
        // clause followed by the WHERE clause, and the SqoopRecord needs to
        // serialize to this layout.

        // Check if user specified --columns parameter
        Set<String> columns = null;
        if (options.getColumns() != null && options.getColumns().length > 0) {
            // If so, put all column in uppercase form into our help set
            columns = new HashSet<String>();
            for(String c : options.getColumns()) {
                columns.add(c.toUpperCase());
            }
        }

        Set<String> updateKeys = new LinkedHashSet<String>();
        Set<String> updateKeysUppercase = new HashSet<String>();
        String updateKeyValue = options.getUpdateKeyCol();
        StringTokenizer stok = new StringTokenizer(updateKeyValue, ",");
        while (stok.hasMoreTokens()) {
            String nextUpdateColumn = stok.nextToken().trim();
            if (nextUpdateColumn.length() > 0) {
                String upperCase = nextUpdateColumn.toUpperCase();

                // We must make sure that --columns is super set of --update-key
                if (columns != null && !columns.contains(upperCase)) {
                    throw new RuntimeException("You must specify all columns from "
                            + "--update-key parameter in --columns parameter.");
                }

                updateKeys.add(nextUpdateColumn);
                updateKeysUppercase.add(upperCase);
            } else {
                throw new RuntimeException("Invalid update key column value specified"
                        + ": '" + updateKeyValue + "'");
            }
        }
        String [] allColNames = getColumnNames(options.getTableName());
        List<String> dbOutCols = new ArrayList<String>();
        for (String col : allColNames) {
            if (!updateKeysUppercase.contains(col.toUpperCase())) {
                // Skip columns that were not explicitly stated on command line
                if (columns != null && !columns.contains(col.toUpperCase())) {
                    continue;
                }

                dbOutCols.add(col); // add non-key columns to the output order list.
            }
        }

        // Then add the update key column last.
        dbOutCols.addAll(updateKeys);
        options.setDbOutputColumns(dbOutCols.toArray(
                new String[dbOutCols.size()]));
    }*/

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



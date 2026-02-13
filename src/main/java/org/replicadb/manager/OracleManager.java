package org.replicadb.manager;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.util.BandwidthThrottling;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Oracle-specific database manager with optimizations for Oracle Database.
 * 
 * <p><b>NULL Handling:</b> This class correctly preserves NULL values for all data types
 * during replication by checking {@code ResultSet.wasNull()} after calling primitive getters
 * (getInt, getDouble, getFloat, etc.) and calling {@code PreparedStatement.setNull()} when
 * NULL is detected. This prevents silent conversion of NULL to default values like 0, 0.0,
 * false, or epoch dates.</p>
 * 
 * <p>Fixes Issue #51: NULL INTEGER values from Denodo were incorrectly converted to 0 in Oracle
 * sink tables. The fix applies to all primitive and temporal types: INTEGER, BIGINT, DOUBLE,
 * FLOAT, DATE, TIMESTAMP, VARCHAR, and BINARY.</p>
 * 
 * @see <a href="https://github.com/osalvador/ReplicaDB/issues/51">Issue #51</a>
 */
public class OracleManager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(OracleManager.class.getName());
    private Long oracleSourceScn = null;  // SCN for flashback query consistency

    public OracleManager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
    }

    @Override
    public String getDriverClass() {
        return JdbcDrivers.ORACLE.getDriverClass();
    }


    @Override
    public ResultSet readTable(String tableName, String[] columns, int nThread) throws SQLException {

        // If table name parameter is null get it from options
        tableName = tableName == null ? this.options.getSourceTable() : tableName;

        // If columns parameter is null, get it from options
        String allColumns = this.options.getSourceColumns() == null ? "*" : this.options.getSourceColumns();

        String sqlCmd;

        // Read table with source-query option specified
        if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
            oracleAlterSession(false);
            if (options.getJobs() == 1)
                sqlCmd = "SELECT  * FROM (" +
                        options.getSourceQuery() + ") where 0 = ?";
            else
                throw new UnsupportedOperationException("ReplicaDB on Oracle still not support custom query parallel process. Use properties instead: source.table, source.columns and source.where ");
        }
        // Read table with source-where option specified
        else if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
            oracleAlterSession(false);
            sqlCmd = "SELECT  " +
                    allColumns +
                    " FROM " +
                    escapeTableName(tableName) + " where " + options.getSourceWhere();
            if (options.getJobs() == 1)
                sqlCmd = sqlCmd + " AND 0 = ?";
            else
                sqlCmd = sqlCmd + " AND ora_hash(rowid," + (options.getJobs() - 1) + ") = ?";
        } else {
            // Full table read. NO_IDEX and Oracle direct Read
            oracleAlterSession(true);
            sqlCmd = "SELECT /*+ NO_INDEX(" + escapeTableName(tableName) + ")*/ " +
                    allColumns +
                    " FROM " +
                    escapeTableName(tableName);

            // Apply flashback query if SCN was captured
            if (this.oracleSourceScn != null) {
                sqlCmd += " AS OF SCN " + this.oracleSourceScn;
                LOG.debug("Using flashback query with SCN {}", this.oracleSourceScn);
            }

            if (options.getJobs() == 1)
                sqlCmd = sqlCmd + " where 0 = ?";
            else
                sqlCmd = sqlCmd + " where ora_hash(rowid," + (options.getJobs() - 1) + ") = ?";


        }

        try {
            return super.execute(sqlCmd, (Object) nThread);
        } catch (SQLException e) {
            // Handle flashback-specific errors
            if (e.getErrorCode() == 8181) {
                // ORA-08181: specified number is not a valid system change number
                LOG.error("Flashback query failed: SCN {} is too old (outside undo retention window). " +
                          "Recommendation: Increase Oracle UNDO_RETENTION parameter or run replication " +
                          "during lower activity periods.", this.oracleSourceScn);
                throw e;
            } else if (e.getErrorCode() == 1555) {
                // ORA-01555: should not occur with flashback, but defensive check
                LOG.error("ORA-01555 occurred despite flashback query. This suggests insufficient " +
                          "undo retention. Current SCN: {}", this.oracleSourceScn);
                throw e;
            }
            // Propagate other SQLExceptions
            throw e;
        }
    }

    public void oracleAlterSession(Boolean directRead) throws SQLException {
        // Specific Oracle Alter sessions for reading data
        Statement stmt = this.getConnection().createStatement();
        stmt.executeUpdate("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'");
        stmt.executeUpdate("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS' ");
        stmt.executeUpdate("ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF' ");
        stmt.executeUpdate("ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZH:TZM' ");
        stmt.executeUpdate("ALTER SESSION ENABLE PARALLEL DML");

        // The recyclebin is available since version 10
        DatabaseMetaData meta = this.getConnection().getMetaData();
        if ( meta.getDatabaseMajorVersion() >= 10 ) stmt.executeUpdate("ALTER SESSION SET recyclebin = OFF");

        if (directRead) stmt.executeUpdate("ALTER SESSION SET \"_serial_direct_read\"=true ");

        stmt.close();
    }

    /**
     * Inserts data from a ResultSet into the Oracle sink table.
     * 
     * <p><b>NULL Handling Pattern:</b> For primitive types (int, double, float, etc.), this method
     * stores the value first, then checks {@code resultSet.wasNull()} to detect SQL NULL. If NULL
     * is detected, it calls {@code ps.setNull(columnIndex, sqlType)} instead of setting the primitive
     * value. This prevents NULL from being silently converted to primitive defaults (0, 0.0, false).</p>
     * 
     * <p>Example pattern for INTEGER:</p>
     * <pre>{@code
     * case Types.INTEGER:
     *     int intValue = resultSet.getInt(i);
     *     if (resultSet.wasNull()) {
     *         ps.setNull(i, Types.INTEGER);  // Preserve NULL
     *     } else {
     *         ps.setInt(i, intValue);        // Set actual value
     *     }
     *     break;
     * }</pre>
     * 
     * <p>This pattern is applied consistently for all primitive and temporal types to ensure
     * data integrity during replication. See Issue #51 for original problem report.</p>
     * 
     * @param resultSet The source data to insert
     * @param taskId The parallel task identifier
     * @return Number of rows inserted
     * @throws SQLException If database operation fails
     * @throws IOException If I/O operation fails
     */
    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) throws SQLException, IOException {

        int totalRows = 0;
        ResultSetMetaData rsmd = resultSet.getMetaData();
        String tableName;

        // Get table name and columns
        if (options.getMode().equals(ReplicationMode.COMPLETE.getModeText())) {
            tableName = getSinkTableName();
        } else {
            tableName = getQualifiedStagingTableName();
        }

        String allColumns = getAllSinkColumns(rsmd);
        int columnsNumber = rsmd.getColumnCount();

        String sqlCdm = getInsertSQLCommand(tableName, allColumns, columnsNumber);
        PreparedStatement ps = this.getConnection().prepareStatement(sqlCdm);

        final int batchSize = options.getFetchSize();
        int count = 0;

        LOG.info("Inserting data with this command: {}",sqlCdm);

        oracleAlterSession(true);

        // Track streams that need to be closed after batch execution
        List<AutoCloseable> openStreams = new ArrayList<>();

        if (resultSet.next()) {
            // Create Bandwidth Throttling
            BandwidthThrottling bt = new BandwidthThrottling(options.getBandwidthThrottling(), options.getFetchSize(), resultSet);

            do {
                bt.acquiere();

                // Get Columns values
                for (int i = 1; i <= columnsNumber; i++) {

                    switch (rsmd.getColumnType(i)) {
                        case Types.VARCHAR:
                        case Types.CHAR:
                        case Types.LONGVARCHAR:
                            String strVal = resultSet.getString(i);
                            if (resultSet.wasNull() || strVal == null) {
                                ps.setNull(i, Types.VARCHAR);
                            } else {
                                ps.setString(i, strVal);
                            }
                            break;
                        case Types.INTEGER:
                        case Types.TINYINT:
                        case Types.SMALLINT:
                            int intVal = resultSet.getInt(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, Types.INTEGER);
                            } else {
                                ps.setInt(i, intVal);
                            }
                            break;
                        case Types.BIGINT:
                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            java.math.BigDecimal bdVal = resultSet.getBigDecimal(i);
                            if (resultSet.wasNull() || bdVal == null) {
                                ps.setNull(i, Types.NUMERIC);
                            } else {
                                ps.setBigDecimal(i, bdVal);
                            }
                            break;
                        case Types.DOUBLE:
                            double doubleVal = resultSet.getDouble(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, Types.DOUBLE);
                            } else {
                                ps.setDouble(i, doubleVal);
                            }
                            break;
                        case Types.FLOAT:
                        case Types.REAL:
                            float floatVal = resultSet.getFloat(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, Types.FLOAT);
                            } else {
                                ps.setFloat(i, floatVal);
                            }
                            break;
                        case Types.DATE:
                            java.sql.Date dateVal = resultSet.getDate(i);
                            if (resultSet.wasNull() || dateVal == null) {
                                ps.setNull(i, Types.DATE);
                            } else {
                                // Use UTC calendar for consistent date handling across databases
                                java.util.Calendar cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
                                ps.setDate(i, dateVal, cal);
                            }
                            break;
                        case Types.TIMESTAMP:
                        case Types.TIMESTAMP_WITH_TIMEZONE:
                        case -101:
                        case -102:
                            java.sql.Timestamp tsVal = resultSet.getTimestamp(i);
                            if (resultSet.wasNull() || tsVal == null) {
                                ps.setNull(i, Types.TIMESTAMP);
                            } else {
                                // Use UTC calendar for consistent timestamp handling across databases
                                java.util.Calendar cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
                                ps.setTimestamp(i, tsVal, cal);
                            }
                            break;
                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.LONGVARBINARY:
                            byte[] bytesVal = resultSet.getBytes(i);
                            if (resultSet.wasNull() || bytesVal == null) {
                                ps.setNull(i, Types.BINARY);
                            } else {
                                ps.setBytes(i, bytesVal);
                            }
                            break;
                        case Types.BLOB:
                            // Stream BLOB without loading into memory
                            // Keep stream open until after executeBatch()
                            Blob blobData = getBlob(resultSet, i);
                            streamBlobToSink(blobData, ps, i, openStreams);
                            break;
                        case Types.CLOB:
                            // Stream CLOB without loading into memory
                            // Keep stream open until after executeBatch()
                            Clob clobData = resultSet.getClob(i);
                            streamClobToSink(clobData, ps, i, openStreams);
                            break;
                        case Types.BOOLEAN:
                        case Types.BIT:
                            // Oracle doesn't have native BOOLEAN, use NUMBER(1) with 0/1 values
                            Boolean boolValue = resultSet.getBoolean(i);
                            if (resultSet.wasNull()) {
                                ps.setNull(i, Types.NUMERIC);
                            } else {
                                ps.setInt(i, boolValue ? 1 : 0);
                            }
                            break;
                        case Types.NVARCHAR:
                        case Types.NCHAR:
                        case Types.LONGNVARCHAR:
                            ps.setNString(i, resultSet.getNString(i));
                            break;
                        case Types.SQLXML:
                            SQLXML sqlXmlData = resultSet.getSQLXML(i);
                            if (!resultSet.wasNull()){
                                SQLXML sinkXmlData = this.getConnection().createSQLXML();
                                IOUtils.copy(sqlXmlData.getBinaryStream(), sinkXmlData.setBinaryStream());
                                ps.setSQLXML(i, sinkXmlData);
                                sqlXmlData.free();
                                sinkXmlData.free();
                            } else {
                                ps.setSQLXML(i, sqlXmlData);
                            }
                            break;
                        case Types.ROWID:
                            ps.setRowId(i, resultSet.getRowId(i));
                            break;
                        case Types.ARRAY:
                            // Convert array to string representation for Oracle
                            java.sql.Array arrayData = resultSet.getArray(i);
                            if (arrayData != null) {
                                ps.setString(i, arrayData.toString());
                            } else {
                                ps.setNull(i, Types.VARCHAR);
                            }
                            break;
                        case Types.STRUCT:
                            ps.setObject(i, resultSet.getObject(i), Types.STRUCT);
                            break;
                        case Types.OTHER:
                            // PostgreSQL specific types (bytea, json, xml, etc.) - convert to string
                            Object otherData = resultSet.getObject(i);
                            if (otherData != null) {
                                if (otherData instanceof byte[]) {
                                    ps.setBytes(i, (byte[]) otherData);
                                } else {
                                    ps.setString(i, otherData.toString());
                                }
                            } else {
                                ps.setNull(i, Types.VARCHAR);
                            }
                            break;
                        case Types.TIME:
                        case Types.TIME_WITH_TIMEZONE:
                            // Oracle doesn't have a TIME-only type
                            // When auto-create maps TIME to TIMESTAMP, convert Time to Timestamp
                            // When explicit column type is VARCHAR, store as string
                            java.sql.Time timeData = resultSet.getTime(i);
                            if (timeData == null || resultSet.wasNull()) {
                                ps.setNull(i, Types.TIMESTAMP);
                            } else {
                                // Convert TIME to TIMESTAMP by setting date portion to epoch (1970-01-01)
                                java.sql.Timestamp tsFromTime = new java.sql.Timestamp(timeData.getTime());
                                java.util.Calendar cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
                                ps.setTimestamp(i, tsFromTime, cal);
                            }
                            break;
                        // Oracle INTERVAL types (not in standard java.sql.Types)
                        case -104: // INTERVALDS (INTERVAL DAY TO SECOND)
                        case -103: // INTERVALYM (INTERVAL YEAR TO MONTH)
                            Object intervalData = resultSet.getObject(i);
                            if (intervalData != null) {
                                ps.setObject(i, intervalData);
                            } else {
                                ps.setNull(i, Types.VARCHAR);
                            }
                            break;
                        default:
                            // Fallback: convert to string to avoid ORA-17004
                            Object defaultData = resultSet.getObject(i);
                            if (defaultData != null) {
                                if (defaultData instanceof byte[]) {
                                    ps.setBytes(i, (byte[]) defaultData);
                                } else if (defaultData instanceof Number) {
                                    ps.setBigDecimal(i, new java.math.BigDecimal(defaultData.toString()));
                                } else {
                                    ps.setString(i, defaultData.toString());
                                }
                            } else {
                                ps.setNull(i, Types.VARCHAR);
                            }
                            LOG.debug("Column {} has unhandled JDBC type {}, converted to string", i, rsmd.getColumnType(i));
                            break;
                    }
                }

                ps.addBatch();

                if (++count % batchSize == 0) {
                    ps.executeBatch();
                    this.getConnection().commit();
                    // Close all streams after successful batch execution
                    closeStreams(openStreams);
                }

                totalRows++;
            } while (resultSet.next());
        }

        ps.executeBatch(); // insert remaining records
        this.getConnection().commit();
        // Close all remaining streams after final batch
        closeStreams(openStreams);
        
        ps.close();

        this.getConnection().commit();
        return totalRows;
    }

    private String getInsertSQLCommand(String tableName, String allColumns, int columnsNumber) {

        StringBuilder sqlCmd = new StringBuilder();

        //sqlCmd.append("INSERT INTO /*+APPEND_VALUES*/ ");
        sqlCmd.append("INSERT INTO ");
        sqlCmd.append(tableName);

        if (allColumns != null) {
            sqlCmd.append(" (");
            sqlCmd.append(allColumns);
            sqlCmd.append(")");
        }

        sqlCmd.append(" VALUES ( ");
        for (int i = 0; i <= columnsNumber - 1; i++) {
            if (i > 0) sqlCmd.append(",");
            sqlCmd.append("?");
        }
        sqlCmd.append(" )");

        return sqlCmd.toString();
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

        String sql = " CREATE TABLE " + sinkStagingTable + " NOLOGGING AS (SELECT " + allSinkColumns + " FROM " + this.getSinkTableName() + " WHERE rownum = -1 ) ";

        LOG.info("Creating staging table with this command: {}", sql);
        statement.executeUpdate(sql);
        statement.close();
        this.getConnection().commit();

    }

    @Override
    protected void mergeStagingTable() throws SQLException {
        this.getConnection().commit();

        Statement statement = this.getConnection().createStatement();

        String[] pks = this.getSinkPrimaryKeys(this.getSinkTableName());
        // Primary key is required
        if (pks == null || pks.length == 0) {
            throw new IllegalArgumentException("Sink table must have at least one primary key column for incremental mode.");
        }

        // options.sinkColumns was set during the insertDataToTable
        String allColls = getAllSinkColumns(null);
        // Oracle use columns uppercase
        //allColls = allColls.toUpperCase();

        StringBuilder sql = new StringBuilder();
        sql.append("MERGE /*+ PARALLEL */ INTO ")
                .append(this.getSinkTableName())
                .append(" trg USING (SELECT ")
                .append(allColls)
                .append(" FROM ")
                .append(getQualifiedStagingTableName())
                .append(" ) src ON ")
                .append(" (");

        for (int i = 0; i <= pks.length - 1; i++) {
            if (i >= 1) sql.append(" AND ");
            sql.append("src.").append(pks[i]).append("= trg.").append(pks[i]);
        }

        sql.append(" ) WHEN MATCHED THEN UPDATE SET ");

        // Set all columns for UPDATE SET statement
        for (String colName : allColls.split("\\s*,\\s*")) {

            boolean contains = Arrays.asList(pks).contains(colName);
            boolean containsUppercase = Arrays.asList(pks).contains(colName.toUpperCase());
            boolean containsQuoted = Arrays.asList(pks).contains("\""+colName.toUpperCase()+"\"");
            if (!contains && !containsUppercase && !containsQuoted)
                sql.append(" trg.").append(colName).append(" = src.").append(colName).append(" ,");
        }
        // Delete the last comma
        sql.setLength(sql.length() - 1);


        sql.append(" WHEN NOT MATCHED THEN INSERT ( ").append(allColls).
                append(" ) VALUES (");

        // all columns for INSERT VALUES statement
        for (String colName : allColls.split("\\s*,\\s*")) {
            sql.append(" src.").append(colName).append(" ,");
        }
        // Delete the last comma
        sql.setLength(sql.length() - 1);

        sql.append(" ) ");

        LOG.info("Merging staging table and sink table with this command: {}", sql);
        statement.executeUpdate(sql.toString());
        statement.close();
        this.getConnection().commit();
    }

    @Override
    public void preSourceTasks() throws Exception {
        // Call parent to probe source metadata if auto-create is enabled
        super.preSourceTasks();
        
        // Capture SCN for flashback query to prevent ORA-01555 on large tables
        try {
            Statement stmt = getConnection().createStatement();
            ResultSet rs = stmt.executeQuery("SELECT CURRENT_SCN FROM V$DATABASE");
            if (rs.next()) {
                this.oracleSourceScn = rs.getLong(1);
                LOG.info("Captured Oracle SCN for consistent read: {}", oracleSourceScn);
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            // Graceful fallback if V$DATABASE is inaccessible (insufficient privileges)
            LOG.warn("Could not capture Oracle SCN (V$DATABASE not accessible). " +
                     "Flashback query disabled. Error: {}", e.getMessage());
            this.oracleSourceScn = null;
        }
    }

    @Override
    public void postSourceTasks() {}

    @Override
    public void dropStagingTable() throws SQLException {
        Statement stmt = null;
        try {
            // Commit any pending transaction before ALTER SESSION
            if (!this.getConnection().getAutoCommit()) {
                this.getConnection().commit();
            }
            // Disable Oracle RECYCLEBIN for DROP operation
            stmt = this.getConnection().createStatement();
            DatabaseMetaData meta = this.getConnection().getMetaData();
            if (meta.getDatabaseMajorVersion() >= 10) {
                stmt.executeUpdate("ALTER SESSION SET recyclebin = OFF");
            }
        } catch (SQLException e) {
            LOG.warn("Could not disable recyclebin: {}", e.getMessage());
        } finally {
            if (stmt != null) {
                try { stmt.close(); } catch (SQLException ignored) {}
            }
        }
        super.dropStagingTable();
    }

    /**
     * Streams BLOB content from source to sink PreparedStatement using chunked transfer.
     * This avoids LOB locator transfer issues (ORA-64219) between different Oracle versions.
     * The BLOB content is read as a binary stream and written directly to the PreparedStatement,
     * never materializing the entire LOB in memory.
     *
     * @param sourceBlob Source BLOB from ResultSet (may be null)
    /**
     * Streams BLOB content from source to sink PreparedStatement.
     * The stream is kept open and tracked for later cleanup after batch execution.
     * This avoids both ORA-64219 (LOB locators) and OutOfMemoryError (full LOB in memory).
     *
     * @param sourceBlob Source BLOB from ResultSet (may be null)
     * @param ps Target PreparedStatement
     * @param columnIndex 1-based column index
     * @param openStreams List to track open streams for cleanup
     * @throws SQLException if database access error occurs
     * @throws IOException if stream read error occurs
     */
    private void streamBlobToSink(Blob sourceBlob, PreparedStatement ps, int columnIndex, List<AutoCloseable> openStreams) 
            throws SQLException, IOException {
        if (sourceBlob == null) {
            ps.setNull(columnIndex, Types.BLOB);
            return;
        }
        
        long blobLength = sourceBlob.length();
        if (blobLength == 0) {
            ps.setNull(columnIndex, Types.BLOB);
            sourceBlob.free();
            return;
        }
        
        // Stream BLOB without loading into memory
        // Stream is kept open and will be closed after executeBatch()
        InputStream blobStream = sourceBlob.getBinaryStream();
        ps.setBinaryStream(columnIndex, blobStream, blobLength);
        
        // Track stream and blob for cleanup after batch execution
        openStreams.add(blobStream);
        openStreams.add(() -> sourceBlob.free());
    }

    /**
     * Streams CLOB content from source to sink PreparedStatement.
     * The stream is kept open and tracked for later cleanup after batch execution.
     * This avoids both ORA-64219 (LOB locators) and OutOfMemoryError (full LOB in memory).
     *
     * @param sourceClob Source CLOB from ResultSet (may be null)
     * @param ps Target PreparedStatement
     * @param columnIndex 1-based column index
     * @param openStreams List to track open streams for cleanup
     * @throws SQLException if database access error occurs
     * @throws IOException if stream read error occurs
     */
    private void streamClobToSink(Clob sourceClob, PreparedStatement ps, int columnIndex, List<AutoCloseable> openStreams) 
            throws SQLException, IOException {
        if (sourceClob == null) {
            ps.setNull(columnIndex, Types.CLOB);
            return;
        }
        
        long clobLength = sourceClob.length();
        if (clobLength == 0) {
            ps.setNull(columnIndex, Types.CLOB);
            sourceClob.free();
            return;
        }
        
        // Stream CLOB without loading into memory
        // Stream is kept open and will be closed after executeBatch()
        Reader clobReader = sourceClob.getCharacterStream();
        ps.setCharacterStream(columnIndex, clobReader, clobLength);
        
        // Track stream and clob for cleanup after batch execution
        openStreams.add(clobReader);
        openStreams.add(() -> sourceClob.free());
    }

    /**
     * Closes all tracked streams and frees LOB resources after batch execution.
     * This is called after executeBatch() to properly cleanup resources.
     *
     * @param openStreams List of streams and LOBs to close/free
     */
    private void closeStreams(List<AutoCloseable> openStreams) {
        for (AutoCloseable resource : openStreams) {
            try {
                if (resource != null) {
                    resource.close();
                }
            } catch (Exception e) {
                LOG.warn("Error closing LOB stream/resource: {}", e.getMessage());
            }
        }
        openStreams.clear();
    }

    @Override
    protected String mapJdbcTypeToNativeDDL(String columnName, int jdbcType, int precision, int scale) {
        switch (jdbcType) {
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
                return "NUMBER(10)";
            case Types.BIGINT:
                return "NUMBER(19)";
            case Types.FLOAT:
            case Types.DOUBLE:
                return "BINARY_DOUBLE";
            case Types.REAL:
                return "BINARY_FLOAT";
            case Types.NUMERIC:
            case Types.DECIMAL:
                // Oracle NUMBER without precision means generic number
                if (precision == 0 && scale == -127) {
                    return "NUMBER";
                } 
                // Oracle uses negative scale (-127) to indicate floating-point approximation
                // REAL/DOUBLE PRECISION/FLOAT are reported as NUMBER with precision 63/126 and scale -127
                // Map these to native binary float types instead of invalid NUMBER syntax
                else if (scale == -127) {
                    if (precision <= 63) {
                        return "BINARY_FLOAT";
                    } else {
                        return "BINARY_DOUBLE";
                    }
                } 
                else if (precision > 0) {
                    return "NUMBER(" + precision + ", " + scale + ")";
                } else {
                    return "NUMBER";
                }
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
                if (precision > 4000) {
                    return "CLOB";
                } else if (precision > 0) {
                    return "VARCHAR2(" + precision + ")";
                } else {
                    return "CLOB";
                }
            case Types.CHAR:
            case Types.NCHAR:
                return "CHAR(" + precision + ")";
            case Types.BOOLEAN:
            case Types.BIT:
                return "NUMBER(1)";
            case Types.DATE:
                return "DATE";
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return "TIMESTAMP";
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                if (precision > 2000) {
                    return "BLOB";
                } else if (precision > 0) {
                    return "RAW(" + precision + ")";
                } else {
                    return "BLOB";
                }
            case Types.BLOB:
                return "BLOB";
            case Types.CLOB:
                return "CLOB";
            // Oracle-specific INTERVAL types
            case -104:  // INTERVAL DAY TO SECOND
                return "INTERVAL DAY TO SECOND";
            case -103:  // INTERVAL YEAR TO MONTH
                return "INTERVAL YEAR TO MONTH";
            // Oracle XMLType
            case 2009:  // SQLXML / XMLType
                return "XMLTYPE";
            default:
                LOG.warn("Unmapped JDBC type {} for column {}, using CLOB as fallback", jdbcType, columnName);
                return "CLOB";
        }
    }
}

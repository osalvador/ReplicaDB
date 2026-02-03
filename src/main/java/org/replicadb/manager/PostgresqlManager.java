package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.ByteConverter;
import org.replicadb.manager.util.BandwidthThrottling;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

import static org.replicadb.manager.SupportedManagers.MARIADB;
import static org.replicadb.manager.SupportedManagers.MYSQL;

/**
 * PostgreSQL-specific database manager implementing binary COPY protocol for high-performance data replication.
 * 
 * <p>Binary COPY Format Implementation:</p>
 * <ul>
 *   <li>Requires PostgreSQL JDBC driver 42.7.2 or later (tested version)</li>
 *   <li>Uses {@code org.postgresql.util.ByteConverter} for native binary encoding of PostgreSQL types</li>
 *   <li>Supports native binary encoding for: BYTEA, BLOB, INTEGER, BIGINT, SMALLINT, BOOLEAN, 
 *       FLOAT, DOUBLE, DATE, TIME, TIMESTAMP, NUMERIC, DECIMAL, VARCHAR, CHAR, CLOB</li>
 *   <li>Falls back to text encoding for exotic types (ARRAY, JSON, XML, geometric types, ranges)</li>
 * </ul>
 * 
 * <p>The binary encoding preserves:</p>
 * <ul>
 *   <li>IEEE 754 precision for FLOAT/DOUBLE (no string conversion rounding)</li>
 *   <li>Microsecond precision for TIMESTAMP (sub-millisecond accuracy)</li>
 *   <li>Exact precision for NUMERIC/DECIMAL (base-10000 internal format)</li>
 *   <li>Date accuracy across PostgreSQL epoch boundary (2000-01-01)</li>
 * </ul>
 * 
 * <p><strong>Version Compatibility:</strong> This implementation uses PostgreSQL JDBC driver internal classes
 * which are stable across 42.x versions but may change in major version updates. Other 42.x driver versions
 * may work but are untested.</p>
 * 
 * @see org.postgresql.util.ByteConverter
 * @see org.postgresql.copy.CopyManager
 */
public class PostgresqlManager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(PostgresqlManager.class.getName());

    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

    private static Long chunkSize = 0L;

    /**
     * Constructs the SqlManager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     */
    public PostgresqlManager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
    }

    @Override
    public String getDriverClass() {
        return JdbcDrivers.POSTGRES.getDriverClass();
    }

    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) throws SQLException, IOException {

        CopyIn copyIn = null;
        int totalRows = 0;

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

            // Get Postgres COPY meta-command manager
            PgConnection copyOperationConnection = this.connection.unwrap(PgConnection.class);
            CopyManager copyManager = new CopyManager(copyOperationConnection);
            
            // Detect binary columns and choose appropriate COPY format
            if (hasBinaryColumns(rsmd)) {
                LOG.info("Binary columns detected, using COPY (FORMAT BINARY) for table {}", tableName);
                String copyCmd = "COPY " + tableName + " (" + allColumns + ") FROM STDIN (FORMAT BINARY)";
                copyIn = copyManager.copyIn(copyCmd);
                
                try {
                    totalRows = insertDataViaBinaryCopy(resultSet, taskId, rsmd, copyIn);
                    this.getConnection().commit();
                    return totalRows;
                } catch (Exception e) {
                    if (copyIn != null && copyIn.isActive()) {
                        copyIn.cancelCopy();
                    }
                    this.connection.rollback();
                    LOG.error("Error during binary COPY to table {}, processed {} rows. Check PostgreSQL version (requires 8.0+) and column types.", 
                              tableName, totalRows, e);
                    throw e;
                } finally {
                    if (copyIn != null && copyIn.isActive()) {
                        copyIn.cancelCopy();
                    }
                }
            }
            
            // No binary columns, use TEXT format (existing behavior)
            LOG.info("No binary columns, using COPY (FORMAT TEXT) for table {}", tableName);
            String copyCmd = getCopyCommand(tableName, allColumns);
            copyIn = copyManager.copyIn(copyCmd);

            char unitSeparator = 0x1F;
            char nullAscii = 0x00;
            int columnsNumber = rsmd.getColumnCount();

            StringBuilder row = new StringBuilder();
            StringBuilder cols = new StringBuilder();

            byte[] bytes;
            String colValue = null;

            // determine if the source database is MySQL or MariaDB
            boolean isMySQL = MYSQL.isTheManagerTypeOf(options, DataSourceType.SOURCE) || MARIADB.isTheManagerTypeOf(options, DataSourceType.SOURCE);


            if (resultSet.next()) {
                // Create Bandwidth Throttling
                BandwidthThrottling bt = new BandwidthThrottling(options.getBandwidthThrottling(), options.getFetchSize(), resultSet);

                do {
                    bt.acquiere();

                    // Get Columns values
                    for (int i = 1; i <= columnsNumber; i++) {
                        if (i > 1) cols.append(unitSeparator);

                        switch (rsmd.getColumnType(i)) {

                            case Types.CLOB:
                                colValue = clobToString(resultSet.getClob(i));
                                break;
                            case Types.BINARY:
                                colValue = bytesToPostgresHex(resultSet.getBytes(i));
                                break;
                            case Types.BLOB:                            
                                colValue = blobToPostgresHex(getBlob(resultSet,i));
                                break;
                            default:
                                if (isMySQL) {
                                    // MySQL and MariaDB have a different way to handle Binary type
                                    List<Integer> binaryTypes = Arrays.asList(-3,-4);
                                    if (binaryTypes.contains(rsmd.getColumnType(i))) {
                                        colValue = blobToPostgresHex(getBlob(resultSet,i));
                                    } else {
                                        // Any other type is converted to String
                                        colValue = resultSet.getString(i);
                                    }
                                } else {
                                    // Any other type is converted to String
                                    colValue = resultSet.getString(i);
                                }                                
                                break;
                        }

                        if (resultSet.wasNull() || colValue == null){
                            colValue = String.valueOf(nullAscii);
                        }
                        cols.append(colValue);
                    }

                    // Escape special chars
                    if (this.options.isSinkDisableEscape())
                        row.append(cols.toString().replace("\u0000", "\\N"));
                    else
                        row.append(cols.toString().replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace("\u0000", "\\N"));

                    // Row ends with \n
                    row.append("\n");

                    // Copy data to postgres
                    bytes = row.toString().getBytes(StandardCharsets.UTF_8);
                    copyIn.writeToCopy(bytes, 0, bytes.length);

                    // Clear StringBuilders
                    row.setLength(0); // set length of buffer to 0
                    row.trimToSize();
                    cols.setLength(0); // set length of buffer to 0
                    cols.trimToSize();
                    totalRows++;
                } while (resultSet.next());
            }

            copyIn.endCopy();

        } catch (Exception e) {
            if (copyIn != null && copyIn.isActive()) {
                copyIn.cancelCopy();
            }
            this.connection.rollback();
            throw e;
        } finally {
            if (copyIn != null && copyIn.isActive()) {
                copyIn.cancelCopy();
            }
        }

        this.getConnection().commit();

        return totalRows;
    }

    private String getCopyCommand(String tableName, String allColumns) {

        StringBuilder copyCmd = new StringBuilder();

        copyCmd.append("COPY ");
        copyCmd.append(tableName);

        if (allColumns != null) {
            copyCmd.append(" (");
            copyCmd.append(allColumns);
            copyCmd.append(")");
        }

        copyCmd.append(" FROM STDIN WITH DELIMITER e'\\x1f' ENCODING 'UTF-8' ");

        LOG.info("Copying data with this command: " + copyCmd.toString());

        return copyCmd.toString();
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
                    options.getSourceQuery() + ") as T1 OFFSET ? ";
        } else {

            sqlCmd = "SELECT " +
                    allColumns +
                    " FROM " +
                    escapeTableName(tableName);

            // Source Where
            if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
                sqlCmd = sqlCmd + " WHERE " + options.getSourceWhere();
            }

            sqlCmd = sqlCmd + " OFFSET ? ";

        }

        String limit = " LIMIT ?";

        if (this.options.getJobs() == nThread + 1) {
            return super.execute(sqlCmd, offset);
        } else {
            sqlCmd = sqlCmd + limit;
            return super.execute(sqlCmd, offset, chunkSize);
        }

    }

    @Override
    protected void createStagingTable() throws SQLException {

        Statement statement = this.getConnection().createStatement();
        try {

            String sinkStagingTable = getQualifiedStagingTableName();

            String sql = "CREATE UNLOGGED TABLE IF NOT EXISTS " + sinkStagingTable + " ( LIKE " + this.getSinkTableName() + " INCLUDING DEFAULTS INCLUDING CONSTRAINTS ) WITH (autovacuum_enabled=false)";

            LOG.info("Creating staging table with this command: " + sql);
            statement.executeUpdate(sql);
            statement.close();
            this.getConnection().commit();

        } catch (Exception e) {
            statement.close();
            this.connection.rollback();
            throw e;
        }

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
                    .append(" ON CONFLICT ")
                    .append(" (").append(String.join(",", pks)).append(" )")
                    .append(" DO UPDATE SET ");

            // Set all columns for DO UPDATE SET statement
            for (String colName : allColls.split(",")) {
                sql.append(" ").append(colName).append(" = excluded.").append(colName).append(" ,");
            }
            // Delete the last comma
            sql.setLength(sql.length() - 1);

            LOG.info("Merging staging table and sink table with this command: {}", sql);
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
    public void preSourceTasks() throws SQLException {

        if (this.options.getJobs() != 1) {

            /**
             * Calculating the chunk size for parallel job processing
             */
            Statement statement = this.getConnection().createStatement();

            try {
                String sql = "SELECT " +
                        " abs(count(*) / " + options.getJobs() + ") chunk_size" +
                        ", count(*) total_rows" +
                        " FROM ";

                // Source Query
                if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
                    sql = sql + "( " + this.options.getSourceQuery() + " ) as T1";

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
            } catch (Exception e) {
                statement.close();
                this.connection.rollback();
                throw e;
            }
        }

    }

    @Override
    public void postSourceTasks() {/*Not implemented*/}

    /*********************************************************************************************
     * From BLOB to Hexadecimal String for Postgres Copy
     * @return string representation of blob
     *********************************************************************************************/
    private String blobToPostgresHex(Blob blobData) throws SQLException {

        String returnData = "";

        if (blobData != null) {
            try {
                byte[] bytes = blobData.getBytes(1, (int) blobData.length());

                returnData = bytesToPostgresHex(bytes);
            } finally {
                // The most important thing here is free the BLOB to avoid memory Leaks
                blobData.free();
            }
        }

        return returnData;

    }

    @NotNull
    private String bytesToPostgresHex (byte[] bytes) {
        if (bytes == null) return "";

        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return  "\\\\x" + new String(hexChars);
    }

    /**
     * Checks if the ResultSet contains any binary column types that require binary COPY format.
     * Binary columns include BINARY, VARBINARY, LONGVARBINARY, and BLOB types.
     *
     * @param rsmd ResultSetMetaData to analyze
     * @return true if any column is a binary type, false otherwise
     * @throws SQLException if metadata access fails
     */
    private boolean hasBinaryColumns(ResultSetMetaData rsmd) throws SQLException {
        if (rsmd == null) {
            return false;
        }
        
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            int type = rsmd.getColumnType(i);
            if (type == Types.BINARY || 
                type == Types.VARBINARY || 
                type == Types.LONGVARBINARY || 
                type == Types.BLOB) {
                return true;
            }
        }
        return false;
    }

    /**
     * Inserts data to PostgreSQL using binary COPY format.
     * This format is required for correct binary data transfer without hex encoding.
     * 
     * <p>PostgreSQL binary COPY format specification:</p>
     * <ul>
     *   <li>Header: 19 bytes (signature 'PGCOPY\\n\\377\\r\\n\\0' + flags + extension length)</li>
     *   <li>Row format: field count (int16) + [field length (int32) + field data] per field</li>
     *   <li>NULL marker: field length = -1 (0xFFFFFFFF)</li>
     *   <li>Trailer: -1 as int16 (0xFFFF)</li>
     * </ul>
     * 
     * <p>Type-specific binary encoding (using {@link org.postgresql.util.ByteConverter}):</p>
     * <ul>
     *   <li><strong>FLOAT/DOUBLE:</strong> IEEE 754 big-endian format (4/8 bytes)</li>
     *   <li><strong>DATE:</strong> Days since PostgreSQL epoch 2000-01-01 (4 bytes int32)</li>
     *   <li><strong>TIME:</strong> Microseconds since midnight (8 bytes int64)</li>
     *   <li><strong>TIMESTAMP:</strong> Microseconds since 2000-01-01 00:00:00 UTC (8 bytes int64)</li>
     *   <li><strong>NUMERIC/DECIMAL:</strong> PostgreSQL internal format (base-10000 digit groups)</li>
     *   <li><strong>INTEGER/BIGINT/SMALLINT:</strong> Big-endian signed integers (4/8/2 bytes)</li>
     *   <li><strong>BOOLEAN:</strong> 1 byte (0x00=false, 0x01=true)</li>
     *   <li><strong>VARCHAR/CHAR/TEXT:</strong> UTF-8 encoded bytes</li>
     *   <li><strong>BYTEA/BLOB/BINARY:</strong> Raw binary data</li>
     *   <li><strong>Exotic types (ARRAY, JSON, XML, geometric):</strong> Text fallback with TRACE logging</li>
     * </ul>
     * 
     * <p><strong>Version Requirements:</strong> Requires PostgreSQL JDBC driver 42.7.2. 
     * Internal ByteConverter class APIs are stable across 42.x versions.</p>
     * 
     * @param resultSet the ResultSet containing data to insert
     * @param copyIn the PostgreSQL CopyIn stream for binary COPY
     * @return number of rows inserted
     * @throws SQLException if data access or binary encoding fails
     * @throws IOException if COPY stream write fails
     * - Header: 19 bytes (signature + flags + extension length)
     * - Rows: field count + field data (length + bytes)
     * - Trailer: 2 bytes (-1 as int16)
     *
     * @param resultSet the ResultSet containing data to insert
     * @param taskId the task identifier for logging
     * @param rsmd ResultSetMetaData for column information
     * @param copyIn the PostgreSQL CopyIn operation
     * @return number of rows inserted
     * @throws SQLException if database operation fails
     * @throws IOException if I/O operation fails
     */
    private int insertDataViaBinaryCopy(ResultSet resultSet, int taskId, 
                                       ResultSetMetaData rsmd, CopyIn copyIn) 
                                       throws SQLException, IOException {
        int totalRows = 0;
        int columnsNumber = rsmd.getColumnCount();
        
        // Write PostgreSQL binary COPY header (19 bytes)
        // Signature: "PGCOPY\n\377\r\n\0" (11 bytes)
        byte[] signature = new byte[] {
            'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte)0xFF, '\r', '\n', 0
        };
        copyIn.writeToCopy(signature, 0, 11);
        
        // Flags field: 0x00000000 (4 bytes, int32 in network byte order)
        // Header extension area length: 0x00000000 (4 bytes, int32)
        ByteArrayOutputStream headerBuf = new ByteArrayOutputStream();
        DataOutputStream headerDos = new DataOutputStream(headerBuf);
        headerDos.writeInt(0); // flags
        headerDos.writeInt(0); // extension length
        byte[] headerBytes = headerBuf.toByteArray();
        copyIn.writeToCopy(headerBytes, 0, headerBytes.length);
        
        // Create Bandwidth Throttling
        BandwidthThrottling bt = new BandwidthThrottling(
            options.getBandwidthThrottling(), 
            options.getFetchSize(), 
            resultSet
        );
        
        if (resultSet.next()) {
            do {
                bt.acquiere();
                
                // Build row data in memory
                ByteArrayOutputStream rowData = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(rowData);
                
                // Field count (int16 = 2 bytes)
                dos.writeShort(columnsNumber);
                
                // For each field
                for (int i = 1; i <= columnsNumber; i++) {
                    int columnType = rsmd.getColumnType(i);
                    
                    switch (columnType) {
                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.LONGVARBINARY:
                            byte[] binaryData = resultSet.getBytes(i);
                            if (resultSet.wasNull() || binaryData == null) {
                                dos.writeInt(-1); // NULL marker
                            } else {
                                dos.writeInt(binaryData.length);
                                dos.write(binaryData);
                            }
                            break;
                            
                        case Types.BLOB:
                            Blob blob = resultSet.getBlob(i);
                            if (resultSet.wasNull() || blob == null) {
                                dos.writeInt(-1);
                            } else {
                                try {
                                    byte[] blobBytes = blob.getBytes(1, (int) blob.length());
                                    dos.writeInt(blobBytes.length);
                                    dos.write(blobBytes);
                                } finally {
                                    blob.free();
                                }
                            }
                            break;
                            
                        case Types.VARCHAR:
                        case Types.CHAR:
                        case Types.LONGVARCHAR:
                            String text = resultSet.getString(i);
                            if (resultSet.wasNull() || text == null) {
                                dos.writeInt(-1);
                            } else {
                                byte[] textBytes = text.getBytes(StandardCharsets.UTF_8);
                                dos.writeInt(textBytes.length);
                                dos.write(textBytes);
                            }
                            break;
                            
                        case Types.CLOB:
                            String clobText = clobToString(resultSet.getClob(i));
                            if (resultSet.wasNull() || clobText == null) {
                                dos.writeInt(-1);
                            } else {
                                byte[] clobBytes = clobText.getBytes(StandardCharsets.UTF_8);
                                dos.writeInt(clobBytes.length);
                                dos.write(clobBytes);
                            }
                            break;
                            
                        case Types.INTEGER:
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                dos.writeInt(4); // int32 is 4 bytes
                                dos.writeInt(resultSet.getInt(i));
                            }
                            break;
                            
                        case Types.BIGINT:
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                dos.writeInt(8); // int64 is 8 bytes
                                dos.writeLong(resultSet.getLong(i));
                            }
                            break;
                            
                        case Types.SMALLINT:
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                dos.writeInt(2); // int16 is 2 bytes
                                dos.writeShort(resultSet.getShort(i));
                            }
                            break;
                            
                        case Types.BOOLEAN:
                        case Types.BIT:
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                dos.writeInt(1); // boolean is 1 byte
                                dos.writeByte(resultSet.getBoolean(i) ? 1 : 0);
                            }
                            break;
                            
                        case Types.FLOAT:
                        case Types.REAL:
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                byte[] floatBytes = new byte[4];
                                ByteConverter.float4(floatBytes, 0, resultSet.getFloat(i));
                                dos.writeInt(4); // float is 4 bytes
                                dos.write(floatBytes);
                            }
                            break;
                            
                        case Types.DOUBLE:
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                byte[] doubleBytes = new byte[8];
                                ByteConverter.float8(doubleBytes, 0, resultSet.getDouble(i));
                                dos.writeInt(8); // double is 8 bytes
                                dos.write(doubleBytes);
                            }
                            break;
                            
                        case Types.DATE:
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                Date date = resultSet.getDate(i);
                                // PostgreSQL epoch: 2000-01-01
                                long pgEpochMillis = Date.valueOf("2000-01-01").getTime();
                                int days = (int)((date.getTime() - pgEpochMillis) / 86400000L);
                                byte[] dateBytes = new byte[4];
                                ByteConverter.int4(dateBytes, 0, days);
                                dos.writeInt(4); // date is 4 bytes
                                dos.write(dateBytes);
                            }
                            break;
                            
                        case Types.TIME:
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                Time time = resultSet.getTime(i);
                                // Microseconds since midnight
                                long microseconds = (time.getTime() % 86400000L) * 1000L;
                                byte[] timeBytes = new byte[8];
                                ByteConverter.int8(timeBytes, 0, microseconds);
                                dos.writeInt(8); // time is 8 bytes
                                dos.write(timeBytes);
                            }
                            break;
                            
                        case Types.TIMESTAMP:
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                Timestamp ts = resultSet.getTimestamp(i);
                                // PostgreSQL epoch: 2000-01-01 00:00:00 UTC
                                long pgEpochMillis = Timestamp.valueOf("2000-01-01 00:00:00").getTime();
                                // Calculate microseconds: (millis * 1000) + (sub-millisecond nanos / 1000)
                                long microseconds = (ts.getTime() - pgEpochMillis) * 1000L + (ts.getNanos() % 1000000) / 1000;
                                byte[] tsBytes = new byte[8];
                                ByteConverter.int8(tsBytes, 0, microseconds);
                                dos.writeInt(8); // timestamp is 8 bytes
                                dos.write(tsBytes);
                            }
                            break;
                            
                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                BigDecimal decimal = resultSet.getBigDecimal(i);
                                byte[] numericBytes = ByteConverter.numeric(decimal);
                                dos.writeInt(numericBytes.length);
                                dos.write(numericBytes);
                            }
                            break;
                            
                        default:
                            // Fallback to text representation for unsupported types
                            LOG.trace("Using string fallback for column {} type {} (code: {})", 
                                     rsmd.getColumnName(i), rsmd.getColumnTypeName(i), columnType);
                            String value = resultSet.getString(i);
                            if (resultSet.wasNull() || value == null) {
                                dos.writeInt(-1);
                            } else {
                                byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
                                dos.writeInt(valueBytes.length);
                                dos.write(valueBytes);
                            }
                            break;
                    }
                }
                
                // Write row to COPY stream
                byte[] rowBytes = rowData.toByteArray();
                copyIn.writeToCopy(rowBytes, 0, rowBytes.length);
                totalRows++;
                
            } while (resultSet.next());
        }
        
        // Write trailer: -1 as int16 (2 bytes: 0xFFFF)
        ByteArrayOutputStream trailer = new ByteArrayOutputStream();
        DataOutputStream trailerDos = new DataOutputStream(trailer);
        trailerDos.writeShort(-1); // End marker
        byte[] trailerBytes = trailer.toByteArray();
        copyIn.writeToCopy(trailerBytes, 0, trailerBytes.length);
        
        copyIn.endCopy();
        return totalRows;
    }


}

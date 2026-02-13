package org.replicadb.manager;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
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

import static org.replicadb.manager.SupportedManagers.DB2;
import static org.replicadb.manager.SupportedManagers.DB2_AS400;
import static org.replicadb.manager.SupportedManagers.MARIADB;
import static org.replicadb.manager.SupportedManagers.MONGODB;
import static org.replicadb.manager.SupportedManagers.MONGODBSRV;
import static org.replicadb.manager.SupportedManagers.MYSQL;

/**
 * PostgreSQL-specific database manager implementing binary COPY protocol for high-performance data replication.
 * Uses strict type separation: Binary COPY for simple types, TEXT COPY for complex types.
 * 
 * <p>Binary COPY Format (simple types only):</p>
 * <ul>
 *   <li>Requires PostgreSQL JDBC driver 42.7.2 or later (tested version)</li>
 *   <li>Uses {@code org.postgresql.util.ByteConverter} for native binary encoding of PostgreSQL types</li>
 *   <li>Supports native binary encoding for: BYTEA, BLOB, INTEGER, BIGINT, SMALLINT, BOOLEAN, 
 *       FLOAT, DOUBLE, DATE, TIME, TIMESTAMP, NUMERIC, DECIMAL, VARCHAR, CHAR, CLOB</li>
 *   <li>Throws exception if complex types (ARRAY, JSON, XML, INTERVAL) are encountered (defensive check)</li>
 * </ul>
 * 
 * <p>TEXT COPY Format (complex types):</p>
 * <ul>
 *   <li>Used for tables containing ARRAY, JSON, JSONB, XML, or INTERVAL columns</li>
 *   <li>Handles PostgreSQL-specific type parsing and serialization</li>
 *   <li>Determined automatically by {@link #shouldUseBinaryCopy(ResultSetMetaData)}</li>
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

    private Long chunkSize = 0L;

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
            
            // Determine appropriate COPY format based on column types
            if (shouldUseBinaryCopy(rsmd)) {
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
                    LOG.error("Error during binary COPY to table {}, processed {} rows. Binary COPY error: {}", 
                              tableName, totalRows, e.getMessage());
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

        // Log method entry with key parameters
        LOG.info("{}: readTable called - tableName: {}, nThread: {}, jobs: {}, chunkSize: {}",
                 Thread.currentThread().getName(),
                 tableName,
                 nThread,
                 this.options.getJobs(),
                 chunkSize);

        // If table name parameter is null get it from options
        tableName = tableName == null ? this.options.getSourceTable() : tableName;

        // If columns parameter is null, get it from options
        String allColumns = this.options.getSourceColumns() == null ? "*" : this.options.getSourceColumns();

        long offset = nThread * chunkSize;
        
        // Log offset calculation details
        LOG.debug("{}: Offset calculation - nThread: {} * chunkSize: {} = offset: {}",
                  Thread.currentThread().getName(),
                  nThread,
                  chunkSize,
                  offset);
        
        String sqlCmd;

        // Read table with source-query option specified
        if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
            sqlCmd = "SELECT  * FROM (" +
                    options.getSourceQuery() + ") as T1 OFFSET ? ";
            
            // Log the source query being wrapped
            LOG.info("{}: Using source-query wrapped in subquery. Original query: {}",
                     Thread.currentThread().getName(),
                     options.getSourceQuery());
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

        // Log which execution path will be taken
        boolean isLastJob = (this.options.getJobs() == nThread + 1);
        LOG.info("{}: Execution path - isLastJob: {}, will add LIMIT: {}",
                 Thread.currentThread().getName(),
                 isLastJob,
                 !isLastJob);

        if (this.options.getJobs() == nThread + 1) {
            // Log exact SQL being executed
            LOG.info("{}: Executing query WITHOUT LIMIT (jobs==nThread+1). SQL: {}",
                     Thread.currentThread().getName(),
                     sqlCmd);
            LOG.info("{}: Calling execute with offset parameter only: {}", 
                     Thread.currentThread().getName(),
                     offset);
            
            // Explicitly call execute(String, Object...) with offset as vararg parameter
            return super.execute(sqlCmd, new Object[]{offset});
        } else {
            sqlCmd = sqlCmd + limit;
            
            // Log exact SQL being executed
            LOG.info("{}: Executing query WITH LIMIT (parallel jobs). SQL: {}",
                     Thread.currentThread().getName(),
                     sqlCmd);
            LOG.info("{}: Calling execute with offset: {} and chunkSize: {}",
                     Thread.currentThread().getName(),
                     offset,
                     chunkSize);
            
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
    public void preSourceTasks() throws Exception {
        // Call parent to probe source metadata if auto-create is enabled
        super.preSourceTasks();

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
     * Determines whether to use binary COPY format based on column types.
     * Returns false for complex types (ARRAY, JSON, JSONB, XML, INTERVAL) which require TEXT COPY.
     * Returns true for simple types when BYTEA/BLOB columns are present, enabling binary transfer.
     *
     * @param rsmd ResultSetMetaData to analyze
     * @return true to use binary COPY format, false to use TEXT COPY format
     * @throws SQLException if metadata access fails
     */
    private boolean shouldUseBinaryCopy(ResultSetMetaData rsmd) throws SQLException {
        if (rsmd == null) {
            return false;
        }
        
        boolean hasBinary = false;
        boolean hasFloatingPoint = false;
        
        // Detect MySQL/MariaDB/MongoDB/DB2 sources for special handling
        boolean isMySQL = MYSQL.isTheManagerTypeOf(options, DataSourceType.SOURCE) || 
                          MARIADB.isTheManagerTypeOf(options, DataSourceType.SOURCE);
        boolean isMongoDB = MONGODB.isTheManagerTypeOf(options, DataSourceType.SOURCE) ||
                            MONGODBSRV.isTheManagerTypeOf(options, DataSourceType.SOURCE);
        boolean isDB2 = DB2.isTheManagerTypeOf(options, DataSourceType.SOURCE) ||
                        DB2_AS400.isTheManagerTypeOf(options, DataSourceType.SOURCE);
        
        // MongoDB BSON type system limitation: BSON types don't align with PostgreSQL binary format
        // - BSON only has 64-bit doubles (no 32-bit floats)
        // - BSON integers may have different byte order or format
        // - Use TEXT COPY for all MongoDB sources for reliable type conversion
        if (isMongoDB) {
            LOG.info("MongoDB source detected. Using text COPY for reliable BSON-to-SQL type conversion.");
            return false;
        }
        
        // DB2 binary type incompatibility: DB2's binary data encoding doesn't match PostgreSQL's expectations
        // - DB2 uses different internal representations for CHAR FOR BIT DATA, VARCHAR FOR BIT DATA
        // - Binary type mapping causes "insufficient data left in message" errors
        // - Use TEXT COPY for DB2 sources to ensure reliable data transfer
        if (isDB2) {
            LOG.info("DB2 source detected. Using text COPY for reliable type conversion and compatibility.");
            return false;
        }
        
        // First check for complex types that MUST use text COPY
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            int type = rsmd.getColumnType(i);
            String typeName = rsmd.getColumnTypeName(i);
            
            // ARRAY types require complex binary encoding (dimensions, bounds, null bitmap)
            // Text format {elem1,elem2} is simpler and reliable
            if (type == Types.ARRAY) {
                LOG.info("ARRAY type detected (column: {}, typeName: {}), using text COPY for safety", 
                         rsmd.getColumnName(i), typeName);
                return false;
            }
            
            // XML type: binary format requires UTF-8 but text COPY is more reliable
            if (type == Types.SQLXML || "xml".equalsIgnoreCase(typeName)) {
                LOG.info("XML type detected (column: {}), using text COPY", 
                         rsmd.getColumnName(i));
                return false;
            }
            
            // JSON/JSONB types: PostgreSQL binary format is complex (version byte + length + data)
            // Text COPY handles JSON as simple text strings
            if ("json".equalsIgnoreCase(typeName) || "jsonb".equalsIgnoreCase(typeName)) {
                LOG.info("JSON/JSONB type detected (column: {}), using text COPY", 
                         rsmd.getColumnName(i));
                return false;
            }
            
            // INTERVAL types require 16-byte binary struct (months, days, microseconds)
            // Text COPY is more reliable for interval representation
            if ("interval".equalsIgnoreCase(typeName)) {
                LOG.info("INTERVAL type detected (column: {}), using text COPY", 
                         rsmd.getColumnName(i));
                return false;
            }
        }
        
        // Then check for binary types (BYTEA/BLOB) and floating-point types
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            int type = rsmd.getColumnType(i);
            if (type == Types.BINARY || 
                type == Types.VARBINARY || 
                type == Types.LONGVARBINARY || 
                type == Types.BLOB) {
                hasBinary = true;
            }
            if (isMySQL && (type == Types.FLOAT || type == Types.DOUBLE || type == Types.REAL)) {
                hasFloatingPoint = true;
            }
        }
        
        if (isMySQL && hasFloatingPoint) {
            LOG.warn("MySQL source with FLOAT/DOUBLE columns detected. Falling back to text COPY " +
                     "because binary COPY format is incompatible with MySQL floating-point representation.");
            return false;  // Force TEXT COPY for MySQL with FLOAT columns
        }
        
        return hasBinary;
    }

    /**
     * Inserts data to PostgreSQL using binary COPY format.
     * Handles ONLY simple types (BYTEA, INTEGER, VARCHAR, etc.).
     * Throws exception if complex types (ARRAY, JSON, XML, INTERVAL) are encountered.
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
                            int intValue = resultSet.getInt(i);
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                dos.writeInt(4); // int32 is 4 bytes
                                dos.writeInt(intValue);
                            }
                            break;
                            
                        case Types.BIGINT:
                            long longValue = resultSet.getLong(i);
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                dos.writeInt(8); // int64 is 8 bytes
                                dos.writeLong(longValue);
                            }
                            break;
                            
                        case Types.SMALLINT:
                            short shortValue = resultSet.getShort(i);
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                dos.writeInt(2); // int16 is 2 bytes
                                dos.writeShort(shortValue);
                            }
                            break;
                            
                        case Types.BOOLEAN:
                        case Types.BIT:
                            boolean boolValue = resultSet.getBoolean(i);
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                dos.writeInt(1); // boolean is 1 byte
                                dos.writeByte(boolValue ? 1 : 0);
                            }
                            break;
                            
                        case Types.FLOAT:
                        case Types.REAL:
                            float floatValue = resultSet.getFloat(i);
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                byte[] floatBytes = new byte[4];
                                ByteConverter.float4(floatBytes, 0, floatValue);
                                dos.writeInt(4);
                                dos.write(floatBytes);
                            }
                            break;
                            
                        case Types.DOUBLE:
                            double doubleValue = resultSet.getDouble(i);
                            if (resultSet.wasNull()) {
                                dos.writeInt(-1);
                            } else {
                                byte[] doubleBytes = new byte[8];
                                ByteConverter.float8(doubleBytes, 0, doubleValue);
                                dos.writeInt(8); // double is 8 bytes
                                dos.write(doubleBytes);
                            }
                            break;
                            
                        case Types.DATE:
                            Date date = resultSet.getDate(i);
                            if (resultSet.wasNull() || date == null) {
                                dos.writeInt(-1);
                            } else {
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
                            Time time = resultSet.getTime(i);
                            if (resultSet.wasNull() || time == null) {
                                dos.writeInt(-1);
                            } else {
                                String typeName = rsmd.getColumnTypeName(i);
                                if ("timetz".equalsIgnoreCase(typeName)) {
                                    // TIME WITH TIME ZONE: 12 bytes (8 bytes time + 4 bytes timezone offset)
                                    // PostgreSQL stores as microseconds since midnight + timezone offset in seconds
                                    long microseconds = (time.getTime() % 86400000L) * 1000L;
                                    byte[] timeBytes = new byte[8];
                                    ByteConverter.int8(timeBytes, 0, microseconds);
                                    // Timezone offset: assuming UTC (0) for now since JDBC Time doesn't carry timezone
                                    byte[] tzBytes = new byte[4];
                                    ByteConverter.int4(tzBytes, 0, 0);
                                    dos.writeInt(12); // 12 bytes total
                                    dos.write(timeBytes);
                                    dos.write(tzBytes);
                                } else {
                                    // TIME WITHOUT TIME ZONE: 8 bytes (microseconds since midnight)
                                    long microseconds = (time.getTime() % 86400000L) * 1000L;
                                    byte[] timeBytes = new byte[8];
                                    ByteConverter.int8(timeBytes, 0, microseconds);
                                    dos.writeInt(8);
                                    dos.write(timeBytes);
                                }
                            }
                            break;
                            
                        case Types.TIMESTAMP:
                            Timestamp ts = resultSet.getTimestamp(i);
                            if (resultSet.wasNull() || ts == null) {
                                dos.writeInt(-1);
                            } else {
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
                            BigDecimal decimal = resultSet.getBigDecimal(i);
                            if (resultSet.wasNull() || decimal == null) {
                                dos.writeInt(-1);
                            } else {
                                try {
                                    // Validate BigDecimal before encoding
                                    // ByteConverter.numeric() is strict and fails on edge cases
                                    byte[] numericBytes = ByteConverter.numeric(decimal);
                                    dos.writeInt(numericBytes.length);
                                    dos.write(numericBytes);
                                } catch (Exception e) {
                                    // ByteConverter.numeric() can throw on invalid values
                                    // Fallback to text encoding for problematic numeric values
                                    LOG.warn("Binary encoding failed for NUMERIC column {} (value: {}), falling back to text: {}", 
                                            rsmd.getColumnName(i), decimal, e.getMessage());
                                    String textValue = decimal.toString();
                                    byte[] textBytes = textValue.getBytes(StandardCharsets.UTF_8);
                                    dos.writeInt(textBytes.length);
                                    dos.write(textBytes);
                                }
                            }
                            break;
                            
                        case Types.ARRAY:
                        case Types.SQLXML:
                            // Complex types should NEVER reach binary COPY path
                            // shouldUseBinaryCopy() must return false for these types
                            String complexColName = rsmd.getColumnName(i);
                            String complexTypeName = rsmd.getColumnTypeName(i);
                            LOG.warn("Binary COPY does not support {} type (column: {}, type code: {})", 
                                      complexTypeName, complexColName, columnType);
                            throw new IllegalStateException(
                                String.format("Binary COPY encountered unsupported type %s in column %s (type code: %d). " +
                                              "This indicates a bug in shouldUseBinaryCopy() detection. " +
                                              "Table should use TEXT COPY format.", 
                                              complexTypeName, complexColName, columnType));
                            
                        case Types.OTHER:
                            // Check for complex types that should use TEXT COPY
                            String typeName = rsmd.getColumnTypeName(i);
                            if ("json".equalsIgnoreCase(typeName) || 
                                "jsonb".equalsIgnoreCase(typeName) ||
                                "interval".equalsIgnoreCase(typeName)) {
                                LOG.warn("Binary COPY does not support {} type (column: {})", 
                                          typeName, rsmd.getColumnName(i));
                                throw new IllegalStateException(
                                    String.format("Binary COPY encountered unsupported type %s in column %s. " +
                                                  "This indicates a bug in shouldUseBinaryCopy() detection. " +
                                                  "Table should use TEXT COPY format.", 
                                                  typeName, rsmd.getColumnName(i)));
                            }
                            
                            // Generic OTHER type - fallback to text for simple types
                            String otherValue = resultSet.getString(i);
                            if (resultSet.wasNull() || otherValue == null) {
                                dos.writeInt(-1);
                            } else {
                                byte[] otherBytes = otherValue.getBytes(StandardCharsets.UTF_8);
                                dos.writeInt(otherBytes.length);
                                dos.write(otherBytes);
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

    @Override
    protected String mapJdbcTypeToNativeDDL(String columnName, int jdbcType, int precision, int scale) {
        switch (jdbcType) {
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
                return "INTEGER";
            case Types.BIGINT:
                return "BIGINT";
            case Types.FLOAT:
            case Types.DOUBLE:
                return "DOUBLE PRECISION";
            case Types.REAL:
                return "REAL";
            case Types.NUMERIC:
            case Types.DECIMAL:
                // Special case: Oracle REAL/DOUBLE PRECISION/FLOAT come through as NUMERIC with scale=-127
                // Map based on precision: REAL (p=63), DOUBLE PRECISION (p=126), or FLOAT (other)
                if (scale == -127) {
                    if (precision == 63) {
                        return "REAL";
                    } else if (precision == 126) {
                        return "DOUBLE PRECISION";
                    } else {
                        return "DOUBLE PRECISION";  // Default to DOUBLE PRECISION for other Oracle FLOATs
                    }
                }
                if (precision > 0) {
                    return "NUMERIC(" + precision + ", " + scale + ")";
                } else {
                    return "NUMERIC";
                }
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
                if (precision > 0 && precision <= 10485760) {
                    return "VARCHAR(" + precision + ")";
                } else {
                    return "TEXT";  // Fallback when precision is 0, undefined, or exceeds PostgreSQL limit
                }
            case Types.CHAR:
            case Types.NCHAR:
                if (precision > 0 && precision <= 10485760) {
                    return "CHAR(" + precision + ")";
                } else {
                    return "TEXT";  // Fallback when precision exceeds PostgreSQL limit
                }
            case Types.BOOLEAN:
            case Types.BIT:
                return "BOOLEAN";
            case Types.DATE:
                return "DATE";
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                return "TIME";
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return "TIMESTAMP";
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return "BYTEA";
            case Types.CLOB:
                return "TEXT";
            default:
                LOG.warn("Unmapped JDBC type {} for column {}, using TEXT as fallback", jdbcType, columnName);
                return "TEXT";
        }
    }


}

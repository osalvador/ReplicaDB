package org.replicadb.manager;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Adapter that wraps a JDBC {@link ResultSet} to be used with SQL Server bulk copy.
 * This adapter provides SQL Server compatible type mappings and streams LOB values
 * as {@link InputStream} / {@link Reader} without materializing them in memory.
 */
public class SQLServerResultSetBulkRecordAdapter implements ISQLServerBulkRecord {

    private static final Logger LOG = LogManager.getLogger(SQLServerResultSetBulkRecordAdapter.class);

    private final ResultSet resultSet;
    private final ResultSetMetaData metaData;
    private final int columnCount;
    private DateTimeFormatter dateTimeFormatter;
    private DateTimeFormatter timeFormatter;

    /**
     * Creates a new adapter wrapping the given {@link ResultSet}.
     *
     * @param resultSet the ResultSet to wrap
     * @throws SQLException if metadata cannot be retrieved
     */
    public SQLServerResultSetBulkRecordAdapter(ResultSet resultSet) throws SQLException {
        this.resultSet = resultSet;
        this.metaData = resultSet.getMetaData();
        this.columnCount = metaData.getColumnCount();
        LOG.debug("Created SQLServerResultSetBulkRecordAdapter with {} columns", columnCount);
    }

    @Override
    /**
     * Returns the 1-based column ordinals in order.
     *
     * @return ordered set of column ordinals
     */
    public Set<Integer> getColumnOrdinals() {
        Set<Integer> ordinals = new LinkedHashSet<>();
        for (int i = 1; i <= columnCount; i++) {
            ordinals.add(i);
        }
        return ordinals;
    }

    @Override
    /**
     * Returns the column name for the given ordinal.
     *
     * @param column 1-based column ordinal
     * @return column name or null on error
     */
    public String getColumnName(int column) {
        try {
            return metaData.getColumnName(column);
        } catch (SQLException e) {
            LOG.error("Error getting column name for column {}", column, e);
            return null;
        }
    }

    @Override
    /**
     * Returns a SQL Server compatible column type.
     *
     * @param column 1-based column ordinal
     * @return JDBC type for bulk copy
     */
    public int getColumnType(int column) {
        try {
            int type = metaData.getColumnType(column);
            
            // Standard JDBC types that appear negative in some drivers (e.g., MariaDB)
            // -5 = BIGINT, -7 = BIT - these are valid and should NOT be mapped to VARCHAR
            if (type == -5) {  // BIGINT
                return Types.BIGINT;
            }
            if (type == -7) {  // BIT
                return Types.BIT;
            }
            
            // Handle other truly unknown/unsupported types (negative or non-standard codes like Oracle's -104)
            if (type < -7) {
                LOG.debug("Mapping unsupported source type {} to VARCHAR for column {}", type, column);
                return Types.VARCHAR;
            }
            
            // Handle Oracle-specific types that SQL Server doesn't support
            if (type == Types.ROWID
                || type == Types.ARRAY
                || type == Types.STRUCT
                || type == Types.SQLXML
                || type == Types.OTHER) {
                LOG.debug("Mapping unsupported type {} to VARCHAR for column {}", type, column);
                return Types.VARCHAR;
            }
            
            if (type == Types.BOOLEAN) {
                return Types.BIT;
            }
            if (type == Types.BLOB || type == Types.LONGVARBINARY) {
                return Types.VARBINARY;
            }
            if (type == Types.CLOB || type == Types.LONGNVARCHAR) {
                return Types.NVARCHAR;
            }
            if (type == Types.BINARY) {
                return Types.VARBINARY;
            }
            return type;
        } catch (SQLException e) {
            LOG.error("Error getting column type for column {}", column, e);
            return Types.VARCHAR;
        }
    }

    @Override
    /**
     * Returns column precision for bulk copy metadata.
     * SQL Server maximum precision is 38 for NUMERIC/DECIMAL types.
     * VARCHAR/TEXT columns can be up to 8000, NVARCHAR up to 4000.
     *
     * @param column 1-based column ordinal
     * @return precision value (capped at 38 for NUMERIC only, appropriate limits for other types)
     */
    public int getPrecision(int column) {
        try {
            int sourceType = metaData.getColumnType(column);
            int precision = metaData.getPrecision(column);
            
            // For date/time types, SQL Server bulk copy has specific precision requirements
            if (sourceType == Types.TIMESTAMP || sourceType == Types.TIMESTAMP_WITH_TIMEZONE) {
                return 23;  // SQL Server DATETIME2(3) compatible precision
            }
            if (sourceType == Types.TIME || sourceType == Types.TIME_WITH_TIMEZONE) {
                return 16;  // SQL Server TIME(3) compatible precision
            }
            if (sourceType == Types.DATE) {
                return 10;  // SQL Server DATE precision
            }
            
            // For unbounded text types (precision <= 0 or very large), return appropriate defaults
            if (precision <= 0) {
                if (sourceType == Types.BLOB || sourceType == Types.LONGVARBINARY
                    || sourceType == Types.CLOB || sourceType == Types.LONGNVARCHAR) {
                    return -1;
                }
                int columnType = getColumnType(column);
                switch (columnType) {
                    case Types.VARCHAR:
                    case Types.CHAR:
                    case Types.LONGVARCHAR:
                        return 8000;
                    case Types.NVARCHAR:
                    case Types.NCHAR:
                    case Types.LONGNVARCHAR:
                        return 4000;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        return 8000;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        return 38;
                    case Types.FLOAT:
                    case Types.DOUBLE:
                    case Types.REAL:
                        return 53;
                    default:
                        return 38;
                }
            }
            
            // Get the target column type for SQL Server
            int columnType = getColumnType(column);
            
            // Only cap NUMERIC/DECIMAL to 38, not VARCHAR types
            if (columnType == Types.NUMERIC || columnType == Types.DECIMAL) {
                if (precision > 38) {
                    LOG.debug("Source precision {} exceeds SQL Server maximum of 38 for column {}, capping to 38", precision, column);
                    precision = 38;
                }
            } else if (columnType == Types.VARCHAR || columnType == Types.CHAR || columnType == Types.LONGVARCHAR) {
                // For VARCHAR types, cap at 8000 (SQL Server varchar limit)
                if (precision > 8000) {
                    LOG.debug("Source VARCHAR precision {} exceeds SQL Server maximum of 8000 for column {}, capping to 8000", precision, column);
                    precision = 8000;
                }
            } else if (columnType == Types.NVARCHAR || columnType == Types.NCHAR || columnType == Types.LONGNVARCHAR) {
                // For NVARCHAR types, cap at 4000 (SQL Server nvarchar limit)
                if (precision > 4000) {
                    LOG.debug("Source NVARCHAR precision {} exceeds SQL Server maximum of 4000 for column {}, capping to 4000", precision, column);
                    precision = 4000;
                }
            }
            
            return precision;
        } catch (SQLException e) {
            LOG.error("Error getting precision for column {}", column, e);
            return 38;
        }
    }

    @Override
    /**
     * Returns column scale for bulk copy metadata.
     * SQL Server requires scale to be between 0 and precision.
     *
     * @param column 1-based column ordinal
     * @return scale value (minimum 0, never negative)
     */
    public int getScale(int column) {
        try {
            int scale = metaData.getScale(column);
            // SQL Server requires scale >= 0. Invalid or negative scales (e.g., from Oracle metadata)
            // should default to 0
            if (scale < 0) {
                LOG.debug("Invalid scale {} for column {}, using default 0", scale, column);
                return 0;
            }
            return scale;
        } catch (SQLException e) {
            LOG.error("Error getting scale for column {}", column, e);
            return 0;
        }
    }

    @Override
    /**
     * Indicates whether the column is auto-increment.
     *
     * @param column 1-based column ordinal
     * @return true if auto-increment
     */
    public boolean isAutoIncrement(int column) {
        try {
            return metaData.isAutoIncrement(column);
        } catch (SQLException e) {
            LOG.error("Error checking auto increment for column {}", column, e);
            return false;
        }
    }

    @Override
    /**
     * Returns the formatter used for date/time columns.
     *
     * @param column 1-based column ordinal
     * @return formatter or null for defaults
     */
    public DateTimeFormatter getColumnDateTimeFormatter(int column) {
        try {
            int type = metaData.getColumnType(column);
            if (type == Types.TIME || type == Types.TIME_WITH_TIMEZONE) {
                return timeFormatter;
            }
            return dateTimeFormatter;
        } catch (SQLException e) {
            LOG.error("Error getting column type for formatter at column {}", column, e);
            return null;
        }
    }

    @Override
    /**
     * Sets timestamp with timezone format using a pattern.
     *
     * @param format date/time format pattern
     */
    public void setTimestampWithTimezoneFormat(String format) {
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(format);
    }

    @Override
    /**
     * Sets timestamp with timezone format using a formatter.
     *
     * @param formatter date/time formatter
     */
    public void setTimestampWithTimezoneFormat(DateTimeFormatter formatter) {
        this.dateTimeFormatter = formatter;
    }

    @Override
    /**
     * Sets time with timezone format using a pattern.
     *
     * @param format time format pattern
     */
    public void setTimeWithTimezoneFormat(String format) {
        this.timeFormatter = DateTimeFormatter.ofPattern(format);
    }

    @Override
    /**
     * Sets time with timezone format using a formatter.
     *
     * @param formatter time formatter
     */
    public void setTimeWithTimezoneFormat(DateTimeFormatter formatter) {
        this.timeFormatter = formatter;
    }

    @Override
    /**
     * No-op. Metadata is read from the ResultSet.
     *
     * @param positionInFile column position
     * @param columnName column name
     * @param jdbcType JDBC type
     * @param precision column precision
     * @param scale column scale
     */
    public void addColumnMetadata(int positionInFile, String columnName, int jdbcType, int precision, int scale) {
        LOG.trace("addColumnMetadata called for column {} at position {}", columnName, positionInFile);
    }

    @Override
    /**
     * No-op. Metadata is read from the ResultSet.
     *
     * @param positionInFile column position
     * @param columnName column name
     * @param jdbcType JDBC type
     * @param precision column precision
     * @param scale column scale
     * @param dateTimeFormatter formatter
     */
    public void addColumnMetadata(int positionInFile, String columnName, int jdbcType, int precision, int scale,
                                  DateTimeFormatter dateTimeFormatter) {
        LOG.trace("addColumnMetadata with formatter called for column {} at position {}", columnName, positionInFile);
    }

    @Override
    /**
     * Returns the current row values for bulk copy, deferring LOB streams until last.
     *
     * @return row values array
     */
    public Object[] getRowData() {
        try {
            Object[] rowData = new Object[columnCount];
            int[] columnTypes = new int[columnCount];
            int[] sourceTypes = new int[columnCount];
            boolean[] streamColumns = new boolean[columnCount];

            for (int i = 1; i <= columnCount; i++) {
                int columnType = getColumnType(i);
                columnTypes[i - 1] = columnType;
                int sourceType = metaData.getColumnType(i);
                sourceTypes[i - 1] = sourceType;
                streamColumns[i - 1] = sourceType == Types.BLOB
                    || sourceType == Types.CLOB
                    || sourceType == Types.LONGVARBINARY
                    || sourceType == Types.LONGNVARCHAR;
            }

            for (int i = 1; i <= columnCount; i++) {
                if (streamColumns[i - 1]) {
                    continue;
                }

                int columnType = columnTypes[i - 1];
                int sourceType = sourceTypes[i - 1];
                Object value;

                // Handle Oracle INTERVAL types by setting to NULL
                // (no direct SQL Server equivalent, string conversion causes bulk copy errors)
                if (sourceType == -104 || sourceType == -103) {  // INTERVALDS or INTERVALYM
                    LOG.debug("Skipping Oracle INTERVAL type {} for column {} (no SQL Server equivalent)", sourceType, i);
                    value = null;
                } else if (sourceType == Types.ROWID) {
                    // Convert ROWID to string
                    java.sql.RowId rowId = resultSet.getRowId(i);
                    value = resultSet.wasNull() ? null : (rowId != null ? new String(rowId.getBytes()) : null);
                    LOG.debug("Converted ROWID to string for column {}", i);
                } else if (sourceType == Types.ARRAY) {
                    // Convert ARRAY to string
                    java.sql.Array arrayData = resultSet.getArray(i);
                    value = resultSet.wasNull() ? null : (arrayData != null ? arrayData.toString() : null);
                    LOG.debug("Converted ARRAY to string for column {}", i);
                } else if (sourceType == Types.STRUCT) {
                    // Convert STRUCT to string
                    Object structObj = resultSet.getObject(i);
                    value = resultSet.wasNull() ? null : (structObj != null ? structObj.toString() : null);
                    LOG.debug("Converted STRUCT to string for column {}", i);
                } else if (sourceType == Types.SQLXML) {
                    // Skip SQLXML - converting to string causes bulk copy hex format errors
                    // SQL Server has native XML type, but bulk copy with string XML content fails
                    LOG.debug("Skipping SQLXML for column {} (causes bulk copy errors)", i);
                    value = null;
                } else if (sourceType == Types.OTHER) {
                    // Handle OTHER type (PostgreSQL specific types, etc.)
                    Object otherObj = resultSet.getObject(i);
                    if (resultSet.wasNull()) {
                        value = null;
                    } else if (otherObj != null) {
                        if (otherObj instanceof byte[]) {
                            value = otherObj;  // Keep as bytes for VARBINARY columns
                            LOG.debug("OTHER type is binary data for column {}", i);
                        } else if (otherObj instanceof String) {
                            // For text-based OTHER types, pass as-is
                            value = otherObj;
                            LOG.debug("OTHER type is string for column {}", i);
                        } else {
                            // For complex types, convert to string representation
                            value = otherObj.toString();
                            LOG.debug("Converted OTHER type to string for column {}", i);
                        }
                    } else {
                        value = null;
                    }
                } else if ((columnType == Types.VARBINARY || columnType == Types.LONGVARBINARY || columnType == Types.BINARY)
                    && sourceType != Types.BLOB) {
                    value = resultSet.getBytes(i);
                    // LOG.debug("Red bytes for binary column {}: {} bytes", i, value != null ? ((byte[]) value).length : "null");
                } else if (columnType == Types.NVARCHAR
                    && sourceType != Types.CLOB
                    && sourceType != Types.LONGNVARCHAR) {
                    value = resultSet.getString(i);
                } else {
                    value = resultSet.getObject(i);
                }

                if (value == null) {
                    rowData[i - 1] = null;
                    continue;
                }

                // Special handling: SQL Server bulk copy requires binary columns to contain
                // byte[] data. If we have non-binary source type with hex string data,
                // convert hex string to bytes. Applies to VARBINARY, LONGVARBINARY (image), BINARY, BLOB
                if ((columnType == Types.VARBINARY || columnType == Types.LONGVARBINARY || 
                     columnType == Types.BINARY || columnType == Types.BLOB) 
                    && sourceType != Types.BLOB && sourceType != Types.LONGVARBINARY 
                    && value instanceof String) {
                    String strValue = (String) value;
                    if (!strValue.isEmpty()) {
                        // Check if string is hex (from PostgreSQL encode(col, 'hex'))
                        if (strValue.matches("(?i)^[0-9a-f]+$")) {
                            // Convert hex string to byte array
                            value = hexStringToBytes(strValue);
                            LOG.debug("Converted hex string to byte[] for binary column {}: {} bytes", i, 
                                ((byte[])value).length);
                        } else {
                            // Not hex, convert string characters to bytes
                            value = strValue.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                            LOG.debug("Converted string to UTF-8 bytes for binary column {}: {} bytes", i, 
                                strValue.length());
                        }
                    } else {
                        value = null;
                    }
                }

                if (value != null && (columnType == Types.VARBINARY || columnType == Types.LONGVARBINARY || columnType == Types.BINARY || columnType == Types.BLOB)) {
                    if (value instanceof byte[]) {
                         LOG.debug("Column {} (Binary/Blob) value is byte array of length: {}", i, ((byte[])value).length);
                    } else {
                         LOG.debug("Column {} (Binary/Blob) value is NOT byte array. Class: {}. Value: {}", i, value.getClass().getName(), value);
                    }
                }

                if (value instanceof Integer && (columnType == Types.BIT || columnType == Types.BOOLEAN)) {
                    value = ((Integer) value) != 0;
                } else if (value instanceof BigDecimal && (columnType == Types.BIT || columnType == Types.BOOLEAN)) {
                    value = ((BigDecimal) value).intValue() != 0;
                } else if (value instanceof Timestamp) {
                    Timestamp ts = (Timestamp) value;
                    int nanos = ts.getNanos();
                    int millis = nanos / 1000000;
                    Timestamp truncated = new Timestamp(ts.getTime());
                    truncated.setNanos(millis * 1000000);
                    value = truncated;
                }

                rowData[i - 1] = value;
            }

            for (int i = 1; i <= columnCount; i++) {
                if (!streamColumns[i - 1]) {
                    continue;
                }

                int columnType = columnTypes[i - 1];
                Object value;

                if (columnType == Types.VARBINARY) {
                    InputStream stream = resultSet.getBinaryStream(i);
                    value = resultSet.wasNull() ? null : stream;
                } else {
                    Reader reader = resultSet.getCharacterStream(i);
                    value = resultSet.wasNull() ? null : reader;
                }

                rowData[i - 1] = value;
            }
            return rowData;
        } catch (SQLException e) {
            LOG.error("Error getting row data", e);
            throw new RuntimeException("Error getting row data from ResultSet", e);
        }
    }

    @Override
    /**
     * Advances to the next row in the ResultSet.
     *
     * @return true if another row is available
     */
    public boolean next() {
        try {
            return resultSet.next();
        } catch (SQLException e) {
            LOG.error("Error advancing to next row", e);
            throw new RuntimeException("Error advancing to next row in ResultSet", e);
        }
    }

    /**
     * Converts a hexadecimal string to byte array (for VARBINARY columns).
     * For example: "48656c6c6f" -> byte[] {0x48, 0x65, 0x6c, 0x6c, 0x6f}
     *
     * @param hexString the hex string to convert (without 0x prefix)
     * @return byte array
     */
    private byte[] hexStringToBytes(String hexString) {
        int len = hexString.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                    + Character.digit(hexString.charAt(i + 1), 16));
        }
        return data;
    }
}

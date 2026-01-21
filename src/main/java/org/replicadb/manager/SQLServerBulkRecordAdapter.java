package org.replicadb.manager;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.RowSet;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Adapter class that wraps any RowSet to be used with SQLServerBulkCopy.
 * This adapter handles type conversions that SQLServerBulkCopy doesn't support natively,
 * such as converting Boolean to Integer (0/1) for BIT columns.
 */
public class SQLServerBulkRecordAdapter implements ISQLServerBulkRecord {

    private static final Logger LOG = LogManager.getLogger(SQLServerBulkRecordAdapter.class);

    private final RowSet rowSet;
    private final ResultSetMetaData metaData;
    private final int columnCount;
    private DateTimeFormatter dateTimeFormatter;
    private DateTimeFormatter timeFormatter;

    /**
     * Creates a new adapter wrapping the given RowSet.
     *
     * @param rowSet the RowSet to wrap
     * @throws SQLException if metadata cannot be retrieved
     */
    public SQLServerBulkRecordAdapter(RowSet rowSet) throws SQLException {
        this.rowSet = rowSet;
        this.metaData = rowSet.getMetaData();
        this.columnCount = metaData.getColumnCount();
        LOG.debug("Created SQLServerBulkRecordAdapter with {} columns", columnCount);
    }

    @Override
    public Set<Integer> getColumnOrdinals() {
        Set<Integer> ordinals = new LinkedHashSet<>();
        for (int i = 1; i <= columnCount; i++) {
            ordinals.add(i);
        }
        return ordinals;
    }

    @Override
    public String getColumnName(int column) {
        try {
            return metaData.getColumnName(column);
        } catch (SQLException e) {
            LOG.error("Error getting column name for column {}", column, e);
            return null;
        }
    }

    @Override
    public int getColumnType(int column) {
        try {
            int type = metaData.getColumnType(column);
            // Convert BOOLEAN type to BIT for SQL Server compatibility
            if (type == Types.BOOLEAN) {
                return Types.BIT;
            }
            // Convert BINARY to VARBINARY for SQL Server BulkCopy compatibility
            // SQL Server BulkCopy expects VARBINARY for binary data, not BINARY
            // This is especially important when the source (e.g., MongoDB) reports Types.BINARY
            // but the sink column is varbinary(max) or image
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
    public int getPrecision(int column) {
        try {
            int precision = metaData.getPrecision(column);
            // SQL Server BulkCopy requires non-zero precision for variable-length types
            if (precision <= 0) {
                int type = getColumnType(column);
                switch (type) {
                    case Types.VARCHAR:
                    case Types.CHAR:
                    case Types.LONGVARCHAR:
                        return 8000; // SQL Server VARCHAR max without MAX
                    case Types.NVARCHAR:
                    case Types.NCHAR:
                    case Types.LONGNVARCHAR:
                        return 4000; // SQL Server NVARCHAR max without MAX
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        return 8000; // SQL Server VARBINARY max without MAX
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        return 38; // SQL Server max precision for DECIMAL
                    case Types.FLOAT:
                    case Types.DOUBLE:
                    case Types.REAL:
                        return 53; // SQL Server FLOAT precision
                    default:
                        return 38; // Safe default
                }
            }
            return precision;
        } catch (SQLException e) {
            LOG.error("Error getting precision for column {}", column, e);
            return 38; // Safe default instead of 0
        }
    }

    @Override
    public int getScale(int column) {
        try {
            int scale = metaData.getScale(column);
            // For DECIMAL/NUMERIC with 0 scale, keep it (it's valid for integers)
            // Only provide default if we can't get it
            return scale;
        } catch (SQLException e) {
            LOG.error("Error getting scale for column {}", column, e);
            return 0; // 0 is valid for scale
        }
    }

    @Override
    public boolean isAutoIncrement(int column) {
        try {
            return metaData.isAutoIncrement(column);
        } catch (SQLException e) {
            LOG.error("Error checking auto increment for column {}", column, e);
            return false;
        }
    }

    @Override
    public DateTimeFormatter getColumnDateTimeFormatter(int column) {
        // Return null to use default formatting
        return null;
    }

    @Override
    public void setTimestampWithTimezoneFormat(String format) {
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(format);
    }

    @Override
    public void setTimestampWithTimezoneFormat(DateTimeFormatter formatter) {
        this.dateTimeFormatter = formatter;
    }

    @Override
    public void setTimeWithTimezoneFormat(String format) {
        this.timeFormatter = DateTimeFormatter.ofPattern(format);
    }

    @Override
    public void setTimeWithTimezoneFormat(DateTimeFormatter formatter) {
        this.timeFormatter = formatter;
    }

    @Override
    public void addColumnMetadata(int positionInFile, String columnName, int jdbcType, int precision, int scale) {
        // Not needed - we read metadata from the RowSet
        LOG.trace("addColumnMetadata called for column {} at position {}", columnName, positionInFile);
    }

    @Override
    public void addColumnMetadata(int positionInFile, String columnName, int jdbcType, int precision, int scale, DateTimeFormatter dateTimeFormatter) {
        // Not needed - we read metadata from the RowSet
        LOG.trace("addColumnMetadata with formatter called for column {} at position {}", columnName, positionInFile);
    }

    @Override
    public Object[] getRowData() {
        try {
            Object[] rowData = new Object[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                Object value = rowSet.getObject(i);
                int columnType = getColumnType(i);

                // Handle null values
                if (value == null) {
                    rowData[i - 1] = null;
                    continue;
                }

                // Convert String values to appropriate types for SQL Server
                if (value instanceof String) {
                    String strValue = (String) value;
                    if (strValue.isEmpty()) {
                        // Empty string handling based on column type
                        switch (columnType) {
                            case Types.BIT:
                            case Types.BOOLEAN:
                            case Types.INTEGER:
                            case Types.BIGINT:
                            case Types.SMALLINT:
                            case Types.TINYINT:
                            case Types.DECIMAL:
                            case Types.NUMERIC:
                            case Types.FLOAT:
                            case Types.DOUBLE:
                            case Types.REAL:
                            case Types.DATE:
                            case Types.TIME:
                            case Types.TIMESTAMP:
                                value = null;
                                break;
                            case Types.BINARY:
                            case Types.VARBINARY:
                            case Types.LONGVARBINARY:
                                value = new byte[0];
                                break;
                            default:
                                break;
                        }
                    } else {
                        // Parse non-empty string values
                        switch (columnType) {
                            case Types.BIT:
                            case Types.BOOLEAN:
                                value = "true".equalsIgnoreCase(strValue) || "1".equals(strValue);
                                break;
                            case Types.INTEGER:
                                value = Integer.parseInt(strValue);
                                break;
                            case Types.BIGINT:
                                value = Long.parseLong(strValue);
                                break;
                            case Types.SMALLINT:
                            case Types.TINYINT:
                                value = Short.parseShort(strValue);
                                break;
                            case Types.FLOAT:
                            case Types.DOUBLE:
                            case Types.REAL:
                                value = Double.parseDouble(strValue);
                                break;
                            case Types.DECIMAL:
                            case Types.NUMERIC:
                                value = new java.math.BigDecimal(strValue);
                                break;
                            case Types.DATE:
                                try {
                                    value = java.sql.Date.valueOf(strValue);
                                } catch (IllegalArgumentException e) {
                                    LOG.warn("Failed to parse date '{}' for column {}, setting to null: {}", strValue, i, e.getMessage());
                                    value = null;
                                }
                                break;
                            case Types.TIME:
                                try {
                                    value = java.sql.Time.valueOf(strValue);
                                } catch (IllegalArgumentException e) {
                                    LOG.warn("Failed to parse time '{}' for column {}, setting to null: {}", strValue, i, e.getMessage());
                                    value = null;
                                }
                                break;
                            case Types.TIMESTAMP:
                                try {
                                    // SQL Server datetime has precision up to milliseconds (3 digits)
                                    // Truncate microseconds if present (more than 3 decimal places)
                                    String timestampStr = strValue;
                                    if (timestampStr.contains(".")) {
                                        String[] parts = timestampStr.split("\\.");
                                        if (parts.length == 2 && parts[1].length() > 3) {
                                            // Truncate to 3 digits (milliseconds only)
                                            timestampStr = parts[0] + "." + parts[1].substring(0, 3);
                                        }
                                    }
                                    value = java.sql.Timestamp.valueOf(timestampStr);
                                } catch (IllegalArgumentException e) {
                                    LOG.warn("Failed to parse timestamp '{}' for column {}, setting to null: {}", strValue, i, e.getMessage());
                                    value = null;
                                }
                                break;
                            case Types.BINARY:
                            case Types.VARBINARY:
                            case Types.LONGVARBINARY:
                                // Convert String to byte array for binary columns
                                value = strValue.getBytes(StandardCharsets.UTF_8);
                                break;
                            default:
                                break;
                        }
                    }
                }
                // Handle byte arrays for binary types - keep as is
                else if (value instanceof byte[]) {
                    // byte[] is already the correct type for BINARY/VARBINARY columns
                }
                // Keep Boolean as Boolean for BIT columns (SQL Server BulkCopy expects Boolean, not Integer)
                else if (value instanceof Boolean) {
                    // No conversion needed - SQL Server BulkCopy handles Boolean for BIT columns
                }
                // Convert Integer to Boolean for BIT columns when source provides Integer
                else if (value instanceof Integer && (columnType == Types.BIT || columnType == Types.BOOLEAN)) {
                    value = ((Integer) value) != 0;
                }
                // Convert BigDecimal to appropriate numeric type for SQL Server
                else if (value instanceof java.math.BigDecimal) {
                    java.math.BigDecimal bd = (java.math.BigDecimal) value;
                    switch (columnType) {
                        case Types.BIT:
                        case Types.BOOLEAN:
                            value = bd.intValue() != 0;
                            break;
                        case Types.BIGINT:
                            value = bd.longValue();
                            break;
                        case Types.INTEGER:
                            value = bd.intValue();
                            break;
                        case Types.SMALLINT:
                        case Types.TINYINT:
                            value = bd.shortValue();
                            break;
                        case Types.FLOAT:
                        case Types.DOUBLE:
                            value = bd.doubleValue();
                            break;
                        case Types.REAL:
                            value = bd.floatValue();
                            break;
                        // For DECIMAL/NUMERIC, keep as BigDecimal
                        default:
                            break;
                    }
                }
                // Handle java.sql.Timestamp with nanoseconds precision for SQL Server
                // SQL Server datetime/datetime2 has limited precision, need to truncate nanoseconds
                // This applies regardless of the declared columnType since Timestamps can be used for various date/time columns
                else if (value instanceof java.sql.Timestamp) {
                    java.sql.Timestamp ts = (java.sql.Timestamp) value;
                    // SQL Server datetime has precision up to milliseconds (3 digits)
                    // Get nanoseconds and truncate to milliseconds precision
                    int nanos = ts.getNanos();
                    int millis = nanos / 1000000; // Convert to milliseconds
                    // Create new timestamp with truncated precision
                    java.sql.Timestamp truncated = new java.sql.Timestamp(ts.getTime());
                    truncated.setNanos(millis * 1000000); // Set back with millisecond precision only
                    value = truncated;
                    LOG.trace("Truncated Timestamp nanoseconds from {} to {} for column {}", nanos, millis * 1000000, i);
                }

                rowData[i - 1] = value;
            }
            return rowData;
        } catch (SQLException e) {
            LOG.error("Error getting row data", e);
            throw new RuntimeException("Error getting row data from RowSet", e);
        }
    }

    @Override
    public boolean next() {
        try {
            return rowSet.next();
        } catch (SQLException e) {
            LOG.error("Error advancing to next row", e);
            throw new RuntimeException("Error advancing to next row in RowSet", e);
        }
    }
}

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
     *
     * @param column 1-based column ordinal
     * @return precision value
     */
    public int getPrecision(int column) {
        try {
            int sourceType = metaData.getColumnType(column);
            
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
            
            int precision = metaData.getPrecision(column);
            if (precision <= 0) {
                if (sourceType == Types.BLOB || sourceType == Types.LONGVARBINARY
                    || sourceType == Types.CLOB || sourceType == Types.LONGNVARCHAR) {
                    return -1;
                }
                int type = getColumnType(column);
                switch (type) {
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
            return precision;
        } catch (SQLException e) {
            LOG.error("Error getting precision for column {}", column, e);
            return 38;
        }
    }

    @Override
    /**
     * Returns column scale for bulk copy metadata.
     *
     * @param column 1-based column ordinal
     * @return scale value
     */
    public int getScale(int column) {
        try {
            return metaData.getScale(column);
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

                if (columnType == Types.VARBINARY
                    && sourceType != Types.BLOB
                    && sourceType != Types.LONGVARBINARY) {
                    value = resultSet.getBytes(i);
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
}

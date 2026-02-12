package org.replicadb.manager.util;

/**
 * Data class to store column metadata extracted from ResultSetMetaData.
 * This avoids holding references to ResultSetMetaData after the statement/resultset is closed.
 */
public class ColumnDescriptor {
    private final String columnName;
    private final int jdbcType;
    private final int precision;
    private final int scale;
    private final int nullable;

    public ColumnDescriptor(String columnName, int jdbcType, int precision, int scale, int nullable) {
        this.columnName = columnName;
        this.jdbcType = jdbcType;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public int getNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return "ColumnDescriptor{" +
                "columnName='" + columnName + '\'' +
                ", jdbcType=" + jdbcType +
                ", precision=" + precision +
                ", scale=" + scale +
                ", nullable=" + nullable +
                '}';
    }
}

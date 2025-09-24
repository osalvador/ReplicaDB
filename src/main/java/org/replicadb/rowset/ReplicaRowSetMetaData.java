package org.replicadb.rowset;

import javax.sql.rowset.RowSetMetaDataImpl;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Custom metadata implementation for ReplicaDB rowsets. Extends the standard
 * RowSetMetaDataImpl to provide additional functionality needed for file-based
 * data sources.
 */
public class ReplicaRowSetMetaData extends RowSetMetaDataImpl {

	/**
	 * Default constructor.
	 */
	public ReplicaRowSetMetaData() {
		super();
	}

	/**
	 * Sets up metadata for a simple single-column rowset. Useful for document-based
	 * sources like MongoDB.
	 *
	 * @param columnName
	 *            the name of the single column
	 * @param columnType
	 *            the SQL type of the column
	 * @throws SQLException
	 *             if an error occurs
	 */
	public void setSingleColumnMetadata(String columnName, int columnType) throws SQLException {
        this.setColumnCount(1);
        this.setColumnName(1, columnName);
        this.setColumnType(1, columnType);
        this.setColumnTypeName(1, this.getTypeName(columnType));
	}

	/**
	 * Sets up metadata for multiple columns with names and types.
	 *
	 * @param columnNames
	 *            array of column names
	 * @param columnTypes
	 *            array of corresponding SQL types
	 * @throws SQLException
	 *             if an error occurs or arrays have different lengths
	 */
	public void setMultiColumnMetadata(String[] columnNames, int[] columnTypes) throws SQLException {
		if (columnNames.length != columnTypes.length) {
			throw new SQLException("Column names and types arrays must have the same length");
		}

		final int columnCount = columnNames.length;
        this.setColumnCount(columnCount);

		for (int i = 0; i < columnCount; i++) {
			final int colIndex = i + 1; // JDBC uses 1-based indexing
            this.setColumnName(colIndex, columnNames[i]);
            this.setColumnType(colIndex, columnTypes[i]);
            this.setColumnTypeName(colIndex, this.getTypeName(columnTypes[i]));

			// Set reasonable defaults
            this.setAutoIncrement(colIndex, false);
            this.setCaseSensitive(colIndex, true);
            this.setSearchable(colIndex, true);
            this.setCurrency(colIndex, false);
            this.setNullable(colIndex, ResultSetMetaData.columnNullable);
            this.setSigned(colIndex, this.isSignedType(columnTypes[i]));

			// Set precision and scale based on type
            this.setPrecision(colIndex, this.getDefaultPrecision(columnTypes[i]));
            this.setScale(colIndex, this.getDefaultScale(columnTypes[i]));
		}
	}

	/**
	 * Gets the type name for a SQL type constant.
	 *
	 * @param sqlType
	 *            the SQL type constant
	 * @return the type name
	 */
	private String getTypeName(int sqlType) {
		switch (sqlType) {
			case Types.VARCHAR :
				return "VARCHAR";
			case Types.CHAR :
				return "CHAR";
			case Types.LONGVARCHAR :
				return "LONGVARCHAR";
			case Types.INTEGER :
				return "INTEGER";
			case Types.BIGINT :
				return "BIGINT";
			case Types.SMALLINT :
				return "SMALLINT";
			case Types.TINYINT :
				return "TINYINT";
			case Types.DOUBLE :
				return "DOUBLE";
			case Types.FLOAT :
				return "FLOAT";
			case Types.DECIMAL :
				return "DECIMAL";
			case Types.NUMERIC :
				return "NUMERIC";
			case Types.BOOLEAN :
				return "BOOLEAN";
			case Types.DATE :
				return "DATE";
			case Types.TIME :
				return "TIME";
			case Types.TIMESTAMP :
				return "TIMESTAMP";
			case Types.TIMESTAMP_WITH_TIMEZONE :
				return "TIMESTAMP_WITH_TIMEZONE";
			case Types.BINARY :
				return "BINARY";
			case Types.VARBINARY :
				return "VARBINARY";
			case Types.LONGVARBINARY :
				return "LONGVARBINARY";
			case Types.ARRAY :
				return "ARRAY";
			case Types.STRUCT :
				return "STRUCT";
			case Types.ROWID :
				return "ROWID";
			case Types.OTHER :
				return "OTHER";
			default :
				return "UNKNOWN";
		}
	}

	/**
	 * Determines if a SQL type is signed.
	 *
	 * @param sqlType
	 *            the SQL type constant
	 * @return true if the type is signed
	 */
	private boolean isSignedType(int sqlType) {
		switch (sqlType) {
			case Types.INTEGER :
			case Types.BIGINT :
			case Types.SMALLINT :
			case Types.TINYINT :
			case Types.DOUBLE :
			case Types.FLOAT :
			case Types.DECIMAL :
			case Types.NUMERIC :
				return true;
			default :
				return false;
		}
	}

	/**
	 * Gets the default precision for a SQL type.
	 *
	 * @param sqlType
	 *            the SQL type constant
	 * @return the default precision
	 */
	private int getDefaultPrecision(int sqlType) {
		switch (sqlType) {
			case Types.INTEGER :
				return 10;
			case Types.BIGINT :
				return 19;
			case Types.SMALLINT :
				return 5;
			case Types.TINYINT :
				return 3;
			case Types.DOUBLE :
				return 15;
			case Types.FLOAT :
				return 7;
			case Types.DECIMAL :
			case Types.NUMERIC :
				return 10;
			default :
				return 0;
		}
	}

	/**
	 * Gets the default scale for a SQL type.
	 *
	 * @param sqlType
	 *            the SQL type constant
	 * @return the default scale
	 */
	private int getDefaultScale(int sqlType) {
		switch (sqlType) {
			case Types.DECIMAL :
			case Types.NUMERIC :
				return 2;
			default :
				return 0;
		}
	}

	/**
	 * Utility method to infer SQL type from a Java object. Used by file-based
	 * implementations to determine appropriate types.
	 *
	 * @param obj
	 *            the Java object
	 * @return the corresponding SQL type constant
	 */
	public static int inferSqlType(Object obj) {
		if (obj == null) {
			return Types.VARCHAR; // Default for null values
		}

		final Class<?> clazz = obj.getClass();

		if (clazz == String.class) {
			return Types.VARCHAR;
		} else if (clazz == Integer.class || clazz == int.class) {
			return Types.INTEGER;
		} else if (clazz == Long.class || clazz == long.class) {
			return Types.BIGINT;
		} else if (clazz == Double.class || clazz == double.class) {
			return Types.DOUBLE;
		} else if (clazz == Float.class || clazz == float.class) {
			return Types.FLOAT;
		} else if (clazz == Boolean.class || clazz == boolean.class) {
			return Types.BOOLEAN;
		} else if (clazz == Short.class || clazz == short.class) {
			return Types.SMALLINT;
		} else if (clazz == Byte.class || clazz == byte.class) {
			return Types.TINYINT;
		} else if (clazz == java.math.BigDecimal.class) {
			return Types.DECIMAL;
		} else if (clazz == java.sql.Date.class) {
			return Types.DATE;
		} else if (clazz == java.sql.Time.class) {
			return Types.TIME;
		} else if (clazz == java.sql.Timestamp.class || clazz == java.util.Date.class) {
			return Types.TIMESTAMP;
		} else if (clazz == byte[].class) {
			return Types.BINARY;
		} else {
			return Types.OTHER;
		}
	}
}

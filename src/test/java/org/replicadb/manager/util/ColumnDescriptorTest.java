package org.replicadb.manager.util;

import org.junit.jupiter.api.Test;

import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ColumnDescriptor data class.
 * Verifies that column metadata is correctly captured and stored.
 */
class ColumnDescriptorTest {

    @Test
    void testConstructor_allFieldsSet() {
        // Given: Column metadata values
        String columnName = "user_id";
        int jdbcType = Types.INTEGER;
        int precision = 10;
        int scale = 0;
        int nullable = ResultSetMetaData.columnNoNulls;

        // When: ColumnDescriptor is created
        ColumnDescriptor descriptor = new ColumnDescriptor(columnName, jdbcType, precision, scale, nullable);

        // Then: All fields should be accessible
        assertEquals(columnName, descriptor.getColumnName(), "Column name should match");
        assertEquals(jdbcType, descriptor.getJdbcType(), "JDBC type should match");
        assertEquals(precision, descriptor.getPrecision(), "Precision should match");
        assertEquals(scale, descriptor.getScale(), "Scale should match");
        assertEquals(nullable, descriptor.getNullable(), "Nullable should match");
    }

    @Test
    void testConstructor_varcharWithPrecision() {
        // Given: VARCHAR column with precision
        ColumnDescriptor descriptor = new ColumnDescriptor(
            "username",
            Types.VARCHAR,
            255,
            0,
            ResultSetMetaData.columnNullable
        );

        // Then: Fields should be correctly stored
        assertEquals("username", descriptor.getColumnName());
        assertEquals(Types.VARCHAR, descriptor.getJdbcType());
        assertEquals(255, descriptor.getPrecision());
        assertEquals(0, descriptor.getScale());
        assertEquals(ResultSetMetaData.columnNullable, descriptor.getNullable());
    }

    @Test
    void testConstructor_decimalWithScale() {
        // Given: DECIMAL column with precision and scale
        ColumnDescriptor descriptor = new ColumnDescriptor(
            "price",
            Types.DECIMAL,
            10,
            2,
            ResultSetMetaData.columnNoNulls
        );

        // Then: Precision and scale should be correctly stored
        assertEquals("price", descriptor.getColumnName());
        assertEquals(Types.DECIMAL, descriptor.getJdbcType());
        assertEquals(10, descriptor.getPrecision());
        assertEquals(2, descriptor.getScale());
        assertEquals(ResultSetMetaData.columnNoNulls, descriptor.getNullable());
    }

    @Test
    void testConstructor_nullableColumn() {
        // Given: Nullable timestamp column
        ColumnDescriptor descriptor = new ColumnDescriptor(
            "created_at",
            Types.TIMESTAMP,
            0,
            0,
            ResultSetMetaData.columnNullable
        );

        // Then: Nullable should be columnNullable
        assertEquals(ResultSetMetaData.columnNullable, descriptor.getNullable());
    }

    @Test
    void testConstructor_notNullColumn() {
        // Given: NOT NULL integer column
        ColumnDescriptor descriptor = new ColumnDescriptor(
            "id",
            Types.BIGINT,
            19,
            0,
            ResultSetMetaData.columnNoNulls
        );

        // Then: Nullable should be columnNoNulls
        assertEquals(ResultSetMetaData.columnNoNulls, descriptor.getNullable());
    }

    @Test
    void testConstructor_binaryTypeWithPrecision() {
        // Given: VARBINARY column
        ColumnDescriptor descriptor = new ColumnDescriptor(
            "file_data",
            Types.VARBINARY,
            1000,
            0,
            ResultSetMetaData.columnNullable
        );

        // Then: Should correctly store binary type
        assertEquals("file_data", descriptor.getColumnName());
        assertEquals(Types.VARBINARY, descriptor.getJdbcType());
        assertEquals(1000, descriptor.getPrecision());
    }

    @Test
    void testConstructor_dateTimeType() {
        // Given: DATE column
        ColumnDescriptor descriptor = new ColumnDescriptor(
            "birth_date",
            Types.DATE,
            0,
            0,
            ResultSetMetaData.columnNullable
        );

        // Then: Should correctly store date type
        assertEquals("birth_date", descriptor.getColumnName());
        assertEquals(Types.DATE, descriptor.getJdbcType());
    }

    @Test
    void testConstructor_clobType() {
        // Given: CLOB column
        ColumnDescriptor descriptor = new ColumnDescriptor(
            "description",
            Types.CLOB,
            0,
            0,
            ResultSetMetaData.columnNullable
        );

        // Then: Should correctly store CLOB type
        assertEquals("description", descriptor.getColumnName());
        assertEquals(Types.CLOB, descriptor.getJdbcType());
    }

    @Test
    void testConstructor_columnNullableUnknown() {
        // Given: Column with unknown nullable status
        ColumnDescriptor descriptor = new ColumnDescriptor(
            "optional_field",
            Types.VARCHAR,
            100,
            0,
            ResultSetMetaData.columnNullableUnknown
        );

        // Then: Should correctly store unknown nullable
        assertEquals(ResultSetMetaData.columnNullableUnknown, descriptor.getNullable());
    }

    @Test
    void testNullable_columnNoNulls() {
        // Given: NOT NULL column
        ColumnDescriptor descriptor = new ColumnDescriptor(
            "id",
            Types.INTEGER,
            10,
            0,
            ResultSetMetaData.columnNoNulls
        );

        // Then: getNullable should return columnNoNulls
        assertEquals(ResultSetMetaData.columnNoNulls, descriptor.getNullable(), 
            "Column with columnNoNulls should return correct value");
    }

    @Test
    void testNullable_columnNullable() {
        // Given: Nullable column
        ColumnDescriptor descriptor = new ColumnDescriptor(
            "optional",
            Types.VARCHAR,
            50,
            0,
            ResultSetMetaData.columnNullable
        );

        // Then: getNullable should return columnNullable
        assertEquals(ResultSetMetaData.columnNullable, descriptor.getNullable(),
            "Column with columnNullable should return correct value");
    }

    @Test
    void testNullable_columnNullableUnknown() {
        // Given: Column with unknown nullable status
        ColumnDescriptor descriptor = new ColumnDescriptor(
            "unknown",
            Types.VARCHAR,
            50,
            0,
            ResultSetMetaData.columnNullableUnknown
        );

        // Then: getNullable should return columnNullableUnknown
        assertEquals(ResultSetMetaData.columnNullableUnknown, descriptor.getNullable(),
            "Column with columnNullableUnknown should return correct value");
    }

    @Test
    void testToString_containsAllFields() {
        // Given: A column descriptor
        ColumnDescriptor descriptor = new ColumnDescriptor(
            "test_column",
            Types.VARCHAR,
            255,
            0,
            ResultSetMetaData.columnNoNulls
        );

        // When: toString is called
        String result = descriptor.toString();

        // Then: Should contain key information
        assertNotNull(result);
        assertTrue(result.contains("test_column"), "toString should contain column name");
        assertTrue(result.contains("VARCHAR") || result.contains("12"), "toString should contain type info");
    }
}

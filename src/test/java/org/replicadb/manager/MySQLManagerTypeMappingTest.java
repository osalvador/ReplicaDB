package org.replicadb.manager;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;

import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for MySQLManager's mapJdbcTypeToNativeDDL method.
 * Tests type mapping behavior including cross-database scenarios (e.g., Oracle -> MySQL).
 */
class MySQLManagerTypeMappingTest {

    private MySQLManager createManager() throws Exception {
        String[] args = {
            "--source-connect", "jdbc:mysql://localhost:3306/source",
            "--source-table", "source_table",
            "--sink-connect", "jdbc:mysql://localhost:3306/sink",
            "--sink-table", "sink_table"
        };
        ToolOptions options = new ToolOptions(args);
        return new MySQLManager(options, DataSourceType.SINK);
    }

    @Test
    void testMapJdbcType_VarcharWithPrecision() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_varchar", Types.VARCHAR, 255, 0);
        assertEquals("VARCHAR(255)", result);
    }

    @Test
    void testMapJdbcType_VarcharMaxLength() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_varchar", Types.VARCHAR, 16383, 0);
        assertEquals("VARCHAR(16383)", result);
    }

    @Test
    void testMapJdbcType_VarcharExceedsMaxLength() throws Exception {
        MySQLManager manager = createManager();
        // Precision exceeds MySQL VARCHAR limit, should fallback to LONGTEXT
        String result = manager.mapJdbcTypeToNativeDDL("c_varchar", Types.VARCHAR, 70000, 0);
        assertEquals("LONGTEXT", result);
    }

    @Test
    void testMapJdbcType_Char() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_char", Types.CHAR, 50, 0);
        assertEquals("CHAR(50)", result);
    }

    @Test
    void testMapJdbcType_CharMaxLength() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_char", Types.CHAR, 255, 0);
        assertEquals("CHAR(255)", result);
    }

    @Test
    void testMapJdbcType_Integer() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_int", Types.INTEGER, 10, 0);
        assertEquals("INT", result);
    }

    @Test
    void testMapJdbcType_BigInt() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_bigint", Types.BIGINT, 19, 0);
        assertEquals("BIGINT", result);
    }

    @Test
    void testMapJdbcType_SmallInt() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_smallint", Types.SMALLINT, 5, 0);
        assertEquals("SMALLINT", result);
    }

    @Test
    void testMapJdbcType_TinyInt() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_tinyint", Types.TINYINT, 3, 0);
        assertEquals("TINYINT", result);
    }

    @Test
    void testMapJdbcType_DecimalWithPrecisionAndScale() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_decimal", Types.DECIMAL, 10, 2);
        assertEquals("DECIMAL(10, 2)", result);
    }

    @Test
    void testMapJdbcType_DecimalWithZeroScale() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_decimal", Types.DECIMAL, 38, 0);
        assertEquals("DECIMAL(38, 0)", result);
    }

    @Test
    void testMapJdbcType_NumericWithPrecisionAndScale() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_numeric", Types.NUMERIC, 20, 5);
        assertEquals("DECIMAL(20, 5)", result);
    }

    @Test
    void testMapJdbcType_Real() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_real", Types.REAL, 7, 0);
        assertEquals("FLOAT", result);
    }

    @Test
    void testMapJdbcType_Float() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_float", Types.FLOAT, 15, 0);
        assertEquals("FLOAT", result);
    }

    @Test
    void testMapJdbcType_Double() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_double", Types.DOUBLE, 15, 0);
        assertEquals("DOUBLE", result);
    }

    @Test
    void testMapJdbcType_Date() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_date", Types.DATE, 0, 0);
        assertEquals("DATE", result);
    }

    @Test
    void testMapJdbcType_Time() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_time", Types.TIME, 0, 0);
        assertEquals("TIME", result);
    }

    @Test
    void testMapJdbcType_Timestamp() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_timestamp", Types.TIMESTAMP, 0, 0);
        assertEquals("DATETIME", result);
    }

    @Test
    void testMapJdbcType_Boolean() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_boolean", Types.BOOLEAN, 0, 0);
        assertEquals("BOOLEAN", result);
    }

    @Test
    void testMapJdbcType_Bit() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_bit", Types.BIT, 1, 0);
        assertEquals("BOOLEAN", result);
    }

    @Test
    void testMapJdbcType_Binary() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_binary", Types.BINARY, 100, 0);
        assertEquals("VARBINARY(100)", result);
    }

    @Test
    void testMapJdbcType_VarBinary() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_varbinary", Types.VARBINARY, 255, 0);
        assertEquals("VARBINARY(255)", result);
    }

    @Test
    void testMapJdbcType_VarBinaryExceedsMaxLength() throws Exception {
        MySQLManager manager = createManager();
        // Precision exceeds MySQL VARBINARY limit, should fallback to LONGBLOB
        String result = manager.mapJdbcTypeToNativeDDL("c_varbinary", Types.VARBINARY, 70000, 0);
        assertEquals("LONGBLOB", result);
    }

    @Test
    void testMapJdbcType_Blob() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_blob", Types.BLOB, 0, 0);
        assertEquals("LONGBLOB", result);
    }

    @Test
    void testMapJdbcType_Clob() throws Exception {
        MySQLManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("c_clob", Types.CLOB, 0, 0);
        assertEquals("LONGTEXT", result);
    }

    // Cross-database type mapping tests: Oracle -> MySQL

    @Test
    void testMapJdbcType_OracleREAL_CrossDatabase() throws Exception {
        MySQLManager manager = createManager();
        // Oracle REAL comes through as NUMERIC with precision=63, scale=-127
        String result = manager.mapJdbcTypeToNativeDDL("c_real", Types.NUMERIC, 63, -127);
        assertEquals("FLOAT", result);
    }

    @Test
    void testMapJdbcType_OracleDOUBLE_CrossDatabase() throws Exception {
        MySQLManager manager = createManager();
        // Oracle DOUBLE PRECISION comes through as NUMERIC with precision=126, scale=-127
        String result = manager.mapJdbcTypeToNativeDDL("c_double", Types.NUMERIC, 126, -127);
        assertEquals("DOUBLE", result);
    }

    @Test
    void testMapJdbcType_OracleFLOAT_CrossDatabase() throws Exception {
        MySQLManager manager = createManager();
        // Oracle FLOAT with custom precision comes through as NUMERIC with scale=-127
        String result = manager.mapJdbcTypeToNativeDDL("c_float", Types.NUMERIC, 100, -127);
        // Should default to DOUBLE for other precisions
        assertEquals("DOUBLE", result);
    }

    @Test
    void testMapJdbcType_UnmappedType() throws Exception {
        MySQLManager manager = createManager();
        // Test that unmapped type falls back to LONGTEXT
        String result = manager.mapJdbcTypeToNativeDDL("c_unknown", 9999, 0, 0);
        assertEquals("LONGTEXT", result);
    }
}

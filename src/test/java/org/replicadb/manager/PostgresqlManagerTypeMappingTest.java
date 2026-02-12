package org.replicadb.manager;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;

import java.sql.Types;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PostgresqlManager's JDBC type to native DDL type mapping.
 * Tests the mapJdbcTypeToNativeDDL() method implementation.
 */
class PostgresqlManagerTypeMappingTest {

    private PostgresqlManager createManager() throws Exception {
        String[] args = {
            "--source-connect", "jdbc:postgresql://localhost:5432/source",
            "--source-table", "source_table",
            "--sink-connect", "jdbc:postgresql://localhost:5432/sink",
            "--sink-table", "sink_table"
        };
        ToolOptions options = new ToolOptions(args);
        return new PostgresqlManager(options, DataSourceType.SINK);
    }

    @Test
    void testMapJdbcType_INTEGER() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.INTEGER, 10, 0);
        assertEquals("INTEGER", result);
    }

    @Test
    void testMapJdbcType_BIGINT() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.BIGINT, 19, 0);
        assertEquals("BIGINT", result);
    }

    @Test
    void testMapJdbcType_SMALLINT() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.SMALLINT, 5, 0);
        // PostgreSQL implementation maps SMALLINT to INTEGER for simplicity
        assertEquals("INTEGER", result);
    }

    @Test
    void testMapJdbcType_TINYINT() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.TINYINT, 3, 0);
        // PostgreSQL implementation maps TINYINT to INTEGER (no native TINYINT in PostgreSQL)
        assertEquals("INTEGER", result);
    }

    @Test
    void testMapJdbcType_BOOLEAN() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.BOOLEAN, 1, 0);
        assertEquals("BOOLEAN", result);
    }

    @Test
    void testMapJdbcType_REAL() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.REAL, 7, 0);
        assertEquals("REAL", result);
    }

    @Test
    void testMapJdbcType_FLOAT() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.FLOAT, 15, 0);
        assertEquals("DOUBLE PRECISION", result);
    }

    @Test
    void testMapJdbcType_DOUBLE() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.DOUBLE, 15, 0);
        assertEquals("DOUBLE PRECISION", result);
    }

    @Test
    void testMapJdbcType_DECIMAL_withPrecisionAndScale() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.DECIMAL, 10, 2);
        // PostgreSQL implementation uses NUMERIC for DECIMAL types
        assertEquals("NUMERIC(10, 2)", result);
    }

    @Test
    void testMapJdbcType_DECIMAL_withoutPrecision() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.DECIMAL, 0, 0);
        // PostgreSQL implementation uses NUMERIC for DECIMAL types
        assertEquals("NUMERIC", result);
    }

    @Test
    void testMapJdbcType_NUMERIC_withPrecisionAndScale() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.NUMERIC, 15, 5);
        assertEquals("NUMERIC(15, 5)", result);
    }

    @Test
    void testMapJdbcType_VARCHAR_withPrecision() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.VARCHAR, 100, 0);
        assertEquals("VARCHAR(100)", result);
    }

    @Test
    void testMapJdbcType_VARCHAR_withoutPrecision() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.VARCHAR, 0, 0);
        assertEquals("TEXT", result);
    }

    @Test
    void testMapJdbcType_CHAR_withPrecision() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.CHAR, 10, 0);
        assertEquals("CHAR(10)", result);
    }

    @Test
    void testMapJdbcType_DATE() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.DATE, 0, 0);
        assertEquals("DATE", result);
    }

    @Test
    void testMapJdbcType_TIME() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.TIME, 0, 0);
        assertEquals("TIME", result);
    }

    @Test
    void testMapJdbcType_TIMESTAMP() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.TIMESTAMP, 0, 0);
        assertEquals("TIMESTAMP", result);
    }

    @Test
    void testMapJdbcType_BINARY() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.BINARY, 16, 0);
        assertEquals("BYTEA", result);
    }

    @Test
    void testMapJdbcType_VARBINARY() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.VARBINARY, 255, 0);
        assertEquals("BYTEA", result);
    }

    @Test
    void testMapJdbcType_BLOB() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.BLOB, 0, 0);
        assertEquals("BYTEA", result);
    }

    @Test
    void testMapJdbcType_CLOB() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.CLOB, 0, 0);
        assertEquals("TEXT", result);
    }

    @Test
    void testMapJdbcType_LONGVARCHAR() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", Types.LONGVARCHAR, 0, 0);
        assertEquals("TEXT", result);
    }

    @Test
    void testMapJdbcType_UNKNOWN() throws Exception {
        PostgresqlManager manager = createManager();
        String result = manager.mapJdbcTypeToNativeDDL("test_col", 999999, 0, 0);
        // Should fall back to TEXT for unknown types
        assertEquals("TEXT", result);
    }

    // Oracle FLOAT/DOUBLE/REAL cross-database mapping tests
    @Test
    void testMapJdbcType_OracleREAL_CrossDatabase() throws Exception {
        PostgresqlManager manager = createManager();
        // Oracle REAL comes through as NUMERIC with precision=63, scale=-127
        String result = manager.mapJdbcTypeToNativeDDL("c_real", Types.NUMERIC, 63, -127);
        assertEquals("REAL", result);
    }

    @Test
    void testMapJdbcType_OracleDOUBLE_CrossDatabase() throws Exception {
        PostgresqlManager manager = createManager();
        // Oracle DOUBLE PRECISION comes through as NUMERIC with precision=126, scale=-127
        String result = manager.mapJdbcTypeToNativeDDL("c_double", Types.NUMERIC, 126, -127);
        assertEquals("DOUBLE PRECISION", result);
    }

    @Test
    void testMapJdbcType_OracleFLOAT_CrossDatabase() throws Exception {
        PostgresqlManager manager = createManager();
        // Oracle FLOAT with custom precision comes through as NUMERIC with scale=-127
        String result = manager.mapJdbcTypeToNativeDDL("c_float", Types.NUMERIC, 100, -127);
        // Should default to DOUBLE PRECISION for other precisions
        assertEquals("DOUBLE PRECISION", result);
    }
}

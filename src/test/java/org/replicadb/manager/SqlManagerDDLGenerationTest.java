package org.replicadb.manager;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.util.ColumnDescriptor;

import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SqlManager's DDL generation methods.
 * Tests focus on generateCreateTableDDL() and mapJdbcTypeToNativeDDL() behavior.
 */
class SqlManagerDDLGenerationTest {

    /**
     * Concrete implementation for testing SqlManager's DDL generation methods.
     * Uses PostgresqlManager as base to avoid implementing all abstract methods.
     */
    private static class TestSqlManager extends PostgresqlManager {
        public TestSqlManager(ToolOptions opts) {
            super(opts, DataSourceType.SINK);
        }

        // Public wrapper to access protected generateCreateTableDDL method for testing
        public String testGenerateCreateTableDDL(String tableName, List<ColumnDescriptor> columns, String[] primaryKeys) {
            return super.generateCreateTableDDL(tableName, columns, primaryKeys);
        }
    }

    private ToolOptions createBasicOptions() throws Exception {
        String[] args = {
            "--source-connect", "jdbc:postgresql://localhost:5432/source",
            "--source-table", "source_table",
            "--sink-connect", "jdbc:postgresql://localhost:5432/sink",
            "--sink-table", "sink_table"
        };
        return new ToolOptions(args);
    }

    @Test
    void testGenerateCreateTableDDL_simpleTable() throws Exception {
        // Given: Simple table with id and name
        ToolOptions options = createBasicOptions();
        TestSqlManager manager = new TestSqlManager(options);

        List<ColumnDescriptor> columns = Arrays.asList(
            new ColumnDescriptor("id", Types.INTEGER, 10, 0, ResultSetMetaData.columnNoNulls),
            new ColumnDescriptor("name", Types.VARCHAR, 100, 0, ResultSetMetaData.columnNullable)
        );

        String[] primaryKeys = {"id"};

        // When: DDL is generated
        String ddl = manager.testGenerateCreateTableDDL("test_table", columns, primaryKeys);

        // Then: Should produce valid CREATE TABLE statement
        assertNotNull(ddl);
        assertTrue(ddl.startsWith("CREATE TABLE"), "DDL should start with CREATE TABLE");
        assertTrue(ddl.contains("test_table"), "DDL should contain table name");
        assertTrue(ddl.contains("id"), "DDL should contain id column");
        assertTrue(ddl.contains("INTEGER"), "DDL should contain INTEGER type");
        assertTrue(ddl.contains("name"), "DDL should contain name column");
        assertTrue(ddl.contains("VARCHAR(100)"), "DDL should contain VARCHAR(100) type");
        assertTrue(ddl.contains("NOT NULL"), "DDL should contain NOT NULL for id");
        assertTrue(ddl.contains("PRIMARY KEY"), "DDL should contain PRIMARY KEY constraint");
        assertTrue(ddl.contains("(id)"), "DDL should specify id as primary key");
    }

    @Test
    void testGenerateCreateTableDDL_noPrimaryKey() throws Exception {
        // Given: Table without primary key
        ToolOptions options = createBasicOptions();
        TestSqlManager manager = new TestSqlManager(options);

        List<ColumnDescriptor> columns = Arrays.asList(
            new ColumnDescriptor("id", Types.INTEGER, 10, 0, ResultSetMetaData.columnNoNulls),
            new ColumnDescriptor("name", Types.VARCHAR, 100, 0, ResultSetMetaData.columnNullable)
        );

        // When: DDL is generated with null primary keys
        String ddl = manager.testGenerateCreateTableDDL("test_table", columns, null);

        // Then: Should not contain PRIMARY KEY constraint
        assertNotNull(ddl);
        assertTrue(ddl.contains("CREATE TABLE"), "DDL should start with CREATE TABLE");
        assertTrue(ddl.contains("id"), "DDL should contain id column");
        assertTrue(ddl.contains("name"), "DDL should contain name column");
        assertFalse(ddl.contains("PRIMARY KEY"), "DDL should not contain PRIMARY KEY when no keys specified");
    }

    @Test
    void testGenerateCreateTableDDL_emptyPrimaryKeyArray() throws Exception {
        // Given: Table with empty primary key array
        ToolOptions options = createBasicOptions();
        TestSqlManager manager = new TestSqlManager(options);

        List<ColumnDescriptor> columns = Arrays.asList(
            new ColumnDescriptor("id", Types.INTEGER, 10, 0, ResultSetMetaData.columnNoNulls)
        );

        String[] emptyKeys = {};

        // When: DDL is generated with empty primary keys
        String ddl = manager.testGenerateCreateTableDDL("test_table", columns, emptyKeys);

        // Then: Should not contain PRIMARY KEY constraint
        assertFalse(ddl.contains("PRIMARY KEY"), "DDL should not contain PRIMARY KEY when keys array is empty");
    }

    @Test
    void testGenerateCreateTableDDL_compositePrimaryKey() throws Exception {
        // Given: Table with composite primary key
        ToolOptions options = createBasicOptions();
        TestSqlManager manager = new TestSqlManager(options);

        List<ColumnDescriptor> columns = Arrays.asList(
            new ColumnDescriptor("user_id", Types.INTEGER, 10, 0, ResultSetMetaData.columnNoNulls),
            new ColumnDescriptor("order_id", Types.INTEGER, 10, 0, ResultSetMetaData.columnNoNulls),
            new ColumnDescriptor("quantity", Types.INTEGER, 10, 0, ResultSetMetaData.columnNullable)
        );

        String[] primaryKeys = {"user_id", "order_id"};

        // When: DDL is generated
        String ddl = manager.testGenerateCreateTableDDL("order_items", columns, primaryKeys);

        // Then: Should contain composite primary key
        assertTrue(ddl.contains("PRIMARY KEY"), "DDL should contain PRIMARY KEY constraint");
        assertTrue(ddl.contains("user_id"), "DDL should contain user_id in PK");
        assertTrue(ddl.contains("order_id"), "DDL should contain order_id in PK");
        assertTrue(ddl.contains(","), "Composite PK should have comma separator");
    }

    @Test
    void testGenerateCreateTableDDL_allNullableColumns() throws Exception {
        // Given: Table with all nullable columns
        ToolOptions options = createBasicOptions();
        TestSqlManager manager = new TestSqlManager(options);

        List<ColumnDescriptor> columns = Arrays.asList(
            new ColumnDescriptor("optional1", Types.VARCHAR, 50, 0, ResultSetMetaData.columnNullable),
            new ColumnDescriptor("optional2", Types.INTEGER, 10, 0, ResultSetMetaData.columnNullable)
        );

        // When: DDL is generated
        String ddl = manager.testGenerateCreateTableDDL("optional_table", columns, null);

        // Then: Should not contain NOT NULL constraints
        assertFalse(ddl.contains("NOT NULL"), "DDL should not contain NOT NULL for nullable columns");
    }

    @Test
    void testGenerateCreateTableDDL_mixedNullability() throws Exception {
        // Given: Table with mixed nullable/not null columns
        ToolOptions options = createBasicOptions();
        TestSqlManager manager = new TestSqlManager(options);

        List<ColumnDescriptor> columns = Arrays.asList(
            new ColumnDescriptor("id", Types.INTEGER, 10, 0, ResultSetMetaData.columnNoNulls),
            new ColumnDescriptor("required_field", Types.VARCHAR, 100, 0, ResultSetMetaData.columnNoNulls),
            new ColumnDescriptor("optional_field", Types.VARCHAR, 100, 0, ResultSetMetaData.columnNullable)
        );

        // When: DDL is generated
        String ddl = manager.testGenerateCreateTableDDL("mixed_table", columns, null);

        // Then: Should contain NOT NULL only for non-nullable columns
        assertTrue(ddl.contains("NOT NULL"), "DDL should contain NOT NULL for required columns");
        // Count NOT NULL occurrences - should be 2 (for id and required_field)
        int notNullCount = ddl.split("NOT NULL", -1).length - 1;
        assertEquals(2, notNullCount, "Should have exactly 2 NOT NULL constraints");
    }

    @Test
    void testGenerateCreateTableDDL_decimalWithScale() throws Exception {
        // Given: Table with decimal column
        ToolOptions options = createBasicOptions();
        TestSqlManager manager = new TestSqlManager(options);

        List<ColumnDescriptor> columns = Arrays.asList(
            new ColumnDescriptor("price", Types.DECIMAL, 10, 2, ResultSetMetaData.columnNoNulls)
        );

        // When: DDL is generated
        String ddl = manager.testGenerateCreateTableDDL("products", columns, null);

        // Then: Should contain DECIMAL with precision and scale
        assertTrue(ddl.contains("DECIMAL") || ddl.contains("NUMERIC"), "DDL should contain DECIMAL or NUMERIC");
        assertTrue(ddl.contains("10") && ddl.contains("2"), "DDL should contain precision 10 and scale 2");
    }

    @Test
    void testGenerateCreateTableDDL_variousTypes() throws Exception {
        // Given: Table with various data types
        ToolOptions options = createBasicOptions();
        TestSqlManager manager = new TestSqlManager(options);

        List<ColumnDescriptor> columns = Arrays.asList(
            new ColumnDescriptor("id", Types.BIGINT, 19, 0, ResultSetMetaData.columnNoNulls),
            new ColumnDescriptor("name", Types.VARCHAR, 255, 0, ResultSetMetaData.columnNullable),
            new ColumnDescriptor("birth_date", Types.DATE, 0, 0, ResultSetMetaData.columnNullable),
            new ColumnDescriptor("created_at", Types.TIMESTAMP, 0, 0, ResultSetMetaData.columnNoNulls),
            new ColumnDescriptor("is_active", Types.BOOLEAN, 1, 0, ResultSetMetaData.columnNoNulls),
            new ColumnDescriptor("description", Types.CLOB, 0, 0, ResultSetMetaData.columnNullable)
        );

        // When: DDL is generated
        String ddl = manager.testGenerateCreateTableDDL("users", columns, null);

        // Then: Should contain all types
        assertTrue(ddl.contains("BIGINT"), "DDL should contain BIGINT");
        assertTrue(ddl.contains("VARCHAR(255)"), "DDL should contain VARCHAR(255)");
        assertTrue(ddl.contains("DATE"), "DDL should contain DATE");
        assertTrue(ddl.contains("TIMESTAMP"), "DDL should contain TIMESTAMP");
        assertTrue(ddl.contains("BOOLEAN"), "DDL should contain BOOLEAN");
        assertTrue(ddl.contains("TEXT"), "DDL should contain TEXT for CLOB");
    }

    @Test
    void testGenerateCreateTableDDL_qualifiedTableName() throws Exception {
        // Given: Table with schema-qualified name
        ToolOptions options = createBasicOptions();
        TestSqlManager manager = new TestSqlManager(options);

        List<ColumnDescriptor> columns = Arrays.asList(
            new ColumnDescriptor("id", Types.INTEGER, 10, 0, ResultSetMetaData.columnNoNulls)
        );

        // When: DDL is generated with qualified table name
        String ddl = manager.testGenerateCreateTableDDL("public.test_table", columns, null);

        // Then: Should contain qualified table name
        assertTrue(ddl.contains("public.test_table"), "DDL should contain schema-qualified table name");
    }

    @Test
    void testGenerateCreateTableDDL_emptyColumnList() throws Exception {
        // Given: Empty column list (should not happen in practice, validation at higher level)
        ToolOptions options = createBasicOptions();
        TestSqlManager manager = new TestSqlManager(options);

        List<ColumnDescriptor> emptyColumns = Arrays.asList();

        // When: DDL is generated with empty column list
        String ddl = manager.testGenerateCreateTableDDL("test_table", emptyColumns, null);

        // Then: Generates syntactically invalid DDL (validation happens at autoCreateSinkTable level)
        assertNotNull(ddl);
        assertTrue(ddl.contains("CREATE TABLE"), "Should contain CREATE TABLE");
        assertTrue(ddl.contains("test_table"), "Should contain table name");
        // Note: This will produce invalid SQL, but validation happens at a higher level (autoCreateSinkTable)
    }

    @Test
    void testGenerateCreateTableDDL_nullColumnList() throws Exception {
        // Given: Null column list
        ToolOptions options = createBasicOptions();
        TestSqlManager manager = new TestSqlManager(options);

        // When/Then: Should throw exception
        assertThrows(NullPointerException.class, () -> {
            manager.testGenerateCreateTableDDL("test_table", null, null);
        }, "Should throw exception for null column list");
    }

    @Test
    void testGenerateCreateTableDDL_wellFormed() throws Exception {
        // Given: A typical table definition
        ToolOptions options = createBasicOptions();
        TestSqlManager manager = new TestSqlManager(options);

        List<ColumnDescriptor> columns = Arrays.asList(
            new ColumnDescriptor("id", Types.INTEGER, 10, 0, ResultSetMetaData.columnNoNulls),
            new ColumnDescriptor("name", Types.VARCHAR, 100, 0, ResultSetMetaData.columnNullable)
        );

        String[] primaryKeys = {"id"};

        // When: DDL is generated
        String ddl = manager.testGenerateCreateTableDDL("test_table", columns, primaryKeys);

        // Then: Should be well-formed SQL
        assertTrue(ddl.contains("CREATE TABLE test_table"), "Should have CREATE TABLE clause");
        assertTrue(ddl.contains("(") && ddl.contains(")"), "Should have parentheses for column list");
        assertTrue(ddl.trim().endsWith(";") || ddl.trim().endsWith(")"), "Should end with ; or )");
        assertFalse(ddl.contains(",,"), "Should not have double commas");
    }
}

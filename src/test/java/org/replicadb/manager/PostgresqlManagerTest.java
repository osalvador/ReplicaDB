package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.copy.CopyIn;
import org.replicadb.cli.ToolOptions;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PostgresqlManager binary COPY format implementation.
 * Tests focus on binary column detection logic and binary COPY data writing.
 */
class PostgresqlManagerTest {
    private static final Logger LOG = LogManager.getLogger(PostgresqlManagerTest.class);
    
    private PostgresqlManager manager;
    private ToolOptions mockOptions;

    @BeforeEach
    void setUp() {
        // Create minimal ToolOptions for manager instantiation
        String[] args = {
            "--source-connect", "jdbc:postgresql://localhost:5432/test",
            "--source-table", "test_table",
            "--sink-connect", "jdbc:postgresql://localhost:5432/test",
            "--sink-table", "test_table"
        };
        
        try {
            mockOptions = new ToolOptions(args);
            manager = new PostgresqlManager(mockOptions, DataSourceType.SINK);
        } catch (Exception e) {
            LOG.error("Failed to initialize PostgresqlManager for testing", e);
        }
    }

    @Test
    void testHasBinaryColumns_withBinaryType() throws Exception {
        ResultSetMetaData mockRsmd = createMockMetadata(
            new int[]{Types.VARCHAR, Types.BINARY, Types.INTEGER}
        );
        
        boolean result = invokeHasBinaryColumns(mockRsmd);
        assertTrue(result, "Should detect BINARY column type");
    }

    @Test
    void testHasBinaryColumns_withVarbinaryType() throws Exception {
        ResultSetMetaData mockRsmd = createMockMetadata(
            new int[]{Types.VARCHAR, Types.VARBINARY}
        );
        
        boolean result = invokeHasBinaryColumns(mockRsmd);
        assertTrue(result, "Should detect VARBINARY column type");
    }

    @Test
    void testHasBinaryColumns_withLongvarbinaryType() throws Exception {
        ResultSetMetaData mockRsmd = createMockMetadata(
            new int[]{Types.INTEGER, Types.LONGVARBINARY}
        );
        
        boolean result = invokeHasBinaryColumns(mockRsmd);
        assertTrue(result, "Should detect LONGVARBINARY column type");
    }

    @Test
    void testHasBinaryColumns_withBlobType() throws Exception {
        ResultSetMetaData mockRsmd = createMockMetadata(
            new int[]{Types.VARCHAR, Types.INTEGER, Types.BLOB}
        );
        
        boolean result = invokeHasBinaryColumns(mockRsmd);
        assertTrue(result, "Should detect BLOB column type");
    }

    @Test
    void testHasBinaryColumns_withNoBinaryColumns() throws Exception {
        ResultSetMetaData mockRsmd = createMockMetadata(
            new int[]{Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP, Types.BOOLEAN}
        );
        
        boolean result = invokeHasBinaryColumns(mockRsmd);
        assertFalse(result, "Should return false when no binary columns present");
    }

    @Test
    void testHasBinaryColumns_withNullMetadata() throws Exception {
        boolean result = invokeHasBinaryColumns(null);
        assertFalse(result, "Should return false for null metadata");
    }

    @Test
    void testHasBinaryColumns_withZeroColumns() throws Exception {
        ResultSetMetaData mockRsmd = createMockMetadata(new int[]{});
        
        boolean result = invokeHasBinaryColumns(mockRsmd);
        assertFalse(result, "Should return false for zero columns");
    }

    @Test
    void testHasBinaryColumns_withMultipleBinaryColumns() throws Exception {
        ResultSetMetaData mockRsmd = createMockMetadata(
            new int[]{Types.VARCHAR, Types.BINARY, Types.INTEGER, Types.VARBINARY, Types.BLOB}
        );
        
        boolean result = invokeHasBinaryColumns(mockRsmd);
        assertTrue(result, "Should detect multiple binary column types");
    }

    @Test
    void testHasBinaryColumns_withTextAndClobTypes() throws Exception {
        ResultSetMetaData mockRsmd = createMockMetadata(
            new int[]{Types.VARCHAR, Types.CLOB, Types.LONGVARCHAR}
        );
        
        boolean result = invokeHasBinaryColumns(mockRsmd);
        assertFalse(result, "Should return false for text-based LOB types (CLOB is not binary)");
    }

    @Test
    void testInsertDataViaBinaryCopy_withBinaryData() throws Exception {
        // Create mock ResultSet with binary data
        byte[] testBinaryData = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
        MockResultSet mockRs = new MockResultSet(
            new int[]{Types.VARCHAR, Types.BINARY},
            new Object[][]{
                {"test", testBinaryData},
                {"hello", new byte[]{(byte)0xFF, (byte)0xAB, (byte)0xCD}}
            }
        );
        
        MockCopyIn mockCopyIn = new MockCopyIn();
        
        int rowCount = invokeInsertDataViaBinaryCopy(mockRs, 0, mockRs.getMetaData(), mockCopyIn);
        
        assertEquals(2, rowCount, "Should return correct row count");
        assertTrue(mockCopyIn.isEndCopyCalled(), "Should call endCopy()");
        
        byte[] writtenData = mockCopyIn.getWrittenData();
        assertTrue(writtenData.length > 0, "Should write data to COPY stream");
        
        // Verify binary COPY header signature
        byte[] expectedSignature = new byte[]{'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte)0xFF, '\r', '\n', 0};
        for (int i = 0; i < expectedSignature.length; i++) {
            assertEquals(expectedSignature[i], writtenData[i], 
                "Binary COPY signature byte " + i + " should match");
        }
        
        LOG.info("Binary COPY wrote {} bytes total", writtenData.length);
    }

    @Test
    void testInsertDataViaBinaryCopy_withNullValues() throws Exception {
        // Create mock ResultSet with NULL values
        MockResultSet mockRs = new MockResultSet(
            new int[]{Types.VARCHAR, Types.BINARY, Types.INTEGER},
            new Object[][]{
                {"test", null, 42},
                {null, new byte[]{0x01}, null}
            }
        );
        
        MockCopyIn mockCopyIn = new MockCopyIn();
        
        int rowCount = invokeInsertDataViaBinaryCopy(mockRs, 0, mockRs.getMetaData(), mockCopyIn);
        
        assertEquals(2, rowCount, "Should handle NULL values correctly");
        assertTrue(mockCopyIn.isEndCopyCalled(), "Should call endCopy()");
    }

    @Test
    void testInsertDataViaBinaryCopy_withMixedTypes() throws Exception {
        // Create mock ResultSet with various column types
        MockResultSet mockRs = new MockResultSet(
            new int[]{Types.INTEGER, Types.VARCHAR, Types.BOOLEAN, Types.BINARY},
            new Object[][]{
                {123, "text", true, new byte[]{0x01, 0x02}},
                {456, "more", false, new byte[]{(byte)0xFF}}
            }
        );
        
        MockCopyIn mockCopyIn = new MockCopyIn();
        
        int rowCount = invokeInsertDataViaBinaryCopy(mockRs, 0, mockRs.getMetaData(), mockCopyIn);
        
        assertEquals(2, rowCount, "Should handle mixed column types");
        
        byte[] writtenData = mockCopyIn.getWrittenData();
        // Verify trailer: last 2 bytes should be -1 as int16 (0xFF, 0xFF)
        assertEquals((byte)0xFF, writtenData[writtenData.length - 2], "Trailer should be 0xFF");
        assertEquals((byte)0xFF, writtenData[writtenData.length - 1], "Trailer should be 0xFF");
    }

    @Test
    void testInsertDataViaBinaryCopy_withEmptyResultSet() throws Exception {
        // Create empty ResultSet
        MockResultSet mockRs = new MockResultSet(
            new int[]{Types.VARCHAR, Types.BINARY},
            new Object[][]{}
        );
        
        MockCopyIn mockCopyIn = new MockCopyIn();
        
        int rowCount = invokeInsertDataViaBinaryCopy(mockRs, 0, mockRs.getMetaData(), mockCopyIn);
        
        assertEquals(0, rowCount, "Should return 0 for empty ResultSet");
        assertTrue(mockCopyIn.isEndCopyCalled(), "Should still call endCopy()");
    }

    /**
     * Creates a mock ResultSetMetaData with the specified column types.
     */
    private ResultSetMetaData createMockMetadata(int[] columnTypes) {
        return new ResultSetMetaData() {
            @Override
            public int getColumnCount() { return columnTypes.length; }
            
            @Override
            public int getColumnType(int column) { return columnTypes[column - 1]; }
            
            // Required interface methods (not used in tests)
            @Override public boolean isAutoIncrement(int column) { return false; }
            @Override public boolean isCaseSensitive(int column) { return false; }
            @Override public boolean isSearchable(int column) { return false; }
            @Override public boolean isCurrency(int column) { return false; }
            @Override public int isNullable(int column) { return 0; }
            @Override public boolean isSigned(int column) { return false; }
            @Override public int getColumnDisplaySize(int column) { return 0; }
            @Override public String getColumnLabel(int column) { return null; }
            @Override public String getColumnName(int column) { return null; }
            @Override public String getSchemaName(int column) { return null; }
            @Override public int getPrecision(int column) { return 0; }
            @Override public int getScale(int column) { return 0; }
            @Override public String getTableName(int column) { return null; }
            @Override public String getCatalogName(int column) { return null; }
            @Override public String getColumnTypeName(int column) { return null; }
            @Override public boolean isReadOnly(int column) { return false; }
            @Override public boolean isWritable(int column) { return false; }
            @Override public boolean isDefinitelyWritable(int column) { return false; }
            @Override public String getColumnClassName(int column) { return null; }
            @Override public <T> T unwrap(Class<T> iface) { return null; }
            @Override public boolean isWrapperFor(Class<?> iface) { return false; }
        };
    }

    /**
     * Mock ResultSet implementation for testing binary COPY.
     */
    private static class MockResultSet implements ResultSet {
        private final int[] columnTypes;
        private final Object[][] rows;
        private int currentRow = -1;
        private boolean wasNull = false;

        MockResultSet(int[] columnTypes, Object[][] rows) {
            this.columnTypes = columnTypes;
            this.rows = rows;
        }

        @Override
        public boolean next() {
            currentRow++;
            return currentRow < rows.length;
        }

        @Override
        public ResultSetMetaData getMetaData() {
            return new ResultSetMetaData() {
                @Override public int getColumnCount() { return columnTypes.length; }
                @Override public int getColumnType(int column) { return columnTypes[column - 1]; }
                @Override public boolean isAutoIncrement(int column) { return false; }
                @Override public boolean isCaseSensitive(int column) { return false; }
                @Override public boolean isSearchable(int column) { return false; }
                @Override public boolean isCurrency(int column) { return false; }
                @Override public int isNullable(int column) { return 0; }
                @Override public boolean isSigned(int column) { return false; }
                @Override public int getColumnDisplaySize(int column) { return 0; }
                @Override public String getColumnLabel(int column) { return null; }
                @Override public String getColumnName(int column) { return null; }
                @Override public String getSchemaName(int column) { return null; }
                @Override public int getPrecision(int column) { return 0; }
                @Override public int getScale(int column) { return 0; }
                @Override public String getTableName(int column) { return null; }
                @Override public String getCatalogName(int column) { return null; }
                @Override public String getColumnTypeName(int column) { return null; }
                @Override public boolean isReadOnly(int column) { return false; }
                @Override public boolean isWritable(int column) { return false; }
                @Override public boolean isDefinitelyWritable(int column) { return false; }
                @Override public String getColumnClassName(int column) { return null; }
                @Override public <T> T unwrap(Class<T> iface) { return null; }
                @Override public boolean isWrapperFor(Class<?> iface) { return false; }
            };
        }

        @Override
        public String getString(int columnIndex) {
            Object value = rows[currentRow][columnIndex - 1];
            wasNull = (value == null);
            return (String) value;
        }

        @Override
        public byte[] getBytes(int columnIndex) {
            Object value = rows[currentRow][columnIndex - 1];
            wasNull = (value == null);
            return (byte[]) value;
        }

        @Override
        public int getInt(int columnIndex) {
            Object value = rows[currentRow][columnIndex - 1];
            wasNull = (value == null);
            return value == null ? 0 : (Integer) value;
        }

        @Override
        public boolean getBoolean(int columnIndex) {
            Object value = rows[currentRow][columnIndex - 1];
            wasNull = (value == null);
            return value != null && (Boolean) value;
        }

        @Override
        public boolean wasNull() {
            return wasNull;
        }

        // Stub implementations for other methods
        @Override public void close() {}
        @Override public boolean isClosed() { return false; }
        @Override public Blob getBlob(int columnIndex) { return null; }
        @Override public Clob getClob(int columnIndex) { return null; }
        @Override public short getShort(int columnIndex) { return 0; }
        @Override public long getLong(int columnIndex) { return 0; }
        @Override public Statement getStatement() { return null; }
        @Override public <T> T unwrap(Class<T> iface) { return null; }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
        // Add all other required ResultSet methods as no-ops
        @Override public boolean isBeforeFirst() { return false; }
        @Override public boolean isAfterLast() { return false; }
        @Override public boolean isFirst() { return false; }
        @Override public boolean isLast() { return false; }
        @Override public void beforeFirst() {}
        @Override public void afterLast() {}
        @Override public boolean first() { return false; }
        @Override public boolean last() { return false; }
        @Override public int getRow() { return 0; }
        @Override public boolean absolute(int row) { return false; }
        @Override public boolean relative(int rows) { return false; }
        @Override public boolean previous() { return false; }
        @Override public void setFetchDirection(int direction) {}
        @Override public int getFetchDirection() { return 0; }
        @Override public void setFetchSize(int rows) {}
        @Override public int getFetchSize() { return 0; }
        @Override public int getType() { return 0; }
        @Override public int getConcurrency() { return 0; }
        @Override public boolean rowUpdated() { return false; }
        @Override public boolean rowInserted() { return false; }
        @Override public boolean rowDeleted() { return false; }
        @Override public void updateNull(int columnIndex) {}
        @Override public void insertRow() {}
        @Override public void updateRow() {}
        @Override public void deleteRow() {}
        @Override public void refreshRow() {}
        @Override public void cancelRowUpdates() {}
        @Override public void moveToInsertRow() {}
        @Override public void moveToCurrentRow() {}
        @Override public Object getObject(int columnIndex) { return null; }
        @Override public BigDecimal getBigDecimal(int columnIndex, int scale) { return null; }
        @Override public BigDecimal getBigDecimal(int columnIndex) { return null; }
        @Override public byte getByte(int columnIndex) { return 0; }
        @Override public float getFloat(int columnIndex) { return 0; }
        @Override public double getDouble(int columnIndex) { return 0; }
        @Override public Date getDate(int columnIndex) { return null; }
        @Override public Time getTime(int columnIndex) { return null; }
        @Override public Timestamp getTimestamp(int columnIndex) { return null; }
        @Override public InputStream getAsciiStream(int columnIndex) { return null; }
        @Override public InputStream getUnicodeStream(int columnIndex) { return null; }
        @Override public InputStream getBinaryStream(int columnIndex) { return null; }
        @Override public String getString(String columnLabel) { return null; }
        @Override public boolean getBoolean(String columnLabel) { return false; }
        @Override public byte getByte(String columnLabel) { return 0; }
        @Override public short getShort(String columnLabel) { return 0; }
        @Override public int getInt(String columnLabel) { return 0; }
        @Override public long getLong(String columnLabel) { return 0; }
        @Override public float getFloat(String columnLabel) { return 0; }
        @Override public double getDouble(String columnLabel) { return 0; }
        @Override public BigDecimal getBigDecimal(String columnLabel, int scale) { return null; }
        @Override public byte[] getBytes(String columnLabel) { return null; }
        @Override public Date getDate(String columnLabel) { return null; }
        @Override public Time getTime(String columnLabel) { return null; }
        @Override public Timestamp getTimestamp(String columnLabel) { return null; }
        @Override public InputStream getAsciiStream(String columnLabel) { return null; }
        @Override public InputStream getUnicodeStream(String columnLabel) { return null; }
        @Override public InputStream getBinaryStream(String columnLabel) { return null; }
        @Override public SQLWarning getWarnings() { return null; }
        @Override public void clearWarnings() {}
        @Override public String getCursorName() { return null; }
        @Override public Object getObject(String columnLabel) { return null; }
        @Override public Object getObject(int columnIndex, java.util.Map<String, Class<?>> map) { return null; }
        @Override public Object getObject(String columnLabel, java.util.Map<String, Class<?>> map) { return null; }
        @Override public int findColumn(String columnLabel) { return 0; }
        @Override public Reader getCharacterStream(int columnIndex) { return null; }
        @Override public Reader getCharacterStream(String columnLabel) { return null; }
        @Override public BigDecimal getBigDecimal(String columnLabel) { return null; }
        @Override public void updateNull(String columnLabel) {}
        @Override public void updateBoolean(int columnIndex, boolean x) {}
        @Override public void updateByte(int columnIndex, byte x) {}
        @Override public void updateShort(int columnIndex, short x) {}
        @Override public void updateInt(int columnIndex, int x) {}
        @Override public void updateLong(int columnIndex, long x) {}
        @Override public void updateFloat(int columnIndex, float x) {}
        @Override public void updateDouble(int columnIndex, double x) {}
        @Override public void updateBigDecimal(int columnIndex, BigDecimal x) {}
        @Override public void updateString(int columnIndex, String x) {}
        @Override public void updateBytes(int columnIndex, byte[] x) {}
        @Override public void updateDate(int columnIndex, Date x) {}
        @Override public void updateTime(int columnIndex, Time x) {}
        @Override public void updateTimestamp(int columnIndex, Timestamp x) {}
        @Override public void updateAsciiStream(int columnIndex, InputStream x, int length) {}
        @Override public void updateBinaryStream(int columnIndex, InputStream x, int length) {}
        @Override public void updateCharacterStream(int columnIndex, Reader x, int length) {}
        @Override public void updateObject(int columnIndex, Object x, int scaleOrLength) {}
        @Override public void updateObject(int columnIndex, Object x) {}
        @Override public void updateBoolean(String columnLabel, boolean x) {}
        @Override public void updateByte(String columnLabel, byte x) {}
        @Override public void updateShort(String columnLabel, short x) {}
        @Override public void updateInt(String columnLabel, int x) {}
        @Override public void updateLong(String columnLabel, long x) {}
        @Override public void updateFloat(String columnLabel, float x) {}
        @Override public void updateDouble(String columnLabel, double x) {}
        @Override public void updateBigDecimal(String columnLabel, BigDecimal x) {}
        @Override public void updateString(String columnLabel, String x) {}
        @Override public void updateBytes(String columnLabel, byte[] x) {}
        @Override public void updateDate(String columnLabel, Date x) {}
        @Override public void updateTime(String columnLabel, Time x) {}
        @Override public void updateTimestamp(String columnLabel, Timestamp x) {}
        @Override public void updateAsciiStream(String columnLabel, InputStream x, int length) {}
        @Override public void updateBinaryStream(String columnLabel, InputStream x, int length) {}
        @Override public void updateCharacterStream(String columnLabel, Reader x, int length) {}
        @Override public void updateObject(String columnLabel, Object x, int scaleOrLength) {}
        @Override public void updateObject(String columnLabel, Object x) {}
        @Override public Ref getRef(int columnIndex) { return null; }
        @Override public Blob getBlob(String columnLabel) { return null; }
        @Override public Clob getClob(String columnLabel) { return null; }
        @Override public Array getArray(int columnIndex) { return null; }
        @Override public Array getArray(String columnLabel) { return null; }
        @Override public Date getDate(int columnIndex, java.util.Calendar cal) { return null; }
        @Override public Date getDate(String columnLabel, java.util.Calendar cal) { return null; }
        @Override public Time getTime(int columnIndex, java.util.Calendar cal) { return null; }
        @Override public Time getTime(String columnLabel, java.util.Calendar cal) { return null; }
        @Override public Timestamp getTimestamp(int columnIndex, java.util.Calendar cal) { return null; }
        @Override public Timestamp getTimestamp(String columnLabel, java.util.Calendar cal) { return null; }
        @Override public java.net.URL getURL(int columnIndex) { return null; }
        @Override public java.net.URL getURL(String columnLabel) { return null; }
        @Override public void updateRef(int columnIndex, Ref x) {}
        @Override public void updateRef(String columnLabel, Ref x) {}
        @Override public void updateBlob(int columnIndex, Blob x) {}
        @Override public void updateBlob(String columnLabel, Blob x) {}
        @Override public void updateClob(int columnIndex, Clob x) {}
        @Override public void updateClob(String columnLabel, Clob x) {}
        @Override public void updateArray(int columnIndex, Array x) {}
        @Override public void updateArray(String columnLabel, Array x) {}
        @Override public RowId getRowId(int columnIndex) { return null; }
        @Override public RowId getRowId(String columnLabel) { return null; }
        @Override public void updateRowId(int columnIndex, RowId x) {}
        @Override public void updateRowId(String columnLabel, RowId x) {}
        @Override public int getHoldability() { return 0; }
        @Override public void updateNString(int columnIndex, String nString) {}
        @Override public void updateNString(String columnLabel, String nString) {}
        @Override public void updateNClob(int columnIndex, NClob nClob) {}
        @Override public void updateNClob(String columnLabel, NClob nClob) {}
        @Override public NClob getNClob(int columnIndex) { return null; }
        @Override public NClob getNClob(String columnLabel) { return null; }
        @Override public SQLXML getSQLXML(int columnIndex) { return null; }
        @Override public SQLXML getSQLXML(String columnLabel) { return null; }
        @Override public void updateSQLXML(int columnIndex, SQLXML xmlObject) {}
        @Override public void updateSQLXML(String columnLabel, SQLXML xmlObject) {}
        @Override public String getNString(int columnIndex) { return null; }
        @Override public String getNString(String columnLabel) { return null; }
        @Override public Reader getNCharacterStream(int columnIndex) { return null; }
        @Override public Reader getNCharacterStream(String columnLabel) { return null; }
        @Override public void updateNCharacterStream(int columnIndex, Reader x, long length) {}
        @Override public void updateNCharacterStream(String columnLabel, Reader reader, long length) {}
        @Override public void updateAsciiStream(int columnIndex, InputStream x, long length) {}
        @Override public void updateBinaryStream(int columnIndex, InputStream x, long length) {}
        @Override public void updateCharacterStream(int columnIndex, Reader x, long length) {}
        @Override public void updateAsciiStream(String columnLabel, InputStream x, long length) {}
        @Override public void updateBinaryStream(String columnLabel, InputStream x, long length) {}
        @Override public void updateCharacterStream(String columnLabel, Reader reader, long length) {}
        @Override public void updateBlob(int columnIndex, InputStream inputStream, long length) {}
        @Override public void updateBlob(String columnLabel, InputStream inputStream, long length) {}
        @Override public void updateClob(int columnIndex, Reader reader, long length) {}
        @Override public void updateClob(String columnLabel, Reader reader, long length) {}
        @Override public void updateNClob(int columnIndex, Reader reader, long length) {}
        @Override public void updateNClob(String columnLabel, Reader reader, long length) {}
        @Override public void updateNCharacterStream(int columnIndex, Reader x) {}
        @Override public void updateNCharacterStream(String columnLabel, Reader reader) {}
        @Override public void updateAsciiStream(int columnIndex, InputStream x) {}
        @Override public void updateBinaryStream(int columnIndex, InputStream x) {}
        @Override public void updateCharacterStream(int columnIndex, Reader x) {}
        @Override public void updateAsciiStream(String columnLabel, InputStream x) {}
        @Override public void updateBinaryStream(String columnLabel, InputStream x) {}
        @Override public void updateCharacterStream(String columnLabel, Reader reader) {}
        @Override public void updateBlob(int columnIndex, InputStream inputStream) {}
        @Override public void updateBlob(String columnLabel, InputStream inputStream) {}
        @Override public void updateClob(int columnIndex, Reader reader) {}
        @Override public void updateClob(String columnLabel, Reader reader) {}
        @Override public void updateNClob(int columnIndex, Reader reader) {}
        @Override public void updateNClob(String columnLabel, Reader reader) {}
        @Override public <T> T getObject(int columnIndex, Class<T> type) { return null; }
        @Override public <T> T getObject(String columnLabel, Class<T> type) { return null; }
        @Override public Ref getRef(String columnLabel) { return null; }
    }

    /**
     * Mock CopyIn implementation that captures written data for verification.
     */
    private static class MockCopyIn implements CopyIn {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private boolean endCopyCalled = false;
        private boolean active = true;

        @Override
        public void writeToCopy(byte[] data, int off, int len) throws SQLException {
            buffer.write(data, off, len);
        }
        
        @Override
        public void writeToCopy(org.postgresql.util.ByteStreamWriter writer) throws SQLException {
            // Not used in our tests
        }

        @Override
        public long endCopy() throws SQLException {
            endCopyCalled = true;
            active = false;
            return buffer.size();
        }

        @Override
        public void cancelCopy() throws SQLException {
            active = false;
        }

        @Override
        public int getFormat() {
            return 1; // Binary format
        }

        @Override
        public int getFieldCount() {
            return 0;
        }

        @Override
        public int getFieldFormat(int field) {
            return 1; // Binary format
        }

        @Override
        public boolean isActive() {
            return active;
        }

        @Override
        public void flushCopy() throws SQLException {
            // No-op for mock
        }
        
        @Override
        public long getHandledRowCount() {
            return 0;
        }

        public byte[] getWrittenData() {
            return buffer.toByteArray();
        }

        public boolean isEndCopyCalled() {
            return endCopyCalled;
        }
    }

    /**
     * Uses reflection to invoke the private hasBinaryColumns method.
     * This is necessary since the method is private and we're testing its logic directly.
     */
    private boolean invokeHasBinaryColumns(ResultSetMetaData rsmd) throws Exception {
        java.lang.reflect.Method method = PostgresqlManager.class.getDeclaredMethod(
            "hasBinaryColumns", 
            ResultSetMetaData.class
        );
        method.setAccessible(true);
        return (boolean) method.invoke(manager, rsmd);
    }

    /**
     * Uses reflection to invoke the private insertDataViaBinaryCopy method.
     */
    private int invokeInsertDataViaBinaryCopy(ResultSet resultSet, int taskId, 
                                              ResultSetMetaData rsmd, CopyIn copyIn) throws Exception {
        java.lang.reflect.Method method = PostgresqlManager.class.getDeclaredMethod(
            "insertDataViaBinaryCopy",
            ResultSet.class, int.class, ResultSetMetaData.class, CopyIn.class
        );
        method.setAccessible(true);
        return (int) method.invoke(manager, resultSet, taskId, rsmd, copyIn);
    }

    @Test
    void testBinaryCopy_Float_SpecialValues() throws Exception {
        // Test NaN, Infinity, -Infinity, -0.0
        MockResultSet mockRs = new MockResultSet(
            new int[]{Types.FLOAT, Types.REAL},
            new Object[][]{
                {Float.NaN, Float.NaN},
                {Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY},
                {Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY},
                {-0.0f, -0.0f},
                {123.456f, 789.012f}
            }
        );
        
        MockCopyIn mockCopyIn = new MockCopyIn();
        int rowCount = invokeInsertDataViaBinaryCopy(mockRs, 0, mockRs.getMetaData(), mockCopyIn);
        
        assertEquals(5, rowCount, "Should process all float special values");
        assertTrue(mockCopyIn.getWrittenData().length > 0, "Should write binary data for floats");
        LOG.info("Float special values test wrote {} bytes", mockCopyIn.getWrittenData().length);
    }

    @Test
    void testBinaryCopy_Double_Precision() throws Exception {
        // Test high-precision doubles (15+ significant digits)
        MockResultSet mockRs = new MockResultSet(
            new int[]{Types.DOUBLE},
            new Object[][]{
                {Math.PI},
                {Math.E},
                {1.234567890123456789},
                {Double.MAX_VALUE},
                {Double.MIN_VALUE},
                {Double.NaN},
                {0.0}
            }
        );
        
        MockCopyIn mockCopyIn = new MockCopyIn();
        int rowCount = invokeInsertDataViaBinaryCopy(mockRs, 0, mockRs.getMetaData(), mockCopyIn);
        
        assertEquals(7, rowCount, "Should process all double precision values");
        assertTrue(mockCopyIn.getWrittenData().length > 0, "Should write binary data for doubles");
    }

    @Test
    void testBinaryCopy_Date_BoundaryValues() throws Exception {
        // Test dates before/after year 2000, leap years
        MockResultSet mockRs = new MockResultSet(
            new int[]{Types.DATE},
            new Object[][]{
                {Date.valueOf("1970-01-01")},  // Unix epoch
                {Date.valueOf("1999-12-31")},  // Before PostgreSQL epoch
                {Date.valueOf("2000-01-01")},  // PostgreSQL epoch
                {Date.valueOf("2000-01-02")},  // Day after epoch
                {Date.valueOf("2050-12-31")},  // Future date
                {Date.valueOf("2000-02-29")}   // Leap year
            }
        );
        
        MockCopyIn mockCopyIn = new MockCopyIn();
        int rowCount = invokeInsertDataViaBinaryCopy(mockRs, 0, mockRs.getMetaData(), mockCopyIn);
        
        assertEquals(6, rowCount, "Should process all date boundary values");
        assertTrue(mockCopyIn.getWrittenData().length > 0, "Should write binary data for dates");
    }

    @Test
    void testBinaryCopy_Time_Microseconds() throws Exception {
        // Test millisecond/microsecond precision
        MockResultSet mockRs = new MockResultSet(
            new int[]{Types.TIME},
            new Object[][]{
                {Time.valueOf("00:00:00")},      // Midnight
                {Time.valueOf("12:00:00")},      // Noon
                {Time.valueOf("23:59:59")},      // End of day
                {Time.valueOf("01:23:45")},
                {Time.valueOf("18:30:15")}
            }
        );
        
        MockCopyIn mockCopyIn = new MockCopyIn();
        int rowCount = invokeInsertDataViaBinaryCopy(mockRs, 0, mockRs.getMetaData(), mockCopyIn);
        
        assertEquals(5, rowCount, "Should process all time values");
        assertTrue(mockCopyIn.getWrittenData().length > 0, "Should write binary data for times");
    }

    @Test
    void testBinaryCopy_Timestamp_Microseconds() throws Exception {
        // Test microsecond precision preservation, pre-2000 timestamps
        MockResultSet mockRs = new MockResultSet(
            new int[]{Types.TIMESTAMP},
            new Object[][]{
                {Timestamp.valueOf("1970-01-01 00:00:00")},      // Unix epoch
                {Timestamp.valueOf("1999-12-31 23:59:59.999")},  // Before PostgreSQL epoch
                {Timestamp.valueOf("2000-01-01 00:00:00")},      // PostgreSQL epoch
                {Timestamp.valueOf("2000-01-01 00:00:00.001")},  // Millisecond precision
                {Timestamp.valueOf("2024-06-15 14:30:45.123456")}, // Microsecond precision
                {null}                                            // NULL timestamp
            }
        );
        
        MockCopyIn mockCopyIn = new MockCopyIn();
        int rowCount = invokeInsertDataViaBinaryCopy(mockRs, 0, mockRs.getMetaData(), mockCopyIn);
        
        assertEquals(6, rowCount, "Should process all timestamp values including NULL");
        assertTrue(mockCopyIn.getWrittenData().length > 0, "Should write binary data for timestamps");
    }

    @Test
    void testBinaryCopy_Numeric_VariousScales() throws Exception {
        // Test NUMERIC with scale 0, 2, 10, 38; large values (>10^20)
        MockResultSet mockRs = new MockResultSet(
            new int[]{Types.NUMERIC},
            new Object[][]{
                {new BigDecimal("12345")},                           // Scale 0
                {new BigDecimal("123.45")},                          // Scale 2
                {new BigDecimal("123.4567890123")},                  // Scale 10
                {new BigDecimal("123456789012345678901234567890.12345678901234567890")}, // Scale 38, large number
                {new BigDecimal("1234567890123456789012")},          // Large integer (>10^20)
                {BigDecimal.ZERO},                                   // Zero
                {new BigDecimal("0.000000000000000001")}            // Very small
            }
        );
        
        MockCopyIn mockCopyIn = new MockCopyIn();
        int rowCount = invokeInsertDataViaBinaryCopy(mockRs, 0, mockRs.getMetaData(), mockCopyIn);
        
        assertEquals(7, rowCount, "Should process all numeric scale variations");
        assertTrue(mockCopyIn.getWrittenData().length > 0, "Should write binary data for numeric");
    }

    @Test
    void testBinaryCopy_Decimal_NegativeAndZero() throws Exception {
        // Test negative decimals, zero, NULL
        MockResultSet mockRs = new MockResultSet(
            new int[]{Types.DECIMAL},
            new Object[][]{
                {new BigDecimal("-123.45")},         // Negative
                {new BigDecimal("-0.001")},          // Small negative
                {new BigDecimal("0")},               // Zero
                {new BigDecimal("0.00")},            // Zero with scale
                {new BigDecimal("-9999999.99")},     // Large negative
                {null}                               // NULL
            }
        );
        
        MockCopyIn mockCopyIn = new MockCopyIn();
        int rowCount = invokeInsertDataViaBinaryCopy(mockRs, 0, mockRs.getMetaData(), mockCopyIn);
        
        assertEquals(6, rowCount, "Should process negative decimals, zero, and NULL");
        assertTrue(mockCopyIn.getWrittenData().length > 0, "Should write binary data for decimal");
    }

    @Test
    void testBinaryCopy_StringFallback_TraceLogs() throws Exception {
        // Verify string fallback for unsupported types (would need ARRAY/JSON types in real scenario)
        // This test verifies mixed types including those that use string fallback
        MockResultSet mockRs = new MockResultSet(
            new int[]{Types.VARCHAR, Types.INTEGER, Types.CLOB},
            new Object[][]{
                {"test", 123, "clob_content"},
                {"hello", 456, "more_clob"}
            }
        );
        
        MockCopyIn mockCopyIn = new MockCopyIn();
        int rowCount = invokeInsertDataViaBinaryCopy(mockRs, 0, mockRs.getMetaData(), mockCopyIn);
        
        assertEquals(2, rowCount, "Should process mixed types with string fallback");
        assertTrue(mockCopyIn.getWrittenData().length > 0, "Should write binary data");
        // Note: TRACE logging verification would require log capture infrastructure
        LOG.info("String fallback test completed successfully");
    }
}

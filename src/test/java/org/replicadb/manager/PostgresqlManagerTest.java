package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.copy.CopyIn;
import org.replicadb.cli.ToolOptions;

import java.io.*;
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
}

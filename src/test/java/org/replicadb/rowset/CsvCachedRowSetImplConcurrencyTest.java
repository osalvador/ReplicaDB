package org.replicadb.rowset;

import org.junit.jupiter.api.Test;
import javax.sql.RowSetMetaData;
import javax.sql.rowset.RowSetMetaDataImpl;
import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.commons.csv.CSVFormat;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test to verify the CsvCachedRowSetImpl moveToInsertRow fix
 */
class CsvCachedRowSetImplConcurrencyTest {

    @Test
    void testMoveToInsertRowDoesNotThrowException() throws Exception {
        // Create a temporary CSV file for testing
        Path tempCsvFile = Files.createTempFile("test", ".csv");
        Files.write(tempCsvFile, "col1,col2\nvalue1,value2\n".getBytes());
        
        try {
            CsvCachedRowSetImpl csvRowSet = new CsvCachedRowSetImpl();
            csvRowSet.setSourceFile(tempCsvFile.toFile());
            csvRowSet.setCsvFormat(CSVFormat.DEFAULT.withFirstRecordAsHeader());
            csvRowSet.setColumnsTypes("VARCHAR,VARCHAR");
            csvRowSet.setColumnsNames("col1,col2");
            
            // Execute to initialize metadata
            csvRowSet.execute();
            
            // This should not throw SQLException: moveToInsertRow : CONCUR_READ_ONLY
            assertDoesNotThrow(() -> csvRowSet.moveToInsertRow(),
                    "moveToInsertRow() should not throw exception in CsvCachedRowSetImpl");
            
            // Test that we can call it multiple times without issues
            assertDoesNotThrow(() -> csvRowSet.moveToInsertRow(),
                    "moveToInsertRow() should handle multiple calls gracefully");
            
        } finally {
            // Clean up temporary file
            Files.deleteIfExists(tempCsvFile);
        }
    }

    @Test
    void testBasicCSVReading() throws Exception {
        // Create a temporary CSV file for testing
        Path tempCsvFile = Files.createTempFile("test", ".csv");
        Files.write(tempCsvFile, "name,age\nJohn,25\nJane,30\n".getBytes());
        
        try {
            CsvCachedRowSetImpl csvRowSet = new CsvCachedRowSetImpl();
            csvRowSet.setSourceFile(tempCsvFile.toFile());
            csvRowSet.setCsvFormat(CSVFormat.DEFAULT.withFirstRecordAsHeader());
            csvRowSet.setColumnsTypes("VARCHAR,INTEGER");
            csvRowSet.setColumnsNames("name,age");
            
            // Execute to initialize
            csvRowSet.execute();
            
            // Basic validation that the rowset is properly configured
            assertNotNull(csvRowSet.getMetaData());
            assertEquals(2, csvRowSet.getMetaData().getColumnCount());
            
            // Test that we can read actual data by calling next() and checking data
            boolean hasData = csvRowSet.next();
            assertTrue(hasData, "CSV should have data available");
            
            // Test accessing actual data values 
            String name = csvRowSet.getString(1);
            int age = csvRowSet.getInt(2);
            assertEquals("John", name, "First record should contain 'John'");
            assertEquals(25, age, "First record should contain age 25");
            
            // Move to second record
            hasData = csvRowSet.next();
            assertTrue(hasData, "CSV should have second record");
            
            name = csvRowSet.getString(1);
            age = csvRowSet.getInt(2);
            assertEquals("Jane", name, "Second record should contain 'Jane'");
            assertEquals(30, age, "Second record should contain age 30");
            
            // Check that we've reached the end
            hasData = csvRowSet.next();
            assertFalse(hasData, "Should be at end of data");
            
        } finally {
            // Clean up temporary file
            Files.deleteIfExists(tempCsvFile);
        }
    }
}

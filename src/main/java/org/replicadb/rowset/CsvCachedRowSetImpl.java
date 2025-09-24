package org.replicadb.rowset;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.RowSetMetaDataImpl;
import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CsvCachedRowSetImpl extends StreamingRowSetImpl {
   private static final Logger LOG = LogManager.getLogger(CsvCachedRowSetImpl.class.getName());

   private File sourceFile;
   private transient Iterable<CSVRecord> records;
   private transient List<CSVRecord> recordList; // Store records in a list for multiple iterations
   private String[] columnsTypes;
   private String[] columnName;
   private CSVFormat csvFormat;
   private static int lineNumber = 0;
   private int rowCount = 0;
   private int currentRecordIndex = 0; // Track current position in record list
   private int fetchSize = 100; // Default fetch size, like MongoDB

   public void setCsvFormat (CSVFormat csvFormat) {
      this.csvFormat = csvFormat;
   }

   public CsvCachedRowSetImpl () throws SQLException {
   }

   public void setSourceFile (File sourceFile) {
      this.sourceFile = sourceFile;
   }

   public void setColumnsTypes (String columnsTypes) {
      this.columnsTypes = columnsTypes.trim().replace(" ", "").toUpperCase().split(",");
   }

   public void setColumnsNames (String columnsNames) {
      if (columnsNames == null) return;
      this.columnName = columnsNames.trim().replace(" ", "").toUpperCase().split(",");
   }
   
   /**
    * Override moveToInsertRow to handle CSV file reading properly.
    * This method bypasses the CONCUR_READ_ONLY check by calling the parent
    * method only when necessary, or providing a no-op implementation.
    */
   @Override
   public void moveToInsertRow() throws SQLException {
      // For CSV reading, we don't actually need to insert rows in the traditional sense
      // This method is called during CSV parsing but we can make it a no-op
      // or handle it gracefully without throwing the CONCUR_READ_ONLY exception
      
      // Simply return without doing anything - CSV data is read-only anyway
      // and we're just processing the data, not actually inserting into the rowset
   }

   public void incrementRowCount () {
      this.rowCount += 1;
   }   
   public int getRowCount() {
      return this.rowCount;
   }

   /**
    * Override getFetchSize to return the stored fetch size instead of 0.
    */
   @Override
   public int getFetchSize() throws SQLException {
      return this.fetchSize;
   }

   /**
    * Override setFetchSize to actually store the fetch size value.
    */
   @Override
   public void setFetchSize(int rows) throws SQLException {
      if (rows < 0) {
         throw new SQLException("Fetch size must be >= 0");
      }
      this.fetchSize = rows;
   }
   @Override
   public void execute () throws SQLException {

      RowSetMetaData rsmd = new RowSetMetaDataImpl();
      rsmd.setColumnCount(this.columnsTypes.length);

      for (int i = 0; i <= this.columnsTypes.length - 1; i++) {

         if (columnName != null) rsmd.setColumnName(i + 1, columnName[i]);

         switch (this.columnsTypes[i]) {
            case "VARCHAR":
               rsmd.setColumnType(i + 1, Types.VARCHAR);
               break;
            case "CHAR":
               rsmd.setColumnType(i + 1, Types.CHAR);
               break;
            case "LONGVARCHAR":
               rsmd.setColumnType(i + 1, Types.LONGVARCHAR);
               break;
            case "INTEGER":
               rsmd.setColumnType(i + 1, Types.INTEGER);
               break;
            case "BIGINT":
               rsmd.setColumnType(i + 1, Types.BIGINT);
               break;
            case "TINYINT":
               rsmd.setColumnType(i + 1, Types.TINYINT);
               break;
            case "SMALLINT":
               rsmd.setColumnType(i + 1, Types.SMALLINT);
               break;
            case "NUMERIC":
               rsmd.setColumnType(i + 1, Types.NUMERIC);
               rsmd.setPrecision(i + 1, 10);
               break;
            case "DECIMAL":
               rsmd.setColumnType(i + 1, Types.DECIMAL);
               rsmd.setPrecision(i + 1, 10);
               break;
            case "DOUBLE":
               rsmd.setColumnType(i + 1, Types.DOUBLE);
               rsmd.setPrecision(i + 1, 10);
               break;
            case "FLOAT":
               rsmd.setColumnType(i + 1, Types.FLOAT);
               rsmd.setPrecision(i + 1, 10);
               break;
            case "DATE":
               rsmd.setColumnType(i + 1, Types.DATE);
               break;
            case "TIMESTAMP":
               rsmd.setColumnType(i + 1, Types.TIMESTAMP);
               break;
            case "TIME":
               rsmd.setColumnType(i + 1, Types.TIME);
               break;
            case "BOOLEAN":
               rsmd.setColumnType(i + 1, Types.BOOLEAN);
               break;
            default:
               rsmd.setColumnType(i + 1, Types.VARCHAR);
               break;
         }
      }

      setMetaData(rsmd);

      BufferedReader reader = null;
      try {
         reader = Files.newBufferedReader(sourceFile.toPath());
         this.records = csvFormat.parse(reader);
         // Convert to list for multiple iterations
         this.recordList = new ArrayList<>();
         for (CSVRecord record : this.records) {
            this.recordList.add(record);
         }
         this.currentRecordIndex = 0;
         LOG.debug("Loaded {} CSV records", this.recordList.size());
      } catch (IOException e) {
         throw new SQLException(e);
      } finally {
         if (reader != null) {
            try {
               reader.close();
            } catch (IOException e) {
               LOG.warn("Error closing CSV reader", e);
            }
         }
      }
   }

   @Override
   public boolean next () throws SQLException {
      /*
       * make sure things look sane. The cursor must be
       * positioned in the rowset or before first (0) or
       * after last (numRows + 1)
       */

      // If no data loaded yet, try to load first batch (following MongoDB pattern)
      if (size() == 0 && recordList != null && !recordList.isEmpty()) {
         readData();
         // After loading data, move to first row (readData positioned us before first)
         if (size() > 0) {
            boolean moved = internalNext(); // Move from before-first to first
            notifyCursorMoved();
            return moved;
         }
         return false;
      }

      // now move and notify
      boolean ret = this.internalNext();
      notifyCursorMoved();

      // If we don't have more rows in current batch, try to load next batch
      if (!ret) {
         ret = hasMoreRecords();
         if (ret) {
            readData();
            // After loading new batch, move from before-first to first
            ret = this.internalNext();
            notifyCursorMoved();
         }
      }
      return ret;
   }

   private boolean hasMoreRecords() {
      return recordList != null && currentRecordIndex < recordList.size();
   }

   private void readData () throws SQLException {

      // Close current cursor and reopen, similar to MongoDB implementation
      int currentFetchSize = getFetchSize();
      setFetchSize(0);
      close();
      setFetchSize(currentFetchSize);

      CSVRecord record;

      // Load CSV records into the rowset using direct row appending (like MongoDB)
      int loadedCount = 0;
      for (int i = 1; i <= currentFetchSize && hasMoreRecords(); i++) {
         lineNumber++;
         try {
            record = recordList.get(currentRecordIndex);
            currentRecordIndex++;

               // Build row array directly (following MongoDB pattern)
               Object[] rowData = new Object[this.columnsTypes.length];

               for (int j = 0; j <= this.columnsTypes.length - 1; j++) {

                  switch (this.columnsTypes[j]) {
                     case "VARCHAR":
                     case "CHAR":
                     case "LONGVARCHAR":
                        rowData[j] = getStringOrNull(record.get(j));
                        break;
                     case "INTEGER":
                        rowData[j] = getStringOrNull(record.get(j)) == null ? null : Integer.parseInt(record.get(j));
                        break;
                     case "TINYINT":
                        rowData[j] = getStringOrNull(record.get(j)) == null ? null : Byte.parseByte(record.get(j));
                        break;
                     case "SMALLINT":
                        rowData[j] = getStringOrNull(record.get(j)) == null ? null : Short.parseShort(record.get(j));
                        break;
                     case "BIGINT":
                        rowData[j] = getStringOrNull(record.get(j)) == null ? null : Long.parseLong(record.get(j));
                        break;
                     case "NUMERIC":
                     case "DECIMAL":
                        rowData[j] = getStringOrNull(record.get(j)) == null ? null : new BigDecimal(record.get(j));
                        break;
                     case "DOUBLE":
                        rowData[j] = getStringOrNull(record.get(j)) == null ? null : Double.parseDouble(record.get(j));
                        break;
                     case "FLOAT":
                        rowData[j] = getStringOrNull(record.get(j)) == null ? null : Float.parseFloat(record.get(j));
                        break;
                     case "DATE":
                        // yyyy-[m]m-[d]d
                        rowData[j] = getStringOrNull(record.get(j)) == null ? null : Date.valueOf(record.get(j));
                        break;
                     case "TIMESTAMP":
                        // yyyy-[m]m-[d]d hh:mm:ss[.f...]
                        rowData[j] = getStringOrNull(record.get(j)) == null ? null : Timestamp.valueOf(record.get(j));
                        break;
                     case "TIME":
                        // hh:mm:ss
                        rowData[j] = getStringOrNull(record.get(j)) == null ? null : Time.valueOf(record.get(j));
                        break;
                     case "BOOLEAN":
                        rowData[j] = getStringOrNull(record.get(j)) == null ? null : Boolean.parseBoolean(record.get(j));
                        break;
                     default:
                        rowData[j] = getStringOrNull(record.get(j));
                        break;
                  }
               }

               // Add row to the RowSet (this is the key fix!)
               this.appendRow(rowData);
               this.incrementRowCount();
               loadedCount++;
         } catch (Exception e) {
            LOG.error("CSV error processing record {}: {}", lineNumber, e.getMessage(), e);
            throw new SQLException(e);
         }
      }

      // Position cursor BEFORE first row so that when PostgresqlManager calls next(), 
      // it will move to the actual first row instead of skipping it (following MongoDB pattern)
      if (this.size() > 0) {
         this.beforeFirst(); // Position BEFORE first row for proper next() behavior
      } else {
         this.beforeFirst(); // No data, position before first
      }
   }

   /**
    * Parses the string argument as a boolean. The boolean returned represents the value true if the
    * string argument is not null and is equal, ignoring case, to the string "true", "yes", "on",
    * "1", "t", "y".
    *
    * @param s the String containing the booleanvalue
    * @return representation to be parsed
    */
   private boolean convertToBoolean (String s) {
      return ("1".equalsIgnoreCase(s)
          || "yes".equalsIgnoreCase(s)
          || "true".equalsIgnoreCase(s)
          || "on".equalsIgnoreCase(s)
          || "y".equalsIgnoreCase(s)
          || "t".equalsIgnoreCase(s));
   }

   /**
    * Checks if the value is empty or null and return a null object
    *
    * @param value
    * @return
    */
   private String getStringOrNull (String value) {
      if (value == null || value.isEmpty()) value = null;
      return value;
   }
}

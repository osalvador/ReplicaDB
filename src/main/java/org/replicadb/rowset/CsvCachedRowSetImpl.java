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

public class CsvCachedRowSetImpl extends StreamingRowSetImpl {
   private static final Logger LOG = LogManager.getLogger(CsvCachedRowSetImpl.class.getName());

   private File sourceFile;
   private transient Iterable<CSVRecord> records;
   private String[] columnsTypes;
   private String[] columnName;
   private CSVFormat csvFormat;
   private static int lineNumber = 0;
   private int rowCount = 0;

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
   
   public void incrementRowCount () {
      this.rowCount += 1;
   }   
    public int getRowCount () {
        return rowCount;
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
      } catch (IOException e) {
         throw new SQLException(e);
      }
   }

   @Override
   public boolean next () throws SQLException {
      /*
       * make sure things look sane. The cursor must be
       * positioned in the rowset or before first (0) or
       * after last (numRows + 1)
       */
    /*if (this.cursorPos < 0 || cursorPos >= numRows + 1) {
        throw new SQLException(resBundle.handleGetObject("cachedrowsetimpl.invalidcp").toString());
    }*/

      // now move and notify
      boolean ret = this.internalNext();
      notifyCursorMoved();

      if (!ret) {
         ret = this.records.iterator().hasNext();
         if (ret) {
            readData();
            internalFirst();
         }
      }
      return ret;
   }

   private void readData () throws SQLException {

      // Close current cursor and reaopen.
      int currentFetchSize = getFetchSize();
      setFetchSize(0);
      close();
      setFetchSize(currentFetchSize);
      moveToInsertRow();

      CSVRecord record;

      for (int i = 1; i <= getFetchSize(); i++) {
         lineNumber++;
         try {

            if (this.records.iterator().hasNext()) {
               record = this.records.iterator().next();

               for (int j = 0; j <= this.columnsTypes.length - 1; j++) {

                  switch (this.columnsTypes[j]) {
                     case "VARCHAR":
                     case "CHAR":
                     case "LONGVARCHAR":
                        if (getStringOrNull(record.get(j)) == null) updateNull(j + 1);
                        else updateString(j + 1, record.get(j));
                        break;
                     case "INTEGER":
                        if (getStringOrNull(record.get(j)) == null) updateNull(j + 1);
                        else updateInt(j + 1, Integer.parseInt(record.get(j)));
                        break;
                     case "TINYINT":
                        if (getStringOrNull(record.get(j)) == null) updateNull(j + 1);
                        else updateByte(j + 1, Byte.parseByte(record.get(j)));
                        break;
                     case "SMALLINT":
                        if (getStringOrNull(record.get(j)) == null) updateNull(j + 1);
                        else updateShort(j + 1, Short.parseShort(record.get(j)));
                        break;
                     case "BIGINT":
                        if (getStringOrNull(record.get(j)) == null) updateNull(j + 1);
                        else updateLong(j + 1, Long.parseLong(record.get(j)));
                        break;
                     case "NUMERIC":
                     case "DECIMAL":
                        /*
                         * "0"            [0,0]
                         * "0.00"         [0,2]
                         * "123"          [123,0]
                         * "-123"         [-123,0]
                         * "1.23E3"       [123,-1]
                         * "1.23E+3"      [123,-1]
                         * "12.3E+7"      [123,-6]
                         * "12.0"         [120,1]
                         * "12.3"         [123,1]
                         * "0.00123"      [123,5]
                         * "-1.23E-12"    [-123,14]
                         * "1234.5E-4"    [12345,5]
                         * "0E+7"         [0,-7]
                         * "-0"           [0,0]
                         */
                        if (getStringOrNull(record.get(j)) == null) updateNull(j + 1);
                        else updateBigDecimal(j + 1, new BigDecimal(record.get(j)));
                        break;
                     case "DOUBLE":
                        if (getStringOrNull(record.get(j)) == null) updateNull(j + 1);
                        else updateDouble(j + 1, Double.parseDouble(record.get(j)));
                        break;
                     case "FLOAT":
                        if (getStringOrNull(record.get(j)) == null) updateNull(j + 1);
                        else updateFloat(j + 1, Float.parseFloat(record.get(j)));
                        break;
                     case "DATE":
                        // yyyy-[m]m-[d]d
                        if (getStringOrNull(record.get(j)) == null) updateNull(j + 1);
                        else updateDate(j + 1, Date.valueOf(record.get(j)));
                        break;
                     case "TIMESTAMP":
                        // yyyy-[m]m-[d]d hh:mm:ss[.f...]
                        if (getStringOrNull(record.get(j)) == null) updateNull(j + 1);
                        else updateTimestamp(j + 1, Timestamp.valueOf(record.get(j)));
                        break;
                     case "TIME":
                        // hh:mm:ss
                        if (getStringOrNull(record.get(j)) == null) updateNull(j + 1);
                        else updateTime(j + 1, Time.valueOf(record.get(j)));
                        break;
                     case "BOOLEAN":
                        if (getStringOrNull(record.get(j)) == null) updateNull(j + 1);
                        else updateBoolean(j + 1, convertToBoolean(record.get(j)));
                        break;
                     default:
                        if (getStringOrNull(record.get(j)) == null) updateNull(j + 1);
                        else updateString(j + 1, record.get(j));
                        break;
                  }
               }

               insertRow();
               incrementRowCount();
            }
         } catch (Exception e) {
            LOG.error("An error has occurred reading line number {} of the CSV file", lineNumber, e);
            throw e;
         }
      }

      moveToCurrentRow();
      beforeFirst();      
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

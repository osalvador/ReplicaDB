package org.replicadb.rowset;

import com.mongodb.client.MongoCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.json.Converter;
import org.bson.json.JsonWriterSettings;
import org.bson.json.StrictJsonWriter;
import org.bson.types.Binary;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.RowSetMetaDataImpl;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MongoDBRowSetImpl extends StreamingRowSetImpl {
   private static final Logger LOG = LogManager.getLogger(MongoDBRowSetImpl.class.getName());

   private transient MongoCursor<Document> cursor;

   private Boolean isSourceAndSinkMongo = false;

   private Document firstDocument = null;

   public MongoDBRowSetImpl () throws SQLException {
      super();
   }

   // get cursor
   public MongoCursor<Document> getCursor () {
      return this.cursor;
   }

   @Override
   public void execute () throws SQLException {

      RowSetMetaData rsmd = new RowSetMetaDataImpl();
      List<String> fields = new ArrayList<>();

      // if the sink database is mongodb, the document will be inserted as is
      if (Boolean.TRUE.equals(this.isSourceAndSinkMongo)) {
         rsmd.setColumnCount(1);
         rsmd.setColumnName(1, "document");
         rsmd.setColumnType(1, java.sql.Types.OTHER);
         setMetaData(rsmd);
         fields.add("document");
      } else {

         // The resultset metadata will be defined by the first document in the cursor
         // If there is other documents with different structure, the fields will be ignored

         if (this.firstDocument != null && this.firstDocument.size() > 0) {
            Document document = this.firstDocument;

            // get column count
            rsmd.setColumnCount(document.size());

            AtomicInteger i = new AtomicInteger(1);

            document.keySet().forEach(key -> {
               Object value = document.get(key);
               String typeString = value == null ? "null" : value.getClass().toString();
               LOG.trace("Key: {},  Value: {} , Type: {} ", key, value, typeString);

               // define java.sql.Types from the type of the value
               int type = getSqlType(typeString);
               try {
                  rsmd.setColumnName(i.get(), key);
                  rsmd.setColumnType(i.get(), type);
                  fields.add(key);
               } catch (SQLException e) {
                  LOG.error(e);
               }
               i.getAndIncrement();
            });
         }
         setMetaData(rsmd);
      }
      // Log fields in order
      LOG.warn("The source columns/fields names are be defined by the first document returned by the cursor in this order: {}", fields);
   }

   /**
    * Maps a given class type to a corresponding SQL type.
    *
    * @param typeString the class type as a string
    * @return the corresponding SQL type, or {@link java.sql.Types#OTHER} if no corresponding type is found.
    */
   static int getSqlType (String typeString) {
      switch (typeString) {
         case "class java.lang.String":
            return java.sql.Types.VARCHAR;
         case "class java.lang.Integer":
            return java.sql.Types.INTEGER;
         case "class java.lang.Long":
            return java.sql.Types.BIGINT;
         case "class java.lang.Double":
            return java.sql.Types.DOUBLE;
         case "class java.lang.Boolean":
            return java.sql.Types.BOOLEAN;
         case "class java.lang.Float":
            return java.sql.Types.FLOAT;
         case "class java.lang.Short":
            return java.sql.Types.SMALLINT;
         case "class java.lang.Byte":
            return java.sql.Types.TINYINT;
         case "class java.math.BigDecimal":
         case "class org.bson.types.Decimal128":
            return java.sql.Types.DECIMAL;
         case "class java.sql.Date":
         case "class java.util.Date":
         case "class java.sql.Timestamp":
            return Types.TIMESTAMP_WITH_TIMEZONE;
         case "class java.sql.Time":
            return java.sql.Types.TIME;
         case "class org.bson.types.Binary":
            return java.sql.Types.BINARY;
         case "class java.util.List":
         case "class java.util.ArrayList":
            return java.sql.Types.ARRAY;
         case "class org.bson.Document":
            return Types.STRUCT;
         case "class org.bson.types.ObjectId":
         case "class java.lang.Object":
         default:
            return java.sql.Types.OTHER;
      }
   }


   @Override
   public boolean next () throws SQLException {
      /*
       * make sure things look sane. The cursor must be
       * positioned in the rowset or before first (0) or
       * after last (numRows + 1)
       */
      // now move and notify
      boolean ret = this.internalNext();
      notifyCursorMoved();

      if (!ret) {
         ret = this.cursor.hasNext();
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

      Document document;

      for (int i = 1; i <= getFetchSize(); i++) {

         try {

            if (this.cursor.hasNext()) {
               document = this.cursor.next();

               // if the sink database is mongodb, the document will be inserted as is
               if (Boolean.TRUE.equals(this.isSourceAndSinkMongo)) {
                  updateObject(1, document);
               } else {

                  for (int j = 0; j <= getMetaData().getColumnCount() - 1; j++) {
                     String columnName = getMetaData().getColumnName(j + 1);
                     int columnType = getMetaData().getColumnType(j + 1);

                     switch (columnType) {
                        case Types.VARCHAR:
                        case Types.CHAR:
                        case Types.LONGVARCHAR:
                           if (document.getString(columnName) == null) updateNull(j + 1);
                           else updateString(j + 1, document.getString(columnName));
                           break;
                        case Types.INTEGER:
                        case Types.TINYINT:
                        case Types.SMALLINT:
                           if (document.getInteger(columnName) == null) updateNull(j + 1);
                           else updateInt(j + 1, document.getInteger(columnName));
                           break;
                        case Types.BIGINT:
                        case Types.NUMERIC:
                        case Types.DECIMAL:
                           if (document.getLong(columnName) == null) updateNull(j + 1);
                           else updateBigDecimal(j + 1, BigDecimal.valueOf(document.getLong(columnName)));
                           break;
                        case Types.DOUBLE:
                           if (document.getDouble(columnName) == null) updateNull(j + 1);
                           else updateDouble(j + 1, document.getDouble(columnName));
                           break;
                        case Types.TIMESTAMP_WITH_TIMEZONE:
                           if (document.getDate(columnName) == null) updateNull(j + 1);
                              // convert to offsetDateTime
                           else updateObject(j + 1, document.getDate(columnName).toInstant().atOffset(ZoneOffset.UTC));
                           break;
                        case Types.BINARY:
                        case Types.BLOB:
                           Binary bin = document.get(columnName, org.bson.types.Binary.class);
                           if (bin == null) updateNull(j + 1);
                           else updateBytes(j + 1, bin.getData());
                           break;
                        case Types.BOOLEAN:
                           if (document.getBoolean(columnName) == null) updateNull(j + 1);
                           else updateBoolean(j + 1, document.getBoolean(columnName));
                           break;
                        default:
                           String json = documentToJson(document.get(columnName, org.bson.Document.class));
                           if (json == null) updateNull(j + 1);
                           else updateString(j + 1, json);
                           break;
                     }
                  }
               }
               insertRow();
               document.clear();
            }
         } catch (Exception e) {
            LOG.error("MongoDB error: {}", e.getMessage(), e);
            throw e;
         }
      }

      moveToCurrentRow();
      beforeFirst();
   }

   public static class JsonDateTimeConverter implements Converter<Long> {
      static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("UTC"));
      @Override
      public void convert (Long value, StrictJsonWriter writer) {
         Instant instant = new Date(value).toInstant();
         String s = DATE_TIME_FORMATTER.format(instant);
         writer.writeString(s);
      }
   }

   private String documentToJson (Document document) {
      if (document == null) return null;
      return document.toJson(JsonWriterSettings
          .builder()
          .dateTimeConverter(new JsonDateTimeConverter())
          .build());
   }

   public void setMongoCursor (MongoCursor<Document> cursor) {
      this.cursor = cursor;
   }

   public void setSinkMongoDB (Boolean sinkMongoDB) {
      isSourceAndSinkMongo = sinkMongoDB;
   }

   public void setMongoFirstDocument (Document firstDocument) {
      this.firstDocument = firstDocument;
   }
}



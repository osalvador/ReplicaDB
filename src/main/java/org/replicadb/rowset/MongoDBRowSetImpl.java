package org.replicadb.rowset;

import com.google.gson.Gson;
import com.mongodb.client.MongoCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.replicadb.manager.util.BsonUtils;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.RowSetMetaDataImpl;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MongoDBRowSetImpl extends StreamingRowSetImpl {
   private static final Logger LOG = LogManager.getLogger(MongoDBRowSetImpl.class.getName());

   private transient MongoCursor<Document> cursor;

   private Boolean isSourceAndSinkMongo = false;

   private Document firstDocument = null;
   private LinkedHashSet<String> mongoProjection;
   private boolean isAggregation;
   private int rowCount = 0;

   public MongoDBRowSetImpl () throws SQLException {
      super();
   }

   // get cursor
   public MongoCursor<Document> getCursor () {
      return this.cursor;
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
      List<String> fields = new ArrayList<>();

      // it the first document is null, it means that the cursor is empty
      if (this.firstDocument != null && this.firstDocument.size() == 0) {
         // log warning
         LOG.warn("No documents found in the source collection");
         setMetaData(rsmd);
         return;
      }

      // if the sink database is mongodb, the document will be inserted as is
      if (Boolean.TRUE.equals(this.isSourceAndSinkMongo)) {
         rsmd.setColumnCount(1);
         rsmd.setColumnName(1, "document");
         rsmd.setColumnType(1, java.sql.Types.OTHER);
         fields.add("document");
      } else {

         // The resultset metadata will be defined by the first document in the cursor
         // If there are other documents with different structure, the fields will be ignored
         Document document = this.firstDocument;

         AtomicInteger i = new AtomicInteger(1);

         // if the query is an aggregation, the fields will be defined by the document keys
         // otherwise, the fields will be defined by the projection
         // cast to List to preserve the order
         List<String> keys = new ArrayList<>();
         if (this.isAggregation) {
            keys.addAll(document.keySet());
            // set document fields to the projection
            this.mongoProjection = new LinkedHashSet<>(keys);
         } else {
            keys.addAll(this.mongoProjection);
         }

         // set column count
         rsmd.setColumnCount(keys.size());

         keys.forEach(key -> {
            Object value = document.get(key);
            // If the value is null, the type will be set to VARCHAR
            // log a warning
            if (value == null) {
               LOG.warn("The value of the field {} is null. The type will be set to VARCHAR", key);
            }

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
               throw new RuntimeException(e);
            }
            i.getAndIncrement();
         });

      }
      setMetaData(rsmd);
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
         case "null":
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
            return Types.ROWID;
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

      // it the first document size is 0, it means that the cursor is empty
      if (this.firstDocument != null && this.firstDocument.size() == 0) {
         return false;
      }

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

      // load fetch size documents into the rowset
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
                        case Types.ROWID:
                           ObjectId oid = document.getObjectId(columnName);
                           if (oid == null) updateNull(j + 1);
                           else updateString(j + 1, oid.toString());
                           break;
                        case Types.ARRAY:
                           List list = document.get(columnName, List.class);
                           String jsonArr = new Gson().toJson(list);
                           if (list == null) updateNull(j + 1);
                           else updateString(j + 1, jsonArr);
                           break;
                        default:
                           String json = BsonUtils.toJson(document.get(columnName, org.bson.Document.class));
                           if (json == null) updateNull(j + 1);
                           else updateString(j + 1, json);
                           break;
                     }
                  }
               }
               insertRow();
               incrementRowCount();
            }
         } catch (Exception e) {
            LOG.error("MongoDB error: {}", e.getMessage(), e);
            throw e;
         }
      }

      moveToCurrentRow();
      beforeFirst();
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

   public void setMongoProjection (BsonDocument projection) {
      LinkedHashSet<String> fields = new LinkedHashSet<>();
      // iterate over the projection document and add the fields to the fields list only if they are not 0
      for (Map.Entry<String, BsonValue> entry : projection.entrySet()) {
         if (!entry.getValue().isInt32() || entry.getValue().asInt32().getValue() != 0) {
            fields.add(entry.getKey());
         }
      }
      this.mongoProjection = fields;
   }

   public void setAggregation (boolean b) {
      this.isAggregation = b;
   }
}



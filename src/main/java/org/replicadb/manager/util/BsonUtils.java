package org.replicadb.manager.util;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.util.List;

public class BsonUtils  {

   public static String toJson (Document document) {
      if (document == null) return null;
      return document.toJson(JsonWriterSettings
          .builder()
          .dateTimeConverter(new BsonDateTimeConverter())
          .build());
   }

   public static String toJson (List<Document> list) {
      if (list == null) return null;
      StringBuilder sb = new StringBuilder();
      sb.append("[");
      for (Document document : list) {
         sb.append(document.toJson(JsonWriterSettings
             .builder()
             .dateTimeConverter(new BsonDateTimeConverter())
             .build()));
         sb.append(",");
      }
      sb.deleteCharAt(sb.length() - 1);
      sb.append("]");
      return sb.toString();
   }

   public static Object toJsonStr (List<BsonDocument> list) {
      if (list == null) return null;
      StringBuilder sb = new StringBuilder();
      sb.append("[");
      for (BsonDocument document : list) {
         sb.append(document.toJson(JsonWriterSettings
             .builder()
             .dateTimeConverter(new BsonDateTimeConverter())
             .build()));
         sb.append(",");
      }
      sb.deleteCharAt(sb.length() - 1);
      sb.append("]");
      return sb.toString();
   }
}

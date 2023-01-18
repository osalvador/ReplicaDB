package org.replicadb.manager;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.client.*;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.WriteModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.BsonArrayCodec;
import org.bson.codecs.DecoderContext;
import org.bson.json.JsonParseException;
import org.bson.json.JsonReader;
import org.jetbrains.annotations.NotNull;
import org.postgresql.util.PGobject;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.util.BandwidthThrottling;
import org.replicadb.manager.util.BsonUtils;
import org.replicadb.rowset.MongoDBRowSetImpl;

import javax.ws.rs.NotSupportedException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.replicadb.manager.SupportedManagers.MONGODB;
import static org.replicadb.manager.SupportedManagers.MONGODBSRV;

public class MongoDBManager extends SqlManager {

   private static final Logger LOG = LogManager.getLogger(MongoDBManager.class.getName());

   private MongoClient sourceMongoClient;
   private MongoClient sinkMongoClient;

   private MongoDatabase sourceDatabase;
   private MongoDatabase sinkDatabase;

   private MongoDBRowSetImpl mongoDbResultSet;

   private static Long chunkSize = 0L;
   private List<String> primaryKeys;

   /**
    * Constructs the SqlManager.
    *
    * @param opts the ReplicaDB ToolOptions describing the user's requested action.
    */
   public MongoDBManager (ToolOptions opts, DataSourceType dsType) {
      super(opts);
      this.dsType = dsType;
      // Mongodb as sink is not compatible with mode complete-atomic
      if (dsType.equals(DataSourceType.SINK) && options.getMode().equals(ReplicationMode.COMPLETE_ATOMIC.getModeText())) {
         throw new NotSupportedException("The complete-atomic mode is not supported in MongoDB as sink.");
      }
   }

   @Override
   protected Connection makeSourceConnection () throws SQLException {
      // Create a MongoDB client using the connection parameters specified in the ToolOptions
      String uri = options.getSourceConnect();
      ConnectionString connectionString = new ConnectionString(uri);

      String databaseName = connectionString.getDatabase();
      // if the database is not specified in the connection string, throw an exception
      if (Objects.isNull(databaseName)) {
         throw new IllegalArgumentException("The database must be specified in the connection string");
      }

      MongoClientSettings settings = MongoClientSettings.builder()
          .compressorList(Arrays.asList(MongoCompressor.createSnappyCompressor(),
              MongoCompressor.createZlibCompressor(),
              MongoCompressor.createZstdCompressor()))
          .applyConnectionString(connectionString)
          .build();
      sourceMongoClient = MongoClients.create(settings);

      sourceDatabase = sourceMongoClient.getDatabase(databaseName);

      // MongoDB does not use traditional JDBC connections, so we can return null here
      return null;
   }

   @Override
   protected Connection makeSinkConnection () throws SQLException {
      // Create a MongoDB client using the connection parameters specified in the ToolOptions
      String uri = options.getSinkConnect();
      ConnectionString connectionString = new ConnectionString(uri);

      String databaseName = connectionString.getDatabase();
      // if the database is not specified in the connection string, throw an exception
      if (Objects.isNull(databaseName)) {
         throw new IllegalArgumentException("The database must be specified in the connection string");
      }

      MongoClientSettings settings = MongoClientSettings.builder()
          .compressorList(Arrays.asList(MongoCompressor.createSnappyCompressor(),
              MongoCompressor.createZlibCompressor(),
              MongoCompressor.createZstdCompressor()))
          .applyConnectionString(connectionString)
          .build();
      sinkMongoClient = MongoClients.create(settings);

      sinkDatabase = sinkMongoClient.getDatabase(databaseName);
      // MongoDB does not use traditional JDBC connections, so we can return null here
      return null;
   }

   @Override
   public ResultSet readTable (String tableName, String[] columns, int nThread) throws SQLException {
      // If table name parameter is null get it from options
      String collectionName = tableName == null ? this.options.getSourceTable() : tableName;
      // if the chunk size is 0 and the current job is greater than 0, return null
      if (chunkSize == 0 && nThread > 0) {
         return null;
      }

      long skip = nThread * chunkSize;
      mongoDbResultSet = new MongoDBRowSetImpl();

      try {
         // set fetch size
         mongoDbResultSet.setFetchSize(options.getFetchSize());
         // get a handle to the collection
         MongoCollection<Document> collection = sourceDatabase.getCollection(collectionName);
         MongoCursor<Document> cursor;
         Document firstDocument;

         // if source query is specified, use it as mongodb aggregation pipeline
         if (options.getSourceQuery() != null) {
            mongoDbResultSet.setAggregation(true);
            String queryAggregation = options.getSourceQuery();
            List<BsonDocument> pipeline = getAggregation(queryAggregation);

            if (this.options.getJobs() == nThread + 1) {
               // If it's the last job, skip the first documents
               pipeline.add(BsonDocument.parse("{ $skip: " + skip + " }"));
            } else {
               // add skip and limit to the pipeline
               pipeline.add(BsonDocument.parse("{ $skip: " + skip + " }"));
               pipeline.add(BsonDocument.parse("{ $limit: " + chunkSize + " }"));
            }

            LOG.info("{}: Using this aggregation query to get data from MongoDB: {}", Thread.currentThread().getName(), BsonUtils.toJsonStr(pipeline));
            // create a MongoCursor to iterate over the results
            cursor = collection.aggregate(pipeline).batchSize(options.getFetchSize()).allowDiskUse(true).cursor();
            firstDocument = collection.aggregate(pipeline).allowDiskUse(true).maxAwaitTime(100, TimeUnit.MINUTES).first();
         } else {
            // create a MongoCursor to iterate over the results
            FindIterable<Document> findIterable = collection.find();

            // Source Where
            if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
               BsonDocument filter = BsonDocument.parse(options.getSourceWhere());
               findIterable.filter(filter);
               LOG.info("{}: Using this clause to filter data from MongoDB: {}", Thread.currentThread().getName(), filter.toJson());
            }
            // Source Fields
            if (options.getSourceColumns() != null && !options.getSourceColumns().isEmpty()) {
               BsonDocument projection = BsonDocument.parse(options.getSourceColumns());
               findIterable.projection(projection);
               mongoDbResultSet.setMongoProjection(projection);
               LOG.info("{}: Using this clause to project data from MongoDB: {}", Thread.currentThread().getName(), projection.toJson());
            }

            if (this.options.getJobs() == nThread + 1) {
               // If it's the last job, skip the first documents
               findIterable.skip((int) skip);
               LOG.info("{}: Skip {} data from source", Thread.currentThread().getName(), skip);
            } else {
               // add skip and limit to the pipeline
               findIterable.skip(Math.toIntExact(skip));
               findIterable.limit(Math.toIntExact(chunkSize));
               LOG.info("{}: Skip {}, Limit {} data from source", Thread.currentThread().getName(), skip, chunkSize);
            }

            // if it is parallel processing
            if (options.getJobs() > 1) {
               // sort by object id
               findIterable.sort(Sorts.ascending("_id"));
               LOG.info("{}: Sort by _id", Thread.currentThread().getName());
               // not compatible with 4.x version
               // findIterable.allowDiskUse(true);
            }

            findIterable.batchSize(options.getFetchSize());
            cursor = findIterable.cursor();
            firstDocument = findIterable.first();
         }

         mongoDbResultSet.setMongoFirstDocument(firstDocument);
         mongoDbResultSet.setSinkMongoDB(isSourceAndSinkMongoDB());
         mongoDbResultSet.execute();
         mongoDbResultSet.setMongoCursor(cursor);

      } catch (JsonParseException jpe) {
         LOG.error("{}: Parse JSON exception in some source parameters where, query, columns: {}", Thread.currentThread().getName(), jpe.getMessage(), jpe);
         // rethrow the exception
         throw jpe;
      } catch (Exception e) {
         LOG.error("{}: Error: {}", Thread.currentThread().getName(), e.getMessage(), e);
         // rethrow the exception
         throw e;
      }
      return mongoDbResultSet;
   }

   @NotNull
   private static List<BsonDocument> getAggregation (String queryAggregation) {
      // parse the aggregation query string into a List of BsonDocument
      return new BsonArrayCodec().decode(new JsonReader(queryAggregation), DecoderContext.builder().build())
          .stream().map(BsonValue::asDocument)
          .collect(Collectors.toList());
   }

   private Boolean isSourceAndSinkMongoDB () {
      // if source and sink are a MongoDB database, return true
      if (
          (MONGODB.isTheManagerTypeOf(options, DataSourceType.SINK) || MONGODBSRV.isTheManagerTypeOf(options, DataSourceType.SINK)) &&
              (MONGODB.isTheManagerTypeOf(options, DataSourceType.SOURCE) || MONGODBSRV.isTheManagerTypeOf(options, DataSourceType.SOURCE))
      ) {
         return true;
      }
      return false;
   }

   @Override
   public int insertDataToTable (ResultSet resultSet, int taskId) throws Exception {
      int totalRows = 0;

      String collectionName = getInsertDataCollection();

      MongoCollection<Document> sinkCollection = sinkDatabase.getCollection(collectionName);
      List<WriteModel<Document>> writeOperations = new ArrayList<>();
      // unordered bulk write
      BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);

      if (resultSet != null && resultSet.next()) {
         // Create Bandwidth Throttling
         BandwidthThrottling bt = new BandwidthThrottling(options.getBandwidthThrottling(), options.getFetchSize(), resultSet);
         do {
            bt.acquiere();
            if (Boolean.TRUE.equals(isSourceAndSinkMongoDB())) {
               // Add document to bulk
               writeOperations.add(new InsertOneModel<>((Document) resultSet.getObject(1)));
            } else {
               // iterate columns
               Document document = new Document();
               for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                  String columnName = resultSet.getMetaData().getColumnName(i);
                  switch (resultSet.getMetaData().getColumnType(i)) {
                     case -104: //Oracle INTERVALDS
                     case -103: //Oracle INTERVALYM
                     case Types.SQLXML:
                        document.put(columnName, resultSet.getString(i));
                        break;
                     case Types.TIMESTAMP:
                     case Types.TIMESTAMP_WITH_TIMEZONE:
                     case -101:
                     case -102:
                        document.put(columnName, resultSet.getTimestamp(i));
                        break;
                     case Types.BINARY:
                     case Types.VARBINARY:
                     case Types.LONGVARBINARY:
                        document.put(columnName, resultSet.getBytes(i));
                        break;
                     case Types.BLOB:
                        Blob blobData = resultSet.getBlob(i);
                        if (blobData != null) {
                           document.put(columnName, blobData.getBytes(1, (int) blobData.length()));
                           blobData.free();
                        }
                        break;
                     case Types.CLOB:
                        Clob clobData = resultSet.getClob(i);
                        document.put(columnName, clobToString(clobData));
                        if (clobData != null) clobData.free();
                        break;
                     case 1111: // Postgres JSON, intervals and others
                        Object object = resultSet.getObject(i);
                        if (object instanceof PGobject) {
                           PGobject pgObject = (PGobject) object;
                           // if Document.parse fails, will be saved as a string
                           try {
                              document.put(columnName, Document.parse(pgObject.getValue()));
                           } catch (Exception e) {
                              document.put(columnName, pgObject.getValue());
                           }
                        } else {
                           document.put(columnName, object);
                        }
                        break;
                     default:
                        document.put(columnName, resultSet.getObject(i));
                        break;
                  }
               }

               // Add document to bulk
               writeOperations.add(new InsertOneModel<>(document));
            }

            totalRows++;
            if (writeOperations.size() == options.getFetchSize()) {
               sinkCollection.bulkWrite(writeOperations, bulkWriteOptions);
               writeOperations.clear();
            }
         } while (resultSet.next());

         // insert remaining documents
         if (!writeOperations.isEmpty()) {
            sinkCollection.bulkWrite(writeOperations, bulkWriteOptions);
            writeOperations.clear();
         }
      }

      return totalRows;
   }

   private String getInsertDataCollection () {
      // get collection name
      String collectionName;
      if (options.getMode().equals(ReplicationMode.COMPLETE.getModeText())) {
         collectionName = getSinkTableName();
      } else {
         collectionName = getQualifiedStagingTableName();
      }
      return collectionName;
   }

   @Override
   public String getDriverClass () {
      // MongoDB does not use JDBC drivers, so this method does not need to be implemented
      return null;
   }

   @Override
   public void preSourceTasks () throws Exception {
      long totalRows = 0;

      if (this.options.getJobs() != 1) {
         this.getConnection();
         // get source collection
         MongoCollection<Document> collection = sourceDatabase.getCollection(this.options.getSourceTable());

         // Source Query
         if (options.getSourceQuery() != null && !options.getSourceQuery().isEmpty()) {
            String queryAggregation = options.getSourceQuery();
            List<BsonDocument> pipeline = getAggregation(queryAggregation);
            // add count
            pipeline.add(BsonDocument.parse("{ $count: \"count\" }"));
            LOG.info("Using this aggregation to count the total number of rows from the MongoDB source: {}", BsonUtils.toJsonStr(pipeline));
            // get the first document from the aggregation with 100 minutes timeout
            Document countDocument = collection.aggregate(pipeline).maxAwaitTime(100, TimeUnit.MINUTES).allowDiskUse(true).first();
            if (countDocument != null) {
               // get integer and cast to long
               totalRows = countDocument.getInteger("count",0);
            }
         } else {
            BsonDocument where = new BsonDocument();
            // Source Where
            if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
               where = BsonDocument.parse(options.getSourceWhere());
            }
            totalRows = collection.countDocuments(where);
         }

         // set chunk size for each task
         this.chunkSize = Math.abs(totalRows / this.options.getJobs());
         LOG.info("Source collection total rows: {}, chunk size per job: {}", totalRows, this.chunkSize);
      }
   }

   @Override
   public void postSourceTasks () throws Exception {
   }

   @Override
   protected void createStagingTable () throws Exception {
      this.getConnection();
      try {

         String sinkStagingCollectionName = getQualifiedStagingTableName();

         // create staging collection
         sinkDatabase.createCollection(sinkStagingCollectionName);

         // get primary keys, getting the unique indexes
         MongoCollection<Document> sinkCollection = sinkDatabase.getCollection(getSinkTableName());
         List<String> primaryKeys = new ArrayList<>();
         for (Document index : sinkCollection.listIndexes()) {
            if (index.get("unique") != null && (Boolean) index.get("unique")) {
               // get all key fields names
               Document key = (Document) index.get("key");
               for (String keyName : key.keySet()) {
                  primaryKeys.add(keyName);
               }
            }
         }

         setPrimaryKeys(primaryKeys);

         LOG.info("Creating staging collection : {}", sinkStagingCollectionName);

      } catch (Exception e) {
         LOG.error("Error creating staging table: {}", e.getMessage(), e);
         throw e;
      }
      this.close();
   }

   private void setPrimaryKeys (List<String> primaryKeys) {
      // if primary keys empty and mode is incremental raise exception
      if ((primaryKeys == null || primaryKeys.isEmpty())
          && options.getMode().equals(ReplicationMode.INCREMENTAL.getModeText())) {
         throw new IllegalArgumentException("Sink collection \"" + getSinkTableName() + "\" must have at least one unique key index for incremental mode.");
      }
      LOG.info("Primary key fields: {}", primaryKeys);
      this.primaryKeys = primaryKeys;
   }

   @Override
   protected void mergeStagingTable () throws Exception {

      this.getConnection();

      // merge staging table with sink table
      try {
         MongoCollection<Document> sinkStagingCollection = sinkDatabase.getCollection(getQualifiedStagingTableName());

         String aggregationQuery = "[ {$project: {_id:0}},{$merge:{ into:\"${SINK_COLLECTION}\", on:[${PRIMARY_KEYS}], whenMatched: \"replace\", whenNotMatched: \"insert\" }} ]";
         aggregationQuery = aggregationQuery.replace("${SINK_COLLECTION}", getSinkTableName());
         // replace primary keys separated by commas and surrounded by quotes
         aggregationQuery = aggregationQuery.replace("${PRIMARY_KEYS}", primaryKeys.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(",")));

         // merge collections
         LOG.info("Merging staging collection with sink collection using aggregation query: {}", aggregationQuery);
         List<BsonDocument> pipeline = getAggregation(aggregationQuery);
         sinkStagingCollection.aggregate(pipeline).allowDiskUse(true).maxAwaitTime(100, TimeUnit.MINUTES).first();


      } catch (Exception e) {
         LOG.error("Error merging staging table with sink table: {}", e.getMessage(), e);
         throw e;
      }
      this.close();
   }

   @Override
   public void close () throws SQLException {

      // close mongoResultSets
      if (mongoDbResultSet != null) {
         mongoDbResultSet.setFetchSize(0);
         mongoDbResultSet.close();
         MongoCursor<Document> cursor = mongoDbResultSet.getCursor();
         if (cursor != null) {
            cursor.close();
         }
      }

      // Close connection, ignore exceptions
      if (this.sourceMongoClient != null) {
         try {
            this.sourceMongoClient.close();
         } catch (Exception e) {
            LOG.error(e);
         }
      }
      if (this.sinkMongoClient != null) {
         try {
            this.sinkMongoClient.close();
         } catch (Exception e) {
            LOG.error(e);
         }
      }
   }

   @Override
   protected void truncateTable () throws SQLException {
      // Delete all documents in the collection
      this.getConnection();
      LOG.info("Deleting all documents from the sink collection: {}", getInsertDataCollection());
      sinkDatabase.getCollection(getInsertDataCollection()).deleteMany(new Document());
      this.close();
   }

   @Override
   public void dropStagingTable () throws SQLException {
      this.getConnection();
      LOG.info("Dropping staging collection: {}", getQualifiedStagingTableName());
      sinkDatabase.getCollection(getQualifiedStagingTableName()).drop();
      this.close();
   }

}

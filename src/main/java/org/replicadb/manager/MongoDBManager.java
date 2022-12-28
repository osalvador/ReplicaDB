package org.replicadb.manager;


import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.BsonArrayCodec;
import org.bson.codecs.DecoderContext;
import org.bson.json.JsonReader;
import org.jetbrains.annotations.NotNull;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.util.BandwidthThrottling;
import org.replicadb.rowset.MongoDBRowSetImpl;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.replicadb.manager.SupportedManagers.MONGODB;
import static org.replicadb.manager.SupportedManagers.MONGODBSRV;

public class MongoDBManager extends SqlManager {

   private static final Logger LOG = LogManager.getLogger(MongoDBManager.class.getName());
   private static final String MONGO_LIMIT = "limit";
   private static final String MONGO_SKIP = "skip";

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
            String queryAggregation = options.getSourceQuery();
            List<BsonDocument> pipeline = getAggregation(queryAggregation);

            if (this.options.getJobs() == nThread + 1) {
               // add skip to the pipeline
               pipeline.add(BsonDocument.parse("{ $skip: "+skip+" }"));
            } else {
               // add skip and limit to the pipeline
               pipeline.add(BsonDocument.parse("{ $skip: "+skip+" }"));
               pipeline.add(BsonDocument.parse("{ $limit: "+chunkSize+" }"));
            }

            LOG.info("Using this aggregation query to get data from MongoDB: {}", pipeline);
            // create a MongoCursor to iterate over the results
            cursor = collection.aggregate(pipeline).allowDiskUse(true).cursor();
            firstDocument = collection.aggregate(pipeline).allowDiskUse(true).first();
         } else {
            // create a MongoCursor to iterate over the results
            FindIterable<Document> findIterable = collection.find();

            // Source Where
            if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
               BsonDocument filter = BsonDocument.parse(options.getSourceWhere());
               findIterable.filter(filter);
               LOG.info("Using this clause to filter data from MongoDB: {}", filter.toJson());
            }
            // Source Fields
            if (options.getSourceColumns() != null && !options.getSourceColumns().isEmpty()) {
               BsonDocument projection = BsonDocument.parse(options.getSourceColumns());
               findIterable.projection(projection);
               LOG.info("Using this clause to project data from MongoDB: {}", projection.toJson());
            }

            if (this.options.getJobs() == nThread + 1) {
               // add skip to the pipeline
               findIterable.skip((int) skip);
               LOG.info("Using this clause to skip data from MongoDB: {}", skip);
            } else {
               // add skip and limit to the pipeline
               findIterable.skip(Math.toIntExact(skip));
               findIterable.limit(Math.toIntExact(chunkSize));
               LOG.info("Using this clause to skip and limit data from MongoDB: {} {}", skip, chunkSize);
            }

            findIterable.batchSize(options.getFetchSize());
            findIterable.allowDiskUse(true);
            cursor = findIterable.cursor();
            firstDocument = findIterable.first();

         }
         mongoDbResultSet.setMongoFirstDocument(firstDocument);
         mongoDbResultSet.setSinkMongoDB(isSourceAndSinkMongoDB());
         mongoDbResultSet.execute();
         mongoDbResultSet.setMongoCursor(cursor);

      } catch (MongoException me) {
         LOG.error("MongoDB error: {}", me.getMessage(), me);
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

      if (resultSet.next()) {
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
                  document.put(resultSet.getMetaData().getColumnName(i), resultSet.getObject(i));
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
            totalRows = (long) collection.aggregate(pipeline).first().getInteger("count");
         } else {
            BsonDocument where = new BsonDocument();
            // Source Where
            if (options.getSourceWhere() != null && !options.getSourceWhere().isEmpty()) {
               where = BsonDocument.parse(options.getSourceWhere());
            }
            totalRows = collection.countDocuments(where);
         }

         // set chunk size for each task
         this.chunkSize =  Math.abs( totalRows / this.options.getJobs());
         LOG.info("Total rows: {}, chunk size: {}", totalRows, this.chunkSize);

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
         sinkStagingCollection.aggregate(pipeline).allowDiskUse(true).first();


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
         cursor.close();
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

package org.replicadb.manager;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
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
	 * @param opts
	 *            the ReplicaDB ToolOptions describing the user's requested action.
	 */
	public MongoDBManager(ToolOptions opts, DataSourceType dsType) {
		super(opts);
		this.dsType = dsType;
		// Mongodb as sink is not compatible with mode complete-atomic
		if (dsType.equals(DataSourceType.SINK)
				&& this.options.getMode().equals(ReplicationMode.COMPLETE_ATOMIC.getModeText())) {
			throw new UnsupportedOperationException("The complete-atomic mode is not supported in MongoDB as sink.");
		}
	}

	@Override
	protected Connection makeSourceConnection() throws SQLException {
		// Create a MongoDB client using the connection parameters specified in the
		// ToolOptions
		final String uri = this.options.getSourceConnect();
		final ConnectionString connectionString = new ConnectionString(uri);

		final String databaseName = connectionString.getDatabase();
		// if the database is not specified in the connection string, throw an exception
		if (Objects.isNull(databaseName)) {
			throw new IllegalArgumentException("The database must be specified in the connection string");
		}

		final MongoClientSettings settings = MongoClientSettings.builder()
				.compressorList(Arrays.asList(MongoCompressor.createSnappyCompressor(),
						MongoCompressor.createZlibCompressor(), MongoCompressor.createZstdCompressor()))
				.applyConnectionString(connectionString).build();
		this.sourceMongoClient = MongoClients.create(settings);

		this.sourceDatabase = this.sourceMongoClient.getDatabase(databaseName);

		// MongoDB does not use traditional JDBC connections, so we can return null here
		return null;
	}

	@Override
	protected Connection makeSinkConnection() throws SQLException {
		// Create a MongoDB client using the connection parameters specified in the
		// ToolOptions
		final String uri = this.options.getSinkConnect();
		final ConnectionString connectionString = new ConnectionString(uri);

		final String databaseName = connectionString.getDatabase();
		// if the database is not specified in the connection string, throw an exception
		if (Objects.isNull(databaseName)) {
			throw new IllegalArgumentException("The database must be specified in the connection string");
		}

		final MongoClientSettings settings = MongoClientSettings.builder()
				.compressorList(Arrays.asList(MongoCompressor.createSnappyCompressor(),
						MongoCompressor.createZlibCompressor(), MongoCompressor.createZstdCompressor()))
				.applyConnectionString(connectionString).build();
		this.sinkMongoClient = MongoClients.create(settings);

		this.sinkDatabase = this.sinkMongoClient.getDatabase(databaseName);
		// MongoDB does not use traditional JDBC connections, so we can return null here
		return null;
	}

	@Override
	public ResultSet readTable(String tableName, String[] columns, int nThread) throws SQLException {
		// If table name parameter is null get it from options
		final String collectionName = tableName == null ? this.options.getSourceTable() : tableName;
		// if the chunk size is 0 and the current job is greater than 0, return null
		if (chunkSize == 0 && nThread > 0) {
			return null;
		}

		final long skip = nThread * chunkSize;
		this.mongoDbResultSet = new MongoDBRowSetImpl();

		try {
			// set fetch size
			this.mongoDbResultSet.setFetchSize(this.options.getFetchSize());
			// get a handle to the collection
			final MongoCollection<Document> collection = this.sourceDatabase.getCollection(collectionName);
			final MongoCursor<Document> cursor;
			final Document firstDocument;

			// if source query is specified, use it as mongodb aggregation pipeline
			if (this.options.getSourceQuery() != null) {
				this.mongoDbResultSet.setAggregation(true);
				final String queryAggregation = this.options.getSourceQuery();
				final List<BsonDocument> pipeline = getAggregation(queryAggregation);

				if (this.options.getJobs() == nThread + 1) {
					// If it's the last job, skip the first documents
					pipeline.add(BsonDocument.parse("{ $skip: " + skip + " }"));
				} else {
					// add skip and limit to the pipeline
					pipeline.add(BsonDocument.parse("{ $skip: " + skip + " }"));
					pipeline.add(BsonDocument.parse("{ $limit: " + chunkSize + " }"));
				}

				LOG.info("{}: Using this aggregation query to get data from MongoDB: {}",
						Thread.currentThread().getName(), BsonUtils.toJsonStr(pipeline));
				// create a MongoCursor to iterate over the results
				cursor = collection.aggregate(pipeline).batchSize(this.options.getFetchSize()).allowDiskUse(true)
						.cursor();
				firstDocument = collection.aggregate(pipeline).allowDiskUse(true).maxAwaitTime(100, TimeUnit.MINUTES)
						.first();
			} else {
				// create a MongoCursor to iterate over the results
				final FindIterable<Document> findIterable = collection.find();

				// Source Where
				if (this.options.getSourceWhere() != null && !this.options.getSourceWhere().isEmpty()) {
					final BsonDocument filter = BsonDocument.parse(this.options.getSourceWhere());
					findIterable.filter(filter);
					LOG.info("{}: Using this clause to filter data from MongoDB: {}", Thread.currentThread().getName(),
							filter.toJson());
				}
				// Source Fields
				if (this.options.getSourceColumns() != null && !this.options.getSourceColumns().isEmpty()) {
					final BsonDocument projection = BsonDocument.parse(this.options.getSourceColumns());
					findIterable.projection(projection);
					this.mongoDbResultSet.setMongoProjection(projection);
					LOG.info("{}: Using this clause to project data from MongoDB: {}", Thread.currentThread().getName(),
							projection.toJson());
				}

				if (this.options.getJobs() == nThread + 1) {
					// If it's the last job, skip the first documents
					findIterable.skip((int) skip);
					LOG.info("{}: Skip {} data from source", Thread.currentThread().getName(), skip);
				} else {
					// add skip and limit to the pipeline
					findIterable.skip(Math.toIntExact(skip));
					findIterable.limit(Math.toIntExact(chunkSize));
					LOG.info("{}: Skip {}, Limit {} data from source", Thread.currentThread().getName(), skip,
							chunkSize);
				}

				// if it is parallel processing
				if (this.options.getJobs() > 1) {
					// sort by object id
					findIterable.sort(Sorts.ascending("_id"));
					LOG.info("{}: Sort by _id", Thread.currentThread().getName());
					// not compatible with 4.x version
					// findIterable.allowDiskUse(true);
				}

				findIterable.batchSize(this.options.getFetchSize());
				cursor = findIterable.cursor();
				firstDocument = findIterable.first();
			}

			this.mongoDbResultSet.setMongoFirstDocument(firstDocument);
			this.mongoDbResultSet.setSinkMongoDB(this.isSourceAndSinkMongoDB());
			this.mongoDbResultSet.execute();
			this.mongoDbResultSet.setMongoCursor(cursor);

		} catch (final JsonParseException jpe) {
			LOG.error("{}: Parse JSON exception in some source parameters where, query, columns: {}",
					Thread.currentThread().getName(), jpe.getMessage(), jpe);
			// rethrow the exception
			throw jpe;
		} catch (final Exception e) {
			LOG.error("{}: Error: {}", Thread.currentThread().getName(), e.getMessage(), e);
			// rethrow the exception
			throw e;
		}
		return this.mongoDbResultSet;
	}

	@NotNull
	private static List<BsonDocument> getAggregation(String queryAggregation) {
		// parse the aggregation query string into a List of BsonDocument
		return new BsonArrayCodec().decode(new JsonReader(queryAggregation), DecoderContext.builder().build()).stream()
				.map(BsonValue::asDocument).collect(Collectors.toList());
	}

	private Boolean isSourceAndSinkMongoDB() {
		// if source and sink are a MongoDB database, return true
		return (MONGODB.isTheManagerTypeOf(this.options, DataSourceType.SINK)
				|| MONGODBSRV.isTheManagerTypeOf(this.options, DataSourceType.SINK))
				&& (MONGODB.isTheManagerTypeOf(this.options, DataSourceType.SOURCE)
						|| MONGODBSRV.isTheManagerTypeOf(this.options, DataSourceType.SOURCE));
	}

	@Override
	public int insertDataToTable(ResultSet resultSet, int taskId) throws Exception {
		int totalRows = 0;

		final String collectionName = this.getInsertDataCollection();

		final MongoCollection<Document> sinkCollection = this.sinkDatabase.getCollection(collectionName);
		final List<WriteModel<Document>> writeOperations = new ArrayList<>();
		// unordered bulk write
		final BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);

		if (resultSet != null && resultSet.next()) {
			// Create Bandwidth Throttling
			final BandwidthThrottling bt = new BandwidthThrottling(this.options.getBandwidthThrottling(),
					this.options.getFetchSize(), resultSet);
			do {
				bt.acquiere();
				if (this.isSourceAndSinkMongoDB()) {
					// Add document to bulk
					writeOperations.add(new InsertOneModel<>((Document) resultSet.getObject(1)));
				} else {
					// iterate columns
					final Document document = new Document();
					for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
						final String columnName = resultSet.getMetaData().getColumnName(i);
						switch (resultSet.getMetaData().getColumnType(i)) {
							case -104 : // Oracle INTERVALDS
							case -103 : // Oracle INTERVALYM
							case Types.SQLXML :
								document.put(columnName, resultSet.getString(i));
								break;
							case Types.TIMESTAMP :
							case Types.TIMESTAMP_WITH_TIMEZONE :
							case -101 :
							case -102 :
								document.put(columnName, resultSet.getTimestamp(i));
								break;
							case Types.BINARY :
							case Types.VARBINARY :
							case Types.LONGVARBINARY :
								document.put(columnName, resultSet.getBytes(i));
								break;
							case Types.BLOB :
								final Blob blobData = resultSet.getBlob(i);
								if (blobData != null) {
									document.put(columnName, blobData.getBytes(1, (int) blobData.length()));
									blobData.free();
								}
								break;
							case Types.CLOB :
								final Clob clobData = resultSet.getClob(i);
								document.put(columnName, this.clobToString(clobData));
								if (clobData != null)
									clobData.free();
								break;
							case 1111 : // Postgres JSON, intervals and others
								final Object object = resultSet.getObject(i);
								if (object instanceof PGobject) {
									final PGobject pgObject = (PGobject) object;
									// if Document.parse fails, will be saved as a string
									try {
										document.put(columnName, Document.parse(pgObject.getValue()));
									} catch (final Exception e) {
										document.put(columnName, pgObject.getValue());
									}
								} else {
									document.put(columnName, object);
								}
								break;
							default :
								document.put(columnName, resultSet.getObject(i));
								break;
						}
					}

					// Add document to bulk
					writeOperations.add(new InsertOneModel<>(document));
				}

				totalRows++;
				if (writeOperations.size() == this.options.getFetchSize()) {
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

	private String getInsertDataCollection() {
		// get collection name
		final String collectionName;
		if (this.options.getMode().equals(ReplicationMode.COMPLETE.getModeText())) {
			collectionName = this.getSinkTableName();
		} else {
			collectionName = this.getQualifiedStagingTableName();
		}
		return collectionName;
	}

	@Override
	public String getDriverClass() {
		// MongoDB does not use JDBC drivers, so this method does not need to be
		// implemented
		return null;
	}

	@Override
	public void preSourceTasks() throws Exception {
		long totalRows = 0;

		if (this.options.getJobs() != 1) {
			this.getConnection();
			// get source collection
			final MongoCollection<Document> collection = this.sourceDatabase
					.getCollection(this.options.getSourceTable());

			// Source Query
			if (this.options.getSourceQuery() != null && !this.options.getSourceQuery().isEmpty()) {
				final String queryAggregation = this.options.getSourceQuery();
				final List<BsonDocument> pipeline = getAggregation(queryAggregation);
				// add count
				pipeline.add(BsonDocument.parse("{ $count: \"count\" }"));
				LOG.info("Using this aggregation to count the total number of rows from the MongoDB source: {}",
						BsonUtils.toJsonStr(pipeline));
				// get the first document from the aggregation with 100 minutes timeout
				final Document countDocument = collection.aggregate(pipeline).maxAwaitTime(100, TimeUnit.MINUTES)
						.allowDiskUse(true).first();
				if (countDocument != null) {
					// get integer and cast to long
					totalRows = countDocument.getInteger("count", 0);
				}
			} else {
				BsonDocument where = new BsonDocument();
				// Source Where
				if (this.options.getSourceWhere() != null && !this.options.getSourceWhere().isEmpty()) {
					where = BsonDocument.parse(this.options.getSourceWhere());
				}
				totalRows = collection.countDocuments(where);
			}

			// set chunk size for each task
			chunkSize = Math.abs(totalRows / this.options.getJobs());
			LOG.info("Source collection total rows: {}, chunk size per job: {}", totalRows, chunkSize);
		}
	}

	@Override
	public void postSourceTasks() throws Exception {
	}

	@Override
	protected void createStagingTable() throws Exception {
		this.getConnection();
		try {

			final String sinkStagingCollectionName = this.getQualifiedStagingTableName();

			// create staging collection
			this.sinkDatabase.createCollection(sinkStagingCollectionName);

			// get primary keys from unique indexes
			final MongoCollection<Document> sinkCollection = this.sinkDatabase.getCollection(this.getSinkTableName());
			final List<String> primaryKeys = this.getUniqueIndexFields(sinkCollection);

			this.setPrimaryKeys(primaryKeys);

			LOG.info("Creating staging collection : {}", sinkStagingCollectionName);

		} catch (final Exception e) {
			LOG.error("Error creating staging table: {}", e.getMessage(), e);
			throw e;
		}
		this.close();
	}

	/**
	 * Retrieves all field names from unique indexes in the collection. This
	 * includes the default _id field and any explicitly created unique indexes.
	 *
	 * @param collection
	 *            the MongoDB collection to analyze
	 * @return list of field names that are part of unique indexes
	 */
	private List<String> getUniqueIndexFields(MongoCollection<Document> collection) {
		// Returns the field list of a single selected unique index (used as primary
		// key).
		// Strategy:
		// 1. Enumerate all unique indexes (including implicit _id_)
		// 2. Prefer the first user defined unique index (name != _id_)
		// 3. Fallback to _id if no other unique index exists or on error
		final List<String> selectedFields = new ArrayList<>();
		final List<String> indexSummaries = new ArrayList<>();
		String selectedIndexName = null;

		try {
			for (final Document index : collection.listIndexes()) {
				boolean isUnique = false;

				final Object uniqueValue = index.get("unique");
				if (uniqueValue instanceof Boolean && (Boolean) uniqueValue) {
					isUnique = true;
				}

				final String indexName = index.getString("name");
				if ("_id_".equals(indexName)) {
					isUnique = true;
				}

				if (!isUnique) {
					continue;
				}

				final Object keyObj = index.get("key");
				if (!(keyObj instanceof Document)) {
					continue;
				}
				final Document key = (Document) keyObj;

				final List<String> fieldsOfIndex = new ArrayList<>(key.keySet()); // preserve order
				indexSummaries.add(indexName + "=" + fieldsOfIndex);

				// Selection rule: pick first non _id_ unique index; if only _id_ exists, use
				// it.
				if (selectedIndexName == null || ("_id_".equals(selectedIndexName) && !"_id_".equals(indexName))) {
					selectedIndexName = indexName;
					selectedFields.clear();
					selectedFields.addAll(fieldsOfIndex);
				}
			}

			if (selectedFields.isEmpty()) {
				selectedFields.add("_id");
				selectedIndexName = "_id_ (implicit)";
			}

			LOG.info("Unique indexes in collection {}: {}. Selected '{}' with fields {}",
					collection.getNamespace().getCollectionName(), indexSummaries, selectedIndexName, selectedFields);

		} catch (final Exception e) {
			LOG.warn("Error retrieving unique indexes from collection {}: {}",
					collection.getNamespace().getCollectionName(), e.getMessage());
			if (selectedFields.isEmpty()) {
				selectedFields.add("_id");
				LOG.info("Fallback to default _id field for unique constraint");
			}
		}

		return selectedFields;
	}

	private void setPrimaryKeys(List<String> primaryKeys) {
		// if primary keys empty and mode is incremental raise exception
		if ((primaryKeys == null || primaryKeys.isEmpty())
				&& this.options.getMode().equals(ReplicationMode.INCREMENTAL.getModeText())) {
			throw new IllegalArgumentException("Sink collection \"" + this.getSinkTableName()
					+ "\" must have at least one unique key index for incremental mode.");
		}
		LOG.info("Primary key fields: {}", primaryKeys);
		this.primaryKeys = primaryKeys;
	}

	/**
	 * Validates that a unique index exists on the sink collection for the specified
	 * fields. MongoDB's $merge operation requires a unique index on the target
	 * collection for the fields specified in the 'on' parameter.
	 *
	 * @param collection
	 *            the MongoDB collection to validate
	 * @param fields
	 *            the list of fields that should have a unique index
	 * @return true if a matching unique index exists, false otherwise
	 */
	private boolean hasUniqueIndexForFields(MongoCollection<Document> collection, List<String> fields) {
		try {
			for (final Document index : collection.listIndexes()) {
				// Check if this is a unique index
				final Object uniqueValue = index.get("unique");
				boolean isUnique = uniqueValue instanceof Boolean && (Boolean) uniqueValue;

				// _id index is always unique
				final String indexName = index.getString("name");
				if ("_id_".equals(indexName)) {
					isUnique = true;
				}

				if (!isUnique) {
					continue;
				}

				// Check if the index key matches the required fields
				final Object keyObj = index.get("key");
				if (!(keyObj instanceof Document)) {
					continue;
				}
				final Document key = (Document) keyObj;
				final List<String> indexFields = new ArrayList<>(key.keySet());

				// Check if the index fields match the required fields (order matters for
				// compound indexes)
				if (indexFields.equals(fields)) {
					LOG.info("Found matching unique index '{}' for fields {} on collection {}", indexName, fields,
							collection.getNamespace().getCollectionName());
					return true;
				}
			}
		} catch (final Exception e) {
			LOG.warn("Error checking for unique index on collection {}: {}",
					collection.getNamespace().getCollectionName(), e.getMessage());
		}
		return false;
	}

	/**
	 * Ensures that the sink collection has a unique index for the specified fields.
	 * Creates the index if it doesn't exist. This is required for MongoDB's $merge
	 * operation.
	 *
	 * @param collection
	 *            the MongoDB collection
	 * @param fields
	 *            the list of fields that need a unique index
	 */
	private void ensureUniqueIndexExists(MongoCollection<Document> collection, List<String> fields) {
		// Skip if it's just _id field (always has unique index by default)
		if (fields.size() == 1 && "_id".equals(fields.get(0))) {
			LOG.info("Using default _id unique index for collection {}", collection.getNamespace().getCollectionName());
			return;
		}

		// Check if unique index already exists
		if (this.hasUniqueIndexForFields(collection, fields)) {
			return;
		}

		// Create unique index for the specified fields
		try {
			LOG.info("Creating unique index for fields {} on collection {}", fields,
					collection.getNamespace().getCollectionName());

			// Build compound index if multiple fields, or single field index
			final IndexOptions indexOptions = new IndexOptions().unique(true);
			final String indexName;

			if (fields.size() == 1) {
				collection.createIndex(Indexes.ascending(fields.get(0)), indexOptions);
				indexName = fields.get(0) + "_1";
			} else {
				// Create compound index for multiple fields
				collection.createIndex(Indexes.ascending(fields), indexOptions);
				indexName = String.join("_", fields) + "_1";
			}

			LOG.info("Successfully created unique index '{}' for fields {} on collection {}", indexName, fields,
					collection.getNamespace().getCollectionName());

		} catch (final Exception e) {
			LOG.error("Failed to create unique index for fields {} on collection {}: {}", fields,
					collection.getNamespace().getCollectionName(), e.getMessage(), e);
			throw new RuntimeException("Cannot create unique index required for $merge operation. "
					+ "Please create a unique index manually on fields: " + fields, e);
		}
	}

	@Override
	protected void mergeStagingTable() throws Exception {

		this.getConnection();

		// merge staging table with sink table
		try {
			final MongoCollection<Document> sinkCollection = this.sinkDatabase.getCollection(this.getSinkTableName());
			final MongoCollection<Document> sinkStagingCollection = this.sinkDatabase
					.getCollection(this.getQualifiedStagingTableName());

			// Ensure the sink collection has a unique index for the merge operation
			// MongoDB's $merge requires a unique index on the target collection for the
			// 'on' fields
			LOG.info("Validating unique index on sink collection for merge operation");
			this.ensureUniqueIndexExists(sinkCollection, this.primaryKeys);

			String aggregationQuery = "[ {$project: {_id:0}},{$merge:{ into:\"${SINK_COLLECTION}\", on:[${PRIMARY_KEYS}], whenMatched: \"replace\", whenNotMatched: \"insert\" }} ]";
			aggregationQuery = aggregationQuery.replace("${SINK_COLLECTION}", this.getSinkTableName());
			// replace primary keys separated by commas and surrounded by quotes
			aggregationQuery = aggregationQuery.replace("${PRIMARY_KEYS}",
					this.primaryKeys.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(",")));

			// merge collections
			LOG.info("Merging staging collection with sink collection using aggregation query: {}", aggregationQuery);
			final List<BsonDocument> pipeline = getAggregation(aggregationQuery);
			sinkStagingCollection.aggregate(pipeline).allowDiskUse(true).maxAwaitTime(100, TimeUnit.MINUTES).first();

		} catch (final Exception e) {
			LOG.error("Error merging staging table with sink table: {}", e.getMessage(), e);
			throw e;
		}
		this.close();
	}

	@Override
	public void close() throws SQLException {

		// close mongoResultSets
		if (this.mongoDbResultSet != null) {
			this.mongoDbResultSet.setFetchSize(0);
			this.mongoDbResultSet.close();
			final MongoCursor<Document> cursor = this.mongoDbResultSet.getCursor();
			if (cursor != null) {
				cursor.close();
			}
		}

		// Close connection, ignore exceptions
		if (this.sourceMongoClient != null) {
			try {
				this.sourceMongoClient.close();
			} catch (final Exception e) {
				LOG.error(e);
			}
		}
		if (this.sinkMongoClient != null) {
			try {
				this.sinkMongoClient.close();
			} catch (final Exception e) {
				LOG.error(e);
			}
		}
	}

	@Override
	protected void truncateTable() throws SQLException {
		// Delete all documents in the collection
		this.getConnection();
		LOG.info("Deleting all documents from the sink collection: {}", this.getInsertDataCollection());
		this.sinkDatabase.getCollection(this.getInsertDataCollection()).deleteMany(new Document());
		this.close();
	}

	@Override
	public void dropStagingTable() throws SQLException {
		this.getConnection();
		LOG.info("Dropping staging collection: {}", this.getQualifiedStagingTableName());
		this.sinkDatabase.getCollection(this.getQualifiedStagingTableName()).drop();
		this.close();
	}

}

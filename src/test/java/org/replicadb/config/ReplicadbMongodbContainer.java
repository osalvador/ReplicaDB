package org.replicadb.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.testcontainers.containers.MongoDBContainer;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class ReplicadbMongodbContainer extends MongoDBContainer {
	private static final Logger LOG = LogManager.getLogger(ReplicadbMongodbContainer.class);
	private static final String IMAGE_VERSION = "mongo:8";
	private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
	private static final String SOURCE_FILE = "/mongo/t_source_data.json";
	private static final String T_SOURCE_COLLECTION = "t_source";
	private static ReplicadbMongodbContainer container;
	private ConnectionString connectionString;

	private ReplicadbMongodbContainer() {
		super(IMAGE_VERSION);
	}

	public static ReplicadbMongodbContainer getInstance() {
		if (container == null) {
			container = new ReplicadbMongodbContainer();
			container.start();
		}
		return container;
	}

	@Override
	public void start() {
		super.start();
		this.connectionString = new ConnectionString(container.getReplicaSetUrl());

		// Create MongoDB client with compression disabled for ARM64 compatibility
		final MongoClientSettings clientSettings = MongoClientSettings.builder()
				.applyConnectionString(this.connectionString).build();

		try (final MongoClient mongoClient = MongoClients.create(clientSettings)) {
			assert this.connectionString.getDatabase() != null;
			final MongoDatabase database = mongoClient.getDatabase(this.connectionString.getDatabase());

			// Ensure collections are properly created
			try {
				// Create source collection and load data
				final List<String> allLines = Files.readAllLines(Paths.get(RESOURCE_DIR + SOURCE_FILE));
				for (final String line : allLines) {
					if (line.trim().isEmpty() || line.trim().startsWith("//")) {
						continue;
					}
					final Document doc = Document.parse(line);
					database.getCollection(T_SOURCE_COLLECTION).insertOne(doc);
				}

				final long count = database.getCollection(T_SOURCE_COLLECTION).countDocuments();
				LOG.info("Inserted " + count + " documents into the '" + T_SOURCE_COLLECTION + "' collection");
				LOG.info("First document: " + database.getCollection(T_SOURCE_COLLECTION).find().first().toJson());

				// Create sink collection with proper error handling
				try {
					database.createCollection("t_sink");
					LOG.info("Successfully created t_sink collection");
				} catch (final Exception e) {
					LOG.warn("Collection t_sink may already exist: " + e.getMessage());
				}

				// Create unique index on the correct field name and validate it
				this.createAndValidateUniqueIndex(database);

			} catch (final Exception e) {
				LOG.error("Failed to initialize MongoDB collections: " + e.getMessage(), e);
				throw new RuntimeException("MongoDB initialization failed", e);
			}
		} catch (final Exception e) {
			LOG.error("Failed to connect to MongoDB: " + e.getMessage(), e);
			throw new RuntimeException("MongoDB connection failed", e);
		}
	}

	@Override
	public void stop() {
		// do nothing, JVM handles shut down
	}

	public ConnectionString getMongoConnectionString() {
		return this.connectionString;
	}

	/**
	 * Creates a unique index on the t_sink collection and validates it was created
	 * successfully. Uses the correct field name C_INTEGER (uppercase) as defined in
	 * the test data schema.
	 */
	private void createAndValidateUniqueIndex(MongoDatabase database) {
		final String indexFieldName = "c_integer"; // Correct field name from test data schema
		final String collectionName = "t_sink";

		try {
			// Create unique index with proper field name
			database.getCollection(collectionName).createIndex(new Document(indexFieldName, 1),
					new IndexOptions().unique(true).name("idx_unique_c_integer"));
			LOG.info("Created unique index 'idx_unique_c_integer' on field '{}'", indexFieldName);

			// Validate the index was created successfully
			if (this.validateUniqueIndexExists(database, collectionName, indexFieldName)) {
				LOG.info("Successfully validated unique index on {}.{}", collectionName, indexFieldName);
			} else {
				throw new RuntimeException("Failed to validate unique index creation");
			}

		} catch (final Exception e) {
			LOG.error("Failed to create or validate unique index on {}.{}: {}", collectionName, indexFieldName,
					e.getMessage(), e);
			throw new RuntimeException("Unique index creation failed", e);
		}
	}

	/**
	 * Validates that a unique index exists on the specified field.
	 *
	 * @param database
	 *            the MongoDB database
	 * @param collectionName
	 *            the collection name to check
	 * @param fieldName
	 *            the field name that should have a unique index
	 * @return true if a unique index exists on the field, false otherwise
	 */
	private boolean validateUniqueIndexExists(MongoDatabase database, String collectionName, String fieldName) {
		try {
			final var collection = database.getCollection(collectionName);

			// Check all indexes on the collection
			for (final Document index : collection.listIndexes()) {
				// Get the unique flag
				final Object uniqueValue = index.get("unique");
				final boolean isUnique = uniqueValue instanceof Boolean && (Boolean) uniqueValue;

				// Check if this index contains our field
				final Object keyObj = index.get("key");
				if (isUnique && keyObj instanceof Document) {
					final Document key = (Document) keyObj;
					if (key.containsKey(fieldName)) {
						final String indexName = index.getString("name");
						LOG.info("Found unique index '{}' on field '{}'", indexName, fieldName);
						return true;
					}
				}
			}

			LOG.warn("No unique index found on field '{}' in collection '{}'", fieldName, collectionName);
			return false;

		} catch (final Exception e) {
			LOG.error("Error validating unique index: {}", e.getMessage(), e);
			return false;
		}
	}
}

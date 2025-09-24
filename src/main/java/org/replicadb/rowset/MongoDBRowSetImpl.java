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
	private int fetchSize = 100; // Default fetch size

	public MongoDBRowSetImpl() throws SQLException {
		super();
	}

	// get cursor
	public MongoCursor<Document> getCursor() {
		return this.cursor;
	}

	public void incrementRowCount() {
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
	public void execute() throws SQLException {

		final RowSetMetaData rsmd = new RowSetMetaDataImpl();
		final List<String> fields = new ArrayList<>();

		// it the first document is null, it means that the cursor is empty
		if (this.firstDocument != null && this.firstDocument.size() == 0) {
			// log warning
			LOG.warn("No documents found in the source collection");
			this.setMetaData(rsmd);
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
			// If there are other documents with different structure, the fields will be
			// ignored
			final Document document = this.firstDocument;

			final AtomicInteger i = new AtomicInteger(1);

			// if the query is an aggregation, the fields will be defined by the document
			// keys
			// otherwise, the fields will be defined by the projection
			// cast to List to preserve the order
			final List<String> keys = new ArrayList<>();
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
				final Object value = document.get(key);
				// If the value is null, the type will be set to VARCHAR
				// log a warning
				if (value == null) {
					LOG.warn("The value of the field {} is null. The type will be set to VARCHAR", key);
				}

				final String typeString = value == null ? "null" : value.getClass().toString();
				LOG.trace("Key: {},  Value: {} , Type: {} ", key, value, typeString);

				// define java.sql.Types from the type of the value
				final int type = getSqlType(typeString);
				try {
					rsmd.setColumnName(i.get(), key);
					rsmd.setColumnType(i.get(), type);
					fields.add(key);
				} catch (final SQLException e) {
					LOG.error(e);
					throw new RuntimeException(e);
				}
				i.getAndIncrement();
			});

		}
		this.setMetaData(rsmd);
		// Log fields in order
		LOG.warn(
				"The source columns/fields names are be defined by the first document returned by the cursor in this order: {}",
				fields);
	}

	/**
	 * Maps a given class type to a corresponding SQL type.
	 *
	 * @param typeString
	 *            the class type as a string
	 * @return the corresponding SQL type, or {@link java.sql.Types#OTHER} if no
	 *         corresponding type is found.
	 */
	static int getSqlType(String typeString) {
		switch (typeString) {
			case "null" :
			case "class java.lang.String" :
				return java.sql.Types.VARCHAR;
			case "class java.lang.Integer" :
				return java.sql.Types.INTEGER;
			case "class java.lang.Long" :
				return java.sql.Types.BIGINT;
			case "class java.lang.Double" :
				return java.sql.Types.DOUBLE;
			case "class java.lang.Boolean" :
				return java.sql.Types.BOOLEAN;
			case "class java.lang.Float" :
				return java.sql.Types.FLOAT;
			case "class java.lang.Short" :
				return java.sql.Types.SMALLINT;
			case "class java.lang.Byte" :
				return java.sql.Types.TINYINT;
			case "class java.math.BigDecimal" :
			case "class org.bson.types.Decimal128" :
				return java.sql.Types.DECIMAL;
			case "class java.sql.Date" :
			case "class java.util.Date" :
			case "class java.sql.Timestamp" :
				return Types.TIMESTAMP_WITH_TIMEZONE;
			case "class java.sql.Time" :
				return java.sql.Types.TIME;
			case "class org.bson.types.Binary" :
				return java.sql.Types.BINARY;
			case "class java.util.List" :
			case "class java.util.ArrayList" :
				return java.sql.Types.ARRAY;
			case "class org.bson.Document" :
				return Types.STRUCT;
			case "class org.bson.types.ObjectId" :
				return Types.ROWID;
			case "class java.lang.Object" :
			default :
				return java.sql.Types.OTHER;
		}
	}

	@Override
	public boolean next() throws SQLException {
		/*
		 * make sure things look sane. The cursor must be positioned in the rowset or
		 * before first (0) or after last (numRows + 1)
		 */
		
		// it the first document size is 0, it means that the cursor is empty
		if (this.firstDocument != null && this.firstDocument.size() == 0) {
			return false;
		}

		// If no data loaded yet, try to load first batch
		if (size() == 0 && cursor != null && cursor.hasNext()) {
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
		this.notifyCursorMoved();

		// If we don't have more rows in current batch, try to load next batch
		if (!ret) {
			ret = this.cursor.hasNext();
			if (ret) {
				this.readData();
				// After loading new batch, move from before-first to first
				ret = this.internalNext();
			}
		}
		return ret;
	}

	private void readData() throws SQLException {
		final int currentFetchSize = this.getFetchSize();
		
		// Temporarily set fetchSize to 0 to avoid validation issues during close()
		// The close() method calls initProperties() which sets maxRows=0 and validates maxRows >= fetchSize
		this.setFetchSize(0);
		
		// Clear existing data to load fresh batch
		this.close();
		
		// Restore fetchSize for the data loading loop
		this.setFetchSize(currentFetchSize);

		Document document;

		// Load fetch size documents into the rowset using direct row appending
		int loadedCount = 0;
		for (int i = 1; i <= currentFetchSize && this.cursor.hasNext(); i++) {
			try {
				document = this.cursor.next();

				if (Boolean.TRUE.equals(this.isSourceAndSinkMongo)) {
					// Single column with Document object
					this.appendRow(new Object[]{document});
				} else {
					// Build row array directly
					final Object[] rowData = new Object[this.getMetaData().getColumnCount()];

					for (int j = 0; j < this.getMetaData().getColumnCount(); j++) {
						final String columnName = this.getMetaData().getColumnName(j + 1);
						final int columnType = this.getMetaData().getColumnType(j + 1);

						// Extract and convert value based on type
						rowData[j] = this.extractAndConvertValue(document, columnName, columnType);
					}

					this.appendRow(rowData);
				}
				this.incrementRowCount();
				loadedCount++;
			} catch (final Exception e) {
				LOG.error("MongoDB error processing document {}: {}", i, e.getMessage(), e);
				throw e;
			}
		}

		// Position cursor BEFORE first row so that when PostgresqlManager calls next(), 
		// it will move to the actual first row instead of skipping it
		if (this.size() > 0) {
			this.beforeFirst(); // Position BEFORE first row for proper next() behavior
		} else {
			this.beforeFirst(); // No data, position before first
		}
	}

	/**
	 * Extracts and converts a value from a MongoDB document based on the target SQL
	 * type.
	 *
	 * @param document
	 *            The MongoDB document
	 * @param columnName
	 *            The column name to extract
	 * @param columnType
	 *            The target SQL type
	 * @return The converted value or null
	 */
	private Object extractAndConvertValue(Document document, String columnName, int columnType) {
		switch (columnType) {
			case Types.VARCHAR :
			case Types.CHAR :
			case Types.LONGVARCHAR :
				return document.getString(columnName);
			case Types.INTEGER :
			case Types.TINYINT :
			case Types.SMALLINT :
				return document.getInteger(columnName);
			case Types.BIGINT :
			case Types.NUMERIC :
			case Types.DECIMAL :
				final Long longValue = document.getLong(columnName);
				return longValue != null ? BigDecimal.valueOf(longValue) : null;
			case Types.DOUBLE :
				return document.getDouble(columnName);
			case Types.TIMESTAMP_WITH_TIMEZONE :
				final java.util.Date date = document.getDate(columnName);
				return date != null ? date.toInstant().atOffset(ZoneOffset.UTC) : null;
			case Types.BINARY :
			case Types.BLOB :
				final Binary bin = document.get(columnName, org.bson.types.Binary.class);
				return bin != null ? bin.getData() : null;
			case Types.BOOLEAN :
				return document.getBoolean(columnName);
			case Types.ROWID :
				final ObjectId oid = document.getObjectId(columnName);
				return oid != null ? oid.toString() : null;
			case Types.ARRAY :
				final List<?> list = document.get(columnName, List.class);
				return list != null ? new Gson().toJson(list) : null;
			default :
				final org.bson.Document subDoc = document.get(columnName, org.bson.Document.class);
				return subDoc != null ? BsonUtils.toJson(subDoc) : null;
		}
	}

	/**
	 * Sets the MongoDB cursor and preloads the first batch of data to ensure
	 * immediate data availability for reading without requiring next() calls.
	 */
	public void setMongoCursor(MongoCursor<Document> cursor) throws SQLException {
		this.cursor = cursor;
		// Preload the first batch of data so it's immediately available for reading
		if (cursor != null && cursor.hasNext()) {
			try {
				readData();
			} catch (Exception e) {
				LOG.error("Failed to preload data in setMongoCursor", e);
				throw e;
			}
		}
	}

	/**
	 * Override getString to ensure data is loaded before accessing it.
	 * This handles cases where PostgresqlManager tries to read data immediately.
	 */
	@Override
	public String getString(int columnIndex) throws SQLException {
		// If no data loaded yet and we have a cursor, try to load and position
		if (size() == 0 && cursor != null && cursor.hasNext()) {
			readData();
			if (size() > 0) {
				next(); // Move from before-first to first row
			}
		}
		
		// Delegate to parent implementation
		return super.getString(columnIndex);
	}

	/**
	 * Override getObject to ensure data is loaded before accessing it.
	 */
	@Override
	public Object getObject(int columnIndex) throws SQLException {
		// If no data loaded yet and we have a cursor, try to load and position
		if (size() == 0 && cursor != null && cursor.hasNext()) {
			readData();
			if (size() > 0) {
				next(); // Move from before-first to first row
			}
		}
		
		// Delegate to parent implementation
		return super.getObject(columnIndex);
	}

	public void setSinkMongoDB(Boolean sinkMongoDB) {
		this.isSourceAndSinkMongo = sinkMongoDB;
	}

	public void setMongoFirstDocument(Document firstDocument) {
		this.firstDocument = firstDocument;
	}

	public void setMongoProjection(BsonDocument projection) {
		final LinkedHashSet<String> fields = new LinkedHashSet<>();
		// iterate over the projection document and add the fields to the fields list
		// only if they are not 0
		for (final Map.Entry<String, BsonValue> entry : projection.entrySet()) {
			if (!entry.getValue().isInt32() || entry.getValue().asInt32().getValue() != 0) {
				fields.add(entry.getKey());
			}
		}
		this.mongoProjection = fields;
	}

	public void setAggregation(boolean b) {
		this.isAggregation = b;
	}
}

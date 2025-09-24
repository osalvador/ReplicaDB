package org.replicadb.rowset;

import javax.sql.RowSetReader;
import javax.sql.RowSetWriter;
import javax.sql.rowset.spi.SyncProvider;
import javax.sql.rowset.spi.SyncProviderException;
import java.sql.SQLException;

/**
 * Minimal SyncProvider implementation for ReplicaDB rowsets. This provider
 * supports read-only operations for file-based data sources and provides the
 * necessary infrastructure for CachedRowSet compliance.
 */
public class ReplicaRowSetProvider extends SyncProvider {

	/**
	 * The provider identification string.
	 */
	public static final String PROVIDER_ID = "org.replicadb.rowset.ReplicaRowSetProvider";

	/**
	 * The version of this provider.
	 */
	private static final String VERSION = "1.0";

	/**
	 * The vendor name.
	 */
	private static final String VENDOR = "ReplicaDB";

	/**
	 * Default constructor.
	 */
	public ReplicaRowSetProvider() {
		super();
	}

	@Override
	public String getProviderID() {
		return PROVIDER_ID;
	}

	@Override
	public RowSetReader getRowSetReader() {
		return new ReplicaRowSetReader();
	}

	@Override
	public RowSetWriter getRowSetWriter() {
		return new ReplicaRowSetWriter();
	}

	@Override
	public int getProviderGrade() {
		// GRADE_NONE indicates no data source synchronization is provided
		// This is appropriate for read-only file-based sources
		return SyncProvider.GRADE_NONE;
	}

	@Override
	public void setDataSourceLock(int datasource_lock) throws SyncProviderException {
		// No-op for file-based sources - locking not applicable
	}

	@Override
	public int getDataSourceLock() throws SyncProviderException {
		return SyncProvider.DATASOURCE_NO_LOCK;
	}

	@Override
	public int supportsUpdatableView() {
		return SyncProvider.NONUPDATABLE_VIEW_SYNC;
	}

	@Override
	public String getVersion() {
		return VERSION;
	}

	@Override
	public String getVendor() {
		return VENDOR;
	}

	/**
	 * Minimal RowSetReader implementation for ReplicaDB. File-based rowsets handle
	 * their own data reading, so this is largely a no-op.
	 */
	private static class ReplicaRowSetReader implements RowSetReader {

		@Override
		public void readData(javax.sql.RowSetInternal caller) throws SQLException {
			// File-based rowsets handle their own data reading in execute() method
			// This method is called by the CachedRowSet framework but is not used
			// in our file-based implementation pattern
		}
	}

	/**
	 * Minimal RowSetWriter implementation for ReplicaDB. Since we're implementing
	 * read-only file-based sources, write operations are not supported.
	 */
	private static class ReplicaRowSetWriter implements RowSetWriter {

		@Override
		public boolean writeData(javax.sql.RowSetInternal caller) throws SQLException {
			throw new UnsupportedOperationException("Write operations not supported - read-only file-based rowsets");
		}
	}
}

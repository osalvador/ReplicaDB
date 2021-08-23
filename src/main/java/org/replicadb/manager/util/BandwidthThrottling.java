package org.replicadb.manager.util;

import org.apache.commons.lang3.concurrent.TimedSemaphore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.TimeUnit;

public class BandwidthThrottling {
    private static final Logger LOG = LogManager.getLogger(BandwidthThrottling.class);

    private TimedSemaphore bandwidthRateLimiter;
    private int rowSize = 0;
    private long fetchs = 0L;
    private int fetchSize = 0;

    /**
     * Create a bandwith cap, estimating the size of the first row returned by the resultset
     * and using it as permits in the rate limit.
     *
     * @param bandwidthThrottling the bandwidth cap for the replication in KB/sec.
     * @param fetchSize the resultSet fechSize
     * @param resultSet the resultset cursor moved to the first row (resultSet.next())
     * @throws SQLException
     */
    public BandwidthThrottling(int bandwidthThrottling, int fetchSize, ResultSet resultSet) throws SQLException {

        this.fetchSize = fetchSize;
        ResultSetMetaData rsmd = resultSet.getMetaData();

        if (bandwidthThrottling > 0) {
            // Stimate the Row Size
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {

                if (rsmd.getColumnType(i) != Types.BLOB) {
                    String columnValue = resultSet.getString(i);
                    if (columnValue != null && !resultSet.getString(i).isEmpty())
                        rowSize = rowSize + resultSet.getString(i).length();
                }
            }

            double limit = ((1.0 * bandwidthThrottling) / rowSize) / (this.fetchSize * 1.0 / 1000);
            if (limit == 0) limit = 1;
            this.bandwidthRateLimiter = new TimedSemaphore(1, TimeUnit.SECONDS, (int) Math.round(limit));

            LOG.info("Estimated Row Size: {} KB. Estimated limit of fetchs per second: {} ", rowSize, limit);


        }
    }


    /**
     * Acquires the <code>rowSize</code> number of permits from this <code>bandwidthRateLimiter</code>,
     * blocking until the request can be granted.
     */
    public void acquiere() {
        // Wait for Sleeping Stopwatch
        if (rowSize != 0) {
            try {
                ++fetchs;
                if (fetchs == fetchSize) {
                    bandwidthRateLimiter.acquire();
                    fetchs = 0;
                }
            } catch (InterruptedException e) {
                LOG.error(e);
            }
        }
    }


}

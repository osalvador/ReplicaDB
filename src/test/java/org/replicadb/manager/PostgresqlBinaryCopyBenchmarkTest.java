package org.replicadb.manager;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbPostgresqlContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises COPY correctness with a small fixture and performance with a scheduled larger run. */
@Testcontainers
class PostgresqlBinaryCopyBenchmarkTest {
    private static final Logger LOG = LogManager.getLogger(PostgresqlBinaryCopyBenchmarkTest.class);
    private static final boolean BENCHMARK = Boolean.getBoolean("replicadb.test.copy-benchmark");
    private static final int ROW_COUNT = BENCHMARK ? 100_000 : 128;

    private Connection postgresConn;
    private static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres;

    @BeforeAll
    static void setUp() {
        postgres = ReplicadbPostgresqlContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.postgresConn = DriverManager.getConnection(
            postgres.getJdbcUrl(), 
            postgres.getUsername(), 
            postgres.getPassword()
        );
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Clean up benchmark tables
        try {
            Statement stmt = postgresConn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS bench_binary_source CASCADE");
            stmt.execute("DROP TABLE IF EXISTS bench_binary_sink CASCADE");
            stmt.execute("DROP TABLE IF EXISTS bench_text_source CASCADE");
            stmt.execute("DROP TABLE IF EXISTS bench_text_sink CASCADE");
        } catch (SQLException e) {
            LOG.warn("Error cleaning up benchmark tables: {}", e.getMessage());
        }
        this.postgresConn.close();
    }

    @Test
    void preservesBytesInBinaryAndTextCopy() throws SQLException, ParseException, IOException {
        LOG.info("=".repeat(80));
        LOG.info("Performance Benchmark: SAME DATA - Binary COPY vs Text COPY");
        LOG.info("=".repeat(80));
        
        // ===== DATASET 1: Text COPY with BYTEA encoded as VARCHAR (hex) =====
        LOG.info("Creating text COPY dataset (BYTEA as VARCHAR hex) with {} rows...", ROW_COUNT);
        Statement stmt = postgresConn.createStatement();
        
        // Create source table with VARCHAR instead of BYTEA to force text COPY
        stmt.execute("CREATE TABLE bench_text_bytea_source (" +
                    "id INTEGER PRIMARY KEY, " +
                    "data VARCHAR(200), " +  // BYTEA encoded as hex string
                    "val NUMERIC(20,5), " +
                    "ts TIMESTAMP, " +
                    "f FLOAT8)");
        
        stmt.execute("CREATE TABLE bench_text_bytea_sink (" +
                    "id INTEGER PRIMARY KEY, " +
                    "data VARCHAR(200), " +
                    "val NUMERIC(20,5), " +
                    "ts TIMESTAMP, " +
                    "f FLOAT8)");
        
        // Insert test rows with hex-encoded binary data
        LOG.info("Inserting {} rows (text mode - BYTEA as hex VARCHAR)...", ROW_COUNT);
        long insertStart = System.nanoTime();
        stmt.execute(
            "INSERT INTO bench_text_bytea_source " +
            "SELECT generate_series, " +
            "       encode(decode(md5(generate_series::text), 'hex'), 'hex'), " + // BYTEA as hex string
            "       (generate_series * random())::numeric(20,5), " +
            "       now() + (generate_series || ' seconds')::interval, " +
            "       random()::float8 " +
            "FROM generate_series(1, " + ROW_COUNT + ")"
        );
        long insertDuration = (System.nanoTime() - insertStart) / 1_000_000;
        LOG.info("Text dataset created in {} ms", insertDuration);
        
        // Benchmark text COPY (will use text format since no binary columns)
        long textStart = System.nanoTime();
        String[] textArgs = {
            "--source-connect", postgres.getJdbcUrl(),
            "--source-user", postgres.getUsername(),
            "--source-password", postgres.getPassword(),
            "--source-table", "bench_text_bytea_source",
            "--sink-connect", postgres.getJdbcUrl(),
            "--sink-user", postgres.getUsername(),
            "--sink-password", postgres.getPassword(),
            "--sink-table", "bench_text_bytea_sink"
        };
        ToolOptions textOptions = new ToolOptions(textArgs);
        assertEquals(0, ReplicaDB.processReplica(textOptions));
        long textDuration = System.nanoTime() - textStart;
        double textThroughput = ROW_COUNT / (textDuration / 1_000_000_000.0);
        
        LOG.info("Text COPY (BYTEA as hex VARCHAR): {} rows in {} ms ({} rows/sec)", 
                ROW_COUNT, textDuration / 1_000_000, String.format("%.2f", textThroughput));
        
        // Verify row count
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM bench_text_bytea_sink");
        rs.next();
        assertEquals(ROW_COUNT, rs.getInt(1), "Text COPY should replicate all rows");
        rs.close();
        
        // ===== DATASET 2: Binary COPY with actual BYTEA column =====
        LOG.info("Creating binary COPY dataset (actual BYTEA) with {} rows...", ROW_COUNT);
        
        // Create source table with actual BYTEA to trigger binary COPY
        stmt.execute("CREATE TABLE bench_binary_bytea_source (" +
                    "id INTEGER PRIMARY KEY, " +
                    "data BYTEA, " +
                    "val NUMERIC(20,5), " +
                    "ts TIMESTAMP, " +
                    "f FLOAT8)");
        
        stmt.execute("CREATE TABLE bench_binary_bytea_sink (" +
                    "id INTEGER PRIMARY KEY, " +
                    "data BYTEA, " +
                    "val NUMERIC(20,5), " +
                    "ts TIMESTAMP, " +
                    "f FLOAT8)");
        
        // Insert test rows with SAME DATA as text test
        LOG.info("Inserting {} rows (binary mode - actual BYTEA)...", ROW_COUNT);
        insertStart = System.nanoTime();
        stmt.execute(
            "INSERT INTO bench_binary_bytea_source " +
            "SELECT generate_series, " +
            "       decode(md5(generate_series::text), 'hex'), " + // Actual BYTEA
            "       (generate_series * random())::numeric(20,5), " +
            "       now() + (generate_series || ' seconds')::interval, " +
            "       random()::float8 " +
            "FROM generate_series(1, " + ROW_COUNT + ")"
        );
        insertDuration = (System.nanoTime() - insertStart) / 1_000_000;
        LOG.info("Binary dataset created in {} ms", insertDuration);
        
        // Benchmark binary COPY (will use binary format due to BYTEA column)
        long binaryStart = System.nanoTime();
        String[] binaryArgs = {
            "--source-connect", postgres.getJdbcUrl(),
            "--source-user", postgres.getUsername(),
            "--source-password", postgres.getPassword(),
            "--source-table", "bench_binary_bytea_source",
            "--sink-connect", postgres.getJdbcUrl(),
            "--sink-user", postgres.getUsername(),
            "--sink-password", postgres.getPassword(),
            "--sink-table", "bench_binary_bytea_sink"
        };
        ToolOptions binaryOptions = new ToolOptions(binaryArgs);
        assertEquals(0, ReplicaDB.processReplica(binaryOptions));
        long binaryDuration = System.nanoTime() - binaryStart;
        double binaryThroughput = ROW_COUNT / (binaryDuration / 1_000_000_000.0);
        
        LOG.info("Binary COPY (actual BYTEA): {} rows in {} ms ({} rows/sec)", 
                ROW_COUNT, binaryDuration / 1_000_000, String.format("%.2f", binaryThroughput));
        
        // Verify row count
        rs = stmt.executeQuery("SELECT COUNT(*) FROM bench_binary_bytea_sink");
        rs.next();
        assertEquals(ROW_COUNT, rs.getInt(1), "Binary COPY should replicate all rows");
        
        try (ResultSet differences = stmt.executeQuery("""
                SELECT COUNT(*) FROM bench_binary_bytea_sink b
                FULL JOIN bench_text_bytea_sink t USING (id)
                WHERE b.id IS NULL OR t.id IS NULL OR encode(b.data, 'hex') IS DISTINCT FROM t.data
                """)) {
            assertTrue(differences.next());
            assertEquals(0, differences.getInt(1), "Binary and text COPY should produce identical bytes");
        }

        stmt.execute("DROP TABLE IF EXISTS bench_text_bytea_source CASCADE");
        stmt.execute("DROP TABLE IF EXISTS bench_text_bytea_sink CASCADE");
        stmt.execute("DROP TABLE IF EXISTS bench_binary_bytea_source CASCADE");
        stmt.execute("DROP TABLE IF EXISTS bench_binary_bytea_sink CASCADE");
        stmt.close();
        
        if (BENCHMARK) {
            double throughputRatio = binaryThroughput / textThroughput;
            LOG.info("Binary COPY throughput relative to text: {}", throughputRatio);
            assertTrue(throughputRatio > 0.2, "Binary COPY throughput should exceed 20% of text COPY");
        }
    }

    @Test
    void copiesBinaryDataWithParallelJobs() throws SQLException, ParseException, IOException {
        LOG.info("=".repeat(80));
        LOG.info("Benchmark: Binary COPY with Parallel Jobs (4 workers)");
        LOG.info("=".repeat(80));
        
        Statement stmt = postgresConn.createStatement();
        
        // Create benchmark table
        stmt.execute("CREATE TABLE bench_parallel_source (" +
                    "id INTEGER PRIMARY KEY, " +
                    "data BYTEA, " +
                    "val NUMERIC(20,5), " +
                    "ts TIMESTAMP)");
        
        stmt.execute("CREATE TABLE bench_parallel_sink (" +
                    "id INTEGER PRIMARY KEY, " +
                    "data BYTEA, " +
                    "val NUMERIC(20,5), " +
                    "ts TIMESTAMP)");
        
        // Insert test rows
        LOG.info("Inserting {} rows...", ROW_COUNT);
        stmt.execute(
            "INSERT INTO bench_parallel_source " +
            "SELECT generate_series, " +
            "       decode(md5(generate_series::text), 'hex'), " +
            "       (generate_series * random())::numeric(20,5), " +
            "       now() + (generate_series || ' seconds')::interval " +
            "FROM generate_series(1, " + ROW_COUNT + ")"
        );
        
        // Benchmark with parallel jobs
        long parallelStart = System.nanoTime();
        String[] parallelArgs = {
            "--source-connect", postgres.getJdbcUrl(),
            "--source-user", postgres.getUsername(),
            "--source-password", postgres.getPassword(),
            "--source-table", "bench_parallel_source",
            "--sink-connect", postgres.getJdbcUrl(),
            "--sink-user", postgres.getUsername(),
            "--sink-password", postgres.getPassword(),
            "--sink-table", "bench_parallel_sink",
            "--jobs", "4"
        };
        ToolOptions parallelOptions = new ToolOptions(parallelArgs);
        assertEquals(0, ReplicaDB.processReplica(parallelOptions));
        long parallelDuration = System.nanoTime() - parallelStart;
        double parallelThroughput = ROW_COUNT / (parallelDuration / 1_000_000_000.0);
        
        LOG.info("Parallel Binary COPY (4 jobs): {} rows in {} ms ({} rows/sec)", 
                ROW_COUNT, parallelDuration / 1_000_000, String.format("%.2f", parallelThroughput));
        
        // Verify row count
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM bench_parallel_sink");
        rs.next();
        assertEquals(ROW_COUNT, rs.getInt(1), "Parallel COPY should replicate all rows");
        rs.close();
        
        stmt.execute("DROP TABLE IF EXISTS bench_parallel_source CASCADE");
        stmt.execute("DROP TABLE IF EXISTS bench_parallel_sink CASCADE");
        stmt.close();
        
        LOG.info("✅ Parallel benchmark completed successfully");
    }
}

package org.replicadb.manager.file;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.*;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.util.BandwidthThrottling;
import org.replicadb.rowset.OrcCachedRowSetImpl;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Date;
import java.sql.*;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.replicadb.manager.LocalFileManager.getFileFromPathString;

/**
 * This class manages the reading and writing ORC files.
 */
public class OrcFileManager extends FileManager {
    private static final Logger LOG = LogManager.getLogger(OrcFileManager.class);

    private OrcCachedRowSetImpl orcResultset;

    public OrcFileManager(ToolOptions opts, DataSourceType dsType) {
        super(opts, dsType);
    }

    @Override
    public void init() throws SQLException {}

    @Override
    public ResultSet readData() {
        return this.orcResultset;
    }

    @Override
    public int writeData(OutputStream out, ResultSet resultSet, int taskId, File tempFile) throws IOException, SQLException {

        // because we'll create a new file and a new output stream
        //out.close();

        // Create configuration and overwrite existing temporal file
        Configuration conf = new Configuration();
        conf.set(OrcConf.OVERWRITE_OUTPUT_FILE.getAttribute(), "true");

        if (tempFile == null) tempFile = createTemporalFile(taskId);
        Path path = new Path(tempFile.getPath());

        // Create the schemas and extract metadata from the schema
        TypeDescription schema = typeDescriptionFromResultSet(resultSet);
        LOG.info("Sink ORC type description: {} ", schema.toString());

        // Create a row batch
        VectorizedRowBatch batch = schema.createRowBatch();

        // Determine compression codec
        Properties orcFileProperties;
        //TODO
        if (dsType == DataSourceType.SOURCE)
            orcFileProperties = options.getSourceConnectionParams();
        else
            orcFileProperties = options.getSinkConnectionParams();

        CompressionKind codec = resolveCompression(orcFileProperties.getProperty("orc.compression"));

        // Open a writer to write the data to an ORC fle
        Writer writer = OrcFile.createWriter(path,
                OrcFile.writerOptions(conf)
                        .compress(codec)
                        .setSchema(schema));

        final int BATCH_SIZE = batch.getMaxSize();

        int processedRows = 0;
        // lines
        if (resultSet.next()) {
            // Create Bandwidth Throttling
            BandwidthThrottling bt = new BandwidthThrottling(options.getBandwidthThrottling(), options.getFetchSize(), resultSet);
            do {
                bt.acquiere();
                processedRows++;
                int row = batch.size++;

                // Bind resultSet values to VectorizedRowBatch
                toOrcVectorized(batch, row, resultSet);

                if (row == BATCH_SIZE - 1) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }

            } while (resultSet.next());
        }

        // insert remaining records
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }

        writer.close();

        // finally return the ORC file inside OutputStream
        LOG.debug("ORC file generated into {}" , tempFile.getPath());
        IOUtils.copy(FileUtils.openInputStream(tempFile), out);
        return processedRows;
    }

    private File createTemporalFile(int taskId) {
        try {
            java.nio.file.Path temp = Files.createTempFile("repdb", ".orc");
            LOG.info("Temporal file path: " + temp.toAbsolutePath());
            // Save the path of temp file
            setTempFilePath(taskId, temp.toAbsolutePath().toString());
            return temp.toFile();
        } catch (IOException e) {
            LOG.error(e);
        }
        return null;
    }

    @Override
    public void mergeFiles() throws IOException, URISyntaxException {
        File finalFile = getFileFromPathString(options.getSinkConnect());
        Path finalFilePath = new Path(finalFile.toPath().toUri());

        int tempFilesIdx = 0;
        if (options.getMode().equals(ReplicationMode.COMPLETE.getModeText())) {
            // Rename first temporal file to the final file
            File firstTemporalFile = getFileFromPathString(getTempFilePath(0));
            String crcFile = "file://" + firstTemporalFile.getParent() + "/." + firstTemporalFile.getName() + ".crc";
            LOG.debug("The crc file: {}", crcFile);
            getFileFromPathString(crcFile).delete();

            Files.move(firstTemporalFile.toPath(), firstTemporalFile.toPath().resolveSibling(finalFile.getPath()), StandardCopyOption.REPLACE_EXISTING);
            tempFilesIdx = 1;
            LOG.info("Complete mode: creating and merging all temp files into: " + finalFile.getPath());
        } else {
            LOG.info("Incremental mode: appending and merging all temp files into: " + finalFile.getPath());
            // The final file must exist
            if (!finalFile.exists()) finalFile.createNewFile();
        }

        Configuration conf = new Configuration();
        conf.set(OrcConf.OVERWRITE_OUTPUT_FILE.getAttribute(), "true");

        for (int i = tempFilesIdx; i <= getTempFilePathSize() - 1; i++) {
            LOG.debug("tempFilesPath.get({}): {}", i, getTempFilePath(i) );
            File tempFile = getFileFromPathString( getTempFilePath(i) );
            Path tempFilePath = new Path(tempFile.toPath().toUri());
            List<Path> filesToMerge = new ArrayList<>();
            filesToMerge.add(tempFilePath);
            filesToMerge.add(finalFilePath);

            String mergeFilePathString = finalFile.toPath().toUri() + ".merged";
            Path mergedFilePath = new Path(mergeFilePathString);
            File mergeFile = getFileFromPathString(mergeFilePathString);
            if (!mergeFile.exists()) mergeFile.createNewFile();

            LOG.debug("Merge temp file : {} into : {}", tempFilePath.toUri(), finalFile.toPath().toUri());
            // Merge files
            OrcFile.mergeFiles(mergedFilePath, OrcFile.writerOptions(conf), filesToMerge);

            // Delete temp file
            tempFile.delete();
            // Delete crc file, hadoop bug https://issues.apache.org/jira/browse/HADOOP-7199
            String crcFile = "file://" + tempFile.getParent() + "/." + tempFile.getName() + ".crc";
            getFileFromPathString(crcFile).delete();

            // Rename Merged file to the final file
            Files.move(mergeFile.toPath(), finalFile.toPath().resolveSibling(finalFile.getPath()), StandardCopyOption.REPLACE_EXISTING);

        }

        // remove Merged file crc
        String crcFile = "file://" + finalFile.getParent() + "/." + finalFile.getName() + ".merged.crc";
        getFileFromPathString(crcFile).delete();
    }

    /**
     * Determine de compression codec based on the name: NONE, ZLIB, SNAPPY, LZO, LZ4, ZSTD
     * Default ZLIB
     *
     * @param codec
     * @return
     */
    private static CompressionKind resolveCompression(String codec) {
        CompressionKind retCodec;

        if (codec == null || codec.isEmpty()) codec = "ZLIB";
        codec = codec.toUpperCase();

        try {
            retCodec = CompressionKind.valueOf(codec);
        } catch (IllegalArgumentException e) {
            LOG.warn("Unable to determine compression codec {}, using ZLIB instead", codec);
            retCodec = CompressionKind.valueOf("ZLIB");
        }

        LOG.info("Setting the compression codec to: {}", retCodec.toString());
        return retCodec;
    }

    /**
     * Bind ResultSet values to the Orc VectorizedRowBatch
     *
     * @param batch
     * @param rowInBatch
     * @param resultSet
     */
    private static void toOrcVectorized(VectorizedRowBatch batch, int rowInBatch, ResultSet resultSet) throws SQLException, IOException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            // VectorizedRowBatch index starts at 0
            int batchColIndex = i - 1;

            // Get data and check for null field
            Object field = resultSet.getObject(i);
            if (field == null) {
                batch.cols[batchColIndex].noNulls = false;
                batch.cols[batchColIndex].isNull[rowInBatch] = true;
            } else {

                switch (rsmd.getColumnType(i)) {
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                        setValueToBytesColumnVector(batch.cols[batchColIndex], rowInBatch, field);
                        break;
                    case Types.INTEGER:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                        ((LongColumnVector) batch.cols[batchColIndex]).vector[rowInBatch] = (int) field;
                        break;
                    case Types.BIGINT:
                        ((LongColumnVector) batch.cols[batchColIndex]).vector[rowInBatch] = (long) field;
                        break;
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        BigDecimal decimalData = (BigDecimal) field;
                        ((DecimalColumnVector) batch.cols[batchColIndex]).vector[rowInBatch] = new HiveDecimalWritable(HiveDecimal.create(decimalData));
                        break;
                    case Types.DOUBLE:
                    case Types.FLOAT:
                        ((DoubleColumnVector) batch.cols[batchColIndex]).vector[rowInBatch] = (Double) field;
                        break;
                    case Types.DATE:
                        ((DateColumnVector) batch.cols[batchColIndex]).vector[rowInBatch] = ((Date) field).toLocalDate().toEpochDay();
                        break;
                    case Types.TIMESTAMP:
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                    case -101:
                    case -102:
                        ((TimestampColumnVector) batch.cols[batchColIndex]).set(rowInBatch, resultSet.getTimestamp(i));
                        break;
                    case Types.BINARY:
                        setValueToBytesColumnVector(batch.cols[batchColIndex], rowInBatch, resultSet.getBytes(i));
                        break;
                    case Types.BLOB:
                        Blob blobData = resultSet.getBlob(i);
                        setValueToBytesColumnVector(batch.cols[batchColIndex], rowInBatch, IOUtils.toByteArray(blobData.getBinaryStream()));
                        blobData.free();
                        break;
                    case Types.CLOB:
                        Clob clobData = resultSet.getClob(i);
                        setValueToBytesColumnVector(batch.cols[batchColIndex], rowInBatch, IOUtils.toByteArray(clobData.getCharacterStream(), UTF_8));
                        clobData.free();
                        break;
                    case Types.BOOLEAN:
                    case Types.BIT: //postgres boolean
                        ((LongColumnVector) batch.cols[batchColIndex]).vector[rowInBatch] = resultSet.getBoolean(i) ? 1 : 0;
                        break;
                    case Types.SQLXML:
                        SQLXML sqlxmlData = resultSet.getSQLXML(i);
                        setValueToBytesColumnVector(batch.cols[batchColIndex], rowInBatch, IOUtils.toByteArray(sqlxmlData.getCharacterStream(), UTF_8));
                        sqlxmlData.free();
                        break;
                    case Types.ARRAY:
                        // Map Array to ListColumnVector
                        Array arrayData = resultSet.getArray(i);
                        ListColumnVector lcv = (ListColumnVector) batch.cols[batchColIndex];
                        // All arrays will be converted to String[] ["1837.3471930087544","1376.46154384128749"]
                        // Even multidimensional arrays ["[2168, 2174]","[3946, 1271]"] // Only in postgres
                        Object[] values = (Object[]) arrayData.getArray();
                        lcv.lengths[rowInBatch] = values.length;
                        lcv.offsets[rowInBatch] = lcv.childCount;
                        lcv.childCount = Math.toIntExact(lcv.lengths[rowInBatch]) ;
                        lcv.child.ensureSize(lcv.childCount, true);
                        for (int j = 0; j < values.length; j++) {
                            setValueToBytesColumnVector(lcv.child, (int) lcv.offsets[rowInBatch] + j, values[j]);
                        }
                        arrayData.free();
                        break;
//                case Types.STRUCT:
//                    schema.addField(rsmd.getColumnName(i), TypeDescription.createStruct());
//                    break;
                    default:
                        setValueToBytesColumnVector(batch.cols[batchColIndex], rowInBatch, resultSet.getString(i));
                        break;
                }
            }
        }

    }

    /**
     * Set a value to BytesColumnVector
     *
     * @param col
     * @param rowInBatch
     * @param value
     */
    private static void setValueToBytesColumnVector(ColumnVector col, int rowInBatch, Object value) {
        byte[] finalValueToSet = new byte[0];
        if (value != null) {
            if (value instanceof String) finalValueToSet = ((String) value).getBytes(UTF_8);
            if (value instanceof byte[]) finalValueToSet = (byte[]) value;
            if (value instanceof Object[]) finalValueToSet = Arrays.deepToString((Object[]) value).getBytes(UTF_8);
        }
        ((BytesColumnVector) col).setVal(rowInBatch, finalValueToSet);
    }

    /**
     * Creates new TypeDescription (ORC schema) from JDBC ResultSet
     *
     * @param resultSet
     * @return
     */
    private static TypeDescription typeDescriptionFromResultSet(ResultSet resultSet) throws SQLException {
        TypeDescription schema = TypeDescription.createStruct();

        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            //LOG.info("columnName: {}, columnTypeName: {}, CassName: {}, sql.Type:{}", rsmd.getColumnName(i), rsmd.getColumnTypeName(i),rsmd.getColumnClassName(i), rsmd.getColumnType(i) );
            switch (rsmd.getColumnType(i)) {
                case Types.CHAR:
                    schema.addField(rsmd.getColumnName(i), TypeDescription.createChar().withMaxLength(rsmd.getPrecision(i)));
                    break;
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CLOB:
                    schema.addField(rsmd.getColumnName(i), TypeDescription.createString());
                    break;
                case Types.INTEGER:
                case Types.TINYINT:
                case Types.SMALLINT:
                    schema.addField(rsmd.getColumnName(i), TypeDescription.createInt());
                    break;
                case Types.BIGINT:
                    schema.addField(rsmd.getColumnName(i), TypeDescription.createLong());
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    schema.addField(rsmd.getColumnName(i), TypeDescription.createDecimal());
                    break;
                case Types.DOUBLE:
                    schema.addField(rsmd.getColumnName(i), TypeDescription.createDouble());
                    break;
                case Types.FLOAT:
                    schema.addField(rsmd.getColumnName(i), TypeDescription.createFloat());
                    break;
                case Types.DATE:
                    schema.addField(rsmd.getColumnName(i), TypeDescription.createDate());
                    break;
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                case -101:
                case -102:
                    schema.addField(rsmd.getColumnName(i), TypeDescription.createTimestamp());
                    break;
                case Types.BINARY:
                case Types.BLOB:
                    schema.addField(rsmd.getColumnName(i), TypeDescription.createBinary());
                    break;
                case Types.BOOLEAN:
                case -7: //postgres boolean
                    schema.addField(rsmd.getColumnName(i), TypeDescription.createBoolean());
                    break;
                case Types.ARRAY:
                    schema.addField(rsmd.getColumnName(i), TypeDescription.createList(TypeDescription.createString()));
                    break;
//                case Types.STRUCT:
//                    schema.addField(rsmd.getColumnName(i), TypeDescription.createStruct());
//                    break;
                default:
                    schema.addField(rsmd.getColumnName(i), TypeDescription.createString());
                    break;
            }
        }
        return schema;
    }


    @Override
    public void cleanUp() {
        // Ensure drop all temporal files
        for (Map.Entry<Integer, String> filePath : getTempFilesPath().entrySet()) {
            File tempFile = null;
            File crcFile = null;
            try {
                tempFile = getFileFromPathString(filePath.getValue());
                String crcPath = "file://" + tempFile.getParent() + "/." + tempFile.getName() + ".crc";
                crcFile = getFileFromPathString(crcPath);
            } catch (MalformedURLException | URISyntaxException e) {
                LOG.error(e);
            }

            if (tempFile.exists()) {
                LOG.debug("Remove temp file {}", tempFile.getPath());
                tempFile.delete();
            }
            if (crcFile.exists()) {
                LOG.debug("Remove crc temp file {}", crcFile.getPath());
                crcFile.delete();
            }
        }
    }
}

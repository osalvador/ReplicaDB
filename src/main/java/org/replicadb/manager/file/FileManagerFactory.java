package org.replicadb.manager.file;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.DataSourceType;

import static org.replicadb.manager.file.FileFormats.CSV;
import static org.replicadb.manager.file.FileFormats.ORC;

public class FileManagerFactory {
    private static final Logger LOG = LogManager.getLogger(FileManagerFactory.class);

    /**
     * Instantiate a FileManager that can meet the requirements of the specified file format
     *
     * @param options the user-provided arguments
     * @param dsType  the type of the DataSource, source or sink.
     * @return
     */
    public FileManager accept(ToolOptions options, DataSourceType dsType) {
        String fileFormat = null;
        if (dsType == DataSourceType.SOURCE) {
            fileFormat = options.getSourceFileFormat();
        } else if (dsType == DataSourceType.SINK) {
            fileFormat = options.getSinkFileFormat();
        } else {
            LOG.error("DataSourceType must be Source or Sink");
        }

        if (ORC.getType().equals(fileFormat)) {
            LOG.info("return OrcFileManager");
            return new OrcFileManager(options, dsType);
        } else if (CSV.getType().equals(fileFormat)) {
            LOG.info("return CsvFileManager");
            return new CsvFileManager(options, dsType);
        } else {
            // CSV is the Default file format
            LOG.warn("The file format is not defined, setting CSV as the default file format.");
            return new CsvFileManager(options, dsType);
        }
    }
}

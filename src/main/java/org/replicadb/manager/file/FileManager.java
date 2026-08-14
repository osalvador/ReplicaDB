package org.replicadb.manager.file;

import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.DataSourceType;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.Map;

/**
 * Abstract interface that manages Files.
 * The implementations of this class drive the actual discussion with
 * the files about formats, compressions, etc.
 */
public abstract class FileManager {

    /**
     * The ReplicaDB options defined by the user
     */
    protected ToolOptions options;
    /**
     * The data source type, defines whether the file is an input or output file
     */
    protected DataSourceType dsType;

    /**
     * String array with the paths of the temporal files
     */
    public FileManager(ToolOptions opts, DataSourceType dsType) {
        this.options = opts;
        this.dsType = dsType;
    }

    /**
     * Write the ResultSet into the `out` OutputStream
     *
     * @param out
     * @param resultSet
     * @param taskId
     * @param tempFile  if neccesary
     * @return the number of total rows processed
     * @throws IOException
     * @throws SQLException
     */
    public abstract int writeData(OutputStream out, ResultSet resultSet, int taskId, File tempFile) throws IOException, SQLException;

    /**
     * Normally multiple jobs files are written into temp files. This method Merge multiple temp files to produce
     * a single one
     *
     * @throws IOException
     * @throws URISyntaxException
     */
    public abstract void mergeFiles() throws IOException, URISyntaxException;

    /**
     * Ensure to cleanup all temporal files or data created.
     *
     * @throws Exception
     */
    public abstract void cleanUp() throws Exception;

    public abstract void init() throws SQLException;

    public abstract ResultSet readData();

    /**
     * Getters and Setters
     */
    public Map<Integer, String> getTempFilesPath() {
        return options.getExecutionContext().getTempFilesPath();
    }

    public void setTempFilePath(int taskId, String path) {
        options.getExecutionContext().setTempFilePath(taskId, path);
    }

    public String getTempFilePath(int idx) {
        return options.getExecutionContext().getTempFilePath(idx);
    }

    public int getTempFilePathSize() {
        return options.getExecutionContext().getTempFilePathSize();
    }

}

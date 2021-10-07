package org.replicadb.manager.file;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.DataSourceType;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract interface that manages Files.
 * The implementations of this class drive the actual discussion with
 * the files about formats, compressions, etc.
 */
public abstract class FileManager {
    private static final Logger LOG = LogManager.getLogger(FileManager.class);

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
    protected static Map<Integer, String> tempFilesPath;


    public FileManager(ToolOptions opts, DataSourceType dsType) {
        this.options = opts;
        this.dsType = dsType;
        newTempFilesPath();
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
    public static synchronized void newTempFilesPath() {
        if (tempFilesPath == null)
            tempFilesPath =  new HashMap<>();
    }

    public static synchronized Map<Integer, String> getTempFilesPath() {
        return tempFilesPath;
    }

    public static synchronized void setTempFilesPath(Map<Integer, String> tempFilesPath) {FileManager.tempFilesPath = tempFilesPath;}

    public static synchronized void setTempFilePath(int taskId, String path) {
        tempFilesPath.put(taskId, path);
    }

    public static synchronized String getTempFilePath(int idx) {
        return tempFilesPath.get(idx);
    }

    public static synchronized int getTempFilePathSize() {
        return tempFilesPath.size();
    }

}

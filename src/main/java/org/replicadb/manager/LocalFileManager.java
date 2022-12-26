package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.file.FileManager;
import org.replicadb.manager.file.FileManagerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class LocalFileManager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(LocalFileManager.class.getName());

    private final FileManager fileManager;

    /**
     * Constructs the SqlManager.
     *
     * @param opts the ReplicaDB ToolOptions describing the user's requested action.
     */
    public LocalFileManager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
        this.fileManager = new FileManagerFactory().accept(opts, dsType);
    }

    @Override
    public Future<Integer> preSinkTasks(ExecutorService executor) {
        // On COMPLETE_ATOMIC mode
        if (options.getMode().equals(ReplicationMode.COMPLETE_ATOMIC.getModeText()))
            throw new UnsupportedOperationException("The 'complete-atomic' mode is not applicable to file as a sink");
        else
            return null;
    }

    @Override
    protected Connection makeSourceConnection() throws SQLException {
        this.fileManager.init();
        return null;
    }

    @Override
    protected Connection makeSinkConnection() {
        /*Not necessary for */
        return null;
    }

    @Override
    public String getDriverClass() {
        return JdbcDrivers.FILE.getDriverClass();
    }

    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) throws Exception {

        // Temporal file name
        String randomFileUrl = options.getSinkConnect() + ".repdb." + (new Random().nextInt(9000) + 1000);
        LOG.info("Temporal file path: {}",randomFileUrl);

        // Save the path of temp file
        FileManager.setTempFilePath(taskId,randomFileUrl);

        // Create the OutputStream
        File tempFile = getFileFromPathString(randomFileUrl);
        OutputStream out = new FileOutputStream(tempFile);

        // Write data to the specific file manager
        int processedRows = this.fileManager.writeData(out, resultSet, taskId, tempFile);

        out.close();
        return processedRows;
    }


    @Override
    public ResultSet readTable(String tableName, String[] columns, int nThread) throws SQLException {
        return this.fileManager.readData();
    }


    @Override
    protected void createStagingTable() {/*Not implemented*/}

    @Override
    /**
     * Merging temporal files
     */
    protected void mergeStagingTable() throws Exception {
        // This method should be delegated to the specific fileFormat manager
        this.fileManager.mergeFiles();
    }


    @Override
    public void postSourceTasks() {/*Not implemented*/}

    @Override
    public void preSourceTasks() {
        if (options.getJobs() > 1)
            throw new IllegalArgumentException("Only one job is allowed when reading from a file. Jobs parameter must be set to 1. jobs=1");

    }

    @Override
    public void postSinkTasks() throws Exception {
        // Always merge data
        this.mergeStagingTable();
    }

    @Override
    public void cleanUp() throws Exception {
        // Ensure drop temporal file
        this.fileManager.cleanUp();
    }


    /**
     * Returns an instance of a File given the url string of the file path.
     * It gives compatibility with the windows, linux and mac URL Strings.
     *
     * @param urlString
     * @return
     * @throws MalformedURLException
     * @throws URISyntaxException
     */
    public static File getFileFromPathString(String urlString) throws MalformedURLException, URISyntaxException {

        //TODO: delete me. Only meanwhile developing the new file manager
        if (urlString != null) urlString = urlString.replaceAll("new:", "");
        //
        URL url = null;
        try {
            url = new URL(urlString);
        } catch (MalformedURLException e) {
            url = new URL("file://"+urlString);
        }
        URI uri = url.toURI();

        if (uri.getAuthority() != null && uri.getAuthority().length() > 0) {
            // Hack for UNC Path
            uri = (new URL("file://" + urlString.substring("file:".length()))).toURI();
        }

        File file = new File(uri);
        return file;
    }

}

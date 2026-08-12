package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.db2.Db2Manager;

import java.util.Properties;

import static org.replicadb.manager.SupportedManagers.*;

/**
 * Contains instantiation code for all ConnManager implementations
 *  ManagerFactories are instantiated by o.a.h.s.ConnFactory and
 *  stored in an ordered list. The ConnFactory.getManager() implementation
 *  calls the accept() method of each ManagerFactory, in order until
 *  one such call returns a non-null ConnManager instance.
 */
public class ManagerFactory {

    private static final Logger LOG = LogManager.getLogger(ManagerFactory.class.getName());

    /**
     * Instantiate a ConnManager that can fulfill the database connection
     * requirements of the task specified in the JobData.
     *
     * @param options the user-provided arguments that configure this
     *                Sqoop job.
     * @return a ConnManager that can connect to the specified database
     * and perform the operations required, or null if this factory cannot
     * find a suitable ConnManager implementation.
     */
    public ConnManager accept(ToolOptions options, DataSourceType dsType) {

        validateAzureAuthenticationConfiguration(options);

        String scheme = extractScheme(options, dsType);

        if (null == scheme) {
            // We don't know if this is a mysql://, hsql://, etc.
            // Can't do anything with this.
            LOG.warn("Null scheme associated with connect string.");
            return null;
        }

        LOG.trace("Trying with scheme: {}", scheme);

        if (POSTGRES.isTheManagerTypeOf(options, dsType)) {
                return new PostgresqlManager(options, dsType);
            } else if (ORACLE.isTheManagerTypeOf(options, dsType)) {
                return new OracleManager(options, dsType);
            } else if (DENODO.isTheManagerTypeOf(options, dsType)) {
                return new DenodoManager(options, dsType);
            } else if (KAFKA.isTheManagerTypeOf(options, dsType)) {
                return new KafkaManager(options, dsType);
            } else if (SQLSERVER.isTheManagerTypeOf(options, dsType)) {
                return new SQLServerManager(options, dsType);
            } else if (S3.isTheManagerTypeOf(options, dsType)) {
                return new S3Manager(options, dsType);
            } else if (MYSQL.isTheManagerTypeOf(options, dsType) || MARIADB.isTheManagerTypeOf(options, dsType)) {
                return new MySQLManager(options, dsType);
            } else if (FILE.isTheManagerTypeOf(options, dsType)) {
                return new LocalFileManager(options, dsType);
            } else if (SQLITE.isTheManagerTypeOf(options, dsType)) {
                return new SqliteManager(options, dsType);
            } else if (MONGODB.isTheManagerTypeOf(options, dsType) || MONGODBSRV.isTheManagerTypeOf(options, dsType)) {
                return new MongoDBManager(options, dsType);
            } else if (DB2.isTheManagerTypeOf(options, dsType) || DB2_AS400.isTheManagerTypeOf(options, dsType)) {
                return new Db2Manager(options, dsType);
            } else {
                LOG.warn("The database with scheme {} was not found. Trying  with standard JDBC manager ", scheme);
                return new StandardJDBCManager(options, dsType);
            }

    }

    public void validateAzureAuthenticationConfiguration(ToolOptions options) {
        options.validateAzureAuthentication();
        validateAzureAuthenticationScheme(options, DataSourceType.SOURCE);
        validateAzureAuthenticationScheme(options, DataSourceType.SINK);
        validateRawInteractiveParallelism(options, DataSourceType.SOURCE);
        validateRawInteractiveParallelism(options, DataSourceType.SINK);
    }

    private void validateAzureAuthenticationScheme(ToolOptions options, DataSourceType dsType) {
        boolean configured = DataSourceType.SOURCE.equals(dsType)
                ? options.getSourceAuthentication().isConfigured()
                : options.getSinkAuthentication().isConfigured();
        if (!configured) {
            return;
        }

        String scheme = extractScheme(options, dsType);
        if (scheme == null || !scheme.startsWith(SQLSERVER.getSchemePrefix())) {
            throw new IllegalArgumentException(
                    "Azure authentication settings are supported only for SQL Server connections: " + dsType);
        }
    }

    private void validateRawInteractiveParallelism(ToolOptions options, DataSourceType dsType) {
        if (options.getJobs() <= 1) {
            return;
        }

        String scheme = extractScheme(options, dsType);
        if (scheme == null || !scheme.startsWith(SQLSERVER.getSchemePrefix())) {
            return;
        }

        String connectString = DataSourceType.SOURCE.equals(dsType)
                ? options.getSourceConnect()
                : options.getSinkConnect();
        Properties connectionParams = DataSourceType.SOURCE.equals(dsType)
                ? options.getSourceConnectionParams()
                : options.getSinkConnectionParams();
        String authentication = getConnectionProperty(connectString, "authentication");
        if (authentication == null && connectionParams != null) {
            authentication = connectionParams.getProperty("authentication");
        }

        if ("ActiveDirectoryInteractive".equalsIgnoreCase(authentication)) {
            throw new IllegalArgumentException(
                    "ActiveDirectoryInteractive authentication requires jobs=1 to avoid concurrent browser flows.");
        }
    }

    private String getConnectionProperty(String connectString, String propertyName) {
        if (connectString == null) {
            return null;
        }
        for (String segment : connectString.split(";")) {
            int separator = segment.indexOf('=');
            if (separator > 0 && propertyName.equalsIgnoreCase(segment.substring(0, separator).trim())) {
                return segment.substring(separator + 1).trim();
            }
        }
        return null;
    }

    protected String extractScheme(ToolOptions options, DataSourceType dsType) {
        return SupportedManagers.extractScheme(options, dsType);
    }

}

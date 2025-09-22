package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;

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
            } else {
                LOG.warn("The database with scheme {} was not found. Trying  with standard JDBC manager ", scheme);
                return new StandardJDBCManager(options, dsType);
            }

    }

    protected String extractScheme(ToolOptions options, DataSourceType dsType) {
        return SupportedManagers.extractScheme(options, dsType);
    }

}

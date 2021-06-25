package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.cdc.OracleManagerCDC;
import org.replicadb.manager.cdc.SQLServerManagerCDC;

import java.util.Properties;

import static org.replicadb.manager.SupportedManagers.*;

/**
 * Contains instantiation code for all ConnManager implementations
 * ???? ManagerFactories are instantiated by o.a.h.s.ConnFactory and
 * ???? stored in an ordered list. The ConnFactory.getManager() implementation
 * ???? calls the accept() method of each ManagerFactory, in order until
 * ???? one such call returns a non-null ConnManager instance.
 */
public class ManagerFactory {

    private static final Logger LOG = LogManager.getLogger(ManagerFactory.class.getName());
    //public static final String NET_SOURCEFORGE_JTDS_JDBC_DRIVER = "net.sourceforge.jtds.jdbc.Driver";

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

        LOG.debug("Trying with scheme: " + scheme);

        if (options.getMode().equals(ReplicationMode.CDC.getModeText())) {
            LOG.debug("CDC Managers");

            if (SQLSERVER.isTheManagerTypeOf(options, dsType)) {
                return new SQLServerManagerCDC(options, dsType);
            } else if (ORACLE.isTheManagerTypeOf(options, dsType)) {
                return new OracleManagerCDC(options, dsType);
            }else {
                throw new IllegalArgumentException("The database with scheme "+scheme+" is not supported in CDC mode");
            }

        } else {
            if (POSTGRES.isTheManagerTypeOf(options, dsType)) {
                return new PostgresqlManager(options, dsType);
            } else if (ORACLE.isTheManagerTypeOf(options, dsType)) {
                return new OracleManager(options, dsType);
            } else if (DENODO.isTheManagerTypeOf(options, dsType)) {
                return new DenodoManager(options, dsType);
            } else if (CSV.isTheManagerTypeOf(options, dsType)) {
                return new CsvManager(options, dsType);
            } else if (KAFKA.isTheManagerTypeOf(options, dsType)) {
                return new KafkaManager(options, dsType);
            } else if (SQLSERVER.isTheManagerTypeOf(options, dsType)) {
                return new SQLServerManager(options, dsType);
            } else if (S3.isTheManagerTypeOf(options, dsType)) {
                return new S3Manager(options, dsType);
            } else if (MYSQL.isTheManagerTypeOf(options, dsType) || MARIADB.isTheManagerTypeOf(options, dsType)) {
                // In MySQL and MariaDB this properties are required
                if (dsType.equals(DataSourceType.SINK)){
                    Properties mysqlProps = new Properties();
                    mysqlProps.setProperty("characterEncoding", "UTF-8");
                    mysqlProps.setProperty("allowLoadLocalInfile", "true");
                    mysqlProps.setProperty("rewriteBatchedStatements","true");
                    options.setSinkConnectionParams(mysqlProps);
                }
                return new MySQLManager(options, dsType);
            } else {
                throw new IllegalArgumentException("The database with scheme "+scheme+" is not supported by ReplicaDB");
            }
        }
/*
        if (MYSQL.isTheManagerTypeOf(options)) {
              //return new MySQLManager(options);
            }
        } else if (SupportedManagers.POSTGRES.isTheManagerTypeOf(options)) {
                return new PostgresqlManager(options);
        } else if (HSQLDB.isTheManagerTypeOf(options)) {
            return new HsqldbManager(options);
        } else if (ORACLE.isTheManagerTypeOf(options)) {
            return new OracleManager(options);
        } else if (SQLSERVER.isTheManagerTypeOf(options)) {
            return new SQLServerManager(options);
        } else if (JTDS_SQLSERVER.isTheManagerTypeOf(options)) {
            return new SQLServerManager(NET_SOURCEFORGE_JTDS_JDBC_DRIVER, options);
        } else if (DB2.isTheManagerTypeOf(options)) {
            return new Db2Manager(options);
        } else if (NETEZZA.isTheManagerTypeOf(options)) {
            if (options.isDirect()) {
                return new DirectNetezzaManager(options);
            } else {
                return new NetezzaManager(options);
            }
        } else if (CUBRID.isTheManagerTypeOf(options)) {
            return new CubridManager(options);
        } else {
            return null;
        }*/
    }

    protected String extractScheme(ToolOptions options, DataSourceType dsType) {
        return SupportedManagers.extractScheme(options, dsType);
    }

}

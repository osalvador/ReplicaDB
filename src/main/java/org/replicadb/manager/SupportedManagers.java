package org.replicadb.manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;

public enum SupportedManagers {
    MYSQL(JdbcDrivers.MYSQL.getSchemePrefix()), MARIADB(JdbcDrivers.MARIADB.getSchemePrefix()), POSTGRES(JdbcDrivers.POSTGRES.getSchemePrefix()),
    HSQLDB(JdbcDrivers.HSQLDB.getSchemePrefix()), ORACLE(JdbcDrivers.ORACLE.getSchemePrefix()),
    SQLSERVER(JdbcDrivers.SQLSERVER.getSchemePrefix()), CUBRID(JdbcDrivers.CUBRID.getSchemePrefix()),
    JTDS_SQLSERVER(JdbcDrivers.JTDS_SQLSERVER.getSchemePrefix()), DB2(JdbcDrivers.DB2.getSchemePrefix()),
    NETEZZA(JdbcDrivers.NETEZZA.getSchemePrefix()), DENODO(JdbcDrivers.DENODO.getSchemePrefix()),
    KAFKA(JdbcDrivers.KAFKA.getSchemePrefix()),
    S3(JdbcDrivers.S3.getSchemePrefix()), FILE(JdbcDrivers.FILE.getSchemePrefix()),
    SQLITE(JdbcDrivers.SQLITE.getSchemePrefix()),
    MONGODB(JdbcDrivers.MONGODB.getSchemePrefix()), MONGODBSRV(JdbcDrivers.MONGODBSRV.getSchemePrefix());

    private final String schemePrefix;

    //private final boolean hasDirectConnector;
    private static final Logger LOG = LogManager.getLogger(SupportedManagers.class.getName());

    SupportedManagers(String schemePrefix) {
        this.schemePrefix = schemePrefix;
    }

    public String getSchemePrefix() {
        return schemePrefix;
    }


    public boolean isTheManagerTypeOf(ToolOptions options, DataSourceType dsType) {
        return (extractScheme(options, dsType)).startsWith(getSchemePrefix());
    }

//    public static SupportedManagers createFrom(ToolOptions options) {
//        String scheme = extractScheme(options);
//        for (SupportedManagers m : values()) {
//            if (scheme.startsWith(m.getSchemePrefix())) {
//                return m;
//            }
//        }
//        return null;
//    }

    static String extractScheme(ToolOptions options, DataSourceType dsType) {

        String connectStr = null;

        if (dsType == DataSourceType.SOURCE) {
            connectStr = options.getSourceConnect();
        } else if (dsType == DataSourceType.SINK) {
            connectStr = options.getSinkConnect();
        } else {
            LOG.error("DataSourceType must be Source or Sink");
        }

        // java.net.URL follows RFC-2396 literally, which does not allow a ':'
        // character in the scheme component (section 3.1). JDBC connect strings,
        // however, commonly have a multi-scheme addressing system. e.g.,
        // jdbc:mysql://...; so we cannot parse the scheme component via URL
        // objects. Instead, attempt to pull out the scheme as best as we can.

        // First, see if this is of the form [scheme://hostname-and-etc..]
        int schemeStopIdx = connectStr.indexOf("//");
        if (-1 == schemeStopIdx) {
            // If no hostname start marker ("//"), then look for the right-most ':'
            // character.
            schemeStopIdx = connectStr.lastIndexOf(':');
            if (-1 == schemeStopIdx) {
                // Warn that this is nonstandard. But we should be as permissive
                // as possible here and let the ConnectionManagers themselves throw
                // out the connect string if it doesn't make sense to them.
                LOG.warn("Could not determine scheme component of connect string");

                // Use the whole string.
                schemeStopIdx = connectStr.length();
            }
        }
        return connectStr.substring(0, schemeStopIdx);
    }

}
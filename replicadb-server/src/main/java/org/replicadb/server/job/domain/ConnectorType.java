package org.replicadb.server.job.domain;

import org.replicadb.manager.SupportedManagers;

import java.util.Locale;

public enum ConnectorType {
    MYSQL("mysql", SupportedManagers.MYSQL),
    MARIADB("mariadb", SupportedManagers.MARIADB),
    POSTGRES("postgres", SupportedManagers.POSTGRES),
    HSQLDB("hsqldb", SupportedManagers.HSQLDB),
    ORACLE("oracle", SupportedManagers.ORACLE),
    SQLSERVER("sqlserver", SupportedManagers.SQLSERVER),
    CUBRID("cubrid", SupportedManagers.CUBRID),
    JTDS_SQLSERVER("jtds-sqlserver", SupportedManagers.JTDS_SQLSERVER),
    DB2("db2", SupportedManagers.DB2),
    DB2_AS400("db2-as400", SupportedManagers.DB2_AS400),
    NETEZZA("netezza", SupportedManagers.NETEZZA),
    DENODO("denodo", SupportedManagers.DENODO),
    KAFKA("kafka", SupportedManagers.KAFKA),
    S3("s3", SupportedManagers.S3),
    FILE("file", SupportedManagers.FILE),
    SQLITE("sqlite", SupportedManagers.SQLITE),
    MONGODB("mongodb", SupportedManagers.MONGODB),
    MONGODBSRV("mongodb+srv", SupportedManagers.MONGODBSRV),
    CUSTOM("custom", null);

    private final String wireValue;
    private final SupportedManagers manager;

    ConnectorType(String wireValue, SupportedManagers manager) {
        this.wireValue = wireValue;
        this.manager = manager;
    }

    public String getWireValue() {
        return wireValue;
    }

    public SupportedManagers getManager() {
        return manager;
    }

    public boolean matchesConnection(String connect) {
        return manager != null && connect != null && connect.startsWith(manager.getSchemePrefix());
    }

    public static ConnectorType fromWireValue(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("connectorType must not be blank");
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (ConnectorType type : values()) {
            if (type.wireValue.equals(normalized) || type.name().equalsIgnoreCase(normalized)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown connector type: " + value);
    }

    public static ConnectorType fromConnection(String connect) {
        if (connect == null || connect.isBlank()) {
            throw new IllegalArgumentException("connect must not be blank");
        }
        for (ConnectorType type : values()) {
            if (type.matchesConnection(connect)) {
                return type;
            }
        }
        return CUSTOM;
    }
}

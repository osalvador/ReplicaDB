package org.replicadb.cli;

import java.util.Locale;

public enum AzureAuthenticationMode {
    ACTIVE_DIRECTORY_INTERACTIVE("ActiveDirectoryInteractive"),
    ACTIVE_DIRECTORY_DEFAULT("ActiveDirectoryDefault"),
    ACTIVE_DIRECTORY_MANAGED_IDENTITY("ActiveDirectoryManagedIdentity"),
    ACTIVE_DIRECTORY_SERVICE_PRINCIPAL("ActiveDirectoryServicePrincipal"),
    ACTIVE_DIRECTORY_SERVICE_PRINCIPAL_CERTIFICATE("ActiveDirectoryServicePrincipalCertificate"),
    ACTIVE_DIRECTORY_INTEGRATED("ActiveDirectoryIntegrated");

    private final String driverValue;

    AzureAuthenticationMode(String driverValue) {
        this.driverValue = driverValue;
    }

    public String getDriverValue() {
        return driverValue;
    }

    public static AzureAuthenticationMode fromValue(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        String normalizedValue = value.trim().toLowerCase(Locale.ROOT);
        if ("activedirectorymsi".equals(normalizedValue)) {
            return ACTIVE_DIRECTORY_MANAGED_IDENTITY;
        }
        if ("activedirectorypassword".equals(normalizedValue)) {
            throw new IllegalArgumentException(
                    "ActiveDirectoryPassword is deprecated; use ActiveDirectoryInteractive, "
                            + "ActiveDirectoryDefault, ActiveDirectoryServicePrincipal, or ActiveDirectoryManagedIdentity.");
        }

        for (AzureAuthenticationMode mode : values()) {
            if (mode.driverValue.toLowerCase(Locale.ROOT).equals(normalizedValue)) {
                return mode;
            }
        }

        throw new IllegalArgumentException("Unsupported Azure authentication mode: " + value);
    }

    @Override
    public String toString() {
        return driverValue;
    }
}

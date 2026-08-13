package org.replicadb.cli;

public final class AzureAuthenticationOptions {

    private AzureAuthenticationMode mode;
    private String principalId;
    private String loginHint;
    private String clientCertificate;
    private String clientKey;

    public AzureAuthenticationOptions() {
    }

    public AzureAuthenticationOptions(AzureAuthenticationOptions other) {
        if (other == null) {
            return;
        }

        this.mode = other.mode;
        this.principalId = other.principalId;
        this.loginHint = other.loginHint;
        this.clientCertificate = other.clientCertificate;
        this.clientKey = other.clientKey;
    }

    public AzureAuthenticationMode getMode() {
        return mode;
    }

    public void setMode(AzureAuthenticationMode mode) {
        this.mode = mode;
    }

    public void setMode(String mode) {
        this.mode = AzureAuthenticationMode.fromValue(mode);
    }

    public String getPrincipalId() {
        return principalId;
    }

    public void setPrincipalId(String principalId) {
        this.principalId = principalId;
    }

    public String getLoginHint() {
        return loginHint;
    }

    public void setLoginHint(String loginHint) {
        this.loginHint = loginHint;
    }

    public String getClientCertificate() {
        return clientCertificate;
    }

    public void setClientCertificate(String clientCertificate) {
        this.clientCertificate = clientCertificate;
    }

    public String getClientKey() {
        return clientKey;
    }

    public void setClientKey(String clientKey) {
        this.clientKey = clientKey;
    }

    public boolean isConfigured() {
        return mode != null;
    }

    public void validate() {
        validate(false, false);
    }

    public void validate(boolean passwordConfigured) {
        validate(false, passwordConfigured);
    }

    public void validate(boolean userConfigured, boolean passwordConfigured) {
        if (mode == null) {
            return;
        }

        switch (mode) {
            case ACTIVE_DIRECTORY_INTERACTIVE:
                requireBlank(principalId, "principal ID", mode);
                requireBlank(clientCertificate, "client certificate", mode);
                requireBlank(clientKey, "client key", mode);
                requireNoPassword(passwordConfigured, mode);
                break;
            case ACTIVE_DIRECTORY_DEFAULT:
                requireBlank(clientCertificate, "client certificate", mode);
                requireBlank(clientKey, "client key", mode);
                requireNoPassword(passwordConfigured, mode);
                break;
            case ACTIVE_DIRECTORY_MANAGED_IDENTITY:
                requireBlank(loginHint, "login hint", mode);
                requireBlank(clientCertificate, "client certificate", mode);
                requireBlank(clientKey, "client key", mode);
                requireNoPassword(passwordConfigured, mode);
                break;
            case ACTIVE_DIRECTORY_SERVICE_PRINCIPAL:
                requireValue(principalId, "principal ID", mode);
                requireBlank(loginHint, "login hint", mode);
                requireBlank(clientCertificate, "client certificate", mode);
                requireBlank(clientKey, "client key", mode);
                if (!passwordConfigured) {
                    throw new IllegalArgumentException(
                            "Password is required for " + mode + " authentication.");
                }
                break;
            case ACTIVE_DIRECTORY_SERVICE_PRINCIPAL_CERTIFICATE:
                requireValue(principalId, "principal ID", mode);
                requireBlank(loginHint, "login hint", mode);
                requireValue(clientCertificate, "client certificate", mode);
                break;
            case ACTIVE_DIRECTORY_INTEGRATED:
                requireBlank(principalId, "principal ID", mode);
                requireBlank(loginHint, "login hint", mode);
                requireBlank(clientCertificate, "client certificate", mode);
                requireBlank(clientKey, "client key", mode);
                if (userConfigured) {
                    throw new IllegalArgumentException("User is not supported for " + mode + " authentication.");
                }
                requireNoPassword(passwordConfigured, mode);
                break;
            default:
                throw new IllegalArgumentException("Unsupported Azure authentication mode: " + mode);
        }
    }

    private static void requireValue(String value, String fieldName, AzureAuthenticationMode mode) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " is required for " + mode + " authentication.");
        }
    }

    private static void requireBlank(String value, String fieldName, AzureAuthenticationMode mode) {
        if (value != null && !value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " is not supported for " + mode + " authentication.");
        }
    }

    private static void requireNoPassword(boolean passwordConfigured, AzureAuthenticationMode mode) {
        if (passwordConfigured) {
            throw new IllegalArgumentException("Password is not supported for " + mode + " authentication.");
        }
    }

    @Override
    public String toString() {
        return "AzureAuthenticationOptions{mode=" + mode + '}';
    }
}

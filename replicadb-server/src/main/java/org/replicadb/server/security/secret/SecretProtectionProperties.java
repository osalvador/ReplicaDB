package org.replicadb.server.security.secret;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "replicadb.security")
public class SecretProtectionProperties {

    public static final String MASTER_KEY_FILE_PROPERTY = "replicadb.security.master-key-file";

    private String masterKeyFile = "/run/secrets/replicadb-master-key";

    public String getMasterKeyFile() {
        return masterKeyFile;
    }

    public void setMasterKeyFile(String masterKeyFile) {
        this.masterKeyFile = masterKeyFile;
    }
}

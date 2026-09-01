package org.replicadb.server.security.secret;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "replicadb.security")
public class SecretProtectionProperties {

    private String masterKeyFile = "/run/secrets/replicadb-master-key";

    public String getMasterKeyFile() {
        return masterKeyFile;
    }

    public void setMasterKeyFile(String masterKeyFile) {
        this.masterKeyFile = masterKeyFile;
    }
}
